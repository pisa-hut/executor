"""Standalone image (SIF) cache.

Single owner of the wrapper-image pull/cache/digest logic. Two callers go
through this module:

- the executor, at task-claim time, calls `resolve_sif()` to turn a manager
  `image_path` into a local `.sif` path (pulling it if needed);
- an operator, ahead of a run, invokes this module as a CLI
  (`uv run -m executor.image_cache <uri>...`, wired to `just update-cache`) to
  pre-pull / refresh those images so the later claim skips the pull.

Both paths write into the same digest-addressed cache dir (`$PISA_DATA_DIR/sif`):
each `.sif` is named after its image's manifest digest, so a prefetch and a claim
— and a tag vs. the `@sha256:…` it resolves to — all land on the same file. The
dependency points one way (executor -> image_cache); this module imports nothing
from the executor.
"""

import argparse
import os
import re
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.request

from loguru import logger
from pathlib import Path
from typing import Optional

# URI schemes apptainer pull accepts. When image_path starts with one of
# these we treat it as a remote reference, pull (cache-aware) into the cache
# dir, and use the local path for `apptainer run`.
_URI_SCHEMES: tuple[str, ...] = (
    "oras://",
    "docker://",
    "library://",
    "http://",
    "https://",
    "shub://",
    "file://",
)

# OCI manifest media types accepted in HEAD probes. Sending these as
# Accept ensures the registry returns the proper Docker-Content-Digest
# header (some registries 404 if Accept doesn't match).
_OCI_MANIFEST_ACCEPT = ",".join(
    [
        "application/vnd.oci.image.manifest.v1+json",
        "application/vnd.oci.image.index.v1+json",
        "application/vnd.docker.distribution.manifest.v2+json",
        "application/vnd.docker.distribution.manifest.list.v2+json",
    ]
)
_MANIFEST_HEAD_ATTEMPTS = 3
_MANIFEST_HEAD_TIMEOUT_SECS = 20

# `…@sha256:<hex>` URIs already embed the manifest digest — no registry
# round-trip needed. Used as the fast path before the HEAD probe.
_DIGEST_PINNED_URI = re.compile(r"@(sha256:[0-9a-fA-F]{64})\b")


def cache_dir() -> Path:
    """The single SIF cache location: `$PISA_DATA_DIR/sif`.

    Mirrors the executor's own default (apptainer_config used the same
    expression before delegating here); `PISA_DATA_DIR` defaults to the
    `/PISA_DATA_DIR` sentinel so a misconfigured environment fails loudly
    rather than silently caching under a wrong root.
    """
    return Path(os.environ.get("PISA_DATA_DIR", "/PISA_DATA_DIR")) / "sif"


def _sanitize(s: str) -> str:
    return re.sub(r"[^A-Za-z0-9._-]", "_", s).strip("_")


def local_sif_name(image_path: str) -> str:
    """Fallback per-URI cache filename, used only when the digest can't be
    resolved: non `[A-Za-z0-9._-]` -> '_', strip stray '_', append '.sif'."""
    return _sanitize(image_path) + ".sif"


def digest_sif_name(digest: str) -> str:
    """Digest-addressed cache filename, e.g. `sha256_<hex>.sif`. A tag and the
    `@sha256:…` it resolves to yield the same name, so switching a DB row between
    them reuses one cached pull instead of re-pulling."""
    return _sanitize(digest) + ".sif"


def _digest_from_uri(image_path: str) -> Optional[str]:
    m = _DIGEST_PINNED_URI.search(image_path)
    return m.group(1) if m else None


def _remote_manifest_digest(image_path: str) -> Optional[str]:
    """Fast HEAD probe for the registry manifest digest.

    Returns the registry's Docker-Content-Digest header (e.g.
    "sha256:abc..."), or None if the probe fails for any reason — in
    which case the caller falls back to a full apptainer pull.

    Only oras://, docker://, http(s):// are handled; other schemes
    return None and the caller does an unconditional pull.
    """
    for scheme in ("oras://", "docker://", "https://", "http://"):
        if image_path.startswith(scheme):
            rest = image_path[len(scheme) :]
            break
    else:
        return None
    if ":" not in rest or "/" not in rest:
        return None
    repo_part, tag = rest.rsplit(":", 1)
    host, _, repo = repo_part.partition("/")
    proto = "http" if image_path.startswith("http://") else "https"
    url = f"{proto}://{host}/v2/{repo}/manifests/{tag}"
    req = urllib.request.Request(
        url, method="HEAD", headers={"Accept": _OCI_MANIFEST_ACCEPT}
    )
    last_exc: BaseException | None = None
    for attempt in range(1, _MANIFEST_HEAD_ATTEMPTS + 1):
        try:
            with urllib.request.urlopen(
                req, timeout=_MANIFEST_HEAD_TIMEOUT_SECS
            ) as resp:
                return resp.headers.get("Docker-Content-Digest")
        except (urllib.error.URLError, TimeoutError, OSError) as exc:
            last_exc = exc
            if attempt < _MANIFEST_HEAD_ATTEMPTS:
                time.sleep(attempt)
                continue

    if last_exc is not None:
        logger.warning(f"manifest HEAD failed for {image_path}: {last_exc}")
    return None


def ensure_cached(
    image_path: str, *, force: bool = False, dir: Optional[Path] = None
) -> str:
    """Pull a URI-shaped image into the cache and return its local `.sif` path.

    The cache is digest-addressed: resolve the manifest digest first (embedded in
    an `@sha256:…` URI, else a registry HEAD probe) and name the `.sif` after it.
    So a tag and the digest it resolves to map to one file — switching a DB row
    between `:main` and its `@sha256:…` (or back) reuses the cached pull. If the
    digest can't be resolved (HEAD failure / non-registry scheme), fall back to a
    URI-derived name and always pull, streaming output to loguru so progress lands
    in the manager Log Drawer.

    `dir` defaults to `cache_dir()`. `force=True` always re-pulls. Raises
    `subprocess.CalledProcessError` if the pull fails. Precondition:
    `image_path.startswith(_URI_SCHEMES)`.
    """
    directory = dir if dir is not None else cache_dir()
    directory.mkdir(parents=True, exist_ok=True)

    remote_digest = _digest_from_uri(image_path) or _remote_manifest_digest(image_path)
    local_name = (
        digest_sif_name(remote_digest) if remote_digest else local_sif_name(image_path)
    )
    local_path = directory / local_name

    # Digest-addressed: the file existing means its content is exactly this
    # digest, so there's nothing to re-verify — skip the pull. A moving tag whose
    # digest has changed resolves to a different (not-yet-present) name and pulls;
    # the old digest's .sif lingers until `just purge-wrappers`.
    if not force and remote_digest and local_path.exists():
        logger.info(
            f"apptainer SIF up-to-date ({remote_digest[:19]}…), skipping pull: {local_path}"
        )
        return str(local_path)

    # apptainer builds the SIF through a temp dir; the default (/tmp) is often
    # too small for multi-GB wrapper images. Point APPTAINER_TMPDIR at a roomy
    # dir under the cache root (respecting any existing override), matching the
    # scheduler's sbatch env (main.go) so a direct CLI pull behaves the same.
    pull_env = dict(os.environ)
    apptainer_tmpdir = pull_env.get("APPTAINER_TMPDIR") or str(directory / "tmp")
    Path(apptainer_tmpdir).mkdir(parents=True, exist_ok=True)
    pull_env["APPTAINER_TMPDIR"] = apptainer_tmpdir

    logger.info(
        f"apptainer pull {image_path} -> {local_path} (APPTAINER_TMPDIR={apptainer_tmpdir})"
    )
    cmd = [
        "apptainer",
        "pull",
        "--force",
        "--dir",
        str(directory),
        local_name,
        image_path,
    ]
    started = time.monotonic()
    # Watchdog so the manager keeps seeing heartbeats while a layer is
    # transferring. Apptainer's progress bar uses carriage returns (no
    # newlines), so the line-reader loop below stays blocked through the
    # entire layer fetch. Without these log lines, no log/append PUT would
    # fire and the reaper (REAPER_STALE_SECS=300 default) would abort the
    # task_run mid-pull.
    heartbeat_stop = threading.Event()

    def _heartbeat() -> None:
        elapsed_s = 0
        while not heartbeat_stop.wait(60):
            elapsed_s += 60
            logger.info(f"apptainer pull still running, {elapsed_s}s elapsed")

    watchdog = threading.Thread(
        target=_heartbeat, name="apptainer-pull-watchdog", daemon=True
    )
    watchdog.start()
    try:
        proc = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,  # merge so order is preserved
            text=True,
            bufsize=1,  # line-buffered
            env=pull_env,
        )
        assert proc.stdout is not None
        for line in proc.stdout:
            stripped = line.rstrip()
            if stripped:
                logger.info(f"apptainer: {stripped}")
        returncode = proc.wait()
        if returncode != 0:
            raise subprocess.CalledProcessError(returncode, cmd)
    finally:
        heartbeat_stop.set()
        watchdog.join(timeout=2)
    elapsed = time.monotonic() - started
    logger.info(f"apptainer pull complete in {elapsed:.1f}s -> {local_path}")

    return str(local_path)


def resolve_sif(image_path: str, *, dir: Optional[Path] = None) -> str:
    """Turn any `image_path` into a local `.sif` path.

    URI-shaped values are pulled (cache-aware) via `ensure_cached`. Anything
    else is treated as a filesystem reference, kept for entities that haven't
    migrated to URIs: an absolute/existing path as-is, else `.sifs/<path>` or
    `sifs/<path>` if present (falling back to the `.sifs/` candidate).
    """
    if image_path.startswith(_URI_SCHEMES):
        return ensure_cached(image_path, dir=dir)

    raw = Path(image_path)
    if raw.is_absolute() or raw.exists():
        return str(raw)

    dot_sifs = Path(".sifs") / image_path
    if dot_sifs.exists():
        return str(dot_sifs)

    sifs = Path("sifs") / image_path
    if sifs.exists():
        return str(sifs)

    return str(dot_sifs)


def _configure_logging(level: str) -> None:
    logger.remove()
    logger.add(sys.stderr, level=level.upper())


def main() -> None:
    """CLI entry: pull/refresh the given image URIs into the cache.

    Sequential and failure-isolated — one image failing never aborts the rest;
    exits 1 if any failed, 0 otherwise, 2 on usage error.
    """
    import dotenv

    parser = argparse.ArgumentParser(
        prog="executor.image_cache",
        description="Pre-pull / refresh wrapper images into $PISA_DATA_DIR/sif.",
    )
    parser.add_argument(
        "uris", nargs="*", help="image URIs to cache (oras://, docker://, …)"
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="re-pull even when the digest-addressed .sif already exists",
    )
    parser.add_argument(
        "--print-names",
        action="store_true",
        help="resolve each URI to its cache .sif filename and print it (one per "
        "line) instead of pulling; used by `just purge-wrappers` for its keep-set",
    )
    parser.add_argument(
        "--log-level",
        default="info",
        choices=["trace", "debug", "info", "warning", "error", "critical"],
    )
    args = parser.parse_args()

    dotenv.load_dotenv()
    _configure_logging(args.log_level)

    uris = list(dict.fromkeys(args.uris))  # dedup, preserve order
    if not uris:
        logger.error("no image URIs given")
        sys.exit(2)

    non_uri = [u for u in uris if not u.startswith(_URI_SCHEMES)]
    if non_uri:
        logger.error(f"not URI-shaped, cannot prefetch: {', '.join(non_uri)}")
        sys.exit(2)

    if args.print_names:
        for uri in uris:
            digest = _digest_from_uri(uri) or _remote_manifest_digest(uri)
            print(digest_sif_name(digest) if digest else local_sif_name(uri))
        sys.exit(0)

    logger.info(
        f"Prefetching {len(uris)} image(s) into {cache_dir()} (force={args.force})"
    )
    succeeded: list[str] = []
    failed: list[tuple[str, str]] = []
    for uri in uris:
        try:
            path = ensure_cached(uri, force=args.force)
            succeeded.append(uri)
            logger.info(f"cached {uri} -> {path}")
        except Exception as exc:  # isolate: one bad image must not abort the rest
            failed.append((uri, f"{type(exc).__name__}: {exc}"))
            logger.error(f"failed {uri}: {exc}")

    logger.info(
        f"Image cache update complete: {len(succeeded)} ok, {len(failed)} failed"
    )
    for uri, err in failed:
        logger.error(f"  FAILED {uri}: {err}")
    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()
