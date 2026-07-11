# Wrapper image cache recipes.
#
# The executor pulls wrapper images into $PISA_DATA_DIR/sif at task-claim time
# via executor.image_cache. Each <name>.sif has a <name>.sif.digest sidecar
# recording the registry manifest digest at last pull; the executor HEAD-probes
# the registry and only re-pulls when the digest differs. `update-cache` runs
# that same pull path ahead of time so a later claim skips the pull.

# Load executor/.env so PISA_DATA_DIR resolves the same way the executor sees it.
set dotenv-load

pisa_data_dir := env_var_or_default("PISA_DATA_DIR", "/PISA_DATA_DIR")
sif_dir := pisa_data_dir / "sif"

# Wrapper images to keep warm — the av + simulator repos from the manager DB,
# pinned to the exact digests in each row's image_path.apptainer. The cache key
# is the full URI (digest included), so these must match the DB verbatim or a
# prefetch caches under a different filename than the task claim resolves. Re-pin
# both here and in the DB when a wrapper is rebuilt. carla-wrapper has two live
# builds: :main (carla simulator) and :native (native-carla simulator).
wrapper_images := "docker://zot.hcislab.org/tonychi/carla-wrapper@sha256:7dd87f52281f73d5b001a02da900dbae38870b3511537bcc3bd14cfef15e97f7 docker://zot.hcislab.org/tonychi/carla-wrapper@sha256:1b702bb55ddfebf7b91c2fc273eb7d6d4411cc78cc7a07712ca1319c849e5848 docker://zot.hcislab.org/tonychi/carla-agent-wrapper@sha256:5860340c8b7882bcbbf4f86058c4ea201f132df20582c571098af3d5fd79668c docker://zot.hcislab.org/tonychi/autoware-wrapper@sha256:0414ad0e0adca2b55d07103ada68ab82224ca02ee1526ee8013a741dcae3ba36 docker://zot.hcislab.org/tonychi/esmini-wrapper@sha256:d404d36cd52918e3f8d2095db05bb6908e84081ca79a9f8b0c1215aac8f5a962 docker://docker.io/tonychi/pcla-wrapper@sha256:de15c1bb652a29a08b32ca034f771bbbe5b85f49a27b5c7ffdce473bc66331cf"

# Prefetch / update wrapper images into the cache (digest-checked, writes the
# sidecar so a later task claim skips the pull). With no args, updates the
# `wrapper_images` list; pass explicit URIs to update just those.
update-cache *args:
    #!/usr/bin/env bash
    set -euo pipefail
    uris="{{args}}"
    if [[ -z "$uris" ]]; then
        uris="{{wrapper_images}}"
    fi
    uv run -m executor.image_cache $uris

# Same as update-cache but re-pulls even when the cached digest already matches.
update-cache-force *args:
    #!/usr/bin/env bash
    set -euo pipefail
    uris="{{args}}"
    if [[ -z "$uris" ]]; then
        uris="{{wrapper_images}}"
    fi
    uv run -m executor.image_cache --force $uris

# Invalidate cached digests so the executor re-verifies/re-pulls (digest-checked)
# on the next task run. Keeps the current .sif as a fallback until then.
update-wrappers:
    #!/usr/bin/env bash
    set -euo pipefail
    shopt -s nullglob
    dir="{{sif_dir}}"
    if [[ ! -d "$dir" ]]; then
        echo "SIF dir not found: $dir" >&2
        exit 1
    fi
    n=0
    for sif in "$dir"/*.sif; do
        rm -f -- "$sif.digest"
        echo "  staled $(basename "$sif")"
        n=$((n + 1))
    done
    if [[ "$n" -eq 0 ]]; then
        echo "No wrapper SIFs found in $dir"
    else
        echo "Invalidated $n wrapper SIF(s) in $dir; next task run pulls latest."
    fi

# Prune stale/duplicate cached images, keeping only the current set (the
# sanitized filenames of `wrapper_images` — the latest tagged builds). Removes
# every other .sif (+ its .digest sidecar), e.g. old per-digest files from a
# previous naming. With the 6 current images pulled, 6 remain.
purge-wrappers:
    #!/usr/bin/env bash
    set -euo pipefail
    shopt -s nullglob
    dir="{{sif_dir}}"
    if [[ ! -d "$dir" ]]; then
        echo "SIF dir not found: $dir" >&2
        exit 1
    fi
    # Keep-set: the sanitized .sif filename for each current image (must match
    # executor.image_cache.local_sif_name).
    declare -A keep=()
    for uri in {{wrapper_images}}; do
        name="$(printf '%s' "$uri" | sed -E 's/[^A-Za-z0-9._-]/_/g; s/^_+//; s/_+$//').sif"
        keep["$name"]=1
    done
    kept=0
    removed=0
    for sif in "$dir"/*.sif; do
        name="$(basename "$sif")"
        if [[ -n "${keep[$name]:-}" ]]; then
            kept=$((kept + 1))
            continue
        fi
        rm -f -- "$sif" "$sif.digest"
        echo "  removed $name"
        removed=$((removed + 1))
    done
    echo "Purge complete: kept $kept current image(s), removed $removed stale image(s) in $dir"

# Deprecated: superseded by `update-cache` (which writes the digest sidecar so
# claims actually skip the pull). Forwards for now.
pull-wrappers *uris:
    #!/usr/bin/env bash
    set -euo pipefail
    echo "pull-wrappers is deprecated; use 'just update-cache {{uris}}'" >&2
    just update-cache {{uris}}
