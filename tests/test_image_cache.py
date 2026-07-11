import os
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from executor import image_cache

A_DIGEST = "sha256:" + "a" * 64
B_DIGEST = "sha256:" + "b" * 64
# Digest-pinned so _digest_from_uri resolves the digest with no network call.
PINNED_URI = f"oras://reg.example.com/team/carla-wrapper-sif@{A_DIGEST}"


def _fake_popen_success() -> mock.MagicMock:
    proc = mock.MagicMock()
    proc.stdout = iter(["Pulling layer...\n", "Done\n"])
    proc.wait.return_value = 0
    return proc


class LocalSifNameTests(unittest.TestCase):
    def test_matches_justfile_sed_rule(self) -> None:
        uri = "oras://zot.hcislab.org/tonychi/carla-wrapper-sif:main"
        self.assertEqual(
            image_cache.local_sif_name(uri),
            "oras___zot.hcislab.org_tonychi_carla-wrapper-sif_main.sif",
        )


class ResolveSifTests(unittest.TestCase):
    def test_uri_routes_to_ensure_cached(self) -> None:
        with mock.patch.object(
            image_cache, "ensure_cached", return_value="/x.sif"
        ) as ec:
            out = image_cache.resolve_sif("docker://x/y:latest")
        ec.assert_called_once_with("docker://x/y:latest", dir=None)
        self.assertEqual(out, "/x.sif")

    def test_non_uri_filesystem_fallback(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "sifs").mkdir()
            (root / "sifs" / "foo.sif").write_text("x")
            cwd = os.getcwd()
            os.chdir(root)
            try:
                out = image_cache.resolve_sif("foo.sif")
            finally:
                os.chdir(cwd)
            self.assertEqual(out, str(Path("sifs") / "foo.sif"))


class EnsureCachedTests(unittest.TestCase):
    def test_digest_match_skips_pull(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            d = Path(tmp)
            name = image_cache.local_sif_name(PINNED_URI)
            (d / name).write_text("sif")
            (d / (name + ".digest")).write_text(A_DIGEST)
            with mock.patch.object(image_cache.subprocess, "Popen") as popen:
                out = image_cache.ensure_cached(PINNED_URI, dir=d)
            popen.assert_not_called()
            self.assertEqual(out, str(d / name))

    def test_digest_mismatch_pulls_and_writes_sidecar(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            d = Path(tmp)
            name = image_cache.local_sif_name(PINNED_URI)
            (d / (name + ".digest")).write_text(B_DIGEST)  # stale
            with mock.patch.object(
                image_cache.subprocess, "Popen", return_value=_fake_popen_success()
            ) as popen:
                out = image_cache.ensure_cached(PINNED_URI, dir=d)
            popen.assert_called_once()
            self.assertIn("--force", popen.call_args.args[0])
            self.assertEqual((d / (name + ".digest")).read_text(), A_DIGEST)
            self.assertEqual(out, str(d / name))

    def test_pull_sets_apptainer_tmpdir_under_cache(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            d = Path(tmp)
            with (
                mock.patch.dict(os.environ, {}, clear=False),
                mock.patch.object(
                    image_cache.subprocess, "Popen", return_value=_fake_popen_success()
                ) as popen,
            ):
                os.environ.pop("APPTAINER_TMPDIR", None)
                image_cache.ensure_cached(PINNED_URI, dir=d)
            env = popen.call_args.kwargs["env"]
            self.assertEqual(env["APPTAINER_TMPDIR"], str(d / "tmp"))
            self.assertTrue((d / "tmp").is_dir())

    def test_pull_respects_existing_apptainer_tmpdir(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            d = Path(tmp)
            override = str(d / "scratch")
            with (
                mock.patch.dict(os.environ, {"APPTAINER_TMPDIR": override}),
                mock.patch.object(
                    image_cache.subprocess, "Popen", return_value=_fake_popen_success()
                ) as popen,
            ):
                image_cache.ensure_cached(PINNED_URI, dir=d)
            self.assertEqual(
                popen.call_args.kwargs["env"]["APPTAINER_TMPDIR"], override
            )

    def test_force_pulls_even_on_digest_match(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            d = Path(tmp)
            name = image_cache.local_sif_name(PINNED_URI)
            (d / name).write_text("sif")
            (d / (name + ".digest")).write_text(A_DIGEST)
            with mock.patch.object(
                image_cache.subprocess, "Popen", return_value=_fake_popen_success()
            ) as popen:
                image_cache.ensure_cached(PINNED_URI, force=True, dir=d)
            popen.assert_called_once()


class MainTests(unittest.TestCase):
    def test_failure_isolation_exits_1(self) -> None:
        uris = ["oras://a/b:main", "oras://c/d:main", "oras://e/f:main"]
        attempted: list[str] = []

        def fake_ensure(uri: str, *, force: bool = False, dir=None) -> str:
            attempted.append(uri)
            if uri == "oras://c/d:main":
                raise RuntimeError("boom")
            return "/cache/" + image_cache.local_sif_name(uri)

        with (
            mock.patch.object(image_cache, "ensure_cached", side_effect=fake_ensure),
            mock.patch.object(sys, "argv", ["executor.image_cache", *uris]),
            mock.patch("dotenv.load_dotenv"),
        ):
            with self.assertRaises(SystemExit) as cm:
                image_cache.main()
        self.assertEqual(cm.exception.code, 1)
        self.assertEqual(attempted, uris)  # all attempted despite the middle failure

    def test_rejects_non_uri_args(self) -> None:
        with (
            mock.patch.object(sys, "argv", ["executor.image_cache", "not-a-uri.sif"]),
            mock.patch("dotenv.load_dotenv"),
            mock.patch.object(image_cache, "ensure_cached") as ec,
        ):
            with self.assertRaises(SystemExit) as cm:
                image_cache.main()
        self.assertEqual(cm.exception.code, 2)
        ec.assert_not_called()


class DelegationTests(unittest.TestCase):
    def test_resolve_sif_path_delegates_to_image_cache(self) -> None:
        from executor.apptainer_utils.apptainer_config import ApptainerServiceConfig

        with mock.patch(
            "executor.apptainer_utils.apptainer_config.resolve_sif",
            return_value="/z.sif",
        ) as rs:
            out = ApptainerServiceConfig._resolve_sif_path("docker://x/y:latest")
        rs.assert_called_once_with("docker://x/y:latest")
        self.assertEqual(out, "/z.sif")


if __name__ == "__main__":
    unittest.main()
