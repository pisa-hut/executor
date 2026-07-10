import os
import tempfile
import unittest
from pathlib import Path

from executor.staging import StagedPaths
from executor.utils import build_services_spec


class BuildServicesSpecTests(unittest.TestCase):
    def test_av_weight_path_resolves_under_pisa_data_dir(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            weights = root / "weights" / "plant_pretrained"
            weights.mkdir(parents=True)
            old_pisa_data_dir = os.environ.get("PISA_DATA_DIR")
            os.environ["PISA_DATA_DIR"] = str(root)
            try:
                spec = build_services_spec(
                    claimed_av={
                        "name": "pcla",
                        "image_path": {"docker": "pcla-wrapper:common-slim"},
                        "weight_path": "weights/plant_pretrained",
                    },
                    claimed_simulator={
                        "name": "carla",
                        "image_path": {"docker": "carla-wrapper:latest"},
                    },
                    claimed_map={},
                    claimed_scenario={},
                    staged=StagedPaths(
                        xodr_dir=root / "xodr",
                        osm_dir=root / "osm",
                        scenario_dir=root / "scenario",
                        av_config=root / "av.yaml",
                        simulator_config=root / "simulator.yaml",
                        sampler_config=None,
                        monitor_config=root / "monitor.yaml",
                    ),
                )
            finally:
                if old_pisa_data_dir is None:
                    os.environ.pop("PISA_DATA_DIR", None)
                else:
                    os.environ["PISA_DATA_DIR"] = old_pisa_data_dir

        self.assertEqual(spec["av"]["bind_mounts"], [(str(weights), "/mnt/weights")])
        self.assertNotIn("bind_mounts", spec["simulator"])


if __name__ == "__main__":
    unittest.main()
