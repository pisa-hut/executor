import os
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from executor import timing


class CsvLineTests(unittest.TestCase):
    def test_full_marks(self) -> None:
        marks = {f: float(i) for i, f in enumerate(timing.MARK_FIELDS)}
        line = timing.csv_line(7, 42, "container", "container-n04", "cp08", marks)
        cols = line.split(",")
        self.assertEqual(cols[:5], ["7", "42", "container", "container-n04", "cp08"])
        self.assertEqual(len(cols), 5 + len(timing.MARK_FIELDS))
        self.assertEqual(cols[5], "0.000000")
        self.assertEqual(cols[-1], f"{len(timing.MARK_FIELDS) - 1}.000000")

    def test_missing_marks_are_empty(self) -> None:
        line = timing.csv_line(7, None, "infra", "l", "h", {"claim_start": 1.5})
        cols = line.split(",")
        self.assertEqual(cols[1], "")
        self.assertEqual(cols[5], "1.500000")
        self.assertEqual(cols[6:], [""] * (len(timing.MARK_FIELDS) - 1))


class WriteLineTests(unittest.TestCase):
    def test_disabled_without_env(self) -> None:
        with mock.patch.dict(os.environ, {}, clear=False):
            os.environ.pop("EXECUTOR_TIMING_DIR", None)
            self.assertIsNone(timing.write_line(1, None, "full", {}))

    def test_appends_one_line_per_call(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            env = {"EXECUTOR_TIMING_DIR": tmp, "EXECUTOR_TIMING_LABEL": "smoke"}
            with mock.patch.dict(os.environ, env):
                path = timing.write_line(1, 2, "infra", {"claim_start": 0.25})
                timing.write_line(3, 4, "infra", {"claim_start": 0.5})
            assert path is not None
            self.assertEqual(path.parent, Path(tmp) / "smoke")
            lines = path.read_text().splitlines()
            self.assertEqual(len(lines), 2)
            self.assertTrue(lines[0].startswith("1,2,infra,smoke,"))
            self.assertTrue(lines[1].startswith("3,4,infra,smoke,"))


if __name__ == "__main__":
    unittest.main()
