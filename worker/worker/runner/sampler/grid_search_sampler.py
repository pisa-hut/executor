# sampler.py
from __future__ import annotations

from typing import Optional, Iterable
from pathlib import Path
from logging import getLogger

from .base import (
    BaseSampler,
    ParamDict,
    TestResult,
    parse_parameter_value_distribution,
)

logger = getLogger(__name__)


class GridSearchSampler(BaseSampler):
    def __init__(
        self,
        cfg_path: Optional[Path] = None,
        past_results: Optional[Iterable[TestResult]] = None,
        param_range_file: Optional[Path] = None,
    ):
        # self.cfg = get_cfg(cfg_path)
        # try:
        #     xml_path = self.cfg["xml_path"]
        # except KeyError:
        #     raise ValueError("Missing 'xml_path' in sampler configuration")
        xml_path = param_range_file
        xml_text = xml_path.read_text(encoding="utf-8")
        specs = parse_parameter_value_distribution(xml_text)
        super().__init__(specs)

        self._names = [s.name for s in specs]
        self._grid = [s.values for s in specs]
        self._indices = [0] * len(self._grid)
        self._done = False

        self._seen = set()
        if past_results:
            for r in past_results:
                params = r.get("params", r)
                key = self._params_to_key(params)
                self._seen.add(key)

        logger.info(
            "GridSearchSampler initialized from xml_path=%s, %d parameters: %s",
            xml_path,
            len(self._names),
            self._names,
        )

    def _params_to_key(self, params: ParamDict):
        return tuple(params.get(name) for name in self._names)

    def _advance_indices(self):
        if self._done:
            return

        for dim in reversed(range(len(self._indices))):
            self._indices[dim] += 1
            if self._indices[dim] < len(self._grid[dim]):
                return
            self._indices[dim] = 0
            if dim == 0:
                self._done = True
                return

    def next(
        self,
        past_results: Optional[Iterable[TestResult]] = None,
    ) -> Optional[ParamDict]:
        if past_results:
            for r in past_results:
                params = r.get("params", r)
                key = self._params_to_key(params)
                self._seen.add(key)

        while not self._done:
            combo: ParamDict = {
                name: str(self._grid[i][idx])
                for i, (name, idx) in enumerate(zip(self._names, self._indices))
            }

            self._advance_indices()

            key = self._params_to_key(combo)
            if key in self._seen:
                continue

            self._seen.add(key)
            return combo

        return None

    def total_permutations(self) -> int:
        total = 1
        for values in self._grid:
            total *= len(values)
        return total

    def remaining_permutations(self) -> int:
        return max(self.total_permutations() - len(self._seen), 0)
