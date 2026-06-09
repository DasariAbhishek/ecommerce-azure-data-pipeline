"""Typed access to the YAML pipeline configuration."""
from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

import yaml

# Project root = three levels up from this file (src/ecommerce_pipeline/config.py)
PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_CONFIG = PROJECT_ROOT / "configs" / "pipeline_config.yaml"


@dataclass
class Paths:
    landing: str
    bronze: str
    silver: str
    gold: str
    warehouse: str


@dataclass
class Config:
    project_name: str
    paths: Paths
    data_generation: dict
    quality: dict
    _root: Path

    def abs(self, key: str) -> str:
        """Return an absolute path for a configured lakehouse location."""
        return str(self._root / getattr(self.paths, key))


def load_config(path: str | os.PathLike | None = None) -> Config:
    cfg_path = Path(path) if path else DEFAULT_CONFIG
    with open(cfg_path, "r", encoding="utf-8") as fh:
        raw = yaml.safe_load(fh)
    return Config(
        project_name=raw["project_name"],
        paths=Paths(**raw["paths"]),
        data_generation=raw["data_generation"],
        quality=raw["quality"],
        _root=PROJECT_ROOT,
    )
