"""Typed, validated configuration (pydantic). All paths resolve against the repo root."""

from __future__ import annotations

from datetime import date
from functools import cached_property
from pathlib import Path
from zoneinfo import ZoneInfo

import yaml
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

ROOT = Path(__file__).resolve().parents[2]


class Model(BaseModel):
    """Strict base: a misspelt or unknown key in the YAML is an error, not a silent default."""

    model_config = ConfigDict(extra="forbid")


class Vessel(Model):
    vessel_id: str
    owner: str
    logger: str
    legs: list[str] = Field(min_length=1)


class Fleet(Model):
    replay_start: date
    vessels: list[Vessel]


class Injection(Model):
    vessel_id: str
    fault: str
    ramp_hours: float = Field(gt=0)
    severity_x_bench: float = Field(ge=0)
    failure_offset_days: float = Field(gt=0)
    noise_frac: float = Field(ge=0)


class Simulator(Model):
    batch_minutes: int = Field(gt=0)
    duplicate_rate: float = Field(ge=0, le=1)
    late_rate: float = Field(ge=0, le=1)
    late_hours: tuple[float, float]
    out_of_order_row_rate: float = Field(ge=0, le=1)
    corrupt_row_rate: float = Field(ge=0, le=1)
    outages_per_vessel: int = Field(ge=0)
    outage_hours: tuple[float, float]
    send_delay_minutes: tuple[float, float]

    @model_validator(mode="after")
    def _ranges(self) -> Simulator:
        for name in ("late_hours", "outage_hours", "send_delay_minutes"):
            lo, hi = getattr(self, name)
            if not 0 <= lo <= hi:
                raise ValueError(f"{name} must be 0 <= low <= high, got {lo}, {hi}")
        return self


class Detection(Model):
    running_frac_of_p99_rpm: float = Field(gt=0, lt=1)
    running_min_minutes: int = Field(ge=1)
    baseline_days: float = Field(gt=0)
    baseline_min_hours: int = Field(ge=2)
    unscored_measurements: list[str]
    sigma_floor_frac: float = Field(ge=0)
    z_threshold: float = Field(gt=0)
    min_sensors: int = Field(ge=1)
    alert_min_hours: int = Field(ge=1)
    alert_close_hours: int = Field(ge=1)
    health_k: float = Field(gt=0)


class CostModel(Model):
    offhire_usd: float
    opex_per_day_usd: float
    offhire_days: float
    offhire_bunkers_usd: float
    severity_weight: dict[str, float]

    @property
    def event_usd(self) -> float:
        return self.offhire_usd + self.opex_per_day_usd * self.offhire_days + self.offhire_bunkers_usd


class Sensor(Model):
    id: str
    system: str
    measurement: str
    unit: str
    min: float
    max: float

    @model_validator(mode="after")
    def _range(self) -> Sensor:
        if self.min >= self.max:
            raise ValueError(f"{self.id}: min must be below max")
        return self


class Tag(Model):
    sensor: str
    scale: float = 1.0
    offset: float = 0.0

    @field_validator("scale")
    @classmethod
    def _scale(cls, v: float) -> float:
        if v == 0:
            raise ValueError("scale must be non-zero")
        return v


class Logger(Model):
    timezone: str
    sources: dict[str, dict[str, Tag]]

    @model_validator(mode="after")
    def _tz(self) -> Logger:
        ZoneInfo(self.timezone)
        return self


class Settings(Model):
    seed: int
    raw_root: Path
    landing_root: Path
    lake_root: Path
    warehouse: Path
    ground_truth: Path
    site_data: Path
    fleet: Fleet
    injection: Injection
    simulator: Simulator
    detection: Detection
    cost_model: CostModel
    registry: list[Sensor]
    loggers: dict[str, Logger]

    @model_validator(mode="after")
    def _check(self) -> Settings:
        ids = {s.id for s in self.registry}
        if len(ids) != len(self.registry):
            raise ValueError("duplicate sensor id in registry")
        for name, lg in self.loggers.items():
            for source, tags in lg.sources.items():
                for tag, t in tags.items():
                    if t.sensor not in ids:
                        raise ValueError(f"{name}/{source}/{tag} maps to unknown sensor {t.sensor}")
        vids = [v.vessel_id for v in self.fleet.vessels]
        if len(set(vids)) != len(vids):
            raise ValueError("duplicate vessel_id")
        for v in self.fleet.vessels:
            if v.logger not in self.loggers:
                raise ValueError(f"{v.vessel_id} uses unknown logger {v.logger}")
        if self.injection.vessel_id not in vids:
            raise ValueError("injection vessel not in fleet")
        return self

    @cached_property
    def sensors(self) -> dict[str, Sensor]:
        return {s.id: s for s in self.registry}

    def vessel(self, vessel_id: str) -> Vessel:
        return next(v for v in self.fleet.vessels if v.vessel_id == vessel_id)

    def path(self, p: Path) -> Path:
        return p if p.is_absolute() else ROOT / p

    @property
    def lake(self) -> Path:
        return self.path(self.lake_root)

    @property
    def landing(self) -> Path:
        return self.path(self.landing_root)

    @property
    def raw(self) -> Path:
        return self.path(self.raw_root)


def load(config_dir: Path = ROOT / "config") -> Settings:
    data = yaml.safe_load((config_dir / "settings.yaml").read_text())
    data |= yaml.safe_load((config_dir / "sensors.yaml").read_text())
    return Settings.model_validate(data)
