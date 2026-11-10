import pytest
import yaml
from pydantic import ValidationError

from secondeng import settings
from secondeng.settings import ROOT, Settings


def test_config_loads_and_every_tag_maps_to_registry():
    s = settings.load()
    ids = set(s.sensors)
    for lg in s.loggers.values():
        for tags in lg.sources.values():
            assert {t.sensor for t in tags.values()} <= ids


def test_unknown_sensor_in_mapping_is_rejected():
    data = yaml.safe_load((ROOT / "config/settings.yaml").read_text())
    data |= yaml.safe_load((ROOT / "config/sensors.yaml").read_text())
    data["loggers"]["gloria_v1"]["sources"]["main_engine"]["engine_rpm"]["sensor"] = "me.nope"
    with pytest.raises(ValidationError, match="unknown sensor"):
        Settings.model_validate(data)


def _config() -> dict:
    data = yaml.safe_load((ROOT / "config/settings.yaml").read_text())
    return data | yaml.safe_load((ROOT / "config/sensors.yaml").read_text())


@pytest.mark.parametrize(
    "mutate",
    [
        lambda d: d["loggers"]["gloria_v1"]["sources"]["main_engine"]["engine_rpm"].update(scal=2.0),  # typo
        lambda d: d["loggers"]["gloria_v1"]["sources"]["main_engine"]["engine_rpm"].update(scale=0),
        lambda d: d["simulator"].update(late_hours=[12, 1]),
        lambda d: d["registry"][0].update(min=10, max=10),
    ],
    ids=["misspelt_key", "zero_scale", "reversed_range", "empty_sensor_range"],
)
def test_bad_config_is_rejected_at_load(mutate):
    data = _config()
    mutate(data)
    with pytest.raises(ValidationError):
        Settings.model_validate(data)
