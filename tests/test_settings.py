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
