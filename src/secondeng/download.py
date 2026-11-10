"""Fetch both open datasets from Zenodo, verify md5, unzip. Idempotent."""

from __future__ import annotations

import hashlib
import zipfile
from pathlib import Path

import requests

RECORDS = {"gloria": "18330661", "bench": "19857425"}


def _md5(path: Path) -> str:
    h = hashlib.md5()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def download(raw_root: Path) -> None:
    for name, record in RECORDS.items():
        dest = raw_root / name
        dest.mkdir(parents=True, exist_ok=True)
        meta = requests.get(f"https://zenodo.org/api/records/{record}", timeout=60)
        meta.raise_for_status()
        for f in meta.json()["files"]:
            target = dest / f["key"]
            expected = f["checksum"].removeprefix("md5:")
            if target.exists() and _md5(target) == expected:
                print(f"ok    {name}/{f['key']}")
            else:
                print(f"fetch {name}/{f['key']} ({f['size'] / 1e6:.1f} MB)")
                tmp = target.with_suffix(target.suffix + ".part")
                with requests.get(f["links"]["self"], stream=True, timeout=600) as r:
                    r.raise_for_status()
                    with tmp.open("wb") as out:
                        for chunk in r.iter_content(1 << 20):
                            out.write(chunk)
                if _md5(tmp) != expected:
                    tmp.unlink()
                    raise RuntimeError(f"md5 mismatch for {f['key']}")
                tmp.rename(target)
            if target.suffix == ".zip":
                marker = dest / f".{target.stem}.unzipped"
                if not marker.exists():
                    with zipfile.ZipFile(target) as z:
                        z.extractall(dest / target.stem)
                    marker.write_text(expected)
