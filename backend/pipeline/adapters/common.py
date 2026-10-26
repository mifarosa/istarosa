"""Helpers for the quirks seen in the real IBB open data files (see docs/data-sources.md)."""
from __future__ import annotations

import csv
import io
import math
from pathlib import Path
from typing import Iterator

# Istanbul bounding box used to sanity-check coordinates.
LAT_RANGE = (40.5, 41.7)
LON_RANGE = (27.9, 29.9)

_MOJIBAKE_MARKERS = ("Ã", "Ä", "Å", "Â")


def decode_bytes(raw: bytes) -> str:
    """UTF-8 (with or without BOM) first; the older operator feeds are cp1254 (ISO-8859-9)."""
    try:
        return raw.decode("utf-8-sig")
    except UnicodeDecodeError:
        return raw.decode("cp1254", errors="replace")


def fix_mojibake(text: str) -> str:
    """Repair text that was UTF-8, decoded as cp1252, then encoded to UTF-8 again.

    Example seen in IETT routes.csv: 'KADIKÃ–Y - KÄ°RAZLITEPE' -> 'KADIKÖY - KİRAZLITEPE'.
    Text without the typical markers, or that does not round-trip, is returned unchanged.
    """
    if not any(m in text for m in _MOJIBAKE_MARKERS):
        return text
    try:
        return text.encode("cp1252").decode("utf-8")
    except (UnicodeEncodeError, UnicodeDecodeError):
        return text


def parse_coord(raw: str, valid: tuple[float, float]) -> float | None:
    """Parse a coordinate that may be formatted correctly ('41.0191700005564') or mangled.

    The IETT stops.csv has values like '410.191.700.005.564': the decimal point is gone and
    a dot was inserted every three digits. We drop the dots and place the decimal point so the
    value falls inside the valid range (the integer part is 2 digits for Istanbul).
    """
    raw = (raw or "").strip().replace(",", ".")
    if not raw:
        return None
    if raw.count(".") <= 1:
        try:
            value = float(raw)
        except ValueError:
            return None
        return value if valid[0] <= value <= valid[1] else None
    digits = raw.replace(".", "")
    if not digits.isdigit():
        return None
    for int_len in (2, 1, 3):
        if len(digits) <= int_len:
            continue
        value = int(digits) / 10 ** (len(digits) - int_len)
        if valid[0] <= value <= valid[1]:
            return value
    return None


def read_table(path: Path) -> Iterator[dict[str, str]]:
    """Yield rows of a GTFS-like CSV as dicts, whatever the delimiter/encoding/BOM.

    Empty header cells (e.g. the trailing ';' in IETT stops.csv) are dropped.
    """
    text = decode_bytes(Path(path).read_bytes())
    header_line = text.split("\n", 1)[0]
    delimiter = ";" if header_line.count(";") > header_line.count(",") else ","
    reader = csv.reader(io.StringIO(text, newline=""), delimiter=delimiter)
    header = [h.strip() for h in next(reader, [])]
    for row in reader:
        if not row:
            continue
        yield {h: (row[i].strip() if i < len(row) else "") for i, h in enumerate(header) if h}


def parse_gtfs_time(value: str) -> int | None:
    """'06:40:00' -> seconds. GTFS allows hours >= 24 for after-midnight trips."""
    if not value:
        return None
    try:
        h, m, s = value.split(":")
        return int(h) * 3600 + int(m) * 60 + int(s)
    except ValueError:
        return None


def haversine_m(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    r = 6_371_000.0
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dp, dl = p2 - p1, math.radians(lon2 - lon1)
    a = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 2 * r * math.asin(math.sqrt(a))
