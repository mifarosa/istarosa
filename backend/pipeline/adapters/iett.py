"""IETT bus GTFS -> internal stops and stop patterns.

Input is the directory of raw CSVs from https://data.ibb.gov.tr/en/dataset/iett-gtfs-verisi
(agency, calendar, routes, stops, stop_times, trips). Differences from a textbook GTFS feed,
all handled here (see docs/data-sources.md):

* semicolon-delimited, UTF-8 with BOM; routes.csv text is double-encoded (mojibake)
* stops.csv coordinates are mangled ('410.191.700.005.564')
* stop_times only carries times at timepoints; intermediate stops are empty
* no frequencies.txt; headways must be derived from trip start times
* every route_id is already one (line, direction, variant) with a fixed stop sequence
"""
from __future__ import annotations

import statistics
from collections import defaultdict
from dataclasses import dataclass, field
from pathlib import Path

from .common import (LAT_RANGE, LON_RANGE, fix_mojibake, haversine_m, parse_coord,
                     parse_gtfs_time, read_table)

MIN_SEGMENT_SECS = 20
FALLBACK_SPEED_MPS = 5.5          # ~20 km/h, used when a stretch has no timepoint at all
HEADWAY_WINDOW = (6 * 3600, 22 * 3600)  # only daytime trips count towards headway
MIN_TRIPS_FOR_HEADWAY = 3


@dataclass(frozen=True)
class Stop:
    id: str
    code: str
    name: str
    lat: float
    lon: float
    direction: str | None  # parsed from stop_desc 'direction: AVCILAR'


@dataclass
class Pattern:
    route_id: str
    line: str                 # route_short_name, e.g. '500T'
    direction: str | None     # 'G' or 'D' from route_code
    name: str
    stop_ids: list[str]
    ride_secs: list[int]      # len(stop_ids) - 1
    headway_secs: int | None  # None when there are too few weekday trips
    mode: str = "bus"


@dataclass
class LoadResult:
    stops: dict[str, Stop]
    patterns: list[Pattern]
    stats: dict[str, int] = field(default_factory=dict)


def load_stops(path: Path, stats: dict[str, int]) -> dict[str, Stop]:
    stops: dict[str, Stop] = {}
    for r in read_table(path):
        lat, lon = parse_coord(r.get("stop_lat", ""), LAT_RANGE), parse_coord(r.get("stop_lon", ""), LON_RANGE)
        if lat is None or lon is None:
            stats["stops_bad_coords"] += 1
            continue
        desc = r.get("stop_desc", "")
        direction = desc.split(":", 1)[1].strip() if desc.lower().startswith("direction:") else None
        stops[r["stop_id"]] = Stop(r["stop_id"], r.get("stop_code", ""), fix_mojibake(r.get("stop_name", "")),
                                   lat, lon, direction)
    return stops


def weekday_service_ids(calendar_path: Path) -> set[str]:
    """Services that run Monday to Friday. Falls back to every service if none matches."""
    rows = list(read_table(calendar_path))
    weekday = {r["service_id"] for r in rows if r.get("monday") == "1" and r.get("friday") == "1"}
    return weekday or {r["service_id"] for r in rows}


def interpolate_segments(stop_ids: list[str], times: list[int | None], stops: dict[str, Stop]) -> list[int]:
    """Ride seconds between consecutive stops.

    Between two timepoints the elapsed time is split proportionally to the straight-line
    distance of each hop. Stretches outside any timepoint pair use a fixed fallback speed.
    """
    n = len(stop_ids)
    dists = [haversine_m(stops[a].lat, stops[a].lon, stops[b].lat, stops[b].lon)
             for a, b in zip(stop_ids, stop_ids[1:])]
    secs: list[float | None] = [None] * (n - 1)

    known = [(i, t) for i, t in enumerate(times) if t is not None]
    # keep only a strictly increasing run of timepoints; bad rows must not produce negative time
    clean: list[tuple[int, int]] = []
    for i, t in known:
        if not clean or t > clean[-1][1]:
            clean.append((i, t))
    for (i0, t0), (i1, t1) in zip(clean, clean[1:]):
        total = sum(dists[i0:i1])
        for k in range(i0, i1):
            share = dists[k] / total if total > 0 else 1 / (i1 - i0)
            secs[k] = (t1 - t0) * share

    return [max(MIN_SEGMENT_SECS, round(s if s is not None else dists[k] / FALLBACK_SPEED_MPS))
            for k, s in enumerate(secs)]


def estimate_headway(first_departures: list[int]) -> int | None:
    day = sorted(t for t in first_departures if HEADWAY_WINDOW[0] <= t <= HEADWAY_WINDOW[1])
    gaps = [b - a for a, b in zip(day, day[1:]) if b > a]
    if len(day) < MIN_TRIPS_FOR_HEADWAY or not gaps:
        return None
    return int(statistics.median(gaps))


def parse_route_code(code: str) -> tuple[str | None, str | None]:
    """'500T_G_D0' -> ('G', 'D0'). Direction is G (gidis) or D (donus)."""
    parts = code.split("_")
    return (parts[1], parts[2]) if len(parts) >= 3 else (None, None)


def load(gtfs_dir: Path) -> LoadResult:
    gtfs_dir = Path(gtfs_dir)
    stats: dict[str, int] = defaultdict(int)
    stops = load_stops(gtfs_dir / "stops.csv", stats)
    weekday = weekday_service_ids(gtfs_dir / "calendar.csv")

    routes = {r["route_id"]: r for r in read_table(gtfs_dir / "routes.csv")}

    # one representative trip per route (prefer a weekday trip) plus all weekday trip ids
    rep_trip: dict[str, str] = {}
    weekday_trips: dict[str, list[str]] = defaultdict(list)
    for t in read_table(gtfs_dir / "trips.csv"):
        rid = t["route_id"]
        if t["service_id"] in weekday:
            weekday_trips[rid].append(t["trip_id"])
            if rid not in rep_trip or rep_trip[rid] not in weekday_trips[rid]:
                rep_trip[rid] = t["trip_id"]
        else:
            rep_trip.setdefault(rid, t["trip_id"])

    rep_ids = set(rep_trip.values())
    weekday_ids = {tid for ids in weekday_trips.values() for tid in ids}
    rep_rows: dict[str, list[tuple[int, str, int | None]]] = defaultdict(list)
    first_dep: dict[str, tuple[int, int]] = {}
    for r in read_table(gtfs_dir / "stop_times.csv"):
        tid = r["trip_id"]
        seq = int(r["stop_sequence"])
        t = parse_gtfs_time(r.get("departure_time", "")) or parse_gtfs_time(r.get("arrival_time", ""))
        if tid in rep_ids:
            rep_rows[tid].append((seq, r["stop_id"], t))
        if t is not None and tid in weekday_ids and (tid not in first_dep or seq < first_dep[tid][0]):
            first_dep[tid] = (seq, t)

    patterns: list[Pattern] = []
    for rid, tid in rep_trip.items():
        route = routes.get(rid)
        rows = sorted(rep_rows.get(tid, []))
        if route is None or len(rows) < 2:
            stats["routes_without_stop_times"] += 1
            continue
        stop_ids = [sid for _, sid, _ in rows]
        if any(sid not in stops for sid in stop_ids):
            stats["routes_dropped_unknown_stop"] += 1
            continue
        direction, _variant = parse_route_code(route.get("route_code", ""))
        firsts = [first_dep[x][1] for x in weekday_trips.get(rid, []) if x in first_dep]
        patterns.append(Pattern(
            route_id=rid, line=route.get("route_short_name", ""), direction=direction,
            name=fix_mojibake(route.get("route_long_name", "")), stop_ids=stop_ids,
            ride_secs=interpolate_segments(stop_ids, [t for _, _, t in rows], stops),
            headway_secs=estimate_headway(firsts)))
    stats["patterns"] = len(patterns)
    stats["stops"] = len(stops)
    return LoadResult(stops, patterns, dict(stats))
