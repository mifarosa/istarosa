import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "pipeline"))

from adapters import common, iett  # noqa: E402


# ---------------------------------------------------------------- common helpers
def test_parse_coord_handles_mangled_and_plain_values():
    assert common.parse_coord("410.191.700.005.564", common.LAT_RANGE) == pytest.approx(41.01917, abs=1e-5)
    assert common.parse_coord("286.843.529.999.755", common.LON_RANGE) == pytest.approx(28.68435, abs=1e-5)
    assert common.parse_coord("41.0833", common.LAT_RANGE) == pytest.approx(41.0833)
    assert common.parse_coord("41,0833", common.LAT_RANGE) == pytest.approx(41.0833)


def test_parse_coord_rejects_garbage_and_out_of_range():
    assert common.parse_coord("", common.LAT_RANGE) is None
    assert common.parse_coord("abc", common.LAT_RANGE) is None
    assert common.parse_coord("12.5", common.LAT_RANGE) is None  # not Istanbul


def test_fix_mojibake_repairs_double_encoding_only():
    assert common.fix_mojibake("KADIKÃ–Y - KÄ°RAZLITEPE") == "KADIKÖY - KİRAZLITEPE"
    assert common.fix_mojibake("KİRAZLITEPE") == "KİRAZLITEPE"  # already fine
    assert common.fix_mojibake("Ãœ??") != ""                       # does not crash on odd input


def test_decode_bytes_falls_back_to_cp1254():
    raw = "Beşiktaş ŞH.".encode("cp1254")
    assert common.decode_bytes(raw) == "Beşiktaş ŞH."
    assert common.decode_bytes("﻿İ".encode("utf-8")) == "İ"


def test_read_table_semicolon_bom_and_trailing_empty_header(tmp_path):
    p = tmp_path / "stops.csv"
    p.write_bytes("﻿stop_id;stop_name;\n1;A;\n".encode("utf-8"))
    assert list(common.read_table(p)) == [{"stop_id": "1", "stop_name": "A"}]


# ---------------------------------------------------------------- adapter
def fixture_feed(tmp_path: Path) -> Path:
    """Tiny IETT-like feed: 4 stops on one line, timepoints only at the ends."""
    def w(name, text):
        (tmp_path / name).write_bytes(("﻿" + text).encode("utf-8"))

    w("calendar.csv",
      "service_id;service_name;monday;tuesday;wednesday;thursday;friday;saturday;sunday;start_date;end_date\n"
      "0;WEEKDAYS;1;1;1;1;1;0;0;20260316;20261231\n"
      "6;SATURDAY;0;0;0;0;0;1;0;20260316;20261231\n")
    # route_long_name is double encoded like the real file
    long_name = "KADIKÖY - KİRAZLITEPE".encode("utf-8").decode("cp1252")
    w("routes.csv",
      "route_id;agency_id;route_short_name;route_long_name;route_type;route_desc;route_code\n"
      f"1;1;1;{long_name};3;;1_G_D0\n")
    # stops 1..4 spaced along a line; stop 3 has a mangled coordinate, stop 4 is junk -> dropped
    w("stops.csv",
      "stop_id;stop_code;stop_name;stop_desc;stop_lat;stop_lon;location_type;\n"
      "1;100001;DURAK A;direction: AVCILAR;41.0000;29.0000;0;\n"
      "2;100002;DURAK B;direction: AVCILAR;41.0100;29.0000;0;\n"
      "3;100003;DURAK C;direction: AVCILAR;410.200.000.000.000;290.000.000.000.000;0;\n"
      "4;100004;DURAK D;direction: AVCILAR;not;a-number;0;\n")
    # weekday trips start every 10 minutes from 07:00 and a saturday trip
    trips = ["trip_id;route_id;service_id;trip_headsign;direction_id"]
    times = ["trip_id;stop_id;stop_sequence;arrival_time;departure_time;timepoint"]
    for i, start in enumerate(range(7 * 3600, 8 * 3600, 600)):
        tid = str(100 + i)
        trips.append(f"{tid};1;0;KIRAZLITEPE;1")
        hh, mm = divmod(start // 60, 60)
        end_hh, end_mm = divmod((start + 600) // 60, 60)
        times += [f"{tid};1;1;{hh:02d}:{mm:02d}:00;{hh:02d}:{mm:02d}:00;1",
                  f"{tid};2;2;;;0",
                  f"{tid};3;3;{end_hh:02d}:{end_mm:02d}:00;{end_hh:02d}:{end_mm:02d}:00;1"]
    trips.append("900;1;6;KIRAZLITEPE;1")
    w("trips.csv", "\n".join(trips) + "\n")
    w("stop_times.csv", "\n".join(times) + "\n")
    return tmp_path


def test_load_builds_pattern_with_interpolation_headway_and_fixes(tmp_path):
    res = iett.load(fixture_feed(tmp_path))

    assert res.stats["stops_bad_coords"] == 1            # stop 4 only
    assert set(res.stops) == {"1", "2", "3"}
    assert res.stops["3"].lat == pytest.approx(41.02)     # mangled value recovered
    assert res.stops["1"].direction == "AVCILAR"

    (p,) = res.patterns
    assert p.name == "KADIKÖY - KİRAZLITEPE"
    assert (p.line, p.direction, p.stop_ids) == ("1", "G", ["1", "2", "3"])
    assert sum(p.ride_secs) == 600                        # timepoint to timepoint total preserved
    assert p.ride_secs[0] == p.ride_secs[1] == 300         # equal hops -> equal split
    assert p.headway_secs == 600


def test_interpolate_without_timepoints_uses_fallback_speed():
    stops = {s: iett.Stop(s, s, s, 41.0 + i * 0.01, 29.0, None) for i, s in enumerate("abc")}
    secs = iett.interpolate_segments(["a", "b", "c"], [None, None, None], stops)
    assert all(s > iett.MIN_SEGMENT_SECS for s in secs)   # ~1.1 km at 20 km/h -> ~200 s


def test_interpolate_ignores_backwards_timepoints():
    stops = {s: iett.Stop(s, s, s, 41.0 + i * 0.01, 29.0, None) for i, s in enumerate("abc")}
    secs = iett.interpolate_segments(["a", "b", "c"], [1000, 900, 1600], stops)
    assert all(s >= iett.MIN_SEGMENT_SECS for s in secs)
    assert sum(secs) == pytest.approx(600, abs=1)         # 1000 -> 1600 across both hops


def test_estimate_headway_needs_enough_daytime_trips():
    assert iett.estimate_headway([7 * 3600, 7 * 3600 + 600]) is None
    assert iett.estimate_headway([7 * 3600, 7 * 3600 + 600, 7 * 3600 + 1200]) == 600
    assert iett.estimate_headway([3 * 3600, 3 * 3600 + 300, 3 * 3600 + 600]) is None  # night only
