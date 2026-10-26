import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "pipeline"))

import discover  # noqa: E402


def test_is_gtfs_resource_by_name_and_url():
    assert discover.is_gtfs_resource({"name": "stop_times.txt"})
    assert discover.is_gtfs_resource({"name": "x", "url": "https://h/files/Stops.csv"})
    assert not discover.is_gtfs_resource({"name": "ridership_2020.csv", "url": "https://h/r.csv"})


def test_sniff_table_semicolon_and_turkish_codepage():
    raw = "durak_kodu;durak_adi;enlem\n1001;Şişli;41.06\n".encode("cp1254")
    info = discover.sniff_table(raw)
    assert info["delimiter"] == ";"
    assert info["columns"] == ["durak_kodu", "durak_adi", "enlem"]
    assert info["rows"][0][1] == "Şişli"


def test_sniff_table_strips_bom():
    raw = "﻿stop_id,stop_name\n1,A\n".encode("utf-8")
    assert discover.sniff_table(raw)["columns"][0] == "stop_id"


def test_render_report_with_no_data_does_not_crash():
    text = discover.render_report({}, {}, None, ["boom"])
    assert "boom" in text
