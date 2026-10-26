"""Data source discovery for istarosa.

Run this on a machine with open internet access. It does three things:

1. Queries the IBB open data portal (CKAN) for the IETT and "public transport" GTFS
   datasets and for anything that looks like ferry/metro/line/stop data, and lists
   every resource (format, size, url, last modified).
2. Peeks into each tabular resource (CKAN datastore schema, or the first bytes of the
   file) so we learn the real column names before writing adapters.
3. Counts what OpenStreetMap knows about Istanbul metro, ferry and bus routes
   (Overpass API) as a fallback/cross-check source.

Output: a Markdown report plus the raw JSON responses. Stdlib only, no installs.

    python backend/pipeline/discover.py --out discovery_out
"""
from __future__ import annotations

import argparse
import csv
import io
import json
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

CKAN = "https://data.ibb.gov.tr/api/3/action"
OVERPASS = "https://overpass-api.de/api/interpreter"
USER_AGENT = "istarosa-discovery/0.1 (+https://github.com/mifarosa/istarosa)"

# Datasets we already know about from the portal pages.
KNOWN_DATASETS = ["iett-gtfs-verisi", "public-transport-gtfs-data"]
# Free-text queries to find anything else relevant (ferry data is the big unknown).
SEARCH_QUERIES = ["gtfs", "iett", "metro", "vapur", "deniz", "iskele", "sefer", "hat", "durak"]

GTFS_FILES = {
    "agency", "stops", "routes", "trips", "stop_times", "calendar", "calendar_dates",
    "shapes", "frequencies", "transfers", "feed_info", "fare_attributes", "fare_rules",
}
TABULAR_FORMATS = {"csv", "txt", "tsv", "json", "xlsx", "xls"}

# Rough Istanbul bounding box: south, west, north, east.
BBOX = (40.80, 28.40, 41.60, 29.70)

OVERPASS_COUNTS = {
    "metro station nodes (station=subway)": 'nwr["railway"="station"]["station"="subway"]',
    "tram stops": 'nwr["railway"="tram_stop"]',
    "ferry terminals": 'nwr["amenity"="ferry_terminal"]',
    "ferry route relations": 'relation["route"="ferry"]',
    "subway route relations": 'relation["route"="subway"]',
    "light_rail route relations": 'relation["route"="light_rail"]',
    "train route relations": 'relation["route"="train"]',
    "bus route relations": 'relation["route"="bus"]',
}


# ---------------------------------------------------------------- helpers
def http(url: str, *, data: bytes | None = None, headers: dict | None = None,
         timeout: int = 60, max_bytes: int | None = None) -> bytes:
    req = urllib.request.Request(url, data=data, headers={"User-Agent": USER_AGENT, **(headers or {})})
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return resp.read(max_bytes) if max_bytes else resp.read()


def get_json(url: str, **kw):
    return json.loads(http(url, **kw).decode("utf-8"))


def ckan(action: str, **params):
    url = f"{CKAN}/{action}?{urllib.parse.urlencode(params)}"
    body = get_json(url)
    if not body.get("success"):
        raise RuntimeError(f"CKAN {action} failed: {body.get('error')}")
    return body["result"]


def is_gtfs_resource(res: dict) -> bool:
    """True if a CKAN resource looks like one of the GTFS spec files."""
    for field in (res.get("name") or "", (res.get("url") or "").rsplit("/", 1)[-1]):
        stem = field.lower().rsplit(".", 1)[0].strip()
        if stem in GTFS_FILES:
            return True
    return False


def sniff_table(raw: bytes) -> dict:
    """Decode the head of a delimited file and return columns plus a few sample rows."""
    text = None
    for enc in ("utf-8-sig", "cp1254", "latin-1"):  # cp1254 = Turkish Windows codepage
        try:
            text = raw.decode(enc)
            break
        except UnicodeDecodeError:
            continue
    lines = text.splitlines()[:6]
    if not lines:
        return {"columns": [], "rows": [], "delimiter": None}
    try:
        delim = csv.Sniffer().sniff(lines[0], delimiters=",;\t|").delimiter
    except csv.Error:
        delim = ","
    rows = list(csv.reader(io.StringIO("\n".join(lines)), delimiter=delim))
    # raw bytes of the first lines: shows real encoding / number formatting problems
    raw_head = [repr(b)[:240] for b in raw.splitlines()[:3]]
    return {"encoding": enc, "delimiter": delim, "columns": rows[0], "rows": rows[1:4], "raw_head": raw_head}


def safe(label: str, fn, errors: list):
    """Run one step; record the error instead of aborting the whole discovery."""
    try:
        return fn()
    except (urllib.error.URLError, urllib.error.HTTPError, TimeoutError, RuntimeError,
            json.JSONDecodeError, OSError) as exc:
        msg = f"{label}: {type(exc).__name__}: {exc}"
        print("  !", msg, file=sys.stderr)
        errors.append(msg)
        return None


# ---------------------------------------------------------------- CKAN
def collect_datasets(errors: list) -> dict[str, dict]:
    found: dict[str, dict] = {}
    for name in KNOWN_DATASETS:
        pkg = safe(f"package_show {name}", lambda n=name: ckan("package_show", id=n), errors)
        if pkg:
            found[pkg["name"]] = pkg
    for q in SEARCH_QUERIES:
        res = safe(f"package_search {q}", lambda q=q: ckan("package_search", q=q, rows=50), errors)
        for pkg in (res or {}).get("results", []):
            found.setdefault(pkg["name"], pkg)
    return found


def peek_datastore(res: dict, errors: list) -> dict | None:
    rid = res.get("id")
    if not res.get("datastore_active"):
        return None
    ds = safe(f"datastore_search {rid}", lambda: ckan("datastore_search", resource_id=rid, limit=3), errors)
    if not ds:
        return None
    columns = [f["id"] for f in ds.get("fields", []) if f["id"] != "_id"]
    return {
        "via": "datastore",
        "total": ds.get("total"),
        "columns": columns,
        # index by column name so the CKAN-internal _id never shifts the values
        "rows": [[str(r.get(c)) for c in columns] for r in ds.get("records", [])],
    }


def peek_file(res: dict, max_bytes: int, errors: list) -> dict | None:
    url, fmt = res.get("url"), (res.get("format") or "").lower()
    if not url or fmt not in TABULAR_FORMATS - {"xlsx", "xls"}:
        return None
    raw = safe(f"download head {url}", lambda: http(url, max_bytes=max_bytes, timeout=90), errors)
    if not raw:
        return None
    info = sniff_table(raw)
    info["via"] = f"file head ({len(raw)} bytes)"
    return info


def peek_resource(res: dict, max_bytes: int, errors: list) -> dict[str, dict]:
    """Learn the schema of one resource. The datastore copy and the raw file can differ
    (encoding, number formatting, truncation), so GTFS files are peeked both ways."""
    out = {}
    ds = peek_datastore(res, errors)
    if ds:
        out["datastore"] = ds
    if not ds or is_gtfs_resource(res):
        fi = peek_file(res, max_bytes, errors)
        if fi:
            out["file"] = fi
    return out


# ---------------------------------------------------------------- deep dive
# Datasets that may hold fresher metro / rail / ferry data than the stale GTFS feed.
DEEP_DATASETS = [
    "metro-istanbul-hat-bilgisi-listesi",
    "metro-istanbul-istasyon-bilgi-listesi",
    "metro-istanbul-hat-id-sine-gore-istasyon-bilgileri-listesi",
    "metro-istanbul-hat-id-sine-gore-yon-bilgisi-listesi",
    "metro-istanbul-istasyonlarin-baslangic-istasyonuna-olan-uzaklik-sureleri-listesi",
    "metro-istanbul-sefer-tarifeleri-listesi-web-servisi",
    "rayli-sistemlere-ait-aktarma-bilgileri",
    "rayli-ulasim-hatlari-vektor-verisi",
    "deniz-ulasim-hatlari-vektor-verisi",
    "deniz-ulasim-istasyonlari-vektor-verisi",
    "istanbul-sehir-hatlari-iskeleleri",
    "sehir-hatlari-sefer-sayilari",
]


def deep_dive(errors: list) -> dict[str, dict]:
    """List resources of the deep datasets and probe API/GeoJSON endpoints (first bytes only)."""
    result: dict[str, dict] = {}
    for name in DEEP_DATASETS:
        pkg = safe(f"package_show {name}", lambda n=name: ckan("package_show", id=n), errors)
        if not pkg:
            continue
        resources = []
        for r in pkg.get("resources", []):
            fmt = (r.get("format") or "").lower()
            entry = {"name": r.get("name"), "format": fmt, "url": r.get("url"),
                     "description": (r.get("description") or "")[:300], "modified": r.get("last_modified")}
            if fmt in {"api", "geojson", "json", "kml"} and r.get("url"):
                raw = safe(f"probe {r['url']}", lambda u=r["url"]: http(u, max_bytes=1500, timeout=30), errors)
                if raw:
                    entry["probe"] = raw.decode("utf-8", errors="replace")
            resources.append(entry)
        result[name] = {"title": pkg.get("title"), "notes": (pkg.get("notes") or "")[:500], "resources": resources}
    return result


# ---------------------------------------------------------------- Overpass
OVERPASS_MIRRORS = [OVERPASS, "https://overpass.kumi.systems/api/interpreter"]


def overpass(query: str, attempts: int = 4) -> dict:
    """Public Overpass instances return 502/504 under load: retry with backoff, rotate mirrors."""
    data = urllib.parse.urlencode({"data": query}).encode()
    last: Exception | None = None
    for i in range(attempts):
        try:
            return json.loads(http(OVERPASS_MIRRORS[i % len(OVERPASS_MIRRORS)], data=data, timeout=180).decode("utf-8"))
        except (urllib.error.HTTPError, urllib.error.URLError, TimeoutError, json.JSONDecodeError) as exc:
            last = exc
            time.sleep(5 * (i + 1))
    raise last  # type: ignore[misc]


def collect_osm(errors: list) -> dict:
    s, w, n, e = BBOX
    bbox = f"({s},{w},{n},{e})"
    counts, ferry_sample = {}, None
    for label, sel in OVERPASS_COUNTS.items():
        out = safe(f"overpass count {label}",
                   lambda sel=sel: overpass(f"[out:json][timeout:120];{sel}{bbox};out count;"), errors)
        if out and out.get("elements"):
            counts[label] = int(out["elements"][0]["tags"]["total"])
        time.sleep(2)  # be polite to the public Overpass instance
    out = safe("overpass ferry routes", lambda: overpass(
        f'[out:json][timeout:120];relation["route"="ferry"]{bbox};out tags;'), errors)
    if out:
        tags = [el.get("tags", {}) for el in out["elements"]]
        ferry_sample = {
            "operators": Counter(t.get("operator", "(none)") for t in tags).most_common(10),
            "names": sorted(t.get("name", t.get("ref", "?")) for t in tags)[:40],
        }
    return {"counts": counts, "ferry": ferry_sample}


# ---------------------------------------------------------------- report
def md_table(rows: list[list[str]], header: list[str]) -> str:
    esc = lambda c: str(c).replace("|", "\\|").replace("\n", " ")
    lines = ["| " + " | ".join(header) + " |", "|" + "---|" * len(header)]
    lines += ["| " + " | ".join(esc(c) for c in r) + " |" for r in rows]
    return "\n".join(lines)


def render_report(datasets: dict, peeks: dict, osm: dict | None, errors: list,
                  deep: dict | None = None) -> str:
    out = [f"# istarosa data source discovery\n",
           f"Generated {datetime.now(timezone.utc).isoformat(timespec='seconds')} by `backend/pipeline/discover.py`.\n"]

    out.append("## IBB open data: datasets\n")
    rows = []
    for name, pkg in sorted(datasets.items()):
        fmts = sorted({(r.get("format") or "?").lower() for r in pkg.get("resources", [])})
        rows.append([f"[{name}](https://data.ibb.gov.tr/en/dataset/{name})", pkg.get("title", ""),
                     len(pkg.get("resources", [])), ", ".join(fmts), pkg.get("metadata_modified", "")[:10],
                     (pkg.get("license_title") or "")])
    out.append(md_table(rows, ["dataset", "title", "resources", "formats", "modified", "license"]) + "\n")

    focus = [n for n in KNOWN_DATASETS if n in datasets]
    for name in focus:
        pkg = datasets[name]
        out.append(f"\n## Resources of `{name}`\n")
        out.append((pkg.get("notes") or "").strip()[:800] + "\n")
        rows = []
        for r in pkg.get("resources", []):
            rows.append([r.get("name", ""), (r.get("format") or "").lower(), r.get("size") or "",
                         "yes" if is_gtfs_resource(r) else "", (r.get("last_modified") or "")[:10], r.get("url", "")])
        out.append(md_table(rows, ["name", "format", "size", "GTFS file", "modified", "url"]) + "\n")
        out.append(f"\nGTFS spec files present: "
                   f"{sorted({(r.get('name') or '').lower().rsplit('.', 1)[0] for r in pkg.get('resources', []) if is_gtfs_resource(r)})}\n")

    out.append("\n## Schemas (columns seen)\n")
    for key, info in peeks.items():
        out.append(f"\n### {key}\n")
        out.append(f"- via: {info.get('via')}" + (f", total rows: {info['total']}" if info.get("total") is not None else ""))
        out.append(f"- columns: `{', '.join(info.get('columns', []))}`")
        for row in info.get("rows", []):
            out.append(f"  - `{row}`")
        for line in info.get("raw_head", []):
            out.append(f"  - raw: `{line}`")

    if deep:
        out.append("\n## Deep dive: metro / rail / ferry datasets\n")
        for name, d in deep.items():
            out.append(f"\n### {name}\n\n{d.get('title')}: {(d.get('notes') or '').strip()[:300]}\n")
            for r in d["resources"]:
                out.append(f"- **{r['name']}** ({r['format']}, {str(r.get('modified') or '')[:10]}) {r['url']}")
                if r.get("description"):
                    out.append(f"  - {r['description']}")
                if r.get("probe"):
                    out.append("  - probe (first bytes):\n\n```\n" + r["probe"][:700] + "\n```")

    out.append("\n## Other candidate datasets (not in the GTFS pair)\n")
    others = [(n, p) for n, p in sorted(datasets.items()) if n not in KNOWN_DATASETS]
    out.append(md_table([[f"[{n}](https://data.ibb.gov.tr/en/dataset/{n})", p.get("title", ""),
                          ", ".join(sorted({(r.get('format') or '?').lower() for r in p.get('resources', [])}))]
                         for n, p in others], ["dataset", "title", "formats"]) + "\n")

    if osm:
        out.append("\n## OpenStreetMap cross-check (Istanbul bbox)\n")
        out.append(md_table([[k, v] for k, v in osm["counts"].items()], ["feature", "count"]) + "\n")
        if osm.get("ferry"):
            out.append(f"\nFerry route operators: {osm['ferry']['operators']}\n")
            out.append(f"Sample ferry route names: {', '.join(osm['ferry']['names'])}\n")

    if errors:
        out.append("\n## Errors during discovery\n")
        out += [f"- `{e}`" for e in errors]
    out.append("\n## Next steps\n- Compare the columns above with GTFS spec files; write `adapters/` against the real schema.\n"
               "- If ferry data is missing from IBB, decide: Sehir Hatlari timetable PDF, formal data request, or OSM.\n")
    return "\n".join(out) + "\n"


# ---------------------------------------------------------------- main
def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--out", type=Path, default=Path("discovery_out"), help="directory for raw JSON and report")
    ap.add_argument("--report", type=Path, default=Path("docs/data-sources.md"), help="Markdown report path")
    ap.add_argument("--max-bytes", type=int, default=64 * 1024, help="bytes to download per file when peeking")
    ap.add_argument("--skip-overpass", action="store_true")
    ap.add_argument("--skip-deep", action="store_true",
                    help="skip the metro/rail/ferry dataset deep dive")
    args = ap.parse_args(argv)

    errors: list[str] = []
    args.out.mkdir(parents=True, exist_ok=True)

    print("[1/3] IBB CKAN datasets")
    datasets = collect_datasets(errors)
    (args.out / "ckan_datasets.json").write_text(json.dumps(datasets, ensure_ascii=False, indent=2), encoding="utf-8")

    print("[2/3] peeking into resources")
    peeks: dict[str, dict] = {}
    for name in KNOWN_DATASETS:
        for res in datasets.get(name, {}).get("resources", []):
            for kind, info in peek_resource(res, args.max_bytes, errors).items():
                peeks[f"{name} / {res.get('name') or res.get('id')} [{kind}]"] = info
    (args.out / "schemas.json").write_text(json.dumps(peeks, ensure_ascii=False, indent=2), encoding="utf-8")

    deep = None
    if not args.skip_deep:
        print("[2b/3] deep dive: metro / rail / ferry datasets")
        deep = deep_dive(errors)
        (args.out / "deep.json").write_text(json.dumps(deep, ensure_ascii=False, indent=2), encoding="utf-8")

    osm = None
    if not args.skip_overpass:
        print("[3/3] OpenStreetMap (Overpass)")
        osm = collect_osm(errors)
        (args.out / "osm.json").write_text(json.dumps(osm, ensure_ascii=False, indent=2), encoding="utf-8")

    args.report.parent.mkdir(parents=True, exist_ok=True)
    args.report.write_text(render_report(datasets, peeks, osm, errors, deep), encoding="utf-8")
    print(f"done: report -> {args.report}, raw -> {args.out}/ ({len(errors)} errors)")
    return 0 if datasets else 1


if __name__ == "__main__":
    raise SystemExit(main())
