# Pipeline

## Data source discovery

`discover.py` inspects the real data sources before we write adapters. It needs outbound
internet access (some sandboxes block `data.ibb.gov.tr`), and uses only the standard library.

```bash
python backend/pipeline/discover.py                  # writes docs/data-sources.md + discovery_out/
python backend/pipeline/discover.py --skip-overpass  # IBB portal only
```

It reports, for the IETT GTFS and "public transport GTFS" datasets: every resource, its
format and URL, and the actual column names. It also lists other candidate datasets
(ferry, metro, lines, stops) and counts Istanbul metro/ferry/bus routes in OpenStreetMap.

Commit the generated `docs/data-sources.md` so the adapter design can be reviewed against it.
