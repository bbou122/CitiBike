# Methodology

## The Data Problem

Citi Bike publishes trip data as monthly ZIP archives. The 2023 full-year download is a ZIP *of* ZIPs — an outer archive containing 12 inner monthly archives, each holding one or more CSVs:

```
2023-citibike-tripdata.zip          (~1.4 GB compressed)
├── 202301-citibike-tripdata.zip    (83 MB → 334 MB uncompressed)
│   ├── 202301-citibike-tripdata_1.csv
│   └── 202301-citibike-tripdata_2.csv
├── 202302-citibike-tripdata.zip    (78 MB → 316 MB uncompressed)
│   └── 202302-citibike-tripdata.csv
...
└── 202312-citibike-tripdata.zip    (93 MB → 411 MB uncompressed)
```

Fully extracted, the 12 months span **~6.8 GB** and **35.1 million rows**. Extracting everything at once fills a standard working disk and makes the pipeline fragile. The naive approach — `unzip` then `pandas.read_csv` — is also slower than necessary because pandas loads entire DataFrames into memory before any computation happens.

---

## Streaming Approach

Instead of extracting to disk, the pipeline uses Python's `zipfile` module to read each inner ZIP directly into a `BytesIO` buffer in memory, then opens that buffer as a second `ZipFile` object:

```python
with zipfile.ZipFile("2023-citibike-tripdata.zip") as outer:
    inner_bytes = outer.read("2023-citibike-tripdata/202301-citibike-tripdata.zip")

with zipfile.ZipFile(io.BytesIO(inner_bytes)) as inner:
    with inner.open("202301-citibike-tripdata_1.csv") as raw:
        f = io.TextIOWrapper(raw, encoding="utf-8")
        reader = csv.reader(f)
        for row in reader:
            # process one row at a time
```

This means:
- **Maximum disk footprint at any moment:** ~180 MB (the largest inner ZIP in RAM) + a growing aggregation dict
- **No temporary files written anywhere**
- Each inner ZIP is garbage-collected when the next month begins

---

## Why `csv.reader` Instead of pandas

For pure aggregation — counting values and accumulating into dicts — Python's built-in `csv.reader` is faster than `pandas.read_csv` with chunking because:

1. No DataFrame allocation per chunk
2. No dtype inference overhead
3. No column-level vectorization needed — we're doing scalar increments

Throughput on this dataset: **~160,000 rows/second** on a standard machine.

---

## Timestamp Parsing Without `datetime`

Parsing 35 million timestamps with `datetime.strptime` would add substantial overhead. Instead, the pipeline uses direct string slicing — the Citi Bike format is consistent: `"YYYY-MM-DD HH:MM:SS.fff"`.

```python
# Extract hour — no datetime object created
hour = int(sa[11:13])

# Extract date string for DOW lookup
day_str = sa[:10]   # "2023-01-15"
```

Day-of-week is looked up from a precomputed dictionary mapping every 2023 calendar date to its weekday integer (0=Mon, 6=Sun), built once at startup using the standard `date` class:

```python
_DOW = {}
for m in range(1, 13):
    for d in range(1, calendar.monthrange(2023, m)[1] + 1):
        _DOW[f"2023-{m:02d}-{d:02d}"] = date(2023, m, d).weekday()
```

Trip duration is computed with integer arithmetic on the time components — no `datetime` subtraction, no `timedelta`:

```python
s_sec = int(sa[11:13]) * 3600 + int(sa[14:16]) * 60 + int(sa[17:19])
e_sec = int(ea[11:13]) * 3600 + int(ea[14:16]) * 60 + int(ea[17:19])
diff  = e_sec - s_sec
if diff < 0 and sa[:10] != ea[:10]:   # midnight crossing
    diff += 86400
```

---

## Resumability

The pipeline serializes state to `citibike_agg.json` after each month completes. On startup it reads any existing file and skips months already present. This means:

- An interrupted run can be resumed without reprocessing completed months
- Individual months can be reprocessed by deleting their key from the JSON
- The final output is identical regardless of how many sessions it took

---

## Aggregations Produced

| Field | Description |
|---|---|
| `monthly[month]` | Per-month counts: total, member, casual, classic, electric, docked |
| `hour[0–23]` | Trip count by start hour, summed across all 12 months |
| `dow[0–6]` | Trip count by day of week (0=Mon), summed across all 12 months |
| `depart[station]` | Total departures per station across all 12 months |
| `arrive[station]` | Total arrivals per station across all 12 months |
| `duration[bucket]` | Trip count per duration bucket (0–5 min, 5–10, … 60+) |

---

## Output

`citibike_agg.json` — 142 KB, embeddable directly in the HTML dashboard. The entire 6.8 GB raw dataset compresses to this file without loss of any information needed for the analysis.

---

## Dependencies

None. The pipeline uses only Python standard library modules:

| Module | Use |
|---|---|
| `zipfile` | Read nested ZIP archives |
| `io` | `BytesIO` buffer and `TextIOWrapper` |
| `csv` | Row-level CSV parsing |
| `json` | Serialize/deserialize aggregation state |
| `datetime`, `calendar` | Build the date→DOW lookup table at startup |
| `time` | Progress timing |

Python 3.10+ recommended for the `str | None` type hint syntax; 3.8+ works if that line is removed.
