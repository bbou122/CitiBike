"""
aggregate.py
------------
Streaming aggregation pipeline for NYC Citi Bike 2023 trip data.

Processes the full-year ZIP archive (a ZIP of ZIPs) without ever
extracting raw CSVs to disk. Each monthly inner ZIP is read into
memory as a BytesIO buffer, streamed row-by-row, and discarded
before the next month loads.

Input:  2023-citibike-tripdata.zip  (~1.4 GB compressed)
Output: citibike_agg.json           (~142 KB)

Usage:
    python aggregate.py

The script is resumable — if citibike_agg.json already exists it
will skip months that have already been processed.
"""

import csv
import io
import json
import time
import zipfile
from datetime import date
import calendar

# ── CONFIG ────────────────────────────────────────────────────────────
ZIP_PATH = "2023-citibike-tripdata.zip"
OUT_PATH = "citibike_agg.json"

MONTHS = [
    "202301", "202302", "202303", "202304",
    "202305", "202306", "202307", "202308",
    "202309", "202310", "202311", "202312",
]

# ── HELPERS ───────────────────────────────────────────────────────────

# Precompute date → day-of-week (0=Mon … 6=Sun) for every 2023 date.
# Much faster than calling datetime.strptime on 35M rows.
_DOW: dict[str, int] = {}
for _m in range(1, 13):
    for _d in range(1, calendar.monthrange(2023, _m)[1] + 1):
        _DOW[f"2023-{_m:02d}-{_d:02d}"] = date(2023, _m, _d).weekday()


def duration_bucket(seconds: float) -> str | None:
    """Map a trip duration in seconds to a labeled bucket."""
    m = seconds / 60
    if m < 0:    return None
    if m < 5:    return "0-5"
    if m < 10:   return "5-10"
    if m < 15:   return "10-15"
    if m < 20:   return "15-20"
    if m < 30:   return "20-30"
    if m < 45:   return "30-45"
    if m < 60:   return "45-60"
    return "60+"


def load_state(path: str) -> dict:
    """Load existing aggregation state or return a fresh one."""
    try:
        with open(path) as f:
            return json.load(f)
    except FileNotFoundError:
        return {
            "monthly":  {},
            "hour":     {str(h): 0 for h in range(24)},
            "dow":      {str(d): 0 for d in range(7)},
            "depart":   {},
            "arrive":   {},
            "duration": {},
        }


def save_state(state: dict, path: str) -> None:
    with open(path, "w") as f:
        json.dump(state, f)


# ── CORE PROCESSING ───────────────────────────────────────────────────

def process_month(outer_zip: zipfile.ZipFile, month: str, state: dict) -> dict:
    """
    Stream all CSVs inside one monthly inner ZIP and accumulate counts
    into `state`. Returns per-month summary totals.
    """
    inner_name = f"2023-citibike-tripdata/{month}-citibike-tripdata.zip"
    inner_bytes = outer_zip.read(inner_name)          # ~80–180 MB in RAM

    totals = {
        "member": 0, "casual": 0,
        "classic": 0, "electric": 0, "docked": 0,
        "total": 0,
    }

    with zipfile.ZipFile(io.BytesIO(inner_bytes)) as inner:
        csv_files = sorted(n for n in inner.namelist() if n.endswith(".csv"))

        for csv_name in csv_files:
            with inner.open(csv_name) as raw:
                f = io.TextIOWrapper(raw, encoding="utf-8", errors="replace")
                reader = csv.reader(f)
                header = next(reader)
                idx = {col: i for i, col in enumerate(header)}

                i_rt = idx["rideable_type"]
                i_sa = idx["started_at"]
                i_ea = idx.get("ended_at", -1)
                i_ss = idx["start_station_name"]
                i_es = idx["end_station_name"]
                i_mc = idx["member_casual"]

                for row in reader:
                    if len(row) < 10:
                        continue

                    totals["total"] += 1

                    # Rider type
                    mc = row[i_mc]
                    if mc == "member":
                        totals["member"] += 1
                    elif mc == "casual":
                        totals["casual"] += 1

                    # Bike type
                    rt = row[i_rt]
                    if rt == "classic_bike":
                        totals["classic"] += 1
                    elif rt == "electric_bike":
                        totals["electric"] += 1
                    elif rt == "docked_bike":
                        totals["docked"] += 1

                    # Hour of day — slice "HH" from "YYYY-MM-DD HH:MM:SS"
                    sa = row[i_sa]
                    if len(sa) >= 13:
                        try:
                            h = int(sa[11:13])
                            state["hour"][str(h)] = state["hour"].get(str(h), 0) + 1
                        except ValueError:
                            pass

                        # Day of week — look up precomputed table
                        day_str = sa[:10]
                        if day_str in _DOW:
                            dw = str(_DOW[day_str])
                            state["dow"][dw] = state["dow"].get(dw, 0) + 1

                    # Station flow
                    ss = row[i_ss].strip()
                    es = row[i_es].strip()
                    if ss:
                        state["depart"][ss] = state["depart"].get(ss, 0) + 1
                    if es:
                        state["arrive"][es] = state["arrive"].get(es, 0) + 1

                    # Trip duration — integer arithmetic, no datetime parsing
                    if i_ea > 0 and len(row) > i_ea:
                        ea = row[i_ea]
                        if len(sa) >= 19 and len(ea) >= 19:
                            try:
                                s_sec = (
                                    int(sa[11:13]) * 3600
                                    + int(sa[14:16]) * 60
                                    + int(sa[17:19])
                                )
                                e_sec = (
                                    int(ea[11:13]) * 3600
                                    + int(ea[14:16]) * 60
                                    + int(ea[17:19])
                                )
                                diff = e_sec - s_sec
                                # Handle midnight crossings
                                if diff < 0 and sa[:10] != ea[:10]:
                                    diff += 86400
                                bucket = duration_bucket(diff)
                                if bucket:
                                    state["duration"][bucket] = (
                                        state["duration"].get(bucket, 0) + 1
                                    )
                            except (ValueError, IndexError):
                                pass

    return totals


# ── MAIN ──────────────────────────────────────────────────────────────

def main() -> None:
    state = load_state(OUT_PATH)
    already_done = set(state["monthly"].keys())

    print(f"Starting. Already processed: {sorted(already_done) or 'none'}")
    grand_total = sum(v["total"] for v in state["monthly"].values())

    with zipfile.ZipFile(ZIP_PATH) as outer:
        for month in MONTHS:
            if month in already_done:
                print(f"  {month}: skip (cached)")
                continue

            print(f"  {month}: processing...", end=" ", flush=True)
            t0 = time.time()

            totals = process_month(outer, month, state)
            state["monthly"][month] = totals
            grand_total += totals["total"]

            elapsed = time.time() - t0
            rate = totals["total"] / elapsed / 1000
            print(f"{totals['total']:>10,} trips  ({elapsed:.1f}s, {rate:.0f}k rows/sec)")

            # Save after each month so progress survives interruption
            save_state(state, OUT_PATH)

    print(f"\nDone. Grand total: {grand_total:,} trips across {len(state['monthly'])} months.")
    print(f"Output written to: {OUT_PATH}")


if __name__ == "__main__":
    main()
