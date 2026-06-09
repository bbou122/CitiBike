# NYC Citi Bike 2023 — Full-Year Ridership Analysis

**35,106,986 trips · 12 months · 1 self-contained HTML dashboard**

A complete analysis of New York City's Citi Bike bike-share system across all of 2023 — built by processing the raw trip data in a streaming pipeline, aggregating on the fly, and presenting findings in a fully offline, dependency-free dashboard.

🔗 **[View the live dashboard →](https://bbou122.github.io/CitiBike/)**

---

## Overview

Citi Bike publishes monthly trip-level CSVs that together represent one of the largest public urban mobility datasets in the US. This project answers a set of core questions:

- When do New Yorkers ride — and does that pattern reveal commuter vs. leisure behavior?
- How has electric bike adoption changed across the year?
- Which stations function as origins vs. destinations, and why?
- What does the membership base look like at scale?

The result is a single-file interactive dashboard with seven chart sections, tooltips, and written analysis — no server, no frameworks, no build step required.

---

## Dataset

| Detail | Value |
|---|---|
| Source | [Citi Bike System Data](https://citibikenyc.com/system-data) |
| Period | January 2023 – December 2023 |
| Raw rows | 35,106,986 trip records |
| Raw size | ~6.8 GB uncompressed |
| Format | Monthly CSVs nested inside ZIP archives |

---

## Methodology

The raw dataset is a ZIP of ZIPs — 12 monthly archives, each containing one or more CSV files totaling ~740 MB at the largest. Extracting everything to disk isn't practical at this scale, so I built a streaming aggregation pipeline:

```
outer .zip
  └── 202301-citibike-tripdata.zip  (83 MB compressed)
        └── 202301-citibike-tripdata_1.csv
        └── 202301-citibike-tripdata_2.csv
  └── 202302-citibike-tripdata.zip
  ...
```

**Processing approach:**
- Each inner ZIP is read into memory as a `BytesIO` buffer (never written to disk)
- The CSV stream is parsed row-by-row using Python's `csv` module with direct string slicing for timestamp fields — avoiding `datetime.strptime` overhead at scale
- Counters are incremented in-place into a running state dictionary
- State is serialized to a small JSON file after each month, enabling safe resumption
- Final disk footprint of aggregated results: **~4 MB**

**Aggregations computed:**
- Monthly totals by rider type (`member` / `casual`) and bike type (`classic` / `electric`)
- Hour-of-day trip distribution (0–23)
- Day-of-week trip distribution (Mon–Sun)
- Trip duration buckets (0–5 min through 60+)
- Per-station departure and arrival counts across all ~1,800 active stations

---

## Key Findings

**Seasonality**
Ridership swells 2.3× from the February low (1.70M) to the August peak (3.96M). The May–October window accounts for 63% of all annual trips despite being only half the calendar year.

**Commuter-first network**
The dual rush-hour signature — 8am (2.1M trips) and 5pm (3.2M trips) — combined with Wednesday and Thursday ranking as the two highest-volume days of the year confirms that the daily commute, not tourism, is the structural backbone of the system.

**Membership loyalty at scale**
81.2% of all trips were made by annual members, and this rate holds up even during July and August when casual tourist demand peaks. The subscription base is deep and habitual.

**Electric inflection point**
Electric bikes entered 2023 at exact parity with classic bikes (50.2% in January) and closed the year at 62% of all rides in December — a consistent month-over-month gain driven by fleet expansion and strong rider preference for e-assist on longer or hillier routes.

**Short-trip, last-mile utility**
67% of rides are completed in under 15 minutes. Citi Bike functions primarily as a last-mile connector — bridging subway stops, office blocks, and residential streets — rather than a recreational vehicle.

**Busiest station: W 21 St & 6 Ave**
139,932 departures and 140,324 arrivals — nearly perfectly balanced, suggesting a through-traffic hub rather than a pure origin or destination.

---

## Dashboard Sections

| Section | What it shows |
|---|---|
| Monthly Volume | Stacked bars — member vs. casual trips by month |
| Hour of Day | Area chart with 5pm peak annotation |
| Day of Week | Bar chart highlighting weekday vs. weekend split |
| Trip Duration | Horizontal histogram — 8 duration buckets |
| Member vs. Casual | Donut — 81.2% / 18.8% split |
| Classic vs. Electric | Donut — 49.9% / 50.1% split |
| Electric Trend | Line chart — monthly electric share Jan → Dec |
| Top 10 Stations | Ranked bar lists for departures and arrivals |
| Key Findings | Written analysis — 6 insight cards |

---

## Tech Stack

- **Python 3** — streaming aggregation pipeline (`zipfile`, `csv`, `json`)
- **Vanilla HTML/CSS/JS** — dashboard with inline SVG charts
- **No external dependencies** — fully offline, opens in any browser

---

## How to View

Clone or download the repo and open `citibike-2023.html` directly in a browser. No web server or installation needed.

```bash
git clone https://github.com/bradenbourg/citibike-2023
open citibike-2023.html
```

Or visit the live GitHub Pages version: **[bradenbourg.github.io/citibike-2023](https://bradenbourg.github.io/citibike-2023)**

---

## About

**Braden Bourg** — data analyst with a focus on large-scale public datasets, urban systems, and clear visual communication.

[braden.bourg@gmail.com](mailto:braden.bourg@gmail.com) · [GitHub](https://github.com/bradenbourg)
