# NYC Taxi → PostgreSQL (Docker) Ingestion with Python

Practical data-engineering project that spins up PostgreSQL in Docker, then ingests NYC Yellow Taxi CSV data in chunks via pandas + SQLAlchemy. It demonstrates containerized databases, schema creation, chunked ingest, and basic validation queries—clean, reproducible, and easy to extend.

## Highlights

* Reproducible Postgres setup using Docker
* Persistent storage via bind mount or named volume
* Chunked CSV ingestion using pandas + SQLAlchemy (handles large files)
* Minimal DDL tailored for Yellow Taxi data
* Clear verification steps (row counts, sample queries)

## Tech Stack

* Docker Desktop + PostgreSQL 13
* Python 3.12, pandas, SQLAlchemy, psycopg (v3)
* Optional: pgcli for a better Postgres CLI experience

## Project Structure

```
dataengineering/
└─ nyc-taxi-postgres-ingestion/
   ├─ README.md
   ├─ ingest_yellow_taxi.py        # chunked CSV → Postgres
   └─ notebooks/
      └─ exploration.ipynb         # optional (EDA, ad-hoc SQL)
```

## Prerequisites

* Docker Desktop running
* Python 3.12 environment
* (Windows) If you use a host-path volume, ensure the drive is shared in Docker Desktop: Settings → Resources → File Sharing

## Setup

### 1) Start PostgreSQL (choose one)

Option A — bind mount (Windows Git Bash path shown; adjust for your OS):

```bash
mkdir -p /d/data/ny_taxi_pgdata

docker run -d --name pg13 \
  -e POSTGRES_USER=root \
  -e POSTGRES_PASSWORD=root \
  -e POSTGRES_DB=ny_taxi \
  -v /d/data/ny_taxi_pgdata:/var/lib/postgresql/data \
  -p 5432:5432 \
  postgres:13

docker logs -f pg13  # wait for "ready to accept connections", Ctrl+C to stop
```

Option B — named volume (no file-sharing needed):

```bash
docker run -d --name pg13 \
  -e POSTGRES_USER=root \
  -e POSTGRES_PASSWORD=root \
  -e POSTGRES_DB=ny_taxi \
  -v pgdata:/var/lib/postgresql/data \
  -p 5432:5432 \
  postgres:13
```

### 2) Python environment

```bash
python -m venv .venv
# Windows: .venv\Scripts\activate
# macOS/Linux: source .venv/bin/activate

pip install -U pip
pip install "psycopg[binary]" sqlalchemy pandas
# optional: pip install pgcli jupyter
```

## Ingestion Script

Create `ingest_yellow_taxi.py` alongside this README:

```python
from time import time
import pandas as pd
from sqlalchemy import create_engine, text

# 1) Engine (psycopg v3)
engine = create_engine("postgresql+psycopg://root:root@localhost:5432/ny_taxi")

# 2) Minimal DDL (adjust columns as needed for your CSV)
DDL = """
CREATE TABLE IF NOT EXISTS yellow_taxi_data (
  vendor_id               int,
  tpep_pickup_datetime    timestamp,
  tpep_dropoff_datetime   timestamp,
  passenger_count         int,
  trip_distance           numeric,
  ratecode_id             int,
  store_and_fwd_flag      text,
  pu_location_id          int,
  do_location_id          int,
  payment_type            int,
  fare_amount             numeric,
  extra                   numeric,
  mta_tax                 numeric,
  tip_amount              numeric,
  tolls_amount            numeric,
  improvement_surcharge   numeric,
  total_amount            numeric,
  congestion_surcharge    numeric
);
"""
with engine.begin() as conn:
    conn.execute(text(DDL))

# 3) CSV → DataFrame iterator (update path & chunksize)
CSV_PATH = r"D:\data\yellow_tripdata_2024-01.csv"
chunksize = 100_000
df_iter = pd.read_csv(CSV_PATH, iterator=True, chunksize=chunksize)

rows_total = 0
while True:
    try:
        t0 = time()
        df = next(df_iter)

        # cast datetimes
        if "tpep_pickup_datetime" in df.columns:
            df["tpep_pickup_datetime"] = pd.to_datetime(df["tpep_pickup_datetime"])
        if "tpep_dropoff_datetime" in df.columns:
            df["tpep_dropoff_datetime"] = pd.to_datetime(df["tpep_dropoff_datetime"])

        # append chunk
        df.to_sql("yellow_taxi_data", engine, if_exists="append", index=False)
        rows_total += len(df)
        print(f"Inserted {len(df):,} rows in {time() - t0:.2f}s (total {rows_total:,})")
    except StopIteration:
        print(f"Done. Total inserted: {rows_total:,}")
        break
```

Run it:

```bash
python ingest_yellow_taxi.py
```

## Verify

Using pgcli or psql:

```sql
-- connect: pgcli postgresql://root:root@localhost:5432/ny_taxi

\d+ yellow_taxi_data
SELECT COUNT(*) FROM yellow_taxi_data;
SELECT * FROM yellow_taxi_data LIMIT 5;

-- sample sanity checks
SELECT
  date_trunc('day', tpep_pickup_datetime) AS day,
  COUNT(*) AS trips
FROM yellow_taxi_data
GROUP BY 1
ORDER BY 1
LIMIT 10;
```

## Troubleshooting

* Cannot connect from Python
  Install driver and use the correct URL:

  ```
  pip install "psycopg[binary]" sqlalchemy
  engine = create_engine("postgresql+psycopg://root:root@localhost:5432/ny_taxi")
  ```

* `NameError: name 'time' is not defined`
  Make sure the script has `from time import time`.

* Bind-mount errors on Windows
  Ensure the drive is shared in Docker Desktop (Settings → Resources → File Sharing), or use the **named volume** option.

* Port already in use
  Map another port, e.g. `-p 6432:5432` and connect to `localhost:6432`.



