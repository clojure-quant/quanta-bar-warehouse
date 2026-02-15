

# Physically sort the table by asset, date (biggest win)

Rebuild the table ordered by (asset, date) 
Rebuild once into a clustered copy:

PRAGMA threads=8;

CREATE TABLE prices2 AS
SELECT asset, date, close, volume
FROM prices
ORDER BY asset, date;

CREATE TABLE prices_sorted AS
SELECT * FROM prices
ORDER BY asset, date;


Then swap names:

ALTER TABLE prices RENAME TO prices_old;
ALTER TABLE prices2 RENAME TO prices;


Why: DuckDB can skip huge ranges when chunks are ordered along your filter columns.

(or (asset_id, date) if you convert). 
That’s the most direct improvement for asset + date range queries on 1B rows in DuckDB.

# Use Parquet partitioning (often the biggest win at this scale)
Partition by asset using Parquet 
Even if you “only use DuckDB”, DuckDB can still store the data as Parquet and query it. With 20k assets, partition-by-asset is realistic and often even faster for your filter pattern.

If your data can live as Parquet files (DuckDB queries Parquet extremely well), do this:

Partition by asset (usually) and keep files internally sorted by date.

Keep reasonable file sizes (not millions of tiny files).

Typical layout:

data/prices/asset=SPY/part-*.parquet
data/prices/asset=AAPL/part-*.parquet
...

Then query like:

SELECT *
FROM read_parquet('data/prices/**/*.parquet')
WHERE asset='AAPL' AND date BETWEEN '2024-01-01' AND '2024-06-01';


# Keep inserts fast with a delta table + periodic repack
Instead of constantly destroying clustering by appending into the big table:
prices_main (sorted)
prices_delta (new rows appended)
Query through a view:

CREATE VIEW prices_all AS
SELECT * FROM prices_main
UNION ALL
SELECT * FROM prices_delta;

Periodically (daily/weekly), repack:

CREATE TABLE prices_main2 AS
SELECT * FROM prices_all
ORDER BY asset, date;

DROP TABLE prices_main;
ALTER TABLE prices_main2 RENAME TO prices_main;
TRUNCATE prices_delta;

# UNIQUE ASSET/BAR 

CREATE TABLE bars (
  asset_id INTEGER,
  ts TIMESTAMP,
  open DOUBLE,
  high DOUBLE,
  low DOUBLE,
  close DOUBLE,
  volume BIGINT,
  PRIMARY KEY(asset_id, ts)
);

PRIMARY KEY(asset_id, ts)
+ INSERT … ON CONFLICT DO UPDATE

# INSERT FORGETS DUPLICATES; keep old

INSERT INTO bars VALUES (...)
ON CONFLICT(asset_id, ts) DO NOTHING;
If a row with the same key exists, the insert is skipped.

# INSERT keeps new (upsert)

INSERT INTO bars AS b
VALUES (...)
ON CONFLICT(asset_id, ts) DO UPDATE SET
  open   = excluded.open,
  high   = excluded.high,
  low    = excluded.low,
  close  = excluded.close,
  volume = excluded.volume;

 # A simple tuning starting point (works well in practice)

If your workload is many asset-by-asset computations (signals/backtests), start here:
pool size = #cores
per connection: PRAGMA threads=1;
data layout:
clustered by (asset_id, ts)
ingest:
appender → staging → merge
This combination is “boring” but very hard to beat.

# Ingest strategy (this is usually where people lose 10×)
Best practice

Batch inserts (big chunks) instead of row-by-row.

Use the C API’s appender if possible (it’s designed for high-throughput ingest).

Land raw data into a staging table, then MERGE/upsert into the main clustered table.

Practical pattern

Append into bars_staging fast

MERGE into bars

Truncate staging

This keeps your main table clean and reduces fragmentation.

# index ??

;; CREATE INDEX s_idx ON films (revenue);