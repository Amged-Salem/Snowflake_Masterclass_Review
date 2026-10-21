# ❄️ Snowflake 
---

##  Table of Contents

1. [Big Picture](#1-big-picture)
2. [Architecture: The 3 Layers](#2-architecture-the-3-layers)
3. [Compute — Virtual Warehouses](#3-compute--virtual-warehouses)
4. [RBAC (Role-Based Access Control)](#4-rbac)
5. [Database / Schema / Table Types](#5-database--schema--table-types)
6. [Views (Regular, Materialized, Secure)](#6-views)
7. [File Formats & Stages](#7-file-formats--stages)
8. [Data Loading Methods](#8-data-loading-methods)
9. [Streams (CDC)](#9-streams-cdc)
10. [Tasks](#10-tasks)
11. [Mini Pipeline — End-to-End Walkthrough](#11-mini-pipeline--end-to-end-walkthrough)
12. [ Time Travel — Deep Dive](#12-time-travel--deep-dive)
13. [ Data Quality Checks](#13-data-quality-checks)
14. [Zero-Copy Cloning](#14-zero-copy-cloning)
15. [Snowpark / Streamlit / Dashboards / ML Forecast](#15-snowpark--streamlit--dashboards--ml-forecast)


---

## 1. Big Picture 

This study covers two parallel tracks:

**Track 1 — Core Concepts:**  
Architecture · RBAC · Tables · Views · File Formats · Stages · Loading Methods · Streams · Tasks · Cloning · Time Travel · Snowpark · Streamlit


---

## 2. Architecture: The 3 Layers

Snowflake separates three independently scalable layers:

### 2.1 Storage Layer
- Data is split into immutable **Micro-partitions** (~50–500 MB raw, ~16 MB compressed on disk).
- Stored in **columnar + compressed** format → reads only the columns selected by a query.
- **Immutable:** UPDATEs and DELETEs write new micro-partitions; old ones are retained. This is the foundation of Time Travel.
- **Metadata** (min/max per column, distinct counts) is stored per partition automatically.
- **Pruning:** When a query has a WHERE filter, Snowflake checks metadata first and skips partitions whose min/max range cannot contain the filter value — no full scan needed.
- **Clustering Key:** For very large tables with random ingestion order, `CLUSTER BY (col)` physically re-orders data so pruning works efficiently again.

### 2.2 Compute Layer (Virtual Warehouses)
- Completely decoupled from storage.
- Multiple warehouses can run **simultaneously on the same data** (analysts, engineers, ML teams — all isolated).
- Scale up (bigger size) or scale out (multi-cluster) independently.

### 2.3 Cloud Services Layer ("the brain")
Manages: metadata · query optimization · security/authentication · transactions · infrastructure management.


---

## 3. Compute — Virtual Warehouses

| Size    | Credits / hour |
|---------|---------------|
| X-Small | 1   |
| Small   | 2   |
| Medium  | 4   |
| Large   | 8   |
| X-Large | 16  |
| 2XL     | 32  |
| 3XL     | 64  |
| 4XL     | 128 |

**Key settings:**
- **Auto-suspend** — warehouse shuts down after X minutes of inactivity (saves cost).
- **Auto-resume** — restarts automatically when a new query arrives.
- **Billing** — per second, but with a **60-second minimum** on every resume.
- **Multi-cluster** (Enterprise+) — min/max clusters; scales out for concurrency (many users), not for a single slow query. Maximum 10 clusters.

> **Scale up vs Scale out:**  
> Scale **up** = larger size → faster heavy queries.  
> Scale **out** = multi-cluster → more concurrent users.

---

## 4. RBAC

**Rule:** Never grant privileges directly to a user. Grant them to a Role, then assign the Role to users.

| Role | Responsibility |
|------|---------------|
| `ACCOUNTADMIN` | Full account control — avoid for daily use |
| `SECURITYADMIN` | Creates users, roles, and grants |
| `SYSADMIN` | Creates databases, schemas, tables, warehouses |
| `USERADMIN` | Creates users and roles only (no grants) |
| `PUBLIC` | Default role every user inherits |

**Example — creating a read-only analyst role:**
```sql
USE ROLE SECURITYADMIN;

CREATE ROLE ANALYST_ROLE;
GRANT USAGE  ON WAREHOUSE COMPUTE_WH                    TO ROLE ANALYST_ROLE;
GRANT USAGE  ON DATABASE  ECOMMERCE_DEMO                TO ROLE ANALYST_ROLE;
GRANT USAGE  ON SCHEMA    ECOMMERCE_DEMO.RAW             TO ROLE ANALYST_ROLE;
GRANT SELECT ON ALL TABLES IN SCHEMA ECOMMERCE_DEMO.RAW  TO ROLE ANALYST_ROLE;
-- For tables created in the future:
GRANT SELECT ON FUTURE TABLES IN SCHEMA ECOMMERCE_DEMO.RAW TO ROLE ANALYST_ROLE;

CREATE USER RAJ PASSWORD='...' DEFAULT_ROLE=ANALYST_ROLE MUST_CHANGE_PASSWORD=FALSE;
GRANT ROLE ANALYST_ROLE TO USER RAJ;
```

**Result:** RAJ can `SELECT` from all tables but gets "Insufficient privileges" on `DROP TABLE`.

---

## 5. Database / Schema / Table Types

**Hierarchy:** `Account → Database → Schema → Objects (Tables, Views, Stages…)`  
**Fully Qualified Name:** `DATABASE.SCHEMA.TABLE`  
**Example:** `ECOMMERCE_DEMO.RAW.SALES_CLEAN`

| Table Type | Lifetime | Time Travel | Fail-safe | Use Case |
|------------|----------|-------------|-----------|----------|
| **Regular (Permanent)** | Until manually dropped | 1–90 days | 7 days | Core / production data |
| **Transient** | Until manually dropped | 0 or 1 day | None | Staging, intermediate, cost-saving |
| **Temporary** | Until session closes | 0 or 1 day | None | Testing, ad-hoc, session-scoped work |

---

## 6. Views

| View Type | Data Stored? | SQL Definition Visible? | Notes |
|-----------|-------------|------------------------|-------|
| **Regular** | No — re-runs query each time | Yes | Compute cost on every access |
| **Materialized** | Yes — result cached & auto-refreshed | Yes | Enterprise+; single source table only; refresh costs credits |
| **Secure** | No — re-runs query | **No** (hidden from non-owners) | Slight optimizer overhead; used for data sharing & row-level security |

```sql
CREATE VIEW total_sales AS
    SELECT SUM(sale) AS total FROM sales;   -- alias required on aggregation

CREATE SECURE VIEW secure_customer_count AS
    SELECT COUNT(customer_id) AS cnt FROM customers;

GRANT SELECT ON VIEW secure_customer_count TO ROLE ANALYST_ROLE;
```

**Key difference from a static snapshot:**  
A `CREATE TABLE AS SELECT` (CTAS) captures data at one moment. A View always reflects the latest state of the source table.

---

## 7. File Formats & Stages

**File Format (tells Snowflake how to parse a file):**
```sql
CREATE OR REPLACE FILE FORMAT SALES_CSV_FORMAT
    TYPE                       = 'CSV'
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    FIELD_DELIMITER            = ','
    SKIP_HEADER                = 1
    NULL_IF                    = ('NULL', 'null', '');
```

| Option | Effect |
|--------|--------|
| `FIELD_OPTIONALLY_ENCLOSED_BY = '"'` | Strip double-quotes; allows commas inside quoted fields |
| `FIELD_DELIMITER = ','` | Column separator character |
| `SKIP_HEADER = 1` | Skip the first row (header line) |
| `NULL_IF = (...)` | Convert these text values to a true SQL NULL |

**Stage types:**
- **Internal** — storage managed by Snowflake (`@~` user stage, `@%` table stage, or Named stage).
- **External** — a pointer to your own cloud bucket (S3 / GCS / Azure Blob). Data stays in your bucket; Snowflake only reads it.

**External Stage (production pattern):**
```sql
CREATE OR REPLACE STAGE GCS_STAGE
    URL                = 'gcs://your-bucket-name'
    STORAGE_INTEGRATION = GCS_INT
    FILE_FORMAT        = SALES_CSV_FORMAT;

LIST @GCS_STAGE;   -- verify files are visible
```

**COPY INTO key options:**

| Option | Behavior |
|--------|----------|
| `ON_ERROR = CONTINUE` | Skip bad rows, load the rest |
| `ON_ERROR = ABORT_STATEMENT` | Stop entire load on first error (default for manual COPY) |
| `VALIDATION_MODE = RETURN_ERRORS` | Dry-run: returns errors without loading any data |
| `FORCE = TRUE` | Re-load a file that Snowflake already processed |

Snowflake tracks loaded files for **64 days** (Load Metadata) to prevent duplicate loads by default.

---

## 8. Data Loading Methods

| Method | Trigger | Latency | Best For |
|--------|---------|---------|----------|
| `COPY INTO` (manual / batch) | Manual or scheduled via Task | Minutes–hours | Historical backfills, daily loads |
| **Snowpipe** (file-based) | Event notification (Pub/Sub / SQS / Event Grid) | Seconds–minutes | Near real-time file drops |
| **Snowpipe Streaming** | SDK push (row-level) | Sub-second | Real-time row-level streaming |
| **Web UI / SnowSQL** | Manual | Immediate | Ad-hoc / small files |

---

## 9. Streams (CDC)

A **Stream** records every net change (INSERT / DELETE / UPDATE representation) on a table **after the stream was created**. It does **not** capture history before creation.

```sql
CREATE STREAM SALES_STREAM ON TABLE SALES_RAW;
```

**Metadata columns added to every stream row:**

| Column | Values | Meaning |
|--------|--------|---------|
| `METADATA$ACTION` | `INSERT` / `DELETE` | Type of DML operation |
| `METADATA$ISUPDATE` | `TRUE` / `FALSE` | Is this row part of an UPDATE pair? |
| `METADATA$ROW_ID` | Unique string | Snowflake internal row identifier |

**How UPDATE is represented — important gotcha:**  
An UPDATE appears as **two rows**:
- One `DELETE` row with `METADATA$ISUPDATE = TRUE` (the old value)
- One `INSERT` row with `METADATA$ISUPDATE = TRUE` (the new value)

`METADATA$ACTION = 'UPDATE'` **never appears** — writing `WHEN ACTION = 'UPDATE'` in a MERGE is dead code.

**Stream is consumed** (cleared) after a DML statement commits within a transaction against it.

```sql
SELECT SYSTEM$STREAM_HAS_DATA('ECOMMERCE_DEMO.RAW.SALES_STREAM');
```

---

## 10. Tasks

A **Task** is a scheduled SQL statement (or stored procedure call).

```sql
CREATE OR REPLACE TASK ECOMMERCE_DEMO.RAW.SALES_MERGE_TASK
    WAREHOUSE = COMPUTE_WH
    SCHEDULE  = '1 minute'
    WHEN SYSTEM$STREAM_HAS_DATA('ECOMMERCE_DEMO.RAW.SALES_STREAM')
AS
    MERGE INTO SALES_CLEAN ... ;

-- REQUIRED — tasks are created in SUSPENDED state:
ALTER TASK ECOMMERCE_DEMO.RAW.SALES_MERGE_TASK RESUME;
```

**Monitor execution:**
```sql
SHOW TASKS IN SCHEMA ECOMMERCE_DEMO.RAW;

SELECT * FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY(
    SCHEDULED_TIME_RANGE_START => DATEADD('hour', -1, CURRENT_TIMESTAMP())
))
ORDER BY SCHEDULED_TIME DESC;

ALTER TASK ECOMMERCE_DEMO.RAW.SALES_MERGE_TASK SUSPEND;
```

---

## 11. Mini Pipeline — End-to-End Walkthrough

### Step 1 — GCS Bucket
Create a GCS bucket with a globally unique name using hyphens (not underscores).
Region must match the Snowflake account region:
```sql
SELECT CURRENT_REGION();
```

### Step 2 — Storage Integration
```sql
CREATE STORAGE INTEGRATION GCS_INT
    TYPE                      = EXTERNAL_STAGE
    STORAGE_PROVIDER          = GCS
    ENABLED                   = TRUE
    STORAGE_ALLOWED_LOCATIONS = ('gcs://your-bucket');
    -- Note: use gcs:// not gs://

DESC STORAGE INTEGRATION GCS_INT;
-- Copy STORAGE_GCP_SERVICE_ACCOUNT → Grant "Storage Object Viewer" in GCP IAM
```

### Step 3 — Pub/Sub Notification Integration
```bash
# GCP Cloud Shell:
gcloud storage buckets notifications create gs://your-bucket \
    --topic=snowflake-notifications \
    --event-types=OBJECT_FINALIZE
```
```sql
CREATE NOTIFICATION INTEGRATION GCS_NOTIFY_INT
    TYPE                         = QUEUE
    ENABLED                      = TRUE
    NOTIFICATION_PROVIDER        = GCP_PUBSUB
    GCP_PUBSUB_SUBSCRIPTION_NAME = 'projects/<project-id>/subscriptions/notify';

DESC NOTIFICATION INTEGRATION GCS_NOTIFY_INT;
-- Copy second service account → Grant "Pub/Sub Subscriber" + "Monitoring Viewer" in GCP IAM
```

> Two different service accounts — both must be granted separately in GCP IAM.

### Step 4 — Stage
```sql
CREATE OR REPLACE STAGE GCS_STAGE
    URL                 = 'gcs://your-bucket'
    STORAGE_INTEGRATION = GCS_INT
    FILE_FORMAT         = SALES_CSV_FORMAT;

LIST @GCS_STAGE;
```

> `NOTIFICATION_INTEGRATION` belongs in the **PIPE**, not the STAGE.

### Step 5 — Raw Table + Pipe
```sql
CREATE TABLE SALES_RAW (DATE_TEXT STRING, SALES_TEXT STRING);

CREATE PIPE SALES_PIPE
    AUTO_INGEST = TRUE
    INTEGRATION = GCS_NOTIFY_INT
AS
    COPY INTO SALES_RAW FROM @GCS_STAGE
    FILE_FORMAT = (FORMAT_NAME = SALES_CSV_FORMAT);

SELECT SYSTEM$PIPE_STATUS('SALES_PIPE');
ALTER PIPE SALES_PIPE REFRESH;   -- for files that existed before pipe creation
```

### Step 6 — Stream
```sql
CREATE STREAM SALES_STREAM ON TABLE SALES_RAW;
```

### Step 7 — Clean Table + Task (production-ready)
```sql
CREATE TABLE SALES_CLEAN (
    SALE_DATE   DATE   NOT NULL,
    SALE_AMOUNT NUMBER NOT NULL,
    PRIMARY KEY (SALE_DATE)   -- informational only, NOT enforced
);

CREATE TABLE SALES_REJECTS (
    DATE_TEXT   STRING,
    SALES_TEXT  STRING,
    REASON      STRING,
    REJECTED_AT TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE TASK SALES_MERGE_TASK
    WAREHOUSE = COMPUTE_WH
    SCHEDULE  = '1 minute'
    WHEN SYSTEM$STREAM_HAS_DATA('SALES_STREAM')
AS
BEGIN
    BEGIN TRANSACTION;

    -- 1) Route invalid rows to quarantine
    INSERT INTO SALES_REJECTS (DATE_TEXT, SALES_TEXT, REASON)
    SELECT DATE_TEXT, SALES_TEXT,
           CASE WHEN TRY_CAST(DATE_TEXT  AS DATE)   IS NULL THEN 'invalid date'
                WHEN TRY_CAST(SALES_TEXT AS NUMBER) IS NULL THEN 'invalid amount'
                ELSE 'non-positive amount' END
    FROM SALES_STREAM
    WHERE METADATA$ACTION = 'INSERT'
      AND (   TRY_CAST(DATE_TEXT  AS DATE)   IS NULL
           OR TRY_CAST(SALES_TEXT AS NUMBER) IS NULL
           OR TRY_CAST(SALES_TEXT AS NUMBER) <= 0);

    -- 2) Merge only valid rows
    MERGE INTO SALES_CLEAN TGT
    USING (
        SELECT TRY_CAST(DATE_TEXT  AS DATE)   AS SALE_DATE,
               TRY_CAST(SALES_TEXT AS NUMBER) AS SALE_AMOUNT,
               METADATA$ACTION   AS ACTION,
               METADATA$ISUPDATE AS IS_UPDATE
        FROM SALES_STREAM
        WHERE TRY_CAST(DATE_TEXT  AS DATE)   IS NOT NULL
          AND TRY_CAST(SALES_TEXT AS NUMBER) > 0
    ) SRC
    ON TGT.SALE_DATE = SRC.SALE_DATE
    WHEN MATCHED AND SRC.ACTION = 'DELETE' AND NOT SRC.IS_UPDATE THEN DELETE
    WHEN MATCHED AND SRC.ACTION = 'INSERT' AND     SRC.IS_UPDATE THEN
        UPDATE SET TGT.SALE_AMOUNT = SRC.SALE_AMOUNT
    WHEN NOT MATCHED AND SRC.ACTION = 'INSERT' THEN
        INSERT (SALE_DATE, SALE_AMOUNT) VALUES (SRC.SALE_DATE, SRC.SALE_AMOUNT);

    COMMIT;
END;

ALTER TASK SALES_MERGE_TASK RESUME;
```

> **Why `TRY_CAST` instead of `CAST`?** — `CAST` throws a runtime error on bad values and kills the entire Task run. `TRY_CAST` returns NULL instead, routing bad rows to the quarantine table.

> **Why does `SALES_CLEAN` have 30 rows and not 59?** — The stream was created after the first file was already loaded. It only captures changes after its creation, so the first 30 rows were invisible to it.

### Pipeline Object Summary

| Object | Role |
|--------|------|
| Storage Integration | IAM authentication to the GCS bucket |
| Notification Integration | Pub/Sub event bridge (triggers Snowpipe) |
| Stage | Pointer to the GCS bucket |
| Pipe | Automated COPY INTO on every new file |
| `SALES_RAW` | Raw landing table (all STRING — Bronze layer) |
| `SALES_STREAM` | CDC tracker on RAW |
| Task (`SALES_MERGE_TASK`) | Scheduled MERGE with validation |
| `SALES_REJECTS` | Quarantine table for invalid rows |
| `SALES_CLEAN` | Typed, analytics-ready table (Silver layer) |

---

## 12.  Time Travel — Deep Dive

### How It Works
Snowflake retains **old versions of micro-partitions** (because they are immutable) for the duration of the **Data Retention Period**. You can query or restore data to any point within that window — no manual backup required.

### Retention Periods by Edition

| Object | Standard Edition | Enterprise+ |
|--------|-----------------|-------------|
| Permanent table / Schema / Database | 0 or 1 day | 0 → 90 days |
| Transient table | 0 or 1 day | 0 or 1 day |
| Temporary table | 0 or 1 day (within session) | 0 or 1 day |

- Default: **1 day** for all editions.
- Setting `0` disables Time Travel entirely.
- Retention inherits from account → database → schema → table (nearest wins).

```sql
SHOW PARAMETERS LIKE 'DATA_RETENTION_TIME_IN_DAYS' IN TABLE SALES_CLEAN;

ALTER TABLE   SALES_CLEAN     SET DATA_RETENTION_TIME_IN_DAYS = 7;
ALTER SCHEMA  RAW             SET DATA_RETENTION_TIME_IN_DAYS = 7;
ALTER DATABASE ECOMMERCE_DEMO SET DATA_RETENTION_TIME_IN_DAYS = 7;
```

### The 3 Time Travel Clauses

| Clause | Syntax | Meaning |
|--------|--------|---------|
| `TIMESTAMP` | `AT (TIMESTAMP => '2025-12-06 10:00:00'::TIMESTAMP_LTZ)` | State at that exact moment |
| `OFFSET` | `AT (OFFSET => -300)` | N seconds ago (negative value) |
| `STATEMENT` | `BEFORE (STATEMENT => '<query_id>')` | State just before that statement ran |

- `AT` is inclusive of changes at that moment.
- `BEFORE` is exclusive (the instant before the statement executed).

> `TIMESTAMP_LTZ` is session-timezone-dependent. Prefer `OFFSET` or `STATEMENT` to avoid timezone bugs.  
> Or set: `ALTER SESSION SET TIMEZONE = 'Africa/Cairo';`

### Use Case 1 — Read Historical Data
```sql
SELECT * FROM SALES_CLEAN AT (OFFSET => -60*10);   -- 10 minutes ago

-- See what rows a specific statement deleted:
SELECT * FROM SALES_CLEAN BEFORE (STATEMENT => '<query_id>')
MINUS
SELECT * FROM SALES_CLEAN;
```

### Use Case 2 — Clone from the Past
```sql
-- By Query ID (most precise):
CREATE TABLE SALES_CLEAN_RESTORE CLONE SALES_CLEAN
    BEFORE (STATEMENT => '<query_id_of_the_DELETE>');

-- By offset:
CREATE TABLE SALES_CLEAN_RESTORE CLONE SALES_CLEAN
    AT (OFFSET => -60*10);

-- Entire schema or database:
CREATE SCHEMA   RAW_RESTORE   CLONE RAW            AT (TIMESTAMP => '...'::TIMESTAMP_LTZ);
CREATE DATABASE ECOM_RESTORE  CLONE ECOMMERCE_DEMO AT (OFFSET => -3600);
```

### Use Case 3 — UNDROP
```sql
UNDROP TABLE    SALES_CLEAN;
UNDROP SCHEMA   RAW;
UNDROP DATABASE ECOMMERCE_DEMO;

SHOW TABLES HISTORY;   -- lists dropped tables still within retention
```
> If a new object with the same name exists, rename it first:  
> `ALTER TABLE new_one RENAME TO new_one_temp;` then `UNDROP TABLE SALES_CLEAN;`

### Use Case 4 — Restore to the Original Table
```sql
-- Option A: SWAP (fastest)
ALTER TABLE SALES_CLEAN SWAP WITH SALES_CLEAN_RESTORE;

-- Option B: INSERT OVERWRITE (in-place, no rename)
INSERT OVERWRITE INTO SALES_CLEAN
    SELECT * FROM SALES_CLEAN BEFORE (STATEMENT => '<query_id>');
```

> Always `ALTER TASK SALES_MERGE_TASK SUSPEND;` before restoring — otherwise the Task overwrites the recovery.  
> Never `CREATE OR REPLACE` on `SALES_RAW` — it breaks the Stream and Pipe references.

### Finding the Query ID
```sql
SELECT QUERY_ID, QUERY_TEXT, START_TIME
FROM TABLE(INFORMATION_SCHEMA.QUERY_HISTORY(RESULT_LIMIT => 50))
WHERE QUERY_TEXT ILIKE 'DELETE FROM SALES_CLEAN%'
ORDER BY START_TIME DESC;

SELECT LAST_QUERY_ID();   -- last query in current session
```

### Fail-Safe
```
[change/DROP] ──► Time Travel (0–90 days, self-service) ──► Fail-safe (7 days, Support only) ──► Gone forever
```
- 7 days, fixed, after Time Travel retention expires. **Permanent tables only.**
- Cannot be queried or recovered by the user — only Snowflake Support can access it.
- Transient / Temporary tables have no Fail-safe.
- Storage cost is charged for both Time Travel and Fail-safe versions.

### Hands-On Exercise
```sql
-- 0) Stop the Task first
ALTER TASK SALES_MERGE_TASK SUSPEND;

-- 1) Mistake #1 — capture the ID immediately
DELETE FROM SALES_CLEAN WHERE SALE_DATE = '2025-12-05';
SET del_id = LAST_QUERY_ID();

-- 2) Mistake #2
INSERT INTO SALES_CLEAN VALUES ('2025-12-06', 3100);

-- 3) Compare — see what was lost
SELECT * FROM SALES_CLEAN BEFORE (STATEMENT => $del_id)
MINUS
SELECT * FROM SALES_CLEAN;

-- 4) Restore in-place
INSERT OVERWRITE INTO SALES_CLEAN
    SELECT * FROM SALES_CLEAN BEFORE (STATEMENT => $del_id);

-- 5) Verify: 30 rows, MAX date = 2025-12-05
SELECT COUNT(*), MAX(SALE_DATE) FROM SALES_CLEAN;

-- 6) Resume
ALTER TASK SALES_MERGE_TASK RESUME;
```

### Important Caveats

| # | Caveat |
|---|--------|
| 1 | Cost scales with **data change volume × retention days**. Use Transient + short retention for high-churn staging tables. |
| 2 | After retention expires → `Time travel data is not available`. No self-service recovery. |
| 3 | Cannot travel back to before the table was created. |
| 4 | `CREATE OR REPLACE TABLE` = DROP + CREATE. Previous history is lost. Avoid on critical tables. |
| 5 | Dropping a Database/Schema applies the Database's retention to all child tables. Drop individual tables first if needed. |
| 6 | An unconsumed stream goes **stale** if the source table's retention expires — must be recreated. |
| 7 | Cloning from a past point requires the retention window to still cover that timestamp. |

---

## 13. Data Quality Checks

### 13.0 What Is Data Quality?

Ensuring data is **correct, complete, and timely** before anyone relies on it for dashboards or decisions.

| Dimension | Question | Example in this project |
|-----------|----------|------------------------|
| **Completeness** | Nulls / missing rows? | `SALE_DATE` or `SALE_AMOUNT` is NULL |
| **Uniqueness** | Duplicates? | Same date loaded twice from two files |
| **Validity** | Values in correct format / range? | `SALES_TEXT = 'abc'` or negative sales |
| **Consistency** | Counts match across layers? | `raw_rows ≠ clean_rows + rejected_rows` |
| **Timeliness / Freshness** | Is data current? | Latest date in `SALES_CLEAN` is stale |
| **Accuracy** | Reflects reality? | Requires an external reference source |

### 13.1 Quality Gates in the Pipeline

```
CSV ──(Gate 1)──► SALES_RAW ──(Gate 2)──► SALES_CLEAN ──(Gate 3)──► Dashboard / ML
      load-time checks       cast/merge checks        ongoing monitoring + alerts
```

### Gate 1 — Load Time
```sql
-- Dry-run validation (no data loaded):
COPY INTO SALES_RAW FROM @GCS_STAGE
FILE_FORMAT = (FORMAT_NAME = SALES_CSV_FORMAT)
VALIDATION_MODE = RETURN_ERRORS;

-- Inspect load history (last 24 hours):
SELECT FILE_NAME, STATUS, ROW_PARSED, ROW_COUNT, ERROR_COUNT, FIRST_ERROR_MESSAGE
FROM TABLE(INFORMATION_SCHEMA.COPY_HISTORY(
    TABLE_NAME => 'SALES_RAW',
    START_TIME => DATEADD(HOUR, -24, CURRENT_TIMESTAMP())))
ORDER BY LAST_LOAD_TIME DESC;

-- Row-level errors from last COPY:
SELECT * FROM TABLE(VALIDATE(SALES_RAW, JOB_ID => '_last'));

-- Pipe health:
SELECT SYSTEM$PIPE_STATUS('SALES_PIPE');
```

### Gate 2 — Transform Time: TRY_CAST + Quarantine

`CAST('abc' AS NUMBER)` throws an error and aborts the entire Task run.  
`TRY_CAST` returns NULL, allowing bad rows to be routed to the quarantine table instead.



### Gate 3 — Ongoing SQL Checks
```sql
-- 1) Reconciliation
SELECT (SELECT COUNT(*) FROM SALES_RAW)     AS raw_rows,
       (SELECT COUNT(*) FROM SALES_CLEAN)   AS clean_rows,
       (SELECT COUNT(*) FROM SALES_REJECTS) AS rejected_rows;

-- 2) Nulls
SELECT COUNT_IF(SALE_DATE   IS NULL) AS null_dates,
       COUNT_IF(SALE_AMOUNT IS NULL) AS null_amounts
FROM SALES_CLEAN;

-- 3) Duplicates on key (must be 0)
SELECT SALE_DATE, COUNT(*) AS cnt
FROM SALES_CLEAN
GROUP BY SALE_DATE
HAVING COUNT(*) > 1;

-- 4) Range / Validity
SELECT * FROM SALES_CLEAN WHERE SALE_AMOUNT <= 0 OR SALE_AMOUNT > 100000;

-- 5) Freshness
SELECT MAX(SALE_DATE)                                  AS last_date,
       DATEDIFF('day', MAX(SALE_DATE), CURRENT_DATE()) AS days_behind
FROM SALES_CLEAN;

-- 6) Date gaps
WITH days AS (
    SELECT DATEADD('day', SEQ4(), (SELECT MIN(SALE_DATE) FROM SALES_CLEAN)) AS dt
    FROM TABLE(GENERATOR(ROWCOUNT => 3650))
)
SELECT dt AS missing_date
FROM days
WHERE dt <= (SELECT MAX(SALE_DATE) FROM SALES_CLEAN)
  AND dt NOT IN (SELECT SALE_DATE FROM SALES_CLEAN)
ORDER BY dt;

-- 7) Pre-cast check on raw
SELECT * FROM SALES_RAW
WHERE TRY_CAST(DATE_TEXT AS DATE) IS NULL
   OR TRY_CAST(SALES_TEXT AS NUMBER) IS NULL;
```

### 13.2 Constraints — Critical Gap

| Constraint | Enforced? |
|-----------|-----------|
| `NOT NULL` | Yes |
| `PRIMARY KEY` / `UNIQUE` / `FOREIGN KEY` | **No** — informational metadata only |

`PRIMARY KEY` does not prevent duplicate inserts on standard tables. Uniqueness must be enforced by MERGE logic or explicit checks.
## 14. Zero-Copy Cloning




```sql
CREATE TABLE SALES_CLEAN_BACKUP CLONE SALES_CLEAN;
```

- **No physical data copy** — new metadata pointing to the same micro-partitions.
- Storage does not double; completes in seconds even for TB tables.
- After cloning both tables are **independent** — writes use copy-on-write semantics.
- Works on tables, schemas, and entire databases.
- Can clone **from a past point** (clone + Time Travel) — the exact mechanism behind the restore pattern.

---

## 15. Snowpark / Streamlit / Dashboards / ML Forecast

### Snowpark
Write Python (or Java / Scala) inside Snowflake for transformations or ML without moving data out. Code is transpiled to SQL and runs inside Snowflake.

### Streamlit
Build interactive apps and dashboards in Snowflake using Python (Create → Streamlit app).

### Dashboard (Snowsight)
```sql
SELECT SALE_DATE, SALE_AMOUNT FROM SALES_CLEAN;   -- Line chart tile
SELECT SUM(SALE_AMOUNT) FROM SALES_CLEAN;         -- KPI Scorecard tile
```
Chart types: Line · Bar · Scatter · Heat grid · Scorecard

### ML Forecasting
```sql
CREATE SNOWFLAKE.ML.FORECAST SALES_FORECAST_V2(
    INPUT_DATA        => SYSTEM$REFERENCE('TABLE', 'ECOMMERCE_DEMO.RAW.SALES_CLEAN'),
    TIMESTAMP_COLNAME => 'SALE_DATE',
    TARGET_COLNAME    => 'SALE_AMOUNT'
);

CALL SALES_FORECAST_V2!FORECAST(FORECASTING_PERIODS => 14);
-- Returns 14 rows: forecast + lower_bound + upper_bound
```

---

---

