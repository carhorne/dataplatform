# Product Requirements Document: Web Analytics Prefect Ingestion Flow

---

## 1. Problem Statement

Adventure Works has no visibility into customer browsing behavior on its e-commerce website. Web analytics data (page views, clicks, add-to-cart events, and purchases) exists in a REST API but is not integrated into the Snowflake data warehouse, meaning analysts cannot connect browsing patterns to purchasing behavior. Without this data, the warehouse only tells us what customers bought — not how they got there.

---

## 2. Desired Outcome

A Prefect 2.0 flow that runs on a configurable schedule inside the existing Docker Compose environment. On each run, the flow pulls web analytics clickstream events from the REST API using an incremental watermark strategy (only fetching events newer than the last run), cleans and validates the data, stages it in a Snowflake internal stage, loads it into RAW_EXT.web_analytics_raw using COPY INTO, removes staged files after a successful load, and logs summary statistics. When complete, the raw table is available as a dbt source for downstream staging and intermediate models.

---

## 3. Acceptance Criteria

- [ ] Flow connects to https://is566-web-analytics-api.fly.dev/analytics/clickstream and retrieves a JSON array of events
- [ ] On first run (no watermark), flow fetches the default 60-minute window; on subsequent runs, passes the last-seen timestamp as the `since` parameter
- [ ] Watermark is derived from the max `timestamp` value in the fetched data (not system clock), and persisted so the next run uses it
- [ ] Data is cleaned: `timestamp` renamed to `event_timestamp` and cast to TIMESTAMP_NTZ, `customer_id` and `product_id` confirmed as integers, nulls in required fields logged and dropped, exact duplicates removed
- [ ] Cleaned data is written to a CSV with a header row and PUT to Snowflake internal stage `@RAW_EXT.WEB_ANALYTICS_STAGE`
- [ ] COPY INTO loads staged CSV into RAW_EXT.web_analytics_raw with FILE_FORMAT = (TYPE='CSV', SKIP_HEADER=1)
- [ ] Staged files are removed after a successful COPY INTO; if COPY INTO fails, files are NOT removed (so they can be retried next cycle)
- [ ] Flow handles HTTP errors (timeouts, 429 rate limits, 5xx) with retries and exponential backoff
- [ ] Flow logs: records fetched, records after cleaning, records loaded, execution time
- [ ] Flow runs end-to-end without manual intervention when started via Docker Compose
- [ ] Prefect UI at localhost:4200 shows flow run history with pass/fail status

---

## 4. Technical Constraints

- **Orchestration framework:** Prefect 2.0+, using `@task` and `@flow` decorators
- **Target warehouse:** Snowflake — all credentials via environment variables from the project `.env` file (SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER, SNOWFLAKE_PASSWORD, SNOWFLAKE_DATABASE, SNOWFLAKE_SCHEMA, SNOWFLAKE_WAREHOUSE, SNOWFLAKE_ROLE)
- **API base URL:** Read from `API_BASE_URL` environment variable (value: https://is566-web-analytics-api.fly.dev)
- **Loading pattern:** CSV → PUT to `@RAW_EXT.WEB_ANALYTICS_STAGE` → COPY INTO `RAW_EXT.web_analytics_raw` → REMOVE stage
- **Containerization:** Flow must run inside Docker, as the `web-analytics-flow` service defined in the existing `compose.yml`. It connects to `prefect-server` using `PREFECT_API_URL=http://prefect-server:4200/api`
- **Scheduling:** Configurable via `FLOW_SCHEDULE_MINUTES` environment variable
- **Snowflake connection:** Use the `snowflake-connector-python` library directly (same pattern as the existing processor), NOT `snowflake-sqlalchemy`
- **Error handling:** All API calls and Snowflake operations must be wrapped in try/except with meaningful log output; no silent failures
- **File location:** Flow code lives at `prefect/flows/web_analytics_flow.py`; dependencies defined in `prefect/pyproject.toml`

---

## 5. Data Schema

### API Response Schema

| Field | Type | Description | Nullable? |
|-------|------|-------------|-----------|
| customer_id | int | Adventure Works customer ID, range 11000–30118. Joins to stg_adventure_db__customers. | No |
| product_id | int | Adventure Works product ID, range 707–999. Joins to stg_adventure_db__products. | No |
| session_id | string | Unique browsing session ID, format: "sess_" + 12 hex chars | No |
| page_url | string | Full URL of the page the customer interacted with | No |
| event_type | string | One of: page_view, click, add_to_cart, purchase | No |
| timestamp | string | ISO 8601 UTC datetime (e.g. "2026-04-13T18:49:49.157758Z"). Rename to event_timestamp on load. | No |

### Target Table Schema (Snowflake: RAW_EXT.web_analytics_raw)

| Column | Type | Source |
|--------|------|--------|
| customer_id | INT NOT NULL | API field `customer_id` |
| product_id | INT NOT NULL | API field `product_id` |
| session_id | VARCHAR(255) NOT NULL | API field `session_id` |
| page_url | VARCHAR(1000) | API field `page_url` |
| event_type | VARCHAR(50) | API field `event_type` |
| event_timestamp | TIMESTAMP_NTZ NOT NULL | API field `timestamp`, renamed and cast |
| _loaded_at | TIMESTAMP_NTZ | Auto-populated by Snowflake DEFAULT CURRENT_TIMESTAMP() |
| _file_name | VARCHAR(255) | Populated by COPY INTO metadata |

---

## 6. Testing Requirements

- [ ] Flow imports cleanly: `python -c "from flows.web_analytics_flow import web_analytics_flow; print('OK')"`
- [ ] Local run succeeds: `uv run python -m flows.web_analytics_flow` fetches events and loads rows
- [ ] Empty API response (no new events since last watermark) is handled gracefully — flow completes without error and logs "0 new events"
- [ ] Null values in customer_id or product_id are logged and the affected rows are dropped before staging
- [ ] Exact duplicate rows (same customer_id + session_id + event_type + timestamp) are deduplicated before loading
- [ ] After a successful run, querying `SELECT COUNT(*) FROM RAW_EXT.web_analytics_raw` returns a non-zero count
- [ ] After a successful run, `LIST @RAW_EXT.WEB_ANALYTICS_STAGE` returns no files (stage was cleaned)
- [ ] Docker run succeeds: flow appears in Prefect UI at localhost:4200 with status "Completed"

---

## 7. Out of Scope

- Do not build dbt staging or intermediate models — those will be built separately in Task 3
- Do not modify any existing dbt models, sources.yml files, or the models-m1 directory
- Do not build a Snowsight dashboard for web analytics data
- Do not implement authentication or API keys — the API is public
- Do not handle pagination — the API returns all results for the time window in a single response
- Do not modify the existing processor (extract.py, load.py, main.py) or any Milestone 1 code

---

## 8. Questions and Assumptions

- **Assumption:** The API does not paginate — a single GET returns all events for the requested time window in one JSON array
- **Assumption:** The `since` parameter accepts any ISO 8601 UTC string and the API filters server-side
- **Assumption:** Rate limiting (HTTP 429) is unlikely but should be handled with a retry using the Retry-After header if present, otherwise exponential backoff
- **Assumption:** The watermark (last-seen timestamp) will be stored in a local file or a Prefect variable between runs, since there is no persistent database available to the flow container beyond Snowflake itself
- **Assumption:** `customer_id` values outside 11000–30118 may appear and will not match the customer dimension; these rows are kept in the raw table (foreign key mismatches are caught by dbt tests downstream, not here)
- **Assumption:** The flow runs as a Prefect deployment registered with the `prefect-server` container, with a worker picking up scheduled runs from the `prefect-worker` container
- **Question resolved:** `_loaded_at` and `_file_name` do not need to be written by the flow — `_loaded_at` uses Snowflake's DEFAULT and `_file_name` is populated automatically by COPY INTO metadata; the CSV only needs the six API fields