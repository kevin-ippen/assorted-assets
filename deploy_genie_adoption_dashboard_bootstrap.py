# Databricks notebook source
# DBTITLE 1,Setup Guide
# MAGIC %md
# MAGIC # Genie Adoption Dashboard — Setup & Deploy
# MAGIC
# MAGIC This notebook deploys a semantic layer + AI/BI dashboard for Genie Adoption analytics (Genie Code and Genie Spaces). It requires access to `system.access.assistant_events`, `system.access.audit`, and `system.access.workspaces_latest`.
# MAGIC
# MAGIC ## Architecture
# MAGIC
# MAGIC The dashboard reads from **metric views** backed by **pre-computed Delta tables**:
# MAGIC
# MAGIC ```
# MAGIC system.access.assistant_events ──▶ _stg_genie_code_sessions (Delta) ──▶ mv_genie_code_usage (metric view)
# MAGIC system.access.audit            ──▶ _stg_genie_spaces_questions (Delta) ─▶ mv_genie_spaces_usage (metric view)
# MAGIC ```
# MAGIC
# MAGIC This design ensures fast dashboard queries (no window functions recomputed per request). Schedule this notebook daily to keep the tables fresh.
# MAGIC
# MAGIC ## Step 1 — Set your parameters (Cell 2)
# MAGIC
# MAGIC | Widget | What to set | Default |
# MAGIC |---|---|---|
# MAGIC | `catalog` | Your Unity Catalog catalog name | `main` |
# MAGIC | `schema` | Schema to create objects in (created if absent) | `genie_analytics` |
# MAGIC | `warehouse_id` | SQL warehouse ID for the dashboard (leave **blank** to auto-select) | _(auto)_ |
# MAGIC | `dashboard_name` | Name for the deployed AI/BI dashboard | `Genie Adoption & Cost Estimation` |
# MAGIC
# MAGIC ## Step 2 — Run All Cells (in order)
# MAGIC
# MAGIC Cell 3 (pre-flight) confirms catalog access before any objects are created.
# MAGIC
# MAGIC ## What gets deployed
# MAGIC
# MAGIC | Object | Type | Description |
# MAGIC |---|---|---|
# MAGIC | `_stg_genie_code_sessions` | Delta Table | Pre-computed sessions (30-min idle-gap sessionization) |
# MAGIC | `_stg_genie_spaces_questions` | Delta Table | Pre-computed question events (Spaces + One + Mobile) |
# MAGIC | `mv_genie_code_usage` | Metric View | Reads from staging table — fast aggregations |
# MAGIC | `mv_genie_spaces_usage` | Metric View | Reads from staging table — fast aggregations |
# MAGIC | _(dashboard name)_ | AI/BI Dashboard | Pre-built adoption & cost estimation dashboard |
# MAGIC
# MAGIC ## Refresh
# MAGIC
# MAGIC Re-run cells 5–6 to refresh the staging tables (or schedule this notebook as a daily job).

# COMMAND ----------

# DBTITLE 1,Cell 2
dbutils.widgets.removeAll()
dbutils.widgets.text("catalog",        "main",                            "Catalog")
dbutils.widgets.text("schema",         "genie_analytics",                 "Schema")
dbutils.widgets.text("warehouse_id",   "",                                "Warehouse ID (leave blank to auto-select)")
dbutils.widgets.text("dashboard_name", "Genie Adoption & Cost Estimation", "Dashboard Name")

# COMMAND ----------

# DBTITLE 1,Pre-flight Check
from databricks.sdk import WorkspaceClient

w  = WorkspaceClient()
me = w.current_user.me().user_name

catalog        = dbutils.widgets.get("catalog")
schema         = dbutils.widgets.get("schema")
warehouse_id   = dbutils.widgets.get("warehouse_id").strip()
dashboard_name = dbutils.widgets.get("dashboard_name")

# --- Warehouse resolution ---
if not warehouse_id:
    warehouses = list(w.warehouses.list())
    if not warehouses:
        raise ValueError("No SQL warehouses found. Create one or set the warehouse_id widget.")
    warehouse_id = warehouses[0].id
    wh_name = warehouses[0].name
    print(f"[warehouse]  auto-selected: {wh_name} ({warehouse_id})")
else:
    wh = w.warehouses.get(warehouse_id)
    print(f"[warehouse]  {wh.name} ({warehouse_id})")

# --- Catalog access ---
try:
    spark.sql(f"SHOW SCHEMAS IN `{catalog}`").limit(1).collect()
    print(f"[catalog]    OK  → {catalog}")
except Exception as e:
    print(f"[catalog]    FAIL → {catalog}")
    print(f"             {e}")
    print("  Update the 'catalog' widget to a catalog you have USE CATALOG access to.")
    raise

# --- system.access tables ---
required = [
    "system.access.assistant_events",
    "system.access.audit",
    "system.access.workspaces_latest",
]
for tbl in required:
    try:
        spark.sql(f"SELECT 1 FROM {tbl} LIMIT 1").collect()
        print(f"[system]     OK  → {tbl}")
    except Exception as e:
        print(f"[system]     FAIL → {tbl} ({e})")

print()
print("=" * 60)
print(f"  Deploying to : {catalog}.{schema}")
print(f"  Dashboard    : {dashboard_name}")
print(f"  Warehouse    : {warehouse_id}")
print(f"  As user      : {me}")
print("=" * 60)
print("Pre-flight complete. Proceed to run cells 4-11.")


# COMMAND ----------

# DBTITLE 1,Cell 3
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema} COMMENT 'Semantic layer for Genie adoption dashboards.'")
print(f"Schema: {catalog}.{schema}")


# COMMAND ----------

# DBTITLE 1,Cell 4
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
spark.sql(f"DROP VIEW IF EXISTS {catalog}.{schema}.v_genie_code_sessions")
spark.sql(f"DROP VIEW IF EXISTS {catalog}.{schema}.v_genie_spaces_questions")
print("Dropped intermediate views (if they existed)")


# COMMAND ----------

# DBTITLE 1,Cell 5
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

# ─── Step 1: Materialize sessionized data into a Delta table ───────────────────
# This eliminates recomputing window functions (LAG/LEAD/SUM OVER) on every
# dashboard query. The metric view below reads from this table instead.
print("Materializing Genie Code sessions...")
spark.sql(f"""
CREATE OR REPLACE TABLE {catalog}.{schema}._stg_genie_code_sessions
COMMENT 'Pre-computed Genie Code sessions (30-min idle-gap). Refresh via this notebook or a scheduled job.'
AS
WITH raw AS (
  SELECT
    e.account_id,
    e.workspace_id,
    e.event_time,
    e.event_date,
    DATE_TRUNC('MONTH', e.event_time) AS usage_month,
    e.initiated_by AS user_email,
    (unix_timestamp(e.event_time) - unix_timestamp(
      LAG(e.event_time) OVER (PARTITION BY e.workspace_id, e.initiated_by ORDER BY e.event_time)
    )) / 60.0 AS mins_since_prev,
    (unix_timestamp(
      LEAD(e.event_time) OVER (PARTITION BY e.workspace_id, e.initiated_by ORDER BY e.event_time)
    ) - unix_timestamp(e.event_time)) / 60.0 AS mins_to_next
  FROM system.access.assistant_events e
  WHERE e.initiated_by IS NOT NULL
    AND e.event_date >= dateadd(MONTH, -13, current_date())
),
with_boundary AS (
  SELECT *,
    CASE WHEN mins_since_prev IS NULL OR mins_since_prev >= 30 THEN 1 ELSE 0 END AS is_session_start
  FROM raw
),
with_session_num AS (
  SELECT *,
    SUM(is_session_start) OVER (
      PARTITION BY workspace_id, user_email
      ORDER BY event_time
      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS session_num
  FROM with_boundary
),
with_session_id AS (
  SELECT *,
    CONCAT(workspace_id, '|', user_email, '|', CAST(session_num AS STRING)) AS session_id,
    CASE WHEN mins_to_next IS NOT NULL AND mins_to_next < 30 THEN mins_to_next ELSE 0.0 END AS engaged_contribution
  FROM with_session_num
),
sessionized AS (
  SELECT
    account_id, workspace_id, user_email, usage_month, session_id,
    MIN(event_date) AS event_date,
    COUNT(*) AS messages_in_session,
    SUM(engaged_contribution) AS session_engaged_minutes,
    (unix_timestamp(MAX(event_time)) - unix_timestamp(MIN(event_time))) / 60.0 AS session_elapsed_minutes
  FROM with_session_id
  GROUP BY account_id, workspace_id, user_email, usage_month, session_id
),
user_month_sessions AS (
  SELECT workspace_id, user_email, usage_month, COUNT(*) AS user_monthly_session_count
  FROM sessionized
  GROUP BY workspace_id, user_email, usage_month
),
ws AS (
  SELECT workspace_id, MAX(workspace_name) AS workspace_name
  FROM system.access.workspaces_latest GROUP BY workspace_id
)
SELECT s.*, u.user_monthly_session_count, w.workspace_name
FROM sessionized s
JOIN user_month_sessions u
  ON s.workspace_id = u.workspace_id AND s.user_email = u.user_email AND s.usage_month = u.usage_month
LEFT JOIN ws w ON s.workspace_id = w.workspace_id
""")
row_count = spark.sql(f"SELECT COUNT(*) FROM {catalog}.{schema}._stg_genie_code_sessions").collect()[0][0]
print(f"  → {row_count:,} sessions materialized")

# ─── Step 2: Create metric view on top of the table ────────────────────────────
# Source is now a simple table read — no window functions at query time.
print("Creating metric view...")
spark.sql(f"""
CREATE OR REPLACE VIEW {catalog}.{schema}.mv_genie_code_usage
WITH METRICS LANGUAGE YAML AS
$$
version: 1.1

source: "SELECT * FROM {catalog}.{schema}._stg_genie_code_sessions"

comment: "Genie Code adoption metrics. Grain: one row per inferred session. Sessions\
  \\ inferred by 30-min idle gap per workspace+user. Backed by a pre-computed Delta\
  \\ table for fast dashboard queries."

dimensions:
  - name: usage_month
    expr: usage_month
    comment: Calendar month of the session
    display_name: Month
    format:
      type: date
      date_format: locale_short_month
  - name: event_date
    expr: event_date
    comment: Session start date
    display_name: Date
  - name: workspace_id
    expr: workspace_id
    comment: Databricks workspace identifier
    display_name: Workspace ID
  - name: workspace_name
    expr: workspace_name
    comment: Workspace name
    display_name: Workspace
  - name: user_email
    expr: user_email
    comment: User who initiated the session
    display_name: User Email
  - name: session_id
    expr: session_id
    comment: "Inferred session ID: workspace_id|user_email|session_num"
    display_name: Session ID
  - name: account_id
    expr: account_id
    comment: Databricks account identifier
    display_name: Account ID
  - name: user_monthly_session_count
    expr: user_monthly_session_count
    comment: Sessions this user had in this calendar month
    display_name: User Monthly Sessions
    format:
      type: number
      decimal_places:
        type: exact
        places: 0

measures:
  - name: active_users
    expr: COUNT(DISTINCT user_email)
    comment: Distinct users with at least one session
    display_name: Active Users
    format:
      type: number
      decimal_places:
        type: exact
        places: 0
    synonyms: [users, unique users, distinct users]
  - name: genie_code_session_count
    expr: COUNT(DISTINCT session_id)
    comment: Total inferred sessions. Primary cost unit.
    display_name: Sessions
    format:
      type: number
      decimal_places:
        type: exact
        places: 0
    synonyms: [session count, total sessions, code sessions]
  - name: genie_code_message_count
    expr: SUM(messages_in_session)
    comment: Total messages sent
    display_name: Messages
    format:
      type: number
      decimal_places:
        type: exact
        places: 0
    synonyms: [message count, total messages]
  - name: avg_messages_per_session
    expr: "MEASURE(genie_code_message_count) / NULLIF(MEASURE(genie_code_session_count), 0)"
    comment: Average messages per session
    display_name: Avg Messages / Session
    format:
      type: number
      decimal_places:
        type: exact
        places: 1
  - name: sessions_per_active_user
    expr: "MEASURE(genie_code_session_count) / NULLIF(MEASURE(active_users), 0)"
    comment: Average sessions per active user
    display_name: Sessions / User
    format:
      type: number
      decimal_places:
        type: exact
        places: 1
  - name: engaged_minutes
    expr: SUM(session_engaged_minutes)
    comment: Total engaged minutes (inter-message gaps under 30 min)
    display_name: Engaged Minutes
    format:
      type: number
      decimal_places:
        type: exact
        places: 1
    synonyms: [engagement time, active minutes]
  - name: avg_engaged_minutes_per_session
    expr: AVG(session_engaged_minutes)
    comment: Average engaged minutes per session
    display_name: Avg Engaged Mins / Session
    format:
      type: number
      decimal_places:
        type: exact
        places: 1
$$
""")
print(f"  → {catalog}.{schema}.mv_genie_code_usage created (backed by Delta table)")

# COMMAND ----------

# DBTITLE 1,Cell 6
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

# ─── Step 1: Materialize question events into a Delta table ───────────────────
print("Materializing Genie Spaces questions...")
spark.sql(f"""
CREATE OR REPLACE TABLE {catalog}.{schema}._stg_genie_spaces_questions
COMMENT 'Pre-computed Genie question events (Spaces + One + Mobile). Refresh via this notebook or a scheduled job.'
AS
WITH genie_spaces_raw AS (
  SELECT
    a.account_id,
    a.workspace_id,
    a.event_id AS event_key,
    a.event_time,
    a.event_date,
    a.action_name,
    a.user_identity.email AS user_email,
    a.request_params['space_id'] AS space_id,
    DATE_TRUNC('MONTH', a.event_time) AS usage_month,
    CASE WHEN a.action_name LIKE 'genie%' THEN 'API' ELSE 'UI' END AS access_path,
    'Genie Spaces' AS surface
  FROM system.access.audit a
  WHERE a.service_name = 'aibiGenie'
    AND a.event_date >= dateadd(MONTH, -13, current_date())
    AND a.action_name IN (
      'genieCreateConversationMessage',
      'createConversationMessage',
      'genieStartConversationMessage',
      'regenerateConversationMessage'
    )
    AND a.user_identity.email IS NOT NULL
    AND a.user_identity.email != 'System-User'
),
onechat_raw AS (
  SELECT
    a.account_id,
    a.workspace_id,
    a.request_id AS event_key,
    a.event_time,
    a.event_date,
    a.action_name,
    a.user_identity.email AS user_email,
    CAST(NULL AS STRING) AS space_id,
    DATE_TRUNC('MONTH', a.event_time) AS usage_month,
    CAST(NULL AS STRING) AS access_path,
    CASE
      WHEN LOWER(a.user_agent) LIKE '%android%'
        OR LOWER(a.user_agent) LIKE '%iphone%'
        OR LOWER(a.user_agent) LIKE '%ipad%'
        OR LOWER(a.user_agent) LIKE '%mobile%'
      THEN 'Mobile'
      ELSE 'Databricks One'
    END AS surface
  FROM system.access.audit a
  WHERE a.service_name = 'genieChat'
    AND a.event_date >= dateadd(MONTH, -13, current_date())
    AND a.action_name = 'steerGenieChatConversation'
    AND a.user_identity.email IS NOT NULL
    AND a.user_identity.email != 'System-User'
),
all_questions AS (
  SELECT * FROM genie_spaces_raw
  UNION ALL
  SELECT * FROM onechat_raw
),
user_month_agg AS (
  SELECT
    workspace_id, user_email, usage_month,
    COUNT(*) AS user_monthly_question_count,
    COUNT(DISTINCT space_id) AS user_monthly_space_count
  FROM all_questions
  GROUP BY workspace_id, user_email, usage_month
),
ws AS (
  SELECT workspace_id, MAX(workspace_name) AS workspace_name
  FROM system.access.workspaces_latest GROUP BY workspace_id
)
SELECT q.*, u.user_monthly_question_count, u.user_monthly_space_count, w.workspace_name
FROM all_questions q
JOIN user_month_agg u
  ON q.workspace_id = u.workspace_id AND q.user_email = u.user_email AND q.usage_month = u.usage_month
LEFT JOIN ws w ON q.workspace_id = w.workspace_id
""")
row_count = spark.sql(f"SELECT COUNT(*) FROM {catalog}.{schema}._stg_genie_spaces_questions").collect()[0][0]
print(f"  → {row_count:,} question events materialized")

# ─── Step 2: Create metric view on top of the table ────────────────────────────
print("Creating metric view...")
spark.sql(f"""
CREATE OR REPLACE VIEW {catalog}.{schema}.mv_genie_spaces_usage
WITH METRICS LANGUAGE YAML AS
$$
version: 1.1

source: "SELECT * FROM {catalog}.{schema}._stg_genie_spaces_questions"

comment: "Genie adoption metrics across all surfaces (Spaces, Databricks One, Mobile).\
  \\ Grain: one row per question event. Backed by a pre-computed Delta table."

dimensions:
  - name: usage_month
    expr: usage_month
    comment: Calendar month of the question event
    display_name: Month
    format:
      type: date
      date_format: locale_short_month
  - name: event_date
    expr: event_date
    comment: Calendar date of the question event
    display_name: Date
  - name: workspace_id
    expr: workspace_id
    comment: Databricks workspace identifier
    display_name: Workspace ID
  - name: workspace_name
    expr: workspace_name
    comment: Workspace name
    display_name: Workspace
  - name: user_email
    expr: user_email
    comment: User who asked the question
    display_name: User Email
  - name: actor_type
    expr: "CASE WHEN user_email LIKE '%@%' THEN 'Human' ELSE 'Service Principal' END"
    comment: Human vs Service Principal
    display_name: Actor Type
  - name: surface
    expr: surface
    comment: "Interface: Genie Spaces, Databricks One, or Mobile"
    display_name: Surface
  - name: space_id
    expr: space_id
    comment: Genie Space identifier (NULL for One/Mobile)
    display_name: Space ID
  - name: access_path
    expr: access_path
    comment: UI or API for Genie Spaces events only
    display_name: Access Path
  - name: action_name
    expr: action_name
    comment: Raw audit action_name
    display_name: Action Name
  - name: account_id
    expr: account_id
    comment: Databricks account identifier
    display_name: Account ID
  - name: user_monthly_question_count
    expr: user_monthly_question_count
    comment: Total questions this user asked in this calendar month
    display_name: User Monthly Questions
    format:
      type: number
      decimal_places:
        type: exact
        places: 0
  - name: user_monthly_space_count
    expr: user_monthly_space_count
    comment: Distinct Genie Spaces visited this month
    display_name: User Monthly Spaces
    format:
      type: number
      decimal_places:
        type: exact
        places: 0

measures:
  - name: active_users
    expr: COUNT(DISTINCT user_email)
    comment: Distinct users who asked at least one question
    display_name: Active Users
    format:
      type: number
      decimal_places:
        type: exact
        places: 0
    synonyms: [users, unique users, distinct users]
  - name: question_count
    expr: COUNT(event_key)
    comment: Total questions. Primary cost unit.
    display_name: Questions
    format:
      type: number
      decimal_places:
        type: exact
        places: 0
    synonyms: [total questions, queries]
  - name: genie_spaces_questions
    expr: COUNT(CASE WHEN surface = 'Genie Spaces' THEN event_key END)
    comment: Questions via Genie Spaces only
    display_name: Genie Spaces Questions
    format:
      type: number
      decimal_places:
        type: exact
        places: 0
  - name: onechat_questions
    expr: COUNT(CASE WHEN surface = 'Databricks One' THEN event_key END)
    comment: Questions via Databricks One
    display_name: Databricks One Questions
    format:
      type: number
      decimal_places:
        type: exact
        places: 0
  - name: mobile_questions
    expr: COUNT(CASE WHEN surface = 'Mobile' THEN event_key END)
    comment: Questions via Mobile
    display_name: Mobile Questions
    format:
      type: number
      decimal_places:
        type: exact
        places: 0
  - name: ui_questions
    expr: COUNT(CASE WHEN access_path = 'UI' THEN event_key END)
    comment: Genie Spaces UI-path questions only
    display_name: UI Questions
    format:
      type: number
      decimal_places:
        type: exact
        places: 0
  - name: api_questions
    expr: COUNT(CASE WHEN access_path = 'API' THEN event_key END)
    comment: Genie Spaces API-path questions only
    display_name: API Questions
    format:
      type: number
      decimal_places:
        type: exact
        places: 0
  - name: active_space_count
    expr: COUNT(DISTINCT space_id)
    comment: Distinct Genie Spaces with a question
    display_name: Active Spaces
    format:
      type: number
      decimal_places:
        type: exact
        places: 0
  - name: questions_per_active_user
    expr: "MEASURE(question_count) / NULLIF(MEASURE(active_users), 0)"
    comment: Questions per active user
    display_name: Questions / User
    format:
      type: number
      decimal_places:
        type: exact
        places: 1
$$
""")
print(f"  → {catalog}.{schema}.mv_genie_spaces_usage created (backed by Delta table)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validation Queries
# MAGIC Run the next two cells after deployment to confirm the metric views resolve and return recent grouped results.
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   usage_month,
# MAGIC   workspace_id,
# MAGIC   MEASURE(active_users) AS active_users,
# MAGIC   MEASURE(genie_code_session_count) AS sessions,
# MAGIC   MEASURE(genie_code_message_count) AS messages,
# MAGIC   MEASURE(avg_messages_per_session) AS avg_messages_per_session,
# MAGIC   MEASURE(engaged_minutes) AS engaged_minutes,
# MAGIC   MEASURE(avg_engaged_minutes_per_session) AS avg_engaged_minutes_per_session
# MAGIC FROM IDENTIFIER(:catalog || '.' || :schema || '.mv_genie_code_usage')
# MAGIC WHERE usage_month >= DATE_TRUNC('MONTH', current_date() - INTERVAL 90 DAYS)
# MAGIC GROUP BY usage_month, workspace_id
# MAGIC ORDER BY usage_month DESC, sessions DESC
# MAGIC LIMIT 20
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   usage_month,
# MAGIC   workspace_id,
# MAGIC   MEASURE(active_users) AS active_users,
# MAGIC   MEASURE(question_count) AS questions,
# MAGIC   MEASURE(ui_questions) AS ui_questions,
# MAGIC   MEASURE(api_questions) AS api_questions,
# MAGIC   MEASURE(active_space_count) AS active_spaces,
# MAGIC   MEASURE(questions_per_active_user) AS questions_per_active_user
# MAGIC FROM IDENTIFIER(:catalog || '.' || :schema || '.mv_genie_spaces_usage')
# MAGIC WHERE usage_month >= DATE_TRUNC('MONTH', current_date() - INTERVAL 90 DAYS)
# MAGIC GROUP BY usage_month, workspace_id
# MAGIC ORDER BY usage_month DESC, questions DESC
# MAGIC LIMIT 20
# MAGIC

# COMMAND ----------

# DBTITLE 1,Cell 11
# MAGIC %md
# MAGIC ## Deploy the dashboard
# MAGIC Retargets the bundled dashboard to your `catalog` / `schema` widgets and creates it via the
# MAGIC Lakeview API, running as the current user.
# MAGIC
# MAGIC Default behavior is workspace-scoped:
# MAGIC * The dashboard workspace filter defaults to the current workspace name
# MAGIC * `cross_workspace_mode` defaults to `false`
# MAGIC * Cross-workspace views are opt-in only
# MAGIC * Monthly cost trend datasets are capped to the last 12 months
# MAGIC
# MAGIC Set `warehouse_id` (blank = auto-pick a warehouse), then run the next cell.

# COMMAND ----------

# DBTITLE 1,Deploy Dashboard
import base64
import copy
import json
import re
from databricks.sdk import WorkspaceClient
from databricks.sdk.service import workspace as ws

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
warehouse_id = dbutils.widgets.get("warehouse_id").strip()
dashboard_name = dbutils.widgets.get("dashboard_name")

w = WorkspaceClient()
if not warehouse_id:
    warehouse_id = next((wh.id for wh in w.warehouses.list()), None)
    print(f"Auto-selected warehouse: {warehouse_id}")
assert warehouse_id, "No SQL warehouse found; set the warehouse_id widget."

workspace_id = w.get_workspace_id()
workspace_name = spark.sql(f"""
SELECT workspace_name
FROM system.access.workspaces_latest
WHERE workspace_id = {workspace_id}
LIMIT 1
""").collect()[0][0]
print(f"Default workspace scope: {workspace_name}")

me = w.current_user.me().user_name
template_path = f"/Users/{me}/Genie Adoption & Cost Estimation.lvdash.json"
try:
    exported = w.workspace.export(path=template_path, format=ws.ExportFormat.AUTO)
except Exception as e:
    raise AssertionError(
        f"Could not load dashboard template from {template_path}. "
        "Create or copy the template dashboard JSON file into your workspace home before deploying."
    ) from e

dash = json.loads(base64.b64decode(exported.content).decode("utf-8"))
dash = copy.deepcopy(dash)

workspace_predicate = """(
  :cross_workspace_mode = 'true'
  OR workspace_id IN (
    SELECT workspace_id FROM system.access.workspaces_latest
    WHERE workspace_name = :workspace_name_filter
  )
)"""
workspace_month_cap_predicate = """usage_month >= DATE_TRUNC('MONTH', ADD_MONTHS(CURRENT_DATE(), -12))
  AND (
    :cross_workspace_mode = 'true'
    OR workspace_id IN (
      SELECT workspace_id FROM system.access.workspaces_latest
      WHERE workspace_name = :workspace_name_filter
    )
  )"""

workspace_filter_pattern = re.compile(
    r"\(?\s*:workspace_name_filter\s*=\s*''\s*OR\s*workspace_id\s+IN\s*\(\s*SELECT\s+workspace_id\s+FROM\s+system\.access\.workspaces_latest\s+WHERE\s+workspace_name\s*=\s*:workspace_name_filter\s*\)\s*\)?",
    flags=re.IGNORECASE | re.DOTALL,
)
view_block_pattern = re.compile(
    r"(FROM\s+mitchell_grewer_meijer\.genie_analytics\.(?:mv_genie_code_usage|mv_genie_spaces_usage))(.*?)(GROUP BY)",
    flags=re.IGNORECASE | re.DOTALL,
)

def ensure_param(ds, keyword, display_name, default_value, data_type="STRING"):
    params = ds.setdefault("parameters", [])
    for p in params:
        if p.get("keyword") == keyword:
            p["displayName"] = display_name
            p["dataType"] = data_type
            p["defaultSelection"] = {"values": {"dataType": data_type, "values": [{"value": default_value}]}}
            return
    params.append({"displayName": display_name, "keyword": keyword, "dataType": data_type,
                   "defaultSelection": {"values": {"dataType": data_type, "values": [{"value": default_value}]}}})

def add_scope_to_view_blocks(query_text, dataset_name):
    predicate = workspace_month_cap_predicate if dataset_name in {"nce_monthly_trend", "nce_cost_by_actor"} else workspace_predicate
    indented = predicate.replace("\n", "\n  ")
    def repl(match):
        from_clause, between, group_by = match.group(1), match.group(2), match.group(3)
        if ":workspace_name_filter" in between or ":cross_workspace_mode" in between:
            return match.group(0)
        trimmed = between.rstrip()
        connector = "AND" if re.search(r"\bWHERE\b", between, flags=re.IGNORECASE) else "WHERE"
        return f"{from_clause}{trimmed}\n  {connector} {indented}\n{group_by}"
    return view_block_pattern.sub(repl, query_text)

def rewrite_query(query_text, dataset_name):
    query_text = workspace_filter_pattern.sub(workspace_predicate, query_text)
    query_text = add_scope_to_view_blocks(query_text, dataset_name)
    return query_text

for ds in dash["datasets"]:
    query_text = "".join(ds.get("queryLines", []))
    if "mv_genie_code_usage" in query_text or "mv_genie_spaces_usage" in query_text:
        ensure_param(ds, "workspace_name_filter", "Workspace", workspace_name)
        ensure_param(ds, "cross_workspace_mode", "Cross Workspace Mode", "false")
        query_text = rewrite_query(query_text, ds["name"])
        ds["queryLines"] = [line + "\n" for line in query_text.split("\n")]

for page in dash["pages"]:
    if page.get("displayName") == "Global Filters":
        layout = page["layout"]
        existing_names = {item.get("widget", {}).get("name") for item in layout}
        template_widget = next(
            item["widget"] for item in layout
            if item.get("widget", {}).get("spec", {}).get("widgetType") == "filter-text-entry"
        )
        if "cross_workspace_mode" not in existing_names:
            cross_datasets = [
                ds["name"] for ds in dash["datasets"]
                if any(p.get("keyword") == "cross_workspace_mode" for p in ds.get("parameters", []))
            ]
            queries, fields = [], []
            for ds_name in cross_datasets:
                qname = f"parameter_{ds_name}_cross_workspace_mode"
                queries.append({"name": qname, "query": {"datasetName": ds_name,
                    "parameters": [{"name": "cross_workspace_mode", "keyword": "cross_workspace_mode"}],
                    "disaggregated": False}})
                fields.append({"parameterName": "cross_workspace_mode", "queryName": qname})
            cross_widget = copy.deepcopy(template_widget)
            cross_widget["name"] = "cross_workspace_mode"
            cross_widget["queries"] = queries
            cross_widget["spec"]["frame"]["title"] = "Cross Workspace Mode"
            cross_widget["spec"]["selection"]["defaultSelection"] = {
                "values": {"dataType": "STRING", "values": [{"value": "false"}]}}
            cross_widget["spec"]["encodings"]["fields"] = fields
            layout.append({"widget": cross_widget, "position": {"x": 8, "y": 2, "width": 4, "height": 1}})
        break

serialized = json.dumps(dash, indent=2)
serialized = serialized.replace("mitchell_grewer_meijer.genie_analytics", f"{catalog}.{schema}")

# Idempotent deploy: check for existing dashboard by name in the user's workspace home.
# The Lakeview GET endpoint accepts the workspace object_id (treeNodeId) as dashboard_id.
existing_item = next(
    (item for item in w.workspace.list(path=f"/Users/{me}", recursive=False)
     if str(item.object_type) == "ObjectType.DASHBOARD"
     and item.path and item.path.endswith(f"/{dashboard_name}.lvdash.json")),
    None,
)

if existing_item:
    # Resolve the UUID via the Lakeview GET endpoint (accepts treeNodeId as the id)
    existing_uuid = w.api_client.do(
        "GET", f"/api/2.0/lakeview/dashboards/{existing_item.object_id}"
    ).get("dashboard_id")
    w.api_client.do("PATCH", f"/api/2.0/lakeview/dashboards/{existing_uuid}",
                    body={"display_name": dashboard_name,
                          "serialized_dashboard": serialized,
                          "warehouse_id": warehouse_id})
    print(f"Updated existing dashboard: {existing_uuid}")
else:
    resp = w.api_client.do("POST", "/api/2.0/lakeview/dashboards",
                           body={"display_name": dashboard_name,
                                 "serialized_dashboard": serialized,
                                 "warehouse_id": warehouse_id,
                                 "parent_path": f"/Users/{me}"})
    print(f"Created dashboard: {resp.get('dashboard_id')}")

print("Workspace filter defaults to the current workspace; cross-workspace mode defaults to false.")
print("Remember to assign a daily refresh schedule, then Publish.")