# analytics-cdc

Builds and populates the **analytics reference (dimension) tables** for the
Ollylake data lake. Two small Java programs run on top of a
[DuckLake](https://ducklake.select) catalog whose data files live in MinIO (S3)
and whose metadata lives in a PostgreSQL catalog database (`metadata-db`):

| Program | Container | Role |
|---|---|---|
| `ai.sentrinox.OllylakeSchemaInitializer` | `ollylake-init` | ATTACH the catalog and run the init SQL (`ollylake/init/*.sql`) to **create** the tables. |
| `ai.sentrinox.SccalReferenceSync` | `analytics-cdc` | Read the `(customerId, tenantId)` pairs, call the **SCCAL internal-sync API**, and **capture changes** (insert/update/delete) into the reference tables. |

Both share one bootstrap: the startup SQL (extensions + S3 secret + `ATTACH`)
is loaded from config via `io.dazzleduck.sql.common.StartupScriptProvider`.

---

## Data flow

```
┌──────────────────┐      ① read (customerId, tenantId) pairs
│  SCCAL Spring    │◄──────────────────────────────┐
│  server  :8080   │                                │
│  (source of      │  ② GET /…/cursors  "who moved?"│
│   truth)         │  ③ GET /…/changes  entity deltas
│                  │─── JSON change entries ─┐      │
└──────────────────┘                         ▼      │
                                ┌─────────────────────┐
                                │  SccalReferenceSync  │
                                │  (analytics-cdc)     │
                                │                      │
                                │  parse JSON entries  │
                                │  → keep last/key     │
                                │  → INSERT/UPDATE/    │
                                │    DELETE (1 txn)    │
                                │  → advance cursors   │
                                └──────────┬───────────┘
                                           │ ④ capture changes + audit
                                           ▼
                          ┌──────────────────────────────┐
                          │  DuckLake catalog 'ollylake'  │
                          │                                │
                          │  metadata → PostgreSQL         │
                          │  data     → Parquet in MinIO   │
                          │             (s3://ollylake/)   │
                          └────────────────────────────────┘
```

In one sentence: the SCCAL server is the source of truth → `SccalReferenceSync`
polls its **entity-change stream** over HTTP (`/cursors` + `/changes` — the only
two endpoints it uses) → applies each explicit insert/update/delete to the
DuckLake reference tables → writes them as Parquet in MinIO, tracked by the
PostgreSQL catalog metadata. There is **no full-snapshot (`/list`) path**.

### Inside `SccalReferenceSync` (one run)

Every reference table is fed from the change stream (`ChangeStreamSync`):

```
saved registry offset (V7 sccal_registry_cursor)
        │
        ▼
GET /internal/sccal/api/v1/cursors?startOffset=…      ── "which tenants advanced?"
        │
        ├─ empty page ──►  idle: cycle over, nothing fetched, nothing written
        │
        ▼  per advanced (customerId, tenantId), from its V7 sccal_change_cursor offset
GET /internal/sccal/api/v1/changes?…&startOffset=…&view=BOTH   ── paged entity_change entries
        │
        ▼
   stage entries (DuckDB json_extract '$.entries[*]')  ──►  keep LAST event per key
        │
        ▼
   ┌─ one transaction ─────────────────────────────────────────┐
   │  append EVERY entry to entity_change_audit (V8 audit log,  │
   │    all entity types, sccal + human payloads)               │
   │  DELETE rows with action = 'DELETE' (explicit tombstones)  │
   │  UPDATE / INSERT the rest                                  │
   │  + advance both cursor tables (same commit)                │
   └────────────────────────────────────────────────────────────┘
```

Two edge cases are handled entirely from the stream (no snapshot fallback):

- **Bootstrap** — a pair with no cursor row **replays its change stream from the
  start** (`startOffset = 1`), reconstructing current state from the
  CREATE/UPDATE/DELETE history, then seeds its cursor where the replay ended.
- **410 Gone** — the saved offset fell below SCCAL's prune watermark; the cursor
  **fast-forwards to the registry head** and a warning is logged. Changes that
  occurred only inside the pruned window are captured only when the entity next
  changes (they cannot be recovered without a snapshot endpoint).

Capture is **idempotent** — replaying a page inserts nothing new, and an idle
poll (`/cursors` returns an empty page) costs one HTTP call and zero writes. The
captured tables — target table, `/changes` entityType, key/data columns and
their JSON fields — are declared in one place, the `analytics_cdc.syncs` list in
`application.conf`.

---

## What tables we are getting

### Captured reference tables (V6)

Every table below is fed from the `/changes` stream, keyed by the entityType its
`analytics_cdc.syncs` entry declares (see [Configuration](#configuration)). Each
captured row is also stamped with the `customer_id` of the pair whose stream it
came from — that value is not in the entity payload, it comes from the poll
context.

| V6 table | `/changes` entityType | Key | `name` from | Notes |
|---|---|---|---|---|
| `"user"` | `USER` | `customer_id, userId` | `userName` (login email) | confirmed live payload shape |
| `workspace` | `WORKSPACE` | `customer_id, workspaceId` | `workspaceName` | field names follow the USER convention (no live sample yet) |
| `"group"` | `USERGROUP` | `customer_id, userGroupId` | `groupName` | GroupServiceImpl publishes USERGROUP (never the GROUP enum value) |
| `llm_access_rule` | `RULE` (ruleType 0/1) | `customer_id, ruleId` | `name` | RULE fans out by the payload's ruleType (`entry_filter` in config) |
| `mcp_access_rule` | `RULE` (ruleType 2/3) | `customer_id, ruleId` | `name` | RULE fans out by the payload's ruleType (`entry_filter` in config) |

All other V6 tables (`vkey`, `provider`, `mcp_server`, `budget_rule`) have
**no corresponding `/changes` entity type** and are created but left empty.
There is no tenant table: the `v_ai_audit` view (V9) serves `tenant_id`
straight from the audit rows with a NULL `tenant_name`.

### Change-stream state and audit tables (V7 / V8)

Written by the change-stream phase, in the same transaction as the data they
describe:

| Table | Content |
|---|---|
| `sccal_change_cursor` (V7) | per `(customer_id, tenant_id)`: the next `/changes` `startOffset` to pull — equals the endpoint's last `nextOffset` once the stream is consumed |
| `sccal_registry_cursor` (V7) | singleton: the next `/cursors` `startOffset` for tenant-advance discovery |
| `entity_change_audit` (V8) | every pulled `/changes` entry verbatim — **all** entity types, even those feeding no reference table (e.g. `ACTIVATION`, `GROUP`, `WORKSPACE`) — with both payload views (`sccal_payload`, `human_payload`) as JSON text |

---

## Configuration

`src/main/resources/application.conf` (HOCON, auto-loaded from the classpath):

```hocon
analytics_cdc {
  sccal_base_url = "http://localhost:8080"   # env SCCAL_BASE_URL overrides
  poll_interval  = 30s                       # env POLL_INTERVAL overrides
  startup_script_provider {
    script_location = "ollylake/startup.sql" # env STARTUP_SQL overrides
  }
  syncs = [ ... ]                            # the captured tables — see below
}
```

### Adding a new reference table (config only — no Java change)

The captured tables are declared in the `analytics_cdc.syncs` list. To capture a
new table:

1. **Create the table** — add a `CREATE TABLE` to a new `ollylake/init/V*.sql`
   migration (include `customer_id BIGINT NOT NULL` and partition by it, like
   the existing V6 tables).
2. **Declare the sync** — add one block to `analytics_cdc.syncs`:

```hocon
{
  entity_type = "WIDGET"                  # the /changes entityType that feeds it
  table = "ollylake.main.widget"          # quote reserved words: "ollylake.main.\"user\""
  key_columns = [                         # identity columns (per-customer CDC key)
    { column = "widget_id", json_fields = ["widgetId", "id"], type = "BIGINT" }
  ]
  data_columns = [                        # non-key attributes; may be omitted
    { column = "name", json_fields = ["widgetName", "name"], type = "VARCHAR" }
  ]
  # entry_filter = "..."                  # optional: split one entityType across
                                          # tables by payload content (see RULE)
}
```

Each column lists its `json_fields` most-specific first; the capture coalesces
them, taking the first field present in the payload. Declarations are validated
at startup (identifier shapes, duplicate tables, statement splicing) and a bad
entry fails fast rather than silently dropping a table from capture.

`poll_interval` controls how the CDC runs: a **positive duration** (`30s` — the
default, `1m`, `5 minutes`) polls — stay alive and re-capture on that interval,
reusing the warm DuckDB connection; cycles that change nothing are rolled back
(no empty snapshot) and a failed cycle is logged and retried on the next tick.
**`0`** (or negative) runs one capture and exits.

`ollylake/startup.sql` holds the bootstrap SQL (INSTALL/LOAD
`ducklake`+`httpfs`+`postgres`, `CREATE SECRET`, `ATTACH`, `USE`). It uses
`${ENV_VAR}` placeholders resolved by `StartupScriptProvider` — **all must be
set**: `MINIO_ROOT_USER`, `MINIO_ROOT_PASSWORD`, `S3_ENDPOINT`, `OLLYLAKE_BUCKET`,
`METADATA_DB_HOST`, `METADATA_DB_PORT`, `METADATA_DB_NAME`, `METADATA_DB_USER`,
`METADATA_DB_PASSWORD`.

---

## Running

> The SCCAL Spring server must already be running on the host at `:8080`. The
> CDC container reaches it via `host.docker.internal:8080` (wired in compose).

```bash
# Build + start the full chain: minio → minio-init → ollylake-init → analytics-cdc
docker compose up --build

# One-shot capture instead of the default 30s poll loop
POLL_INTERVAL=0 docker compose up analytics-cdc

# Logs (prints an ins / upd / del count per table)
docker compose logs -f analytics-cdc

# Tear down (add -v to also wipe MinIO + catalog volumes)
docker compose down
```

To point the CDC at a different API host:

```bash
SCCAL_BASE_URL=http://my-host:8080 docker compose up --build analytics-cdc
```

---

## Build

```bash
mvn -B -DskipTests package      # produces target/analytics-cdc.jar (shaded)
```

The shaded jar's default main class is `OllylakeSchemaInitializer`; the CDC job
runs via `java -cp target/analytics-cdc.jar ai.sentrinox.SccalReferenceSync`.
