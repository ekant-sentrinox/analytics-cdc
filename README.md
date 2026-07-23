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
polls its **entity-change stream** over HTTP (`/cursors` + `/changes`) →
applies each explicit insert/update/delete to the DuckLake reference tables →
writes them as Parquet in MinIO, tracked by the PostgreSQL catalog metadata.
The stream-fed tables have **no full-snapshot (`/list`) fallback**.

A second, **global tier** runs in the same cycle: the catalogue dimensions
(providers, models, MCP servers/tools) plus `tenant` are deliberately **not**
in the change stream — they sync from `GET /…/{objectType}/list` by full
snapshot on first contact, then **`updatedSince` incremental deltas** with
`isDeleted` tombstones (see [Catalogue pull](#the-catalogue-list-pull-global-tier)).

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

### The catalogue `/list` pull (global tier)

After the stream phase, the same cycle captures the **global catalog** tables
(`CatalogueListSync`) — the tier the admin layer's `global-catalog-sync.md`
excludes from the change stream because its rows only change when a global
Flyway migration lands upstream:

```
globalCatalog.schemaRevision              ── embedded in every /cursors page ("the gate")
        │
        ├─ unchanged vs V7 sccal_catalogue_revision ──►  skip the /list endpoints entirely
        │
        ▼  per catalogue objectType (providercatalogue, modelcatalogue, mcp…)
GET /internal/sccal/api/v1/{objectType}/list?customerId=…&tenantId=…[&updatedSince=…]
        │        no saved cursor → full live snapshot; cursor → strict updated_at > since delta
        ▼
   JSON map id → {…fields…, isDeleted}   ──►  shred ($.*), soft-DELETE / UPDATE / INSERT
        │
        ▼
   ┌─ one transaction ──────────────────────────────────────────────┐
   │  merge each catalogue table (declaration = FK order)            │
   │  advance each pulled type's updatedSince cursor (V7)            │
   │  store the seen gate revision (V7)                              │
   │  + snapshot `tenant` for every known (customer, tenant) pair    │
   └──────────────────────────────────────────────────────────────────┘
```

- **Gate** — catalogue pulls are skipped while `schemaRevision` (a near-free
  value riding on the existing `/cursors` poll) matches the stored one; a
  server that does not embed the gate falls back to pulling every cycle.
- **Cursor** — the `/list` payload carries no `updatedAt`, so after a
  non-empty pull the cursor advances to *request start − 60 s* (clock-skew
  lap) and never moves backwards; the server bound is strict (`>`), the merge
  idempotent, so the overlap is harmless. An empty delta writes nothing.
- **Tombstones** — a retired catalogue row arrives as `isDeleted: true` and is
  soft-deleted (kept, flagged `is_deleted`), same as the stream path.
- **`tenant`** — its `/list` is snapshot-only upstream (`updatedSince` inert)
  and answers one row per `(customerId, tenantId)`, so it is re-pulled per
  pair known to the change stream on every cycle, not gated; replays no-op.

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

The remaining V6 tables (`vkey`, `budget_rule`) have no source on either
capture path and are created but left empty.

### Captured catalogue tables (V6, `/list`-fed)

Fed from the `/list` pull (see above), declared in `analytics_cdc.catalogue_syncs`.
The four global lookups are cluster-shared — **no `customer_id`**, not
partitioned; ids are the source cluster's Snowflake ids copied verbatim, so
they join to fact data (hence `provider_id BIGINT`). `tenant` is per-customer
and stamped from the poll context like the stream-fed tables. Per the V6
philosophy the tables hold id + name (+ the parent-id join key for
`model`/`mcp_tool`); richer attributes stay in the source of truth.

| V6 table | objectType | Key | Attributes |
|---|---|---|---|
| `provider` | `providercatalogue` | `providerCatalogueId` | `name` |
| `model` | `modelcatalogue` | `modelCatalogueId` | `name`, `provider_id` |
| `mcp_server` | `mcpservercatalogue` | `mcpServerCatalogueId` | `name` |
| `mcp_tool` | `mcptoolcatalogue` | `mcpToolCatalogueId` | `name`, `mcp_server_id` |
| `tenant` | `tenant` (per pair) | `customer_id, tenantId` | `name` (never tombstoned — the endpoint always answers `isDeleted:false`) |

### Sync state and audit tables (V7 / V8)

Written in the same transaction as the data they describe:

| Table | Content |
|---|---|
| `sccal_change_cursor` (V7) | per `(customer_id, tenant_id)`: the next `/changes` `startOffset` to pull — equals the endpoint's last `nextOffset` once the stream is consumed |
| `sccal_registry_cursor` (V7) | singleton: the next `/cursors` `startOffset` for tenant-advance discovery |
| `entity_change_audit` (V8) | every pulled `/changes` entry verbatim — **all** entity types, even those feeding no reference table (e.g. `ACTIVATION`, `GROUP`, `WORKSPACE`) — with both payload views (`sccal_payload`, `human_payload`) as JSON text |
| `sccal_catalogue_cursor` (V7) | per catalogue objectType: the `updatedSince` (ISO-8601 UTC) for the next `/list` delta pull |
| `sccal_catalogue_revision` (V7) | singleton: the last-seen `globalCatalog.schemaRevision` gate value |

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
  syncs = [ ... ]                            # stream-fed tables — see below
  catalogue_syncs = [ ... ]                  # /list-fed catalogue tables (FK order)
  catalogue_context {                        # customerId/tenantId sent on global
    customer_id = "1"                        #   catalogue pulls — required by the
    tenant_id   = "1"                        #   endpoint, inert for global types;
  }                                          #   env CATALOGUE_CUSTOMER_ID /
}                                            #   CATALOGUE_TENANT_ID override
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

A **catalogue** (`/list`-fed) table works the same way in
`analytics_cdc.catalogue_syncs`, with `object_type` (the `/list` path segment,
lowercase) instead of `entity_type`, and an optional `per_tenant = true` for
endpoints that answer per `(customerId, tenantId)` pair (see the `tenant`
entry). Keep the list in referenced-before-referencing (FK) order — entries
apply in declaration order.

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
