# analytics-cdc

Builds and populates the **analytics reference (dimension) tables** for the
Ollylake data lake. Two small Java programs run on top of a
[DuckLake](https://ducklake.select) catalog whose data files live in MinIO (S3)
and whose metadata lives in a persisted `.ducklake` file:

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
│  (source of      │      ② GET /…/{objectType}/list│
│   truth)         │─── JSON snapshot ──┐           │
└──────────────────┘                    ▼           │
                                ┌─────────────────────┐
                                │  SccalReferenceSync  │
                                │  (analytics-cdc)     │
                                │                      │
                                │  parse JSON          │
                                │  → TEMP staging      │
                                │  → dedupe by key     │
                                │  → INSERT/UPDATE/    │
                                │    DELETE (1 txn)    │
                                └──────────┬───────────┘
                                           │ ③ capture changes
                                           ▼
                          ┌──────────────────────────────┐
                          │  DuckLake catalog 'ollylake'  │
                          │                                │
                          │  metadata → .ducklake file     │
                          │  data     → Parquet in MinIO   │
                          │             (s3://ollylake/)   │
                          └────────────────────────────────┘
```

In one sentence: the SCCAL server is the source of truth → `SccalReferenceSync`
pulls each full JSON snapshot over HTTP → diffs it against the DuckLake reference
tables → writes inserts/updates/deletes as Parquet in MinIO, tracked by the
`.ducklake` metadata file.

### Inside `SccalReferenceSync` (one run)

Each cycle has **two phases**:

**Phase 1 — change stream** (`ChangeStreamSync`, stream-fed types: `user`):

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

A pair with no cursor row is **bootstrapped**: the stream-fed tables are
captured from full snapshots and the cursors seeded from the registry head.
The same fallback runs when `/changes` answers **410 Gone** (the saved offset
was pruned upstream).

**Phase 2 — snapshot diff** (all other types):

```
read (customerId, tenantId) pairs  ──►  customer_tenant_reference  (V1 table)
        │
        ├─ global object types  (providercatalogue …) ── fetched ONCE
        └─ tenant object types  ── fetched concurrently per pair  ──►  SCCAL API
        │
        ▼
   raw JSON map  ──►  bound as ?::JSON  ──►  DuckDB json_extract('$.*')  (no JSON lib)
        │
        ▼
   per-table TEMP staging  ──►  de-dupe by key (QUALIFY row_number)
        │
        ▼
   ┌─ one transaction (atomic DuckLake snapshot) ──────────────┐
   │  DELETE rows absent from snapshot / isDeleted:true        │
   │  UPDATE rows whose attributes changed                     │
   │  INSERT rows new to the table                             │
   └───────────────────────────────────────────────────────────┘
        │
        ▼
   V6 reference tables  (data → MinIO, metadata → catalog)
```

The snapshot API is queried **without `commitId`**, so every response is a full
snapshot and "absent from snapshot" is a safe delete signal. Both phases are
**idempotent** — re-running with unchanged data captures nothing, and an idle
poll (`/cursors` returns an empty page) costs one HTTP call and zero writes.
As SCCAL wires more capture types into its entity-change log, move their object
types from phase 2 to phase 1 by adding one `objectType → entityType` entry to
`SccalReferenceSync.STREAM_ENTITY_TYPES` — both phases (and
`ChangeStreamSync.SYNCS_BY_ENTITY_TYPE`) derive from it.

---

## What tables we are getting

### Captured reference tables (V6)

| V6 table | SCCAL object type | Key | Notes |
|---|---|---|---|
| `workspace` | `workspace` | `workspaceId` | |
| `"user"` | `user` | `userId` | name = `userName` (login email) — **stream-fed** (`/changes` entityType `USER`); snapshot used only for bootstrap / 410 resync |
| `"group"` | `usergroup` | `userGroupId` | |
| `user_group_mapping` | `usergroupmembership` | `userId, userGroupId` | edge table (no name) |
| `provider` | `providercatalogue` | `type` | enum (0–8); `provider_id` is `INTEGER` — global type, fetched once; catalogue entries sharing a `type` (provider variants) dedupe to one row |
| `llm_access_rule` | `rule` (`ruleType IN (0, 1)`) | `ruleId` | provider/LLM rules: 0 = TenantProvider, 1 = WorkspaceProvider |
| `mcp_access_rule` | `rule` (`ruleType IN (2, 3)`) | `ruleId` | MCP rules: 2 = TenantMcp, 3 = WorkspaceMcp |

`tenant`, `vkey`, `mcp_server`, `budget_rule` have **no source endpoint** among
the SCCAL object types and are created but left empty.

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
  poll_interval  = 1m                        # env POLL_INTERVAL overrides
  startup_script_provider {
    script_location = "ollylake/startup.sql" # env STARTUP_SQL overrides
  }
}
```

`poll_interval` controls how the CDC runs: a **positive duration** (`1m` — the
default, `30s`, `5 minutes`) polls — stay alive and re-capture on that interval,
reusing the warm DuckDB connection; cycles that change nothing are rolled back
(no empty snapshot) and a failed cycle is logged and retried on the next tick.
**`0`** (or negative) runs one capture and exits.

`ollylake/startup.sql` holds the bootstrap SQL (INSTALL/LOAD `ducklake`+`httpfs`,
`CREATE SECRET`, `ATTACH`, `USE`). It uses `${ENV_VAR}` placeholders resolved by
`StartupScriptProvider` — **all must be set**:
`MINIO_ROOT_USER`, `MINIO_ROOT_PASSWORD`, `S3_ENDPOINT`, `OLLYLAKE_BUCKET`, `CATALOG_PATH`.

---

## Running

> The SCCAL Spring server must already be running on the host at `:8080`. The
> CDC container reaches it via `host.docker.internal:8080` (wired in compose).

```bash
# Build + start the full chain: minio → minio-init → ollylake-init → analytics-cdc
docker compose up --build

# One-shot capture instead of the default 1m poll loop
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
