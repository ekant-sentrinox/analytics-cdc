package ai.sentrinox;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for the change-stream SQL the {@link SccalReferenceSync.Sync}
 * records generate (the /changes staging + merge variants), and for the
 * stream/snapshot split. Pure string assertions — no DB, no network.
 */
class ChangeStreamSqlTest {

    private static SccalReferenceSync.Sync sync(String table) {
        return SccalReferenceSync.SYNCS.stream()
            .filter(s -> s.table().equals(table))
            .findFirst()
            .orElseThrow(() -> new AssertionError("no sync for " + table));
    }

    @Test
    void everyObjectTypeIsStreamFed() {
        // With the snapshot (/list) path removed, every reference table is fed
        // from the change stream: each sync's objectType must map to a /changes
        // entityType, and each entityType must resolve back to its sync(s).
        for (SccalReferenceSync.Sync s : SccalReferenceSync.SYNCS) {
            String entityType = SccalReferenceSync.STREAM_ENTITY_TYPES.get(s.objectType());
            assertTrue(entityType != null,
                "no entityType mapping for objectType " + s.objectType());
            assertTrue(ChangeStreamSync.SYNCS_BY_ENTITY_TYPE.get(entityType).contains(s),
                "sync not reachable via entityType " + entityType);
        }
    }

    @Test
    void stageChangesExtractsSccalPayloadActionAndChangeId() {
        String sql = sync("ollylake.main.\"user\"").stageChangesSql("USER", 7L);

        assertTrue(sql.startsWith(
            "INSERT INTO cdc_chg_ollylake_main__user_ (customer_id, user_id, name,"
                + " __action, __change_id)"), sql);
        // customer_id is stamped from the poll context, not extracted from JSON;
        // the outer projection casts the materialized raw columns.
        assertTrue(sql.contains("__change_id) SELECT 7, user_id::BIGINT AS user_id"), sql);
        assertTrue(sql.contains("unnest(json_extract(?::JSON, '$.entries[*]'))"), sql);
        assertTrue(sql.contains("entry->'sccal' AS e"), sql);
        assertTrue(sql.contains("entry->>'entityType' = 'USER'"), sql);
        // The inner projection extracts each column (coalescing the real shape
        // then the generic shape); the outer projection casts it to its SQL type.
        assertTrue(sql.contains("coalesce(e->>'userId', e->>'id') AS user_id"), sql);
        assertTrue(sql.contains("coalesce(e->>'userName', e->>'email') AS name"), sql);
        assertTrue(sql.contains("name::VARCHAR AS name"), sql);
    }

    @Test
    void softDeletesNormaliseToTombstones() {
        // A non-DELETE action carrying sccal.isDeleted:true is a soft-delete;
        // the stream path must delete such rows rather than apply them as updates.
        String sql = sync("ollylake.main.\"user\"").stageChangesSql("USER", 7L);
        // isDeleted/op are materialized in the inner projection; the CASE in the
        // outer projection reads those plain columns (never `e`).
        assertTrue(sql.contains("e->>'isDeleted' AS __is_deleted"), sql);
        assertTrue(sql.contains("e->>'op' AS __op"), sql);
        // Both delete conventions normalise to a tombstone: isDeleted (real) and op (generic).
        assertTrue(sql.contains(
            "CASE WHEN coalesce(__is_deleted::BOOLEAN, false) OR __op = 'delete'"), sql);
        assertTrue(sql.contains("THEN 'DELETE' ELSE action END AS __action"), sql);
    }

    @Test
    void dedupeKeepsTheLastEventPerKey() {
        String sql = sync("ollylake.main.\"user\"").changeDedupeSql();
        assertTrue(sql.contains("PARTITION BY customer_id, user_id ORDER BY __change_id DESC"), sql);
    }

    @Test
    void deleteOnlyAppliesTombstones() {
        assertEquals(
            "DELETE FROM ollylake.main.\"user\" t WHERE EXISTS "
                + "(SELECT 1 FROM cdc_chg_ollylake_main__user_ s "
                + "WHERE t.customer_id = s.customer_id AND t.user_id = s.user_id"
                + " AND s.__action = 'DELETE')",
            sync("ollylake.main.\"user\"").changeDeleteSql());
    }

    @Test
    void insertAndUpdateSkipTombstones() {
        String insert = sync("ollylake.main.\"user\"").changeInsertSql();
        assertTrue(insert.contains("s.__action <> 'DELETE'"), insert);
        assertTrue(insert.contains("NOT EXISTS"), insert);

        String update = sync("ollylake.main.\"user\"").changeUpdateSql();
        assertTrue(update.contains("s.__action <> 'DELETE'"), update);
        assertTrue(update.contains("t.name IS DISTINCT FROM s.name"), update);
    }

    @Test
    void changeStageHasStableNamePerTable() {
        SccalReferenceSync.Sync user = sync("ollylake.main.\"user\"");
        // The staging table name is derived from the target table, so it is
        // stable across cycles and CREATE OR REPLACE reuses it.
        assertTrue(user.createChangeStageSql().startsWith(
            "CREATE OR REPLACE TEMP TABLE cdc_chg_ollylake_main__user_ ("),
            user.createChangeStageSql());
    }
}
