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
    void userIsStreamFedAndOutOfTheSnapshotSet() {
        assertTrue(SccalReferenceSync.STREAM_SYNCS.stream()
            .anyMatch(s -> s.objectType().equals("user")));
        assertTrue(SccalReferenceSync.SNAPSHOT_SYNCS.stream()
            .noneMatch(s -> s.objectType().equals("user")));
        // Everything else still snapshots.
        assertEquals(SccalReferenceSync.SYNCS.size() - SccalReferenceSync.STREAM_SYNCS.size(),
            SccalReferenceSync.SNAPSHOT_SYNCS.size());
    }

    @Test
    void stageChangesExtractsSccalPayloadActionAndChangeId() {
        String sql = sync("ollylake.main.\"user\"").stageChangesSql("USER");

        assertTrue(sql.startsWith(
            "INSERT INTO cdc_chg_ollylake_main__user_ (user_id, name, __action, __change_id)"), sql);
        assertTrue(sql.contains("unnest(json_extract(?::JSON, '$.entries[*]'))"), sql);
        assertTrue(sql.contains("entry->'sccal' AS e"), sql);
        assertTrue(sql.contains("entry->>'entityType' = 'USER'"), sql);
        assertTrue(sql.contains("(e->>'userId')::BIGINT"), sql);
        assertTrue(sql.contains("(e->>'userName')::VARCHAR"), sql);
    }

    @Test
    void stageChangesAppendsTheSyncFilterButExemptsTombstones() {
        // RULE fan-out (future stream type): the ruleType filter must survive,
        // but a DELETE tombstone whose minimal payload omits ruleType must not
        // be dropped by it (a NULL predicate would silently lose the delete).
        assertTrue(sync("ollylake.main.llm_access_rule").stageChangesSql("RULE")
            .endsWith("WHERE (__action = 'DELETE' OR (e->>'ruleType')::INTEGER IN (0, 1))"));
    }

    @Test
    void softDeletesNormaliseToTombstones() {
        // A non-DELETE action carrying sccal.isDeleted:true is a soft-delete;
        // the snapshot path drops such rows, so the stream path must delete them.
        String sql = sync("ollylake.main.\"user\"").stageChangesSql("USER");
        assertTrue(sql.contains(
            "CASE WHEN coalesce((entry->'sccal'->>'isDeleted')::BOOLEAN, false)"), sql);
        assertTrue(sql.contains("THEN 'DELETE' ELSE entry->>'action' END AS __action"), sql);
    }

    @Test
    void dedupeKeepsTheLastEventPerKey() {
        String sql = sync("ollylake.main.\"user\"").changeDedupeSql();
        assertTrue(sql.contains("PARTITION BY user_id ORDER BY __change_id DESC"), sql);
    }

    @Test
    void deleteOnlyAppliesTombstones() {
        assertEquals(
            "DELETE FROM ollylake.main.\"user\" t WHERE EXISTS "
                + "(SELECT 1 FROM cdc_chg_ollylake_main__user_ s "
                + "WHERE t.user_id = s.user_id AND s.__action = 'DELETE')",
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
    void changeStageIsDistinctFromSnapshotStage() {
        SccalReferenceSync.Sync user = sync("ollylake.main.\"user\"");
        // Both temp tables can exist in one cycle (bootstrap uses the snapshot
        // stage; the next poll uses the change stage on the same connection).
        assertTrue(user.createChangeStageSql().startsWith(
            "CREATE OR REPLACE TEMP TABLE cdc_chg_ollylake_main__user_ ("),
            user.createChangeStageSql());
    }
}
