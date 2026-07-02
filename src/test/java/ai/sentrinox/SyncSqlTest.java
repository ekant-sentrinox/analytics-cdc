package ai.sentrinox;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for the SQL the {@link SccalReferenceSync.Sync} records generate.
 * Pure string assertions — no DB, no network.
 */
class
SyncSqlTest {

    private static SccalReferenceSync.Sync sync(String table) {
        return SccalReferenceSync.SYNCS.stream()
            .filter(s -> s.table().equals(table))
            .findFirst()
            .orElseThrow(() -> new AssertionError("no sync for " + table));
    }

    @Test
    void stageSqlExtractsAndCastsColumnsAndFiltersDeleted() {
        String sql = sync("ollylake.main.\"user\"").stageSql();

        assertTrue(sql.startsWith("INSERT INTO cdc_stg_ollylake_main__user_ (user_id, name) SELECT "), sql);
        assertTrue(sql.contains("(e->>'userId')::BIGINT"), sql);
        assertTrue(sql.contains("(e->>'userName')::VARCHAR"), sql);
        assertTrue(sql.contains("unnest(json_extract(?::JSON, '$.*'))"), sql);
        assertTrue(sql.contains("coalesce((e->>'isDeleted')::BOOLEAN, false) = false"), sql);
        // No object-type filter for plain user.
        assertFalse(sql.contains("ruleType"), sql);
    }

    @Test
    void ruleSyncsAppendTheirRuleTypeFilter() {
        // ruleType enum: 0 = TenantProvider, 1 = WorkspaceProvider (both LLM),
        // 2 = TenantMcp, 3 = WorkspaceMcp (both MCP).
        assertTrue(sync("ollylake.main.llm_access_rule").stageSql()
            .endsWith("AND (e->>'ruleType')::INTEGER IN (0, 1)"));
        assertTrue(sync("ollylake.main.mcp_access_rule").stageSql()
            .endsWith("AND (e->>'ruleType')::INTEGER IN (2, 3)"));
    }

    @Test
    void deleteRemovesRowsAbsentFromStaging() {
        assertEquals(
            "DELETE FROM ollylake.main.\"user\" t WHERE NOT EXISTS "
                + "(SELECT 1 FROM cdc_stg_ollylake_main__user_ s WHERE t.user_id = s.user_id)",
            sync("ollylake.main.\"user\"").deleteSql());
    }

    @Test
    void insertAddsRowsMissingFromTarget() {
        assertEquals(
            "INSERT INTO ollylake.main.\"user\" (user_id, name) SELECT user_id, name "
                + "FROM cdc_stg_ollylake_main__user_ s WHERE NOT EXISTS "
                + "(SELECT 1 FROM ollylake.main.\"user\" t WHERE t.user_id = s.user_id)",
            sync("ollylake.main.\"user\"").insertSql());
    }

    @Test
    void updateOnlyTouchesChangedAttributes() {
        String sql = sync("ollylake.main.\"user\"").updateSql();
        assertTrue(sql.contains("SET name = s.name"), sql);
        assertTrue(sql.contains("t.user_id = s.user_id"), sql);
        assertTrue(sql.contains("t.name IS DISTINCT FROM s.name"), sql);
    }

    @Test
    void edgeTableHasCompositeKeyAndNoDataColumns() {
        SccalReferenceSync.Sync edge = sync("ollylake.main.user_group_mapping");

        assertEquals(2, edge.keyCols().size());
        assertTrue(edge.dataCols().isEmpty());
        // Composite key joins on both columns.
        assertTrue(edge.deleteSql().contains("t.user_id = s.user_id AND t.group_id = s.group_id"),
            edge.deleteSql());
    }

    @Test
    void dedupeOrdersByAllColumnsForDeterministicSurvivor() {
        // ORDER BY must span all columns (not just the partition keys), otherwise
        // every row in a partition ties and row_number()=1 keeps an arbitrary one.
        String sql = sync("ollylake.main.\"user\"").dedupeSql();
        assertTrue(sql.contains("PARTITION BY user_id ORDER BY user_id, name"), sql);
    }

    @Test
    void providerUsesTheSmallIntegerTypeEnum() {
        String sql = sync("ollylake.main.provider").stageSql();
        assertTrue(sql.contains("(e->>'type')::INTEGER"), sql);
    }
}
