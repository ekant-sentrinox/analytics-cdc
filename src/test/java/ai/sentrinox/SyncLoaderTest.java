package ai.sentrinox;

import ai.sentrinox.SccalReferenceSync.Sync;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for {@link SccalReferenceSync#loadSyncs}: the shipped application.conf
 * declarations, and the fail-fast validation of malformed entries (every
 * config value spliced into generated SQL must be shape-checked at load).
 */
class SyncLoaderTest {

    // ---- the shipped declarations ------------------------------------------

    @Test
    void applicationConfDeclaresTheFiveReferenceSyncs() {
        assertEquals(
            List.of("ollylake.main.workspace", "ollylake.main.\"user\"",
                "ollylake.main.\"group\"", "ollylake.main.llm_access_rule",
                "ollylake.main.mcp_access_rule"),
            SccalReferenceSync.SYNCS.stream().map(Sync::table).toList());
    }

    @Test
    void ruleFansOutToBothAccessRuleTablesByPayloadFilter() {
        List<Sync> rules = SccalReferenceSync.SYNCS.stream()
            .filter(s -> s.entityType().equals("RULE"))
            .toList();
        assertEquals(2, rules.size());
        for (Sync s : rules) {
            assertTrue(s.entryFilterSql().contains("ruleType"),
                s.table() + " must split RULE by ruleType, got: " + s.entryFilterSql());
        }
    }

    @Test
    void columnsCoalesceTheRealShapeBeforeTheGenericShape() {
        Sync user = SccalReferenceSync.SYNCS.stream()
            .filter(s -> s.table().equals("ollylake.main.\"user\""))
            .findFirst().orElseThrow();
        // The most-specific (real Sentrinox) field must be declared first so
        // coalesce prefers it over the generic shape.
        assertTrue(user.stageChangesSql(1L)
            .contains("coalesce(e->>'userId', e->>'id') AS user_id"));
    }

    // ---- validation ---------------------------------------------------------

    private static final String VALID = """
        { entity_type = "USER", table = "ollylake.main.t",
          key_columns = [{ column = "id", json_fields = ["userId", "id"], type = "BIGINT" }],
          data_columns = [{ column = "name", json_fields = ["name"], type = "VARCHAR" }] }""";

    private static List<Sync> load(String... entries) {
        Config config = ConfigFactory.parseString(
            "syncs = [" + String.join(",\n", entries) + "]");
        return SccalReferenceSync.loadSyncs(config);
    }

    @Test
    void loadsAValidEntry() {
        Sync s = load(VALID).get(0);
        assertEquals("USER", s.entityType());
        assertEquals("ollylake.main.t", s.table());
        assertEquals(1, s.keyCols().size());
        assertEquals(1, s.dataCols().size());
        assertNull(s.entryFilterSql());
    }

    @Test
    void dataColumnsAndEntryFilterAreOptional() {
        Sync s = load("""
            { entity_type = "EDGE", table = "ollylake.main.edge",
              key_columns = [{ column = "a", json_fields = ["a"], type = "BIGINT" },
                             { column = "b", json_fields = ["b"], type = "BIGINT" }] }""")
            .get(0);
        assertTrue(s.dataCols().isEmpty());
        assertNull(s.entryFilterSql());
    }

    @Test
    void rejectsAnEmptySyncsList() {
        assertThrows(IllegalArgumentException.class, () -> load());
    }

    @Test
    void rejectsADuplicateTable() {
        assertThrows(IllegalArgumentException.class, () -> load(VALID, VALID));
    }

    @Test
    void rejectsSqlSplicingInIdentifiers() {
        // Each of these values is spliced into generated SQL — a value that
        // escapes its strict shape must abort the load.
        assertThrows(IllegalArgumentException.class,
            () -> load(VALID.replace("ollylake.main.t", "ollylake.main.t; DROP TABLE x")));
        assertThrows(IllegalArgumentException.class,
            () -> load(VALID.replace("column = \"id\"", "column = \"id, evil\"")));
        assertThrows(IllegalArgumentException.class,
            () -> load(VALID.replace("\"userId\"", "\"userId'--\"")));
        assertThrows(IllegalArgumentException.class,
            () -> load(VALID.replace("BIGINT", "BIGINT; DROP TABLE x")));
    }

    @Test
    void rejectsMissingKeyColumnsAndStatementSplicingInEntryFilter() {
        assertThrows(IllegalArgumentException.class,
            () -> load(VALID.replace("key_columns = [{ column = \"id\","
                + " json_fields = [\"userId\", \"id\"], type = \"BIGINT\" }]", "key_columns = []")));
        assertThrows(IllegalArgumentException.class,
            () -> load(VALID.replace("data_columns",
                "entry_filter = \"1 = 1; DROP TABLE x\", data_columns")));
    }
}
