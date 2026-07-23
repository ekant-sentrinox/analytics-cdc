package ai.sentrinox;

import ai.sentrinox.CatalogueListSync.CatalogueSync;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for {@link CatalogueListSync#loadCatalogueSyncs} and
 * {@link CatalogueListSync#loadContext}: the shipped application.conf
 * declarations, the generated /list staging SQL, and the fail-fast validation
 * of malformed entries (every config value spliced into generated SQL or a
 * request URL must be shape-checked at load).
 */
class CatalogueSyncLoaderTest {

    // ---- the shipped declarations ------------------------------------------

    @Test
    void applicationConfDeclaresTheFiveCatalogueSyncsInFkOrder() {
        // Declaration order is apply order — referenced-before-referencing:
        // providers before models, MCP servers before tools, tenant last.
        assertEquals(
            List.of("ollylake.main.provider", "ollylake.main.model",
                "ollylake.main.mcp_server", "ollylake.main.mcp_tool",
                "ollylake.main.tenant"),
            CatalogueListSync.SYNCS.stream().map(CatalogueSync::table).toList());
    }

    @Test
    void onlyTenantIsPerTenant() {
        for (CatalogueSync s : CatalogueListSync.SYNCS) {
            assertEquals("tenant".equals(s.objectType()), s.perTenant(),
                s.objectType() + " per_tenant flag");
        }
    }

    @Test
    void shippedContextIsLoaded() {
        assertFalse(CatalogueListSync.CONTEXT.customerId().isBlank());
        assertFalse(CatalogueListSync.CONTEXT.tenantId().isBlank());
    }

    @Test
    void globalStagingShredsTheListMapWithoutACustomerStamp() {
        CatalogueSync model = CatalogueListSync.SYNCS.stream()
            .filter(s -> s.objectType().equals("modelcatalogue")).findFirst().orElseThrow();
        String sql = model.stageListSql(0L);

        // The /list body is a map id → entity, shredded via the $.* wildcard;
        // the inner projection extracts raw text, the outer one casts it.
        assertTrue(sql.contains("unnest(json_extract(?::JSON, '$.*')) AS e"), sql);
        assertTrue(sql.contains("e->>'modelCatalogueId' AS model_id"), sql);
        assertTrue(sql.contains("TRY_CAST(model_id AS BIGINT) AS model_id"), sql);
        // Global tables carry no customer_id.
        assertFalse(sql.contains("customer_id"), sql);
        assertFalse(model.createStageSql().contains("customer_id"), sql);
    }

    @Test
    void tenantStagingStampsTheCustomerIdFromThePollContext() {
        CatalogueSync tenant = CatalogueListSync.SYNCS.stream()
            .filter(CatalogueSync::perTenant).findFirst().orElseThrow();
        String sql = tenant.stageListSql(7L);

        assertTrue(sql.contains("(customer_id, tenant_id, name, __is_deleted) SELECT 7, "), sql);
        assertTrue(tenant.insertSql().contains("customer_id, tenant_id, name"),
            tenant.insertSql());
        assertTrue(tenant.softDeleteSql().contains("t.customer_id = s.customer_id"),
            tenant.softDeleteSql());
    }

    // ---- validation ---------------------------------------------------------

    private static final String VALID = """
        { object_type = "modelcatalogue", table = "ollylake.main.t",
          key_columns = [{ column = "id", json_fields = ["modelCatalogueId"], type = "BIGINT" }],
          data_columns = [{ column = "name", json_fields = ["name"], type = "VARCHAR" }] }""";

    private static List<CatalogueSync> load(String... entries) {
        Config config = ConfigFactory.parseString(
            "catalogue_syncs = [" + String.join(",\n", entries) + "]");
        return CatalogueListSync.loadCatalogueSyncs(config);
    }

    @Test
    void loadsAValidEntry() {
        CatalogueSync s = load(VALID).get(0);
        assertEquals("modelcatalogue", s.objectType());
        assertEquals("ollylake.main.t", s.table());
        assertFalse(s.perTenant());
    }

    @Test
    void rejectsAnEmptyListAndDuplicateTables() {
        assertThrows(IllegalArgumentException.class, () -> load());
        assertThrows(IllegalArgumentException.class, () -> load(VALID, VALID));
    }

    @Test
    void rejectsAMalformedObjectType() {
        // The objectType is spliced into the request path — lowercase only.
        assertThrows(IllegalArgumentException.class,
            () -> load(VALID.replace("modelcatalogue", "MODELCATALOGUE")));
        assertThrows(IllegalArgumentException.class,
            () -> load(VALID.replace("modelcatalogue", "model/../../etc")));
    }

    @Test
    void rejectsSqlSplicingInIdentifiers() {
        assertThrows(IllegalArgumentException.class,
            () -> load(VALID.replace("ollylake.main.t", "ollylake.main.t; DROP TABLE x")));
        assertThrows(IllegalArgumentException.class,
            () -> load(VALID.replace("column = \"id\"", "column = \"id, evil\"")));
        assertThrows(IllegalArgumentException.class,
            () -> load(VALID.replace("BIGINT", "BIGINT; DROP TABLE x")));
    }

    @Test
    void rejectsMissingKeyColumns() {
        assertThrows(IllegalArgumentException.class,
            () -> load(VALID.replace("key_columns = [{ column = \"id\","
                + " json_fields = [\"modelCatalogueId\"], type = \"BIGINT\" }]",
                "key_columns = []")));
    }

    @Test
    void rejectsAContextValueThatEscapesTheUrl() {
        assertThrows(IllegalArgumentException.class, () -> CatalogueListSync.loadContext(
            ConfigFactory.parseString(
                "catalogue_context { customer_id = \"a&b=c\", tenant_id = \"1\" }")));
    }
}
