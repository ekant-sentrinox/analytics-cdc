package ai.sentrinox;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.Statement;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Stream;

/**
 * Creates the DuckLake tables defined in {@code ollylake/init} using the DuckDB
 * JDBC driver.
 *
 * <p>Connects to an in-memory DuckDB, attaches a DuckLake catalog named
 * {@code ollylake} whose data files live in MinIO (S3) and whose metadata is a
 * persisted {@code .ducklake} file, then runs every {@code .sql} file in the
 * init directory in lexical order.
 */
public final class OllylakeSchemaInitializer {

    public static void main(String[] args) throws Exception {
        Path initDir = Paths.get(env("INIT_DIR", "/init"));

        // INSTALL/LOAD the DuckLake + S3 extensions, CREATE the MinIO secret and
        // ATTACH the catalog — all from the shared startup script in the config.
        Config config = ConfigFactory.load().getConfig("analytics_cdc");

        try (Connection conn = SqlScripts.bootstrap(config);
             Statement st = conn.createStatement()) {

            for (Path file : sqlFiles(initDir)) {
                System.out.println("-- applying " + file.getFileName());
                for (String stmt : SqlScripts.splitStatements(Files.readString(file))) {
                    st.execute(stmt);
                }
            }
        }

        System.out.println("ollylake tables created.");
    }

    private static List<Path> sqlFiles(Path dir) throws Exception {
        try (Stream<Path> stream = Files.list(dir)) {
            return stream
                .filter(p -> p.getFileName().toString().endsWith(".sql"))
                // Order by numeric migration version, not lexically: a plain string
                // sort runs "V10__" before "V2__"/"V6__", applying migrations out of
                // dependency order once a two-digit version appears.
                .sorted(Comparator.comparingInt(OllylakeSchemaInitializer::migrationVersion)
                    .thenComparing(p -> p.getFileName().toString()))
                .toList();
        }
    }

    /** Numeric version from a {@code V<n>__...} migration name; un-versioned files sort last. */
    private static int migrationVersion(Path file) {
        String name = file.getFileName().toString();
        if (name.isEmpty() || (name.charAt(0) != 'V' && name.charAt(0) != 'v')) {
            return Integer.MAX_VALUE;
        }
        int i = 1;
        while (i < name.length() && Character.isDigit(name.charAt(i))) {
            i++;
        }
        if (i == 1) {
            return Integer.MAX_VALUE;        // no digits after the leading 'V'
        }
        try {
            return Integer.parseInt(name.substring(1, i));
        } catch (NumberFormatException overflow) {
            return Integer.MAX_VALUE;
        }
    }

    private static String env(String key, String fallback) {
        String value = System.getenv(key);
        return (value == null || value.isBlank()) ? fallback : value;
    }

    private OllylakeSchemaInitializer() {
    }
}
