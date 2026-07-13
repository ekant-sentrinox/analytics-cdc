package ai.sentrinox;

import com.typesafe.config.Config;
import io.dazzleduck.sql.common.StartupScriptProvider;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

/** Helpers for working with multi-statement SQL scripts. */
final class SqlScripts {

    /**
     * Open an in-memory DuckDB connection and run the config's startup script —
     * INSTALL/LOAD the DuckLake + S3 extensions, CREATE the MinIO secret and
     * ATTACH the catalog. Both entry points share this exact bootstrap. The
     * connection is returned open for the caller to close; if the script fails
     * it is closed before the error propagates, so it never leaks.
     */
    static Connection bootstrap(Config config) throws Exception {
        String startupScript = StartupScriptProvider.load(config).getStartupScript();
        Connection conn = DriverManager.getConnection("jdbc:duckdb:");
        try (Statement st = conn.createStatement()) {
            for (String stmt : splitStatements(startupScript)) {
                st.execute(stmt);
            }
            return conn;
        } catch (Throwable e) {
            try {
                conn.close();
            } catch (Exception closeError) {
                e.addSuppressed(closeError);
            }
            throw e;
        }
    }

    /** Body of a lake transaction; returns true to commit, false to roll back. */
    interface TxnBody {
        boolean run() throws SQLException;
    }

    /**
     * Run {@code body} in a transaction: autoCommit off, commit or roll back
     * per the body's return value, roll back on a throw, always restore
     * autoCommit.
     */
    static void inTransaction(Connection conn, TxnBody body) throws SQLException {
        conn.setAutoCommit(false);
        try {
            if (body.run()) {
                conn.commit();
            } else {
                conn.rollback();
            }
        } catch (SQLException | RuntimeException e) {
            try {
                conn.rollback();
            } catch (SQLException ignored) {
                // Connection unusable — the original error propagates.
            }
            try {
                conn.setAutoCommit(true);
            } catch (SQLException suppressed) {
                // A restore failure on a dead connection must not replace the
                // original error (a bare finally would) — attach it instead.
                e.addSuppressed(suppressed);
            }
            throw e;
        }
        conn.setAutoCommit(true);
    }

    /**
     * Split a multi-statement SQL script on {@code ';'}, aware of
     * single-quoted literals (with {@code ''} escaping) so a semicolon inside
     * a quoted value — e.g. a MinIO password in {@code CREATE SECRET} — is not
     * a terminator. Line comments are stripped first; blank statements dropped.
     */
    static List<String> splitStatements(String sql) {
        List<String> statements = new ArrayList<>();
        StringBuilder current = new StringBuilder();
        boolean inString = false;
        boolean inLineComment = false;

        for (int i = 0; i < sql.length(); i++) {
            char c = sql.charAt(i);

            if (inLineComment) {
                if (c == '\n') {
                    inLineComment = false;
                    current.append(c);
                }
                continue;
            }
            if (inString) {
                current.append(c);
                if (c == '\'') {
                    // A doubled '' is an escaped quote — stay inside the string.
                    if (i + 1 < sql.length() && sql.charAt(i + 1) == '\'') {
                        current.append(sql.charAt(++i));
                    } else {
                        inString = false;
                    }
                }
                continue;
            }
            if (c == '-' && i + 1 < sql.length() && sql.charAt(i + 1) == '-') {
                inLineComment = true;
                i++;                       // also consume the second '-'
                continue;
            }
            if (c == '\'') {
                inString = true;
                current.append(c);
                continue;
            }
            if (c == ';') {
                addStatement(statements, current);
                current.setLength(0);
                continue;
            }
            current.append(c);
        }
        addStatement(statements, current);
        return statements;
    }

    private static void addStatement(List<String> statements, StringBuilder sb) {
        String statement = sb.toString().trim();
        if (!statement.isBlank()) {
            statements.add(statement);
        }
    }

    private SqlScripts() {
    }
}
