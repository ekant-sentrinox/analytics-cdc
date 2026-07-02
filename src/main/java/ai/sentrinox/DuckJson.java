package ai.sentrinox;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * DuckDB-as-JSON-parser helpers. Raw API bodies are bound as a {@code JSON}
 * parameter and shredded in SQL, so the job needs no JSON library beyond the
 * DuckDB driver already on the classpath. Every place that binds a body to a
 * statement goes through here.
 */
final class DuckJson {

    /** Run a DML statement whose single parameter is a raw JSON body; returns the row count. */
    static int executeWithJson(Connection conn, String sql, String json) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, json);
            return ps.executeUpdate();
        }
    }

    /** Single-row long out of a query whose one parameter is a raw JSON body. */
    static long queryLong(Connection conn, String sql, String json) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, json);
            try (ResultSet rs = ps.executeQuery()) {
                rs.next();
                return rs.getLong(1);
            }
        }
    }

    /** Top-level scalar out of a JSON body, with a fallback for absent/null. */
    static long extractLong(Connection conn, String json, String field, long fallback)
            throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(
            "SELECT (?::JSON->>'" + field + "')::BIGINT")) {
            ps.setString(1, json);
            try (ResultSet rs = ps.executeQuery()) {
                rs.next();
                long v = rs.getLong(1);
                return rs.wasNull() ? fallback : v;
            }
        }
    }

    /** Top-level boolean out of a JSON body; absent/null reads as false. */
    static boolean extractBool(Connection conn, String json, String field) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(
            "SELECT coalesce((?::JSON->>'" + field + "')::BOOLEAN, false)")) {
            ps.setString(1, json);
            try (ResultSet rs = ps.executeQuery()) {
                rs.next();
                return rs.getBoolean(1);
            }
        }
    }

    private DuckJson() {
    }
}
