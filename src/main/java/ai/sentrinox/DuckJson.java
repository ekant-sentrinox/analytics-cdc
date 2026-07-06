package ai.sentrinox;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * DuckDB-as-JSON-parser helpers. Raw API bodies are bound as a {@code JSON}
 * parameter and shredded in SQL, so the job needs no JSON library beyond the
 * DuckDB driver already on the classpath. Every place that binds a body to a
 * statement goes through here.
 */
final class DuckJson {

    /** Maps one result row to a value; used by {@link #queryAll}. */
    @FunctionalInterface
    interface RowMapper<T> {
        T map(ResultSet rs) throws SQLException;
    }

    /** The offset-paging fields every SCCAL page carries. See {@link #extractPage}. */
    record Page(long nextOffset, boolean hasMore) {
    }

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

    /** Map every row of a query whose single parameter is a raw JSON body. */
    static <T> List<T> queryAll(Connection conn, String sql, String json, RowMapper<T> mapper)
            throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, json);
            try (ResultSet rs = ps.executeQuery()) {
                List<T> rows = new ArrayList<>();
                while (rs.next()) {
                    rows.add(mapper.map(rs));
                }
                return rows;
            }
        }
    }

    /**
     * The {@code nextOffset} (falling back to {@code fallbackOffset} when
     * absent/null) and {@code hasMore} (false when absent/null) of a paged body —
     * parsed in a single statement, so the body is shredded once rather than once
     * per field.
     */
    static Page extractPage(Connection conn, String json, long fallbackOffset) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(
            "SELECT coalesce((j->>'nextOffset')::BIGINT, ?),"
                + " coalesce((j->>'hasMore')::BOOLEAN, false)"
                + " FROM (SELECT ?::JSON AS j)")) {
            ps.setLong(1, fallbackOffset);
            ps.setString(2, json);
            try (ResultSet rs = ps.executeQuery()) {
                rs.next();
                return new Page(rs.getLong(1), rs.getBoolean(2));
            }
        }
    }

    private DuckJson() {
    }
}
