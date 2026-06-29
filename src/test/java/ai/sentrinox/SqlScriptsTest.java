package ai.sentrinox;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for {@link SqlScripts#splitStatements}: top-level splitting plus
 * the quote- and comment-awareness that keeps a ';' inside a value (e.g. a
 * CREATE SECRET password) from being treated as a statement terminator.
 */
class SqlScriptsTest {

    @Test
    void splitsOnTopLevelSemicolons() {
        List<String> stmts = SqlScripts.splitStatements(
            "CREATE TABLE a (x INT); CREATE TABLE b (y INT);");
        assertEquals(List.of("CREATE TABLE a (x INT)", "CREATE TABLE b (y INT)"), stmts);
    }

    @Test
    void semicolonInsideQuotedValueDoesNotSplit() {
        // A CREATE SECRET whose substituted password/endpoint contains ';'.
        List<String> stmts = SqlScripts.splitStatements(
            "CREATE SECRET s (TYPE s3, SECRET 'pa;ss;word'); USE ollylake;");
        assertEquals(2, stmts.size(), stmts.toString());
        assertEquals("CREATE SECRET s (TYPE s3, SECRET 'pa;ss;word')", stmts.get(0));
        assertEquals("USE ollylake", stmts.get(1));
    }

    @Test
    void doubledQuoteIsEscapedNotStringEnd() {
        List<String> stmts = SqlScripts.splitStatements("SELECT 'O''Brien; Co'; SELECT 2;");
        assertEquals(2, stmts.size(), stmts.toString());
        assertEquals("SELECT 'O''Brien; Co'", stmts.get(0));
        assertEquals("SELECT 2", stmts.get(1));
    }

    @Test
    void stripsFullLineAndInlineComments() {
        String sql = "-- a comment with ; in it\n"
            + "CREATE TABLE a (x INT); -- trailing comment with ; too\n"
            + "CREATE TABLE b (y INT);";
        List<String> stmts = SqlScripts.splitStatements(sql);
        assertEquals(2, stmts.size(), stmts.toString());
        // Inline comment is removed and its ';' does not cause a spurious split.
        assertEquals("CREATE TABLE a (x INT)", stmts.get(0));
        assertEquals("CREATE TABLE b (y INT)", stmts.get(1));
    }

    @Test
    void dropsBlankAndCommentOnlyStatements() {
        List<String> stmts = SqlScripts.splitStatements("  ;\n-- only a comment\n;\nSELECT 1;");
        assertEquals(List.of("SELECT 1"), stmts);
    }

    @Test
    void finalStatementWithoutTrailingSemicolonIsKept() {
        List<String> stmts = SqlScripts.splitStatements("SELECT 1;\nSELECT 2");
        assertEquals(List.of("SELECT 1", "SELECT 2"), stmts);
    }
}
