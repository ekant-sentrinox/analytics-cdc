package ai.sentrinox;

import java.util.ArrayList;
import java.util.List;

/** Helpers for working with multi-statement SQL scripts. */
final class SqlScripts {

    /**
     * Split a multi-statement SQL script into individual statements.
     *
     * <p>Splits on {@code ';'} but is aware of single-quoted string literals
     * (with {@code ''} escaping), so a semicolon inside a quoted value — e.g. a
     * MinIO password or S3 endpoint substituted into {@code CREATE SECRET} — is
     * not mistaken for a statement terminator. Line comments ({@code -- ...} to
     * end of line) are stripped, including comments trailing a statement on the
     * same line, before splitting. Blank statements are dropped.
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
