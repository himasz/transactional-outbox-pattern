package de.ebrahim.inbox.store;

import javax.sql.DataSource;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Applies {@code inbox-schema.sql}.
 *
 * <p>Intended for the consuming service, which owns the database the inbox lives
 * in, and for tests. Unlike the outbox there is no separate deployment that
 * might race on this migration — the inbox has no relay — but replicas of the
 * consumer itself boot together, so the advisory lock is still required.
 */
public final class InboxSchema {

    private InboxSchema() { }

    public static void apply(DataSource dataSource) throws SQLException, IOException {
        String ddl = read("inbox-schema.sql");
        try (Connection c = dataSource.getConnection(); Statement st = c.createStatement()) {
            // PostgreSQL's own IF NOT EXISTS is not safe against two callers
            // applying the schema at the same instant: both can see "doesn't
            // exist" and race on the pg_type catalog. A transaction-scoped
            // advisory lock serialises them and is released automatically at the
            // end of this statement's implicit transaction.
            st.execute("SELECT pg_advisory_xact_lock(hashtext('de.ebrahim.inbox.schema')); " + ddl);
        }
    }

    private static String read(String resource) throws IOException {
        try (InputStream in = InboxSchema.class.getResourceAsStream(resource)) {
            if (in == null) throw new IOException("classpath resource not found: " + resource);
            return new String(in.readAllBytes(), StandardCharsets.UTF_8);
        }
    }
}
