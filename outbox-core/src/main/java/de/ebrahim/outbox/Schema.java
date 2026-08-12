package de.ebrahim.outbox;

import javax.sql.DataSource;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Applies {@code schema.sql}.
 *
 * <p>Intended for the service that owns the database, and for tests. The
 * standalone relay deliberately does not call this: when the relay is deployed
 * separately it treats the schema as a read contract owned by the service, so
 * that two independently released artifacts never race on migrations.
 */
public final class Schema {

    private Schema() { }

    public static void apply(DataSource dataSource) throws SQLException, IOException {
        String ddl = read("schema.sql");
        try (Connection c = dataSource.getConnection(); Statement st = c.createStatement()) {
            // Postgres' own IF NOT EXISTS is not safe against two callers applying
            // the schema at the same instant (e.g. several service replicas booting
            // together): both can see "doesn't exist" and race on the pg_type
            // catalog. A transaction-scoped advisory lock serializes them; it is
            // released automatically at the end of this statement's implicit
            // transaction, so there is no separate unlock to forget.
            st.execute("SELECT pg_advisory_xact_lock(hashtext('de.ebrahim.outbox.schema')); " + ddl);
        }
    }

    private static String read(String resource) throws IOException {
        try (InputStream in = Schema.class.getResourceAsStream(resource)) {
            if (in == null) throw new IOException("classpath resource not found: " + resource);
            return new String(in.readAllBytes(), StandardCharsets.UTF_8);
        }
    }
}
