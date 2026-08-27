package de.ebrahim.saga;

import javax.sql.DataSource;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Applies {@code saga-schema.sql} — the participants' business tables only.
 *
 * <p>The outbox and inbox schemas are applied separately by whoever owns the
 * database, exactly as they are in the other examples. Three small migrations
 * with three clear owners, rather than one that quietly couples the saga's
 * domain to the messaging libraries' internals.
 */
public final class SagaSchema {

    private SagaSchema() { }

    public static void apply(DataSource dataSource) throws SQLException, IOException {
        String ddl = read("saga-schema.sql");
        try (Connection c = dataSource.getConnection(); Statement st = c.createStatement()) {
            // Four participants boot together, so IF NOT EXISTS alone does not
            // serialise them — they can all see "doesn't exist" and race on the
            // catalog. Transaction-scoped advisory lock, released automatically.
            st.execute("SELECT pg_advisory_xact_lock(hashtext('de.ebrahim.saga.schema')); " + ddl);
        }
    }

    private static String read(String resource) throws IOException {
        try (InputStream in = SagaSchema.class.getResourceAsStream(resource)) {
            if (in == null) throw new IOException("classpath resource not found: " + resource);
            return new String(in.readAllBytes(), StandardCharsets.UTF_8);
        }
    }
}
