import org.example.EpoxyCoordinator;
import org.example.MongoDBShim;
import org.example.PostgresShim;
import org.example.TransactionContext;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;

import static org.junit.Assert.*;

public class EpoxyTest {
    private EpoxyCoordinator coordinator;
    private PostgresShim postgresShim;
    private MongoDBShim mongoDBShim;

    @Before
    public void setUp() throws Exception {
        coordinator = new EpoxyCoordinator("jdbc:postgresql://localhost:5432/epoxy_test", "postgres", "test987");
        postgresShim = new PostgresShim("jdbc:postgresql://localhost:5432/epoxy_test", "postgres", "test987");
        mongoDBShim = new MongoDBShim("mongodb://localhost:27017", "epoxy_test");

        try (Connection conn = DriverManager.getConnection("jdbc:postgresql://localhost:5432/epoxy_test", "postgres", "test987");
             Statement stmt = conn.createStatement()) {
            stmt.executeUpdate("DROP TABLE IF EXISTS epoxy_data");
            stmt.executeUpdate("CREATE TABLE IF NOT EXISTS epoxy_data (" +
                "id SERIAL PRIMARY KEY, " +
                "key VARCHAR(255) NOT NULL UNIQUE, " +
                "value VARCHAR(255) NOT NULL, " +
                "begin_txn BIGINT NOT NULL, " +
                "end_txn BIGINT NOT NULL)");
        }
    }

    @Test
    public void testCrossDataStoreTransaction() throws Exception {
        TransactionContext txn = coordinator.beginTransaction();

        postgresShim.update(txn, "key1", "value1");
        mongoDBShim.update(txn, "key2", "value2");

        coordinator.commitTransaction(txn);

        txn = coordinator.beginTransaction();
        String value1 = postgresShim.query(txn, "key1");
        String value2 = mongoDBShim.query(txn, "key2");

        assertEquals("value1", value1);
        assertEquals("value2", value2);
    }
}
