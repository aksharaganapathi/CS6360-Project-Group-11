package org.example.shims;

import java.sql.*;
import java.util.concurrent.ConcurrentHashMap;

import org.example.TransactionContext;

public class PostgresShim implements DataStoreShim {
    private Connection connection;
    private ConcurrentHashMap<String, Object> locks;

    public PostgresShim(String jdbcUrl, String username, String password) throws SQLException {
        this.connection = DriverManager.getConnection(jdbcUrl, username, password);
        this.locks = new ConcurrentHashMap<>();
    }

    @Override
    public void update(TransactionContext txn, String key, String value) {
        locks.computeIfAbsent(key, k -> new Object());
        synchronized (locks.get(key)) {
            try {
                String sql = "INSERT INTO epoxy_data (key, value, begin_txn, end_txn) VALUES (?, ?, ?, ?) " +
                             "ON CONFLICT (key) DO UPDATE SET value = excluded.value, begin_txn = excluded.begin_txn, end_txn = excluded.end_txn";
                try (PreparedStatement stmt = connection.prepareStatement(sql)) {
                    stmt.setString(1, key);
                    stmt.setString(2, value);
                    stmt.setLong(3, txn.getTxnId());
                    stmt.setLong(4, Long.MAX_VALUE);
                    stmt.executeUpdate();
                }
                txn.addModifiedKey(this, key);
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    public String query(TransactionContext txn, String key) {
        try {
            String sql = "SELECT value FROM epoxy_data WHERE key = ? AND begin_txn <= ? AND (end_txn > ? OR end_txn = ?) " +
                         "AND (begin_txn < ? OR begin_txn = ANY (?)) ORDER BY begin_txn DESC LIMIT 1";
            try (PreparedStatement stmt = connection.prepareStatement(sql)) {
                stmt.setString(1, key);
                stmt.setLong(2, txn.getTxnId());
                stmt.setLong(3, txn.getXmin());
                stmt.setLong(4, Long.MAX_VALUE);
                stmt.setLong(5, txn.getXmin());
                Array rcTxnsArray = connection.createArrayOf("BIGINT", txn.getRcTxns().toArray());
                stmt.setArray(6, rcTxnsArray);
                ResultSet rs = stmt.executeQuery();
                if (rs.next()) {
                    return rs.getString("value");
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return null;
    }

    @Override
    public boolean validateTransaction(TransactionContext txn) {
        try {
            String sql = "SELECT 1 FROM epoxy_data WHERE key = ANY(?) AND begin_txn >= ? AND begin_txn != ALL(?)";
            try (PreparedStatement stmt = connection.prepareStatement(sql)) {
                Array keysArray = connection.createArrayOf("VARCHAR", txn.getModifiedKeys(this).toArray());
                stmt.setArray(1, keysArray);
                stmt.setLong(2, txn.getXmin());
                Array rcTxnsArray = connection.createArrayOf("BIGINT", txn.getRcTxns().toArray());
                stmt.setArray(3, rcTxnsArray);
                ResultSet rs = stmt.executeQuery();
                return !rs.next(); // If there's a result, validation fails
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void prepareCommit(TransactionContext txn) {
        // No action needed for Postgres
    }

    @Override
    public void finalizeCommit(TransactionContext txn) {
        // Release locks
        for (String key : txn.getModifiedKeys(this)) {
            locks.remove(key);
        }
    }

    @Override
    public void abortTransaction(TransactionContext txn) {
        try {
            String sql = "DELETE FROM epoxy_data WHERE begin_txn = ?";
            try (PreparedStatement stmt = connection.prepareStatement(sql)) {
                stmt.setLong(1, txn.getTxnId());
                stmt.executeUpdate();
            }
            // Release locks
            for (String key : txn.getModifiedKeys(this)) {
                locks.remove(key);
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void garbageCollect(long globalXmin) {
        try {
            String sql = "DELETE FROM epoxy_data WHERE end_txn < ?";
            try (PreparedStatement stmt = connection.prepareStatement(sql)) {
                stmt.setLong(1, globalXmin);
                stmt.executeUpdate();
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
