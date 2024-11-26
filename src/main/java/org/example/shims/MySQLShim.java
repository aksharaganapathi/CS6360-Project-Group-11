package org.example.shims;

import org.example.TransactionContext;
import java.sql.*;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class MySQLShim implements DataStoreShim {
    private Connection connection;
    private ConcurrentHashMap<String, Object> locks;

    public MySQLShim(String jdbcUrl, String username, String password) throws SQLException {
        this.connection = DriverManager.getConnection(jdbcUrl, username, password);
        this.locks = new ConcurrentHashMap<>();

        // Create necessary tables
        try (Statement stmt = connection.createStatement()) {
            stmt.execute(
                "CREATE TABLE IF NOT EXISTS warehouse (" +
                "w_id VARCHAR(255) PRIMARY KEY, " +
                "w_data TEXT, " +
                "begin_txn BIGINT, " +
                "end_txn BIGINT" +
                ")"
            );

            stmt.execute(
                "CREATE TABLE IF NOT EXISTS district (" +
                "d_id VARCHAR(255) PRIMARY KEY, " +
                "d_data TEXT, " +
                "begin_txn BIGINT, " +
                "end_txn BIGINT" +
                ")"
            );
        }
    }

    @Override
    public void update(TransactionContext txn, String key, String value) {
        locks.computeIfAbsent(key, k -> new Object());
        synchronized (locks.get(key)) {
            try {
                String sql = "INSERT INTO warehouse (w_id, w_data, begin_txn, end_txn) " +
                           "VALUES (?, ?, ?, ?) " +
                           "ON DUPLICATE KEY UPDATE w_data = VALUES(w_data), " +
                           "begin_txn = VALUES(begin_txn), end_txn = VALUES(end_txn)";
                
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
            String sql = "SELECT w_data FROM warehouse WHERE w_id = ? AND begin_txn <= ? " +
                        "AND (end_txn > ? OR end_txn = ?) " +
                        "AND (begin_txn < ? OR begin_txn = ANY (?)) " +
                        "ORDER BY begin_txn DESC LIMIT 1";
            
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
                    return rs.getString("w_data");
                }
            }
            return null;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean validateTransaction(TransactionContext txn) {
        try {
            Set<String> keys = txn.getModifiedKeys(this);
            Set<Long> rcTxns = txn.getRcTxns();
            
            // If no keys were modified, validation passes
            if (keys.isEmpty()) {
                return true;
            }

            // Build query for MySQL which doesn't support array parameters
            StringBuilder sql = new StringBuilder();
            sql.append("SELECT 1 FROM warehouse WHERE w_id IN (");
            
            // Add placeholders for keys
            String keyPlaceholders = String.join(",", Collections.nCopies(keys.size(), "?"));
            sql.append(keyPlaceholders);
            sql.append(") AND begin_txn >= ?");
            
            // Add NOT IN clause only if there are recently committed transactions
            if (!rcTxns.isEmpty()) {
                sql.append(" AND begin_txn NOT IN (");
                String txnPlaceholders = String.join(",", Collections.nCopies(rcTxns.size(), "?"));
                sql.append(txnPlaceholders);
                sql.append(")");
            }
            
            try (PreparedStatement stmt = connection.prepareStatement(sql.toString())) {
                int paramIndex = 1;
                
                // Set key parameters
                for (String key : keys) {
                    stmt.setString(paramIndex++, key);
                }
                
                // Set xmin parameter
                stmt.setLong(paramIndex++, txn.getXmin());
                
                // Set rcTxns parameters if any exist
                if (!rcTxns.isEmpty()) {
                    for (Long txnId : rcTxns) {
                        stmt.setLong(paramIndex++, txnId);
                    }
                }
                
                ResultSet rs = stmt.executeQuery();
                return !rs.next(); // If there's a result, validation fails
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void prepareCommit(TransactionContext txn) {
        // No specific prepare phase needed for MySQL
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
            String sql = "DELETE FROM warehouse WHERE begin_txn = ?";
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
            String sql = "DELETE FROM warehouse WHERE end_txn < ?";
            try (PreparedStatement stmt = connection.prepareStatement(sql)) {
                stmt.setLong(1, globalXmin);
                stmt.executeUpdate();
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}