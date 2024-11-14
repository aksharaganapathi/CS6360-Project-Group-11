package org.example;

import java.sql.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

public class EpoxyCoordinator {
    private Connection primaryDb;
    private AtomicLong txnIdGenerator;
    private ConcurrentHashMap<Long, TransactionContext> activeTxns;
    private List<DataStoreShim> secondaryStores;
    private ScheduledExecutorService garbageCollector;
    private ConcurrentHashMap<String, Object> globalLocks;

    public EpoxyCoordinator(String jdbcUrl, String username, String password) throws SQLException {
        this.primaryDb = DriverManager.getConnection(jdbcUrl, username, password);
        this.txnIdGenerator = new AtomicLong(1);
        this.activeTxns = new ConcurrentHashMap<>();
        this.secondaryStores = new ArrayList<>();
        this.globalLocks = new ConcurrentHashMap<>();
        this.garbageCollector = Executors.newSingleThreadScheduledExecutor();
        this.garbageCollector.scheduleAtFixedRate(this::performGarbageCollection, 0, 1, TimeUnit.MINUTES);
    }

    public void addSecondaryStore(DataStoreShim shim) {
        secondaryStores.add(shim);
    }

    public TransactionContext beginTransaction() throws SQLException {
        long txnId = txnIdGenerator.getAndIncrement();
        primaryDb.setAutoCommit(false);
        
        long xmin = getXmin();
        long xmax = txnId;
        Set<Long> rcTxns = getRecentlyCommittedTransactions(xmin);

        TransactionContext txn = new TransactionContext(txnId, xmin, xmax, rcTxns);
        activeTxns.put(txnId, txn);
        return txn;
    }

    public void commitTransaction(TransactionContext txn) throws SQLException {
        if (validateTransaction(txn)) {
            for (DataStoreShim shim : secondaryStores) {
                shim.prepareCommit(txn);
            }
            primaryDb.commit();
            for (DataStoreShim shim : secondaryStores) {
                shim.finalizeCommit(txn);
            }
            activeTxns.remove(txn.getTxnId());
        } else {
            abortTransaction(txn);
            throw new SQLException("Transaction validation failed");
        }
    }

    public void abortTransaction(TransactionContext txn) throws SQLException {
        primaryDb.rollback();
        for (DataStoreShim shim : secondaryStores) {
            shim.abortTransaction(txn);
        }
        activeTxns.remove(txn.getTxnId());
    }

    private boolean validateTransaction(TransactionContext txn) {
        for (DataStoreShim shim : secondaryStores) {
            if (!shim.validateTransaction(txn)) {
                return false;
            }
        }
        return true;
    }

    private long getXmin() {
        return activeTxns.values().stream()
                .mapToLong(TransactionContext::getTxnId)
                .min()
                .orElse(txnIdGenerator.get());
    }

    private Set<Long> getRecentlyCommittedTransactions(long xmin) {
        // In a real implementation, this would query the primary database
        // For simplicity, we'll return an empty set
        return new HashSet<>();
    }

    private void performGarbageCollection() {
        long globalXmin = getXmin();
        for (DataStoreShim shim : secondaryStores) {
            shim.garbageCollect(globalXmin);
        }
    }

    public Object acquireGlobalLock(String key) {
        return globalLocks.computeIfAbsent(key, k -> new Object());
    }

    public void releaseGlobalLock(String key) {
        globalLocks.remove(key);
    }
}