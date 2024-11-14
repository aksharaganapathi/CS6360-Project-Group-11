package org.example;

public interface DataStoreShim {
    void update(TransactionContext txn, String key, String value);
    String query(TransactionContext txn, String key);
    boolean validateTransaction(TransactionContext txn);
    void commitTransaction(TransactionContext txn);
    void abortTransaction(TransactionContext txn);
    void garbageCollect(long globalXmin);
}