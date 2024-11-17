package org.example;

import java.util.HashSet;
import java.util.Set;
import org.example.shims.DataStoreShim;

import java.util.*;

public class TransactionContext {
    private final long txnId;
    private final long xmin;
    private final long xmax;
    private final Set<Long> rcTxns;
    private final Map<DataStoreShim, Set<String>> modifiedKeys;

    public TransactionContext(long txnId, long xmin, long xmax, Set<Long> rcTxns) {
        this.txnId = txnId;
        this.xmin = xmin;
        this.xmax = xmax;
        this.rcTxns = rcTxns;
        this.modifiedKeys = new HashMap<>();
    }

    public void addModifiedKey(DataStoreShim shim, String key) {
        modifiedKeys.computeIfAbsent(shim, k -> new HashSet<>()).add(key);
    }

    public long getTxnId() {
        return txnId;
    }

    public long getXmin() {
        return xmin;
    }

    public long getXmax() {
        return xmax;
    }

    public Set<Long> getRcTxns() {
        return rcTxns;
    }

    public Set<String> getModifiedKeys(DataStoreShim shim) {
        return modifiedKeys.getOrDefault(shim, new HashSet<>());
    }
}
