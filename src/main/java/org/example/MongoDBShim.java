package org.example;

import com.mongodb.client.*;
import com.mongodb.client.model.UpdateOptions;

import org.bson.Document;

import java.util.Arrays;
import java.util.concurrent.ConcurrentHashMap;

public class MongoDBShim implements DataStoreShim {
    private MongoClient mongoClient;
    private MongoDatabase database;
    private ConcurrentHashMap<String, Object> locks;

    public MongoDBShim(String connectionString, String databaseName) {
        this.mongoClient = MongoClients.create(connectionString);
        this.database = mongoClient.getDatabase(databaseName);
        this.locks = new ConcurrentHashMap<>();
    }

    @Override
    public void update(TransactionContext txn, String key, String value) {
        locks.computeIfAbsent(key, k -> new Object());
        synchronized (locks.get(key)) {
            MongoCollection<Document> collection = database.getCollection("epoxy_data");
            Document filter = new Document("key", key);
            Document update = new Document("$set", new Document()
                    .append("value", value)
                    .append("begin_txn", txn.getTxnId())
                    .append("end_txn", Long.MAX_VALUE));
            collection.updateOne(filter, update, new UpdateOptions().upsert(true));
            txn.addModifiedKey(this, key);
        }
    }

    @Override
    public String query(TransactionContext txn, String key) {
        MongoCollection<Document> collection = database.getCollection("epoxy_data");
        Document filter = new Document("key", key)
                .append("begin_txn", new Document("$lte", txn.getTxnId()))
                .append("end_txn", new Document("$gt", txn.getXmin()))
                .append("$or", Arrays.asList(
                        new Document("begin_txn", new Document("$lt", txn.getXmin())),
                        new Document("begin_txn", new Document("$in", txn.getRcTxns()))
                ));
        Document result = collection.find(filter)
                .sort(new Document("begin_txn", -1))
                .limit(1)
                .first();
        return result != null ? result.getString("value") : null;
    }

    @Override
    public boolean validateTransaction(TransactionContext txn) {
        MongoCollection<Document> collection = database.getCollection("epoxy_data");
        Document filter = new Document("key", new Document("$in", txn.getModifiedKeys()))
                .append("begin_txn", new Document("$gte", txn.getXmin())
                        .append("$nin", txn.getRcTxns()));
        return collection.countDocuments(filter) == 0;
    }

    @Override
    public void commitTransaction(TransactionContext txn) {
        // No action needed for MongoDB as the primary database handles the commit
    }

    @Override
    public void abortTransaction(TransactionContext txn) {
        MongoCollection<Document> collection = database.getCollection("epoxy_data");
        collection.deleteMany(new Document("begin_txn", txn.getTxnId()));
    }

    @Override
    public void garbageCollect(long globalXmin) {
        MongoCollection<Document> collection = database.getCollection("epoxy_data");
        collection.deleteMany(new Document("end_txn", new Document("$lt", globalXmin)));
    }
}