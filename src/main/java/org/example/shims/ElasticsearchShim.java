package org.example.shims;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.core.DeleteRequest;
import co.elastic.clients.elasticsearch.core.bulk.BulkOperation;
import co.elastic.clients.elasticsearch._types.Refresh;
import co.elastic.clients.elasticsearch.indices.CreateIndexResponse;
import co.elastic.clients.elasticsearch.indices.ExistsRequest;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.ElasticsearchTransport;
import co.elastic.clients.transport.rest_client.RestClientOptions;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import org.apache.http.HttpHost;
import org.apache.http.message.BasicHeader;
import org.apache.http.Header;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.Node;
import org.example.TransactionContext;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.Collections;
import java.util.ArrayList;
import java.util.List;

public class ElasticsearchShim implements DataStoreShim {
    private final ElasticsearchClient client;
    private final String indexName = "epoxy_data";
    private final ConcurrentHashMap<String, Object> locks;
    private static final int BULK_SIZE = 100; // Adjust as needed
    private final List<BulkOperation> bulkOperations = Collections.synchronizedList(new ArrayList<>());
    private final ExecutorService bulkExecutor = Executors.newSingleThreadExecutor();
    
    public ElasticsearchShim(String hostname) {
        // Create the low-level client with optimized settings
        RestClient restClient = RestClient.builder(
            new HttpHost(hostname, 9200, "http"))
        .setDefaultHeaders(new Header[]{
            new BasicHeader("Accept", "*/*"),
            new BasicHeader("Content-Type", "application/json"),
            new BasicHeader("X-Elastic-Product", "Elasticsearch")
        })
        // Add connection pool settings
        .setHttpClientConfigCallback(httpClientBuilder -> 
            httpClientBuilder
                .setMaxConnTotal(100)
                .setMaxConnPerRoute(100)
                .setKeepAliveStrategy((response, context) -> 30000))
        .build();

        // Create transport with performance optimizations
        RestClientTransport transport = new RestClientTransport(
            restClient, 
            new JacksonJsonpMapper()
        );

        this.client = new ElasticsearchClient(transport);
        this.locks = new ConcurrentHashMap<>();
        
        // Initialize index with optimized settings
        initializeIndexWithOptimizedSettings();
    }

    private void initializeIndexWithOptimizedSettings() {
        // Retry logic remains the same, but add optimized index settings
        try {
            boolean indexExists = client.indices().exists(
                ExistsRequest.of(e -> e.index(indexName))
            ).value();
            
            if (!indexExists) {
                CreateIndexResponse response = client.indices().create(c -> c
                    .index(indexName)
                    .settings(s -> s
                        .numberOfShards("1")
                        .numberOfReplicas("0")
                    )
                );
                if (!response.acknowledged()) {
                    throw new RuntimeException("Failed to create index");
                }
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to initialize index", e);
        }
    }

    @Override
    public void update(TransactionContext txn, String key, String value) {
        BulkOperation operation = new BulkOperation.Builder()
            .index(idx -> idx
                .index(indexName)
                .id(key)
                .document(new Document(
                    value,
                    txn.getTxnId(),
                    Long.MAX_VALUE
                ))
            ).build();

        bulkOperations.add(operation);

        if (bulkOperations.size() >= BULK_SIZE) {
            flushBulkOperations();
        }

        txn.addModifiedKey(this, key);
    }

    private void flushBulkOperations() {
        List<BulkOperation> operationsToFlush;
        synchronized (bulkOperations) {
            if (bulkOperations.isEmpty()) {
                return;
            }
            operationsToFlush = new ArrayList<>(bulkOperations);
            bulkOperations.clear();
        }

        bulkExecutor.submit(() -> {
            try {
                client.bulk(b -> b.operations(operationsToFlush));
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
    }

    @Override
    public String query(TransactionContext txn, String key) {
        try {
            var response = client.get(g -> g
                .index(indexName)
                .id(key),
                Document.class
            );
            if (response.found()) {
                return response.source().value();
            }
            return null;
        } catch (IOException e) {
            throw new RuntimeException("Failed to query document from Elasticsearch", e);
        }
    }

    // Helper class for document structure
    private record Document(
        String value,
        long begin_txn,
        long end_txn
    ) {}

    @Override
    public boolean validateTransaction(TransactionContext txn) {
        // For Elasticsearch, we'll use optimistic concurrency control
        return true;
    }

    @Override
    public void prepareCommit(TransactionContext txn) {
        // No specific prepare phase needed for Elasticsearch
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
        // Delete any documents created by this transaction
        try {
            for (String key : txn.getModifiedKeys(this)) {
                client.delete(
                    DeleteRequest.of(d -> d.index(indexName).id(key))
                );
                locks.remove(key);
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to abort transaction in Elasticsearch", e);
        }
    }

    @Override
    public void garbageCollect(long globalXmin) {
        // Elasticsearch handles its own cleanup
    }
}