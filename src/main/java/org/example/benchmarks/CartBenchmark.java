package org.example.benchmarks;

import org.example.EpoxyCoordinator;
import org.example.TransactionContext;
import org.example.shims.PostgresShim;
import org.example.shims.ElasticsearchShim;

import java.sql.SQLException;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class CartBenchmark {
    private final EpoxyCoordinator coordinator;
    private final PostgresShim postgresShim;
    private final ElasticsearchShim elasticsearchShim;
    private final Random random;
    private static final int NUM_ITEMS = 1000; // Smaller number for testing

    public CartBenchmark() throws SQLException {
        coordinator = new EpoxyCoordinator("jdbc:postgresql://localhost:5432/epoxy_test", "postgres", "test987");
        postgresShim = new PostgresShim("jdbc:postgresql://localhost:5432/epoxy_test", "postgres", "test987");
        
        // Fix: Add http:// protocol to Elasticsearch URL
        elasticsearchShim = new ElasticsearchShim("localhost");
        
        coordinator.addSecondaryStore(postgresShim);
        coordinator.addSecondaryStore(elasticsearchShim);
        random = new Random();
        initializeCatalog();
    }

    private void initializeCatalog() throws SQLException {
        TransactionContext txn = coordinator.beginTransaction();
        try {
            // Initialize catalog with items
            for (int i = 0; i < NUM_ITEMS; i++) {
                String itemId = "item" + i;
                double price = 10.0 + random.nextDouble() * 90.0; // Random price between 10 and 100
                String itemInfo = String.format("{\"name\":\"Item %d\",\"price\":%.2f}", i, price);
                
                // Store in both Postgres and Elasticsearch
                postgresShim.update(txn, "catalog_" + itemId, itemInfo);
                elasticsearchShim.update(txn, "catalog_" + itemId, itemInfo);
            }
            coordinator.commitTransaction(txn);
        } catch (Exception e) {
            coordinator.abortTransaction(txn);
            throw e;
        }
    }

    public void runBenchmark(int numThreads, int numOperations) throws InterruptedException {
        ExecutorService executor = Executors.newFixedThreadPool(numThreads);
        CountDownLatch latch = new CountDownLatch(numOperations);
        BenchmarkMetrics metrics = new BenchmarkMetrics();
        
        // Initialize metrics
        metrics.start(numOperations);

        for (int i = 0; i < numOperations; i++) {
            executor.submit(() -> {
                try {
                    long startTime = System.nanoTime();
                    searchAndAddToCart();
                    metrics.recordLatency(startTime);
                } catch (SQLException e) {
                    System.out.println("Error in searchAndAddToCart: " + e.getMessage());
                    e.printStackTrace();
                } finally {
                    latch.countDown();
                }
            });
        }

        latch.await();
        metrics.end();
        executor.shutdown();
        if (!executor.awaitTermination(60, TimeUnit.SECONDS)) {
            System.err.println("Executor did not terminate in the specified time.");
        }

        metrics.printMetrics("Cart Benchmark");
    }

    private void searchAndAddToCart() throws SQLException {
        TransactionContext txn = coordinator.beginTransaction();
        try {
            String itemId = "item" + random.nextInt(NUM_ITEMS);
            String cartId = "cart" + random.nextInt(1000);
            
            // Search in Elasticsearch
            String itemInfo = elasticsearchShim.query(txn, "catalog_" + itemId);
            
            if (itemInfo != null) {
                // Add to cart in Postgres
                String cartKey = cartId + "_" + itemId;
                postgresShim.update(txn, cartKey, itemInfo);
            }
            
            coordinator.commitTransaction(txn);
        } catch (Exception e) {
            coordinator.abortTransaction(txn);
            throw e;
        }
    }

    private void checkout() throws SQLException {
        TransactionContext txn = coordinator.beginTransaction();
        try {
            String cartId = "cart" + random.nextInt(1000);
            String orderId = "order_" + System.nanoTime();
            
            // Move items from cart to order
            String cartItems = postgresShim.query(txn, cartId);
            if (cartItems != null) {
                postgresShim.update(txn, orderId, cartItems);
                postgresShim.update(txn, cartId, null); // Clear cart
            }
            
            coordinator.commitTransaction(txn);
        } catch (Exception e) {
            coordinator.abortTransaction(txn);
            throw e;
        }
    }

    private void insertCatalogItem() throws SQLException {
        TransactionContext txn = coordinator.beginTransaction();
        try {
            String itemId = "item_" + System.nanoTime();
            double price = 10.0 + random.nextDouble() * 90.0;
            String itemInfo = String.format("{\"name\":\"%s\",\"price\":%.2f}", itemId, price);
            
            // Insert in both stores
            postgresShim.update(txn, "catalog_" + itemId, itemInfo);
            elasticsearchShim.update(txn, "catalog_" + itemId, itemInfo);
            
            coordinator.commitTransaction(txn);
        } catch (Exception e) {
            coordinator.abortTransaction(txn);
            throw e;
        }
    }

    private void updateCatalogItem() throws SQLException {
        TransactionContext txn = coordinator.beginTransaction();
        try {
            String itemId = "item" + random.nextInt(NUM_ITEMS);
            double newPrice = 10.0 + random.nextDouble() * 90.0;
            String itemInfo = String.format("{\"name\":\"Item %s\",\"price\":%.2f}", itemId, newPrice);
            
            // Update in both stores
            postgresShim.update(txn, "catalog_" + itemId, itemInfo);
            elasticsearchShim.update(txn, "catalog_" + itemId, itemInfo);
            
            coordinator.commitTransaction(txn);
        } catch (Exception e) {
            coordinator.abortTransaction(txn);
            throw e;
        }
    }
}