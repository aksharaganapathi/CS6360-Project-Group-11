package org.example.benchmarks;

import java.sql.SQLException;
import java.util.concurrent.*;

import org.example.EpoxyCoordinator;
import org.example.TransactionContext;
import org.example.shims.MongoDBShim;
import org.example.shims.PostgresShim;

import java.util.Random;

public class EndToEndMicroserviceBenchmark {
    private EpoxyCoordinator coordinator;
    private PostgresShim postgresShim;
    private MongoDBShim mongoDBShim;
    private Random random;

    public EndToEndMicroserviceBenchmark() throws SQLException {
        coordinator = new EpoxyCoordinator("jdbc:postgresql://localhost:5432/epoxy_test", "postgres", "test987");
        postgresShim = new PostgresShim("jdbc:postgresql://localhost:5432/epoxy_test", "postgres", "test987");
        mongoDBShim = new MongoDBShim("mongodb://localhost:27017", "epoxy_test");
        coordinator.addSecondaryStore(postgresShim);
        coordinator.addSecondaryStore(mongoDBShim);
        random = new Random();
    }

    public void runBenchmark(int numThreads, int numTransactions, double readWriteRatio, double crossStoreRatio) throws InterruptedException {
        ExecutorService executor = Executors.newFixedThreadPool(numThreads);
        CountDownLatch latch = new CountDownLatch(numTransactions);
        BenchmarkMetrics metrics = new BenchmarkMetrics();
        metrics.start(numTransactions);
        long startTime = System.currentTimeMillis();

        for (int i = 0; i < numTransactions; i++) {
            executor.submit(() -> {
                long txnStartTime = System.nanoTime();
                try {
                    if (random.nextDouble() < readWriteRatio) {
                        runReadTransaction(crossStoreRatio);
                    } else {
                        runWriteTransaction(crossStoreRatio);
                    }
                } catch (SQLException e) {
                    e.printStackTrace();
                } finally {
                    metrics.recordLatency(txnStartTime);
                    latch.countDown();
                }
            });
        }

        latch.await();
        long endTime = System.currentTimeMillis();
        executor.shutdown();

        metrics.end();
        metrics.printMetrics("Microservice Benchmark");
    }

    private void runReadTransaction(double crossStoreRatio) throws SQLException {
        String key = "key" + random.nextInt(1000);
        Object lock = coordinator.acquireGlobalLock(key);
        
        synchronized(lock) {
            TransactionContext txn = coordinator.beginTransaction();
            try {
                if (random.nextDouble() < crossStoreRatio) {
                    postgresShim.query(txn, key);
                    mongoDBShim.query(txn, key);
                } else {
                    if (random.nextBoolean()) {
                        postgresShim.query(txn, key);
                    } else {
                        mongoDBShim.query(txn, key);
                    }
                }
                coordinator.commitTransaction(txn);
            } catch (Exception e) {
                coordinator.abortTransaction(txn);
                throw e;
            } finally {
                coordinator.releaseGlobalLock(key);
            }
        }
    }

    private void runWriteTransaction(double crossStoreRatio) throws SQLException {
        String key = "key_" + System.nanoTime() + "_" + random.nextInt(1000);
        Object lock = coordinator.acquireGlobalLock(key);
        
        synchronized(lock) {
            TransactionContext txn = coordinator.beginTransaction();
            try {
                String value = "value_" + System.nanoTime();
                
                if (random.nextDouble() < crossStoreRatio) {
                    postgresShim.update(txn, key, value);
                    mongoDBShim.update(txn, key, value);
                } else {
                    if (random.nextBoolean()) {
                        postgresShim.update(txn, key, value);
                    } else {
                        mongoDBShim.update(txn, key, value);
                    }
                }
                coordinator.commitTransaction(txn);
            } catch (Exception e) {
                coordinator.abortTransaction(txn);
                throw e;
            } finally {
                coordinator.releaseGlobalLock(key);
            }
        }
    }
}