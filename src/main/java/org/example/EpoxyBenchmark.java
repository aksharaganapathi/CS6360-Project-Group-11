package org.example;

import java.sql.SQLException;
import java.util.concurrent.*;

import org.example.shims.MongoDBShim;
import org.example.shims.PostgresShim;

import java.util.Random;

public class EpoxyBenchmark {
    private EpoxyCoordinator coordinator;
    private PostgresShim postgresShim;
    private MongoDBShim mongoDBShim;
    private Random random;

    public EpoxyBenchmark() throws SQLException {
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
        long startTime = System.currentTimeMillis();

        for (int i = 0; i < numTransactions; i++) {
            executor.submit(() -> {
                try {
                    if (random.nextDouble() < readWriteRatio) {
                        runReadTransaction(crossStoreRatio);
                    } else {
                        runWriteTransaction(crossStoreRatio);
                    }
                } catch (SQLException e) {
                    e.printStackTrace();
                } finally {
                    latch.countDown();
                }
            });
        }

        latch.await();
        long endTime = System.currentTimeMillis();
        executor.shutdown();

        double throughput = numTransactions / ((endTime - startTime) / 1000.0);
        System.out.println("Throughput: " + throughput + " transactions per second");
    }

    private void runReadTransaction(double crossStoreRatio) throws SQLException {
        TransactionContext txn = coordinator.beginTransaction();
        try {
            String key = "key" + random.nextInt(1000);
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
        }
    }

    private void runWriteTransaction(double crossStoreRatio) throws SQLException {
        TransactionContext txn = coordinator.beginTransaction();
        try {
            String key = "key" + random.nextInt(1000);
            String value = "value" + random.nextInt(1000);
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
        }
    }

    public static void main(String[] args) throws SQLException, InterruptedException {
        EpoxyBenchmark benchmark = new EpoxyBenchmark();
        
        System.out.println("Running read-heavy workload (90% reads, 50% cross-store)");
        benchmark.runBenchmark(10, 10000, 0.9, 0.5);
        
        System.out.println("Running write-heavy workload (30% reads, 50% cross-store)");
        benchmark.runBenchmark(10, 10000, 0.3, 0.5);
        
        System.out.println("Running mixed workload (50% reads, 80% cross-store)");
        benchmark.runBenchmark(10, 10000, 0.5, 0.8);
    }
}