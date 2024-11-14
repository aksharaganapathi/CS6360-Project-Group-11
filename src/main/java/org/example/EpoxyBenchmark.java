package org.example;

import java.sql.SQLException;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

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

    public void runBenchmark(int numThreads, int numTransactions) throws InterruptedException {
        ExecutorService executor = Executors.newFixedThreadPool(numThreads);
        CountDownLatch latch = new CountDownLatch(numTransactions);
        long startTime = System.currentTimeMillis();

        for (int i = 0; i < numTransactions; i++) {
            executor.submit(() -> {
                try {
                    runTransaction();
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

    private void runTransaction() throws SQLException {
        TransactionContext txn = coordinator.beginTransaction();
        try {
            String key = "key" + random.nextInt(1000);
            String value = "value" + random.nextInt(1000);

            if (random.nextBoolean()) {
                postgresShim.update(txn, key, value);
            } else {
                mongoDBShim.update(txn, key, value);
            }

            if (random.nextBoolean()) {
                String readKey = "key" + random.nextInt(1000);
                if (random.nextBoolean()) {
                    postgresShim.query(txn, readKey);
                } else {
                    mongoDBShim.query(txn, readKey);
                }
            }

            if (coordinator.validateTransaction(txn)) {
                coordinator.commitTransaction(txn);
            } else {
                coordinator.abortTransaction(txn);
            }
        } catch (Exception e) {
            coordinator.abortTransaction(txn);
            throw e;
        }
    }

    public static void main(String[] args) throws SQLException, InterruptedException {
        EpoxyBenchmark benchmark = new EpoxyBenchmark();
        int numThreads = 10;
        int numTransactions = 10000;
        
        System.out.println("Starting Epoxy benchmark...");
        System.out.println("Number of threads: " + numThreads);
        System.out.println("Number of transactions: " + numTransactions);
        
        benchmark.runBenchmark(numThreads, numTransactions);
    }
}


