package org.example.benchmarks;

import org.example.EpoxyCoordinator;
import org.example.TransactionContext;
import org.example.shims.PostgresShim;
import org.example.shims.MySQLShim;

import java.sql.SQLException;
import java.util.Random;
import java.util.concurrent.*;

public class TPCCBenchmark {
    private EpoxyCoordinator coordinator;
    private PostgresShim postgresShim;
    private MySQLShim mysqlShim;
    private Random random;
    private static final int NUM_WAREHOUSES = 40;

    public TPCCBenchmark() throws SQLException {
        coordinator = new EpoxyCoordinator(
            "jdbc:postgresql://localhost:5432/epoxy_test", 
            "postgres", 
            "test987"
        );
        postgresShim = new PostgresShim(
            "jdbc:postgresql://localhost:5432/epoxy_test",
            "postgres",
            "test987"
        );
        mysqlShim = new MySQLShim(
            "jdbc:mysql://localhost:3306/epoxy_test",
            "mysql",
            "test987"
        );
        
        coordinator.addSecondaryStore(postgresShim);
        coordinator.addSecondaryStore(mysqlShim);
        random = new Random();
        initializeWarehouses();
    }

    private void initializeWarehouses() throws SQLException {
        TransactionContext txn = coordinator.beginTransaction();
        try {
            for (int i = 0; i < NUM_WAREHOUSES; i++) {
                String wId = String.format("W_%d", i);
                String data = String.format("{\"name\":\"Warehouse %d\",\"ytd\":0.0}", i);
                
                if (i < NUM_WAREHOUSES/2) {
                    postgresShim.update(txn, wId, data);
                } else {
                    mysqlShim.update(txn, wId, data);
                }

                // Initialize districts for this warehouse
                for (int d = 0; d < 10; d++) {
                    String dId = String.format("D_%d_%d", i, d);
                    String dData = String.format("{\"name\":\"District %d-%d\",\"ytd\":0.0}", i, d);
                    
                    if (i < NUM_WAREHOUSES/2) {
                        postgresShim.update(txn, dId, dData);
                    } else {
                        mysqlShim.update(txn, dId, dData);
                    }
                }
            }
            coordinator.commitTransaction(txn);
        } catch (Exception e) {
            coordinator.abortTransaction(txn);
            throw e;
        }
    }

    private void newOrderTransaction() throws SQLException {
        TransactionContext txn = coordinator.beginTransaction();
        try {
            int wId = random.nextInt(NUM_WAREHOUSES);
            int dId = random.nextInt(10);
            String warehouseKey = String.format("W_%d", wId);
            String districtKey = String.format("D_%d_%d", wId, dId);
            
            // Access warehouse and district data
            String warehouseData = (wId < NUM_WAREHOUSES/2) ? 
                postgresShim.query(txn, warehouseKey) :
                mysqlShim.query(txn, warehouseKey);
                
            String districtData = (wId < NUM_WAREHOUSES/2) ?
                postgresShim.query(txn, districtKey) :
                mysqlShim.query(txn, districtKey);
                
            // Create new order
            String orderId = String.format("O_%d_%d_%d", wId, dId, System.nanoTime());
            String orderData = String.format("{\"w_id\":%d,\"d_id\":%d,\"timestamp\":%d}", 
                wId, dId, System.currentTimeMillis());
                
            if (wId < NUM_WAREHOUSES/2) {
                postgresShim.update(txn, orderId, orderData);
            } else {
                mysqlShim.update(txn, orderId, orderData);
            }
            
            coordinator.commitTransaction(txn);
        } catch (Exception e) {
            coordinator.abortTransaction(txn);
            throw e;
        }
    }

    private void paymentTransaction() throws SQLException {
        TransactionContext txn = coordinator.beginTransaction();
        try {
            int wId = random.nextInt(NUM_WAREHOUSES);
            int dId = random.nextInt(10);
            String warehouseKey = String.format("W_%d", wId);
            String districtKey = String.format("D_%d_%d", wId, dId);
            
            // Update warehouse and district YTD
            double paymentAmount = 100.0 + random.nextDouble() * 900.0;
            
            if (wId < NUM_WAREHOUSES/2) {
                String currentData = postgresShim.query(txn, warehouseKey);
                // Update YTD in warehouse data
                postgresShim.update(txn, warehouseKey, updateYTD(currentData, paymentAmount));
                
                currentData = postgresShim.query(txn, districtKey);
                // Update YTD in district data
                postgresShim.update(txn, districtKey, updateYTD(currentData, paymentAmount));
            } else {
                String currentData = mysqlShim.query(txn, warehouseKey);
                // Update YTD in warehouse data
                mysqlShim.update(txn, warehouseKey, updateYTD(currentData, paymentAmount));
                
                currentData = mysqlShim.query(txn, districtKey);
                // Update YTD in district data
                mysqlShim.update(txn, districtKey, updateYTD(currentData, paymentAmount));
            }
            
            coordinator.commitTransaction(txn);
        } catch (Exception e) {
            coordinator.abortTransaction(txn);
            throw e;
        }
    }

    private String updateYTD(String data, double amount) {
        // Simple string manipulation to update YTD in JSON data
        // In real implementation, use proper JSON parsing
        return data.replaceFirst("\"ytd\":[0-9.]+", String.format("\"ytd\":%.2f", amount));
    }

    public void runBenchmark(int numThreads, int numTransactions) throws InterruptedException {
        ExecutorService executor = Executors.newFixedThreadPool(numThreads);
        CountDownLatch latch = new CountDownLatch(numTransactions);
        BenchmarkMetrics metrics = new BenchmarkMetrics();
        metrics.start(numTransactions);

        for (int i = 0; i < numTransactions; i++) {
            executor.submit(() -> {
                long startTime = System.nanoTime();
                try {
                    if (random.nextBoolean()) {
                        newOrderTransaction();
                    } else {
                        paymentTransaction();
                    }
                } catch (SQLException e) {
                    e.printStackTrace();
                } finally {
                    metrics.recordLatency(startTime);
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
        metrics.printMetrics("TPC-C Benchmark");
    }
}