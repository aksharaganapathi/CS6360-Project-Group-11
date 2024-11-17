package org.example.benchmarks;

import java.sql.SQLException;
import java.util.concurrent.*;

import org.example.shims.PostgresShim;
import org.example.EpoxyCoordinator;
import org.example.TransactionContext;
import org.example.shims.MongoDBShim;

import java.util.Random;
import java.util.List;
import java.util.ArrayList;
import java.util.Collections;

public class HotelBenchmark {
    private EpoxyCoordinator coordinator;
    private PostgresShim postgresShim;
    private MongoDBShim mongoDBShim;
    private Random random;
    private static final int NUM_HOTELS = 100;

    public HotelBenchmark() throws SQLException {
        // Now connect to the database and create table if needed
        coordinator = new EpoxyCoordinator("jdbc:postgresql://localhost:5432/epoxy_test", "postgres", "test987");
        postgresShim = new PostgresShim("jdbc:postgresql://localhost:5432/epoxy_test", "postgres", "test987");
        mongoDBShim = new MongoDBShim("mongodb://localhost:27017", "epoxy_test");
        coordinator.addSecondaryStore(postgresShim);
        coordinator.addSecondaryStore(mongoDBShim);
        random = new Random();
        initializeHotels();
    }

    private void initializeHotels() throws SQLException {
        TransactionContext txn = coordinator.beginTransaction();
        try {
            for (int i = 0; i < NUM_HOTELS; i++) {
                String hotelId = "hotel" + i;
                // Initialize room availability in Postgres
                postgresShim.update(txn, hotelId + "_rooms", "50"); // Each hotel starts with 50 rooms
                
                // Initialize hotel info in MongoDB with random coordinates
                double lat = random.nextDouble() * 180 - 90; // Random latitude between -90 and 90
                double lon = random.nextDouble() * 360 - 180; // Random longitude between -180 and 180
                String hotelInfo = String.format("{\"name\":\"Hotel %d\",\"location\":{\"type\":\"Point\",\"coordinates\":[%f,%f]}}", 
                    i, lon, lat);
                mongoDBShim.update(txn, hotelId + "_info", hotelInfo);
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
        metrics.start(numOperations);

        try {
            for (int i = 0; i < numOperations; i++) {
                executor.submit(() -> {
                    long startTime = System.nanoTime();
                    try {
                        if (random.nextDouble() < 0.8) {
                            searchAvailableRooms();
                        } else {
                            makeReservation();
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
        } finally {
            executor.shutdownNow();
            if (!executor.awaitTermination(60, TimeUnit.SECONDS)) {
                System.err.println("Executor did not terminate in the specified time.");
            }
            metrics.printMetrics("Hotel Benchmark");
            System.exit(0);
        }
    }

    private void searchAvailableRooms() throws SQLException {
        TransactionContext txn = coordinator.beginTransaction();
        try {
            String hotelId = "hotel" + random.nextInt(NUM_HOTELS);
            
            // Query room availability from Postgres
            String availableRooms = postgresShim.query(txn, hotelId + "_rooms");
            
            // Query hotel info from MongoDB (including geospatial data)
            String hotelInfo = mongoDBShim.query(txn, hotelId + "_info");
            
            coordinator.commitTransaction(txn);
        } catch (Exception e) {
            coordinator.abortTransaction(txn);
            throw e;
        }
    }

    private void makeReservation() throws SQLException {
        TransactionContext txn = coordinator.beginTransaction();
        try {
            String hotelId = "hotel" + random.nextInt(NUM_HOTELS);
            
            // Check and update room availability in Postgres
            String currentRooms = postgresShim.query(txn, hotelId + "_rooms");
            int availableRooms = Integer.parseInt(currentRooms);
            
            if (availableRooms > 0) {
                // Update room count in Postgres
                postgresShim.update(txn, hotelId + "_rooms", String.valueOf(availableRooms - 1));
                
                // Create reservation in MongoDB
                String reservationId = "res_" + System.currentTimeMillis() + "_" + random.nextInt(1000);
                String reservationInfo = String.format("{\"hotelId\":\"%s\",\"timestamp\":%d}", 
                    hotelId, System.currentTimeMillis());
                mongoDBShim.update(txn, reservationId, reservationInfo);
                
                coordinator.commitTransaction(txn);
            } else {
                coordinator.abortTransaction(txn);
            }
        } catch (Exception e) {
            coordinator.abortTransaction(txn);
            throw e;
        }
    }
}

class BenchmarkMetrics {
    private final List<Long> latencies = Collections.synchronizedList(new ArrayList<>());
    private long startTime;
    private long endTime;
    private int totalOperations;

    public void start(int operations) {
        this.totalOperations = operations;
        this.startTime = System.currentTimeMillis();
        this.latencies.clear();
    }

    public void recordLatency(long startNanos) {
        latencies.add(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos));
    }

    public void end() {
        this.endTime = System.currentTimeMillis();
    }

    public void printMetrics(String benchmarkName) {
        double durationSeconds = (endTime - startTime) / 1000.0;
        double throughput = totalOperations / durationSeconds;
        
        Collections.sort(latencies);
        long p50 = latencies.get((int)(latencies.size() * 0.5));
        long p99 = latencies.get((int)(latencies.size() * 0.99));

        System.out.printf("%s Results:\n", benchmarkName);
        System.out.printf("Throughput (QPS): %.2f\n", throughput);
        System.out.printf("P50 Latency (ms): %d\n", p50);
        System.out.printf("P99 Latency (ms): %d\n", p99);
        System.out.println("----------------------------------------");
    }
} 