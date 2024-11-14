package org.example.benchmarks;

import java.sql.SQLException;
import java.util.concurrent.*;

import org.example.shims.PostgresShim;
import org.example.EpoxyCoordinator;
import org.example.TransactionContext;
import org.example.shims.MongoDBShim;

import java.util.Random;

public class HotelBenchmark {
    private EpoxyCoordinator coordinator;
    private PostgresShim postgresShim;
    private MongoDBShim mongoDBShim;
    private Random random;
    private static final int NUM_HOTELS = 100;

    public HotelBenchmark() throws SQLException {
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
        long startTime = System.currentTimeMillis();

        for (int i = 0; i < numOperations; i++) {
            executor.submit(() -> {
                try {
                    if (random.nextDouble() < 0.8) { // 80% search operations
                        searchAvailableRooms();
                    } else { // 20% reservation operations
                        makeReservation();
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

        double throughput = numOperations / ((endTime - startTime) / 1000.0);
        System.out.println("Hotel Benchmark Throughput: " + throughput + " operations per second");
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

    public static void main(String[] args) throws SQLException, InterruptedException {
        HotelBenchmark benchmark = new HotelBenchmark();
        System.out.println("Running Hotel Benchmark (80% searches, 20% reservations)");
        benchmark.runBenchmark(10, 10000);
    }
} 