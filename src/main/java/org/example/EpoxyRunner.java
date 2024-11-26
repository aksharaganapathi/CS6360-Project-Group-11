package org.example;

import org.example.benchmarks.CartBenchmark;
import org.example.benchmarks.EndToEndMicroserviceBenchmark;
import org.example.benchmarks.HotelBenchmark;
import org.example.benchmarks.TPCCBenchmark;

import java.sql.SQLException;

public class EpoxyRunner {
    public static void main(String[] args) throws SQLException, InterruptedException {
        System.out.println("Running TPC-C Benchmark");
        TPCCBenchmark tpccBenchmark = new TPCCBenchmark();
        tpccBenchmark.runBenchmark(1, 10000);

        System.out.println("Running Epoxy Benchmarks: ");
        System.out.println("Running Hotel Benchmark (80% searches, 20% reservations)");
        HotelBenchmark hotelBenchmark = new HotelBenchmark();
        hotelBenchmark.runBenchmark(1, 10000);

        System.out.println("Running Microservice Benchmark: ");
        EndToEndMicroserviceBenchmark endToEndMicroserviceBenchmark = new EndToEndMicroserviceBenchmark();

        System.out.println("Running read-heavy workload (90% reads, 50% cross-store)");
        endToEndMicroserviceBenchmark.runBenchmark(1, 10000, 0.9, 0.5);

        System.out.println("Running write-heavy workload (30% reads, 50% cross-store)");
        endToEndMicroserviceBenchmark.runBenchmark(1, 10000, 0.3, 0.5);

        System.out.println("Running mixed workload (50% reads, 80% cross-store)");
        endToEndMicroserviceBenchmark.runBenchmark(1, 10000, 0.5, 0.8);

        System.out.println("Running Cart Benchmark");
        CartBenchmark cartBenchmark = new CartBenchmark();
        cartBenchmark.runBenchmark(1, 10000);

        System.exit(0);
    }
}
