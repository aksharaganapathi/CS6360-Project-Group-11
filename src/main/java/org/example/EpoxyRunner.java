package org.example;

import org.example.benchmarks.HotelBenchmark;

import java.sql.SQLException;

public class EpoxyRunner {
    public static void main(String[] args) throws SQLException, InterruptedException {
        System.out.println("Running Epoxy Benchmarks: ");
        HotelBenchmark benchmark = new HotelBenchmark();
        System.out.println("Running Hotel Benchmark (80% searches, 20% reservations)");
        benchmark.runBenchmark(10, 10000);
    }
}
