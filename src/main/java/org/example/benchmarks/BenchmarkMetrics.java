package org.example.benchmarks;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

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
