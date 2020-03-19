package com.pluralsight.kafka.consumer;

import lombok.extern.slf4j.Slf4j;
import java.util.Arrays;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Slf4j
public class Main {

    public static void main(String[] args) {

        int tasks = 1;
        ExecutorService executor = Executors.newFixedThreadPool(tasks);
        for (int i = 1; i <= tasks; i++) {

            Runnable worker = new ConsumerLoop(i, Arrays.asList("farmax_suc_999"));
            executor.execute(worker);
        }
        executor.shutdown();
        // Wait until all threads are finish
        while (!executor.isTerminated()) {

        }
        System.out.println("\nFinished all threads");

    }
}
