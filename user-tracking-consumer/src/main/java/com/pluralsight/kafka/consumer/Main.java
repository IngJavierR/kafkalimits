package com.pluralsight.kafka.consumer;

import lombok.extern.slf4j.Slf4j;
import java.util.Arrays;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Slf4j
public class Main {

    public static void main(String[] args) {

        int tasks = 1;
        int initTask = 1;

        /*if (args.length < 2) {
            throw new IllegalArgumentException("Pelase specify numer of tasks");
        }
        int tasks = Integer.parseInt(args[0]);
        int initTask = Integer.parseInt(args[1]);*/
        
        ExecutorService executor = Executors.newFixedThreadPool(tasks);
        
        for (int i = initTask; i <= tasks; i++) {
        	//Runnable worker = new ConsumerLoop(i, Arrays.asList("Transactions"));
            Runnable worker = new ConsumerLoop(i, Arrays.asList("farmaxprices"));
            executor.execute(worker);
        }
        executor.shutdown();
        // Wait until all threads are finish
        while (!executor.isTerminated()) {

        }
        System.out.println("\nFinished all threads");

    }
}
