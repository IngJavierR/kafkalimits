package com.pluralsight.kafka.consumer;

import lombok.extern.slf4j.Slf4j;
import java.util.Arrays;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Slf4j
public class Main {

    public static void main(String[] args) {
    	
    	if (args.length < 2) {
    		throw new IllegalArgumentException("Pelase specify numer of tasks");
    	}

        int tasks = Integer.parseInt(args[0]);
        int initTask = Integer.parseInt(args[1]);
        
        ExecutorService executor = Executors.newFixedThreadPool(tasks);
        
        for (int i = initTask; i <= tasks; i++) {
        	Runnable worker = new ConsumerLoop(i, Arrays.asList("Transactions"));
            //Runnable worker = new ConsumerLoop(i, Arrays.asList("farmax_suc_" + i));
            executor.execute(worker);
        }
        executor.shutdown();
        // Wait until all threads are finish
        while (!executor.isTerminated()) {

        }
        System.out.println("\nFinished all threads");

    }
}
