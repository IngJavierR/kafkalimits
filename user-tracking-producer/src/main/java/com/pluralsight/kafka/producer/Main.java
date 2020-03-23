package com.pluralsight.kafka.producer;

import lombok.extern.slf4j.Slf4j;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Slf4j
public class Main {

    public static void main(String[] args) throws InterruptedException {

        //KafkaAdmin kafkaAdmin = new KafkaAdmin();
        //kafkaAdmin.createTopics(2000, "farmax");
    	
    	
    	if (args.length < 3) {
    		throw new IllegalArgumentException("Pelase specify numer of tasks");
    	}
    	
    	

        int tasks =  Integer.parseInt(args[0]);
        int numMsj = Integer.parseInt(args[1]);
        int msjSize = Integer.parseInt(args[2]);
        int iniTask = Integer.parseInt(args[3]);
        boolean mode = Boolean.parseBoolean(args[4]);
        
        ExecutorService executor = Executors.newFixedThreadPool(tasks);
        
        for (int i = iniTask; i <= tasks; i++) {

        	if(mode) {
        		Runnable worker = new ProducerLoop("farmax_suc_" + i, numMsj, msjSize, "MS1_" + i);
                executor.execute(worker);		
        	} else {
        		
        		ProducerLoop worker = new ProducerLoop("farmax_suc_" + i, numMsj, msjSize, "MS1_" + i);
                worker.run();
        	}
        	
        }
       
        
        executor.shutdown();
        // Wait until all threads are finish
        while (!executor.isTerminated()) {

        }
        System.out.println("\nFinished all threads");

    }

}
