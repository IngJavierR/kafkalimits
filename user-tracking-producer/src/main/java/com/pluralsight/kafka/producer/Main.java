package com.pluralsight.kafka.producer;

import lombok.extern.slf4j.Slf4j;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Slf4j
public class Main {

    public static void main(String[] args) throws InterruptedException {

        //KafkaAdmin kafkaAdmin = new KafkaAdmin();
        //kafkaAdmin.createTopics(2000, "store-consumer-");

        int tasks =  100;
        int numMsj = 10;
        int msjSize = 1000;
        int iniTask = 1;
        boolean mode = true;

        /*if (args.length < 3) {
            throw new IllegalArgumentException("Pelase specify numer of tasks");
        }
        int tasks =  Integer.parseInt(args[0]);
        int numMsj = Integer.parseInt(args[1]);
        int msjSize = Integer.parseInt(args[2]);//1000
        int iniTask = Integer.parseInt(args[3]);
        boolean mode = Boolean.parseBoolean(args[4]);*/
        
        ExecutorService executor = Executors.newFixedThreadPool(tasks);
        
        for (int i = iniTask; i <= tasks; i++) {

        	if(mode) {
        	    //Runnable worker = new ProducerLoop("farmaxprices", numMsj, msjSize, "Suc_" + (i-1), i);
        		Runnable worker = new ProducerLoop("store-consumer-" + (i-1), numMsj, msjSize, "Suc_" + (i-1), i);
                executor.execute(worker);		
        	} else {
        		//ProducerLoop worker = new ProducerLoop("farmaxprices", numMsj, msjSize, "Suc_" + (i-1), i);
        		ProducerLoop worker = new ProducerLoop("store-consumer-" + (i-1), numMsj, msjSize, "Suc_" + (i-1), i);
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
