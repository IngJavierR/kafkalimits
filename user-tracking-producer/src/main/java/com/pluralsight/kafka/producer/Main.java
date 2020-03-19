package com.pluralsight.kafka.producer;

import lombok.extern.slf4j.Slf4j;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Slf4j
public class Main {

    public static void main(String[] args) throws InterruptedException {

        //afkaAdmin kafkaAdmin = new KafkaAdmin();
        //kafkaAdmin.createTopics(2000, "farmax");

        int tasks = 2000;
        int numMsj = 1;
        int msjSize = 1000;
        ExecutorService executor = Executors.newFixedThreadPool(tasks);
        for (int i = 1; i <= tasks; i++) {

            Runnable worker = new ProducerLoop("farmax_suc_" + i, numMsj, msjSize, "MS1_" + i);
            executor.execute(worker);
        }
        executor.shutdown();
        // Wait until all threads are finish
        while (!executor.isTerminated()) {

        }
        System.out.println("\nFinished all threads");

    }

}
