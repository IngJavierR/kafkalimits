package com.pluralsight.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class ConsumerLoop implements Runnable {

    private final KafkaConsumer<String, String> consumer;
    private final List<String> topics;
    private final int id;
    private int countMsj = 0;

    public ConsumerLoop(int id, List<String> topics) {
        this.id = id;
        this.topics = topics;
        Properties props = new Properties();
        //props.put("bootstrap.servers", "192.168.0.171:9093,192.168.0.172:9094,192.168.0.173:9095");
        //props.put("bootstrap.servers", "localhost:9093");
        //props.put("bootstrap.servers", "192.168.0.170:9093");
        props.put("bootstrap.servers", "pkc-epwny.eastus.azure.confluent.cloud:9092");
        props.put("security.protocol","SASL_SSL");
        String configJaas = "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"WPEYWBQCN6X76JPN\"  password=\"8qgNZLyrh7e7BRsnZaRSXG5GLy7ZAUmKVy65Td1FrdcCQT38k+6tE8j9/WvQPqF6\";";
        props.put("sasl.jaas.config",configJaas);
        props.put("ssl.endpoint.identification.algorithm","https");
        props.put("sasl.mechanism","PLAIN");
        props.put("group.id", "user-tracking-consumer1_"+this.id);
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        this.consumer = new KafkaConsumer<String, String>(props);
    }

    public void run() {
        System.out.println("Task:" + this.id + " Thread:" + Thread.currentThread().getName());
        try {
            consumer.subscribe(topics);
            while (true) {
				Duration timeout = null;
				ConsumerRecords<String, String> records = consumer.poll(timeout.ofMillis(1000));
                for (ConsumerRecord<String, String> record : records) {
                    Map<String, Object> data = new HashMap<String, Object>();
                    data.put("partition", record.partition());
                    data.put("key", record.key());
                    data.put("group.id", "user-tracking-consumer1_"+this.id);
                    //data.put("value", record.value());
                    System.out.println("Counter:" + (countMsj++) + " Id:" + this.id + " Msj:" + data + " Thread:" + Thread.currentThread().getName());
                }
            }
        } catch (WakeupException e) {
            System.out.println("Ignore for shutdown" + e.getMessage());
        } finally {
            consumer.close();
        }
    }
}
