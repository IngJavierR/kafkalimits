package com.pluralsight.kafka.consumer;

import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.*;

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
        //props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9094");
        ///props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.0.172:9094");
        /*props.put("bootstrap.servers", "pkc-epwny.eastus.azure.confluent.cloud:9092");
        props.put("security.protocol","SASL_SSL");
        String configJaas = "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"WPEYWBQCN6X76JPN\"  password=\"8qgNZLyrh7e7BRsnZaRSXG5GLy7ZAUmKVy65Td1FrdcCQT38k+6tE8j9/WvQPqF6\";";
        props.put("sasl.jaas.config",configJaas);
        props.put("ssl.endpoint.identification.algorithm","https");
        props.put("sasl.mechanism","PLAIN");*/
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "aximxfarma2:9193,aximxfarma3:9193,aximxfarma4:9193");
        props.put(AdminClientConfig.SECURITY_PROTOCOL_CONFIG,"SASL_SSL");
        props.put(SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG, SslConfigs.DEFAULT_SSL_ENDPOINT_IDENTIFICATION_ALGORITHM);
        String configJaas = "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"posClient\" password=\"P05C%li3n\";";
        props.put(SaslConfigs.SASL_JAAS_CONFIG,configJaas);
        props.put(SaslConfigs.SASL_MECHANISM,"SCRAM-SHA-256");
        props.put(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, getClass().getClassLoader().getResource("kafka.server.keystore.jks").toString().replace("file:/", ""));
        props.put(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, "FaPos2015");
        props.put(SslConfigs.SSL_KEY_PASSWORD_CONFIG, "FaPos2015");
        props.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, getClass().getClassLoader().getResource("kafka.server.truststore.jks").toString().replace("file:/", ""));
        props.put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, "FaPos2015");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "farmaxprices-gpo_"+(this.id - 1));
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        this.consumer = new KafkaConsumer<String, String>(props);
    }

    public void run() {
        System.out.println("Task:" + this.id + " Thread:" + Thread.currentThread().getName());
        try {
            //consumer.assign(Collections.singletonList(new TopicPartition("farmaxprices", (this.id - 1)  )));
            consumer.subscribe(topics);
            while (true) {
				Duration timeout = null;
				ConsumerRecords<String, String> records = consumer.poll(timeout.ofMillis(1000));
                for (ConsumerRecord<String, String> record : records) {
                    Map<String, Object> data = new HashMap<String, Object>();
                    data.put("partition", record.partition());
                    data.put("key", record.key());
                    data.put("group.id", "farmax-consumer_"+this.id);
                    //data.put("value", record.value());
                    System.out.println("Message Counter:" + (countMsj++) + " Id:" + this.id + " Msj:" + data + " Thread:" + Thread.currentThread().getName());
                    consumer.commitSync();
                }
            }
        } catch (WakeupException e) {
            System.out.println("Ignore for shutdown" + e.getMessage());
        } finally {
            consumer.close();
        }
    }
}
