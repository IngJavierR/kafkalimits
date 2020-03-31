package com.pluralsight.kafka.producer;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateAclsOptions;
import org.apache.kafka.clients.admin.CreateTopicsOptions;
import org.apache.kafka.clients.admin.NewTopic;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class KafkaAdmin {

    private static AdminClient adminClient;

    public KafkaAdmin() {
        Properties props = new Properties();
        //props.put("bootstrap.servers", "192.168.0.171:9093,192.168.0.172:9094,192.168.0.173:9095");
        //props.put("bootstrap.servers", "localhost:9093,localhost:9094");
        props.put("bootstrap.servers", "192.168.0.170:9093");
        /*props.put("bootstrap.servers", "pkc-epwny.eastus.azure.confluent.cloud:9092");
        props.put("security.protocol","SASL_SSL");
        String configJaas = "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"WPEYWBQCN6X76JPN\"  password=\"8qgNZLyrh7e7BRsnZaRSXG5GLy7ZAUmKVy65Td1FrdcCQT38k+6tE8j9/WvQPqF6\";";
        props.put("sasl.jaas.config",configJaas);
        props.put("ssl.endpoint.identification.algorithm","https");
        props.put("sasl.mechanism","PLAIN");*/
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        KafkaAdmin.adminClient = AdminClient.create(props);
    }

    public void createTopics(int number, String prefix) {

        List<NewTopic> newTopics = createListOfTopics(number, prefix);
        List<String> existingTopics = listOfTopics(number, prefix);
        //KafkaAdmin.adminClient.createTopics(newTopics);



        KafkaAdmin.adminClient.deleteTopics(existingTopics);
        KafkaAdmin.adminClient.close();
    }



    private List<NewTopic> createListOfTopics(int number, String prefix){

        List<NewTopic> newTopics = new ArrayList<>();

        for (int i = 0; i < number; i++) {
            NewTopic newTopic = new NewTopic(prefix + i, 1, (short)1);
            newTopics.add(newTopic);
        }
        return newTopics;
    }

    private List<String> listOfTopics(int number, String prefix){

        List<String> newTopics = new ArrayList<>();

        for (int i = 0; i < number; i++) {
            newTopics.add(prefix + i);
        }
        return newTopics;
    }

}
