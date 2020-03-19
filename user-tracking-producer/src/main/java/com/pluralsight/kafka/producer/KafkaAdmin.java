package com.pluralsight.kafka.producer;

import org.apache.kafka.clients.admin.AdminClient;
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
        props.put("bootstrap.servers", "localhost:9093");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        this.adminClient = AdminClient.create(props);
    }

    public void createTopics(int number, String prefix) {

        List<NewTopic> newTopics = createListOfTopics(number, prefix);
        this.adminClient.createTopics(newTopics);
        this.adminClient.close();
    }



    private List<NewTopic> createListOfTopics(int number, String prefix){

        List<NewTopic> newTopics = new ArrayList<>();

        for (int i = 0; i < number; i++) {
            NewTopic newTopic = new NewTopic(prefix + "_suc_" + i, 2, (short)1);
            newTopics.add(newTopic);
        }
        return newTopics;
    }

}
