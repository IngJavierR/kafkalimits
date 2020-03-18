package com.pluralsight.kafka.producer;

import com.google.gson.Gson;
import com.pluralsight.kafka.producer.model.Prices;
import com.pluralsight.kafka.producer.model.Product;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.math.BigInteger;
import java.util.*;

public class ProducerLoop implements Runnable {

    private final Producer<String, String> producer;
    private final String topic;
    private final int numMsj;
    private final int msjSize;
    private final String idPrefix;
    private int countMsj = 0;

    public ProducerLoop(String topic, int numMsj, int msjSize, String idPrefix) {
        this.topic = topic;
        this.numMsj = numMsj;
        this.msjSize = msjSize;
        this.idPrefix = idPrefix;
        Properties props = new Properties();
        //props.put("bootstrap.servers", "192.168.0.171:9093,192.168.0.172:9094,192.168.0.173:9095");
        //props.put("bootstrap.servers", "localhost:9093,localhost:9094");
        props.put("bootstrap.servers", "192.168.0.170:9093");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        producer = new KafkaProducer<>(props);
    }

    @Override
    public void run() {
        System.out.println("Topic:" + this.topic + " Thread " + Thread.currentThread().getName());

        //char[] chars = new char[this.msjSize];
        //Arrays.fill(chars, 'f');
        //String msg = new String(chars);

        List<Product> products = getProductPrice(20000);

        String msg = new Gson().toJson(products);

        for (int i = 0; i <= this.numMsj; i++) {
            String key = this.idPrefix + "_" + i;
            //String value = "id_" + i + msg;
            String value = msg;

            ProducerRecord<String, String> producerRecord = new ProducerRecord<>(this.topic, key, value);
            System.out.println("Counter:" + (countMsj++) + " key:" + key + " Thread:" + Thread.currentThread().getName());
            this.producer.send(producerRecord);
        }
        this.producer.close();
    }

    private List<Product> getProductPrice(int numTiendas){

        List<Product> products = new ArrayList<>();

        Product product;
        for (int i = 0; i <= numTiendas; i++){
            product = new Product();
            product.setIdTienda(i);
            product.setPrice(BigInteger.valueOf(i + 1));
            product.setSku("MS_" + i + "QA");
            products.add(product);
        }
        return products;
    }
}
