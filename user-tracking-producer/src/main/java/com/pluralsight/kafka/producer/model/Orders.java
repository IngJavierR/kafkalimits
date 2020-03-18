package com.pluralsight.kafka.producer.model;

import java.io.Serializable;

public class Orders implements Serializable {

    private int id;
    private String name;
    private int quantiy;

    public Orders(int id, String name, int quantiy) {
        this.id = id;
        this.name = name;
        this.quantiy = quantiy;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getQuantiy() {
        return quantiy;
    }

    public void setQuantiy(int quantiy) {
        this.quantiy = quantiy;
    }


}
