package com.pluralsight.kafka.producer.model;

import java.util.List;

public class Prices {

    private int idTienda;
    private List<Product> products;

    public int getIdTienda() {
        return idTienda;
    }

    public void setIdTienda(int idTienda) {
        this.idTienda = idTienda;
    }

    public List<Product> getProducts() {
        return products;
    }

    public void setProducts(List<Product> products) {
        this.products = products;
    }
}
