package com.pluralsight.kafka.producer.model;

import java.math.BigInteger;

public class Product {

    private int idTienda;

    private String sku;

    private BigInteger Price;

    public int getIdTienda() {
        return idTienda;
    }

    public void setIdTienda(int idTienda) {
        this.idTienda = idTienda;
    }

    public String getSku() {
        return sku;
    }

    public void setSku(String sku) {
        this.sku = sku;
    }

    public BigInteger getPrice() {
        return Price;
    }

    public void setPrice(BigInteger price) {
        Price = price;
    }
}
