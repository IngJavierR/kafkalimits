package com.pluralsight.kafka.producer.model;

import lombok.Builder;
import lombok.Data;


@Data
@Builder
public class Event {

    private User user;

    private Product product;

    public User getUser() {
        return user;
    }

    public void setUser(User user) {
        this.user = user;
    }

    public Product getProduct() {
        return product;
    }

    public void setProduct(Product product) {
        this.product = product;
    }
}
