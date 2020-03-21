package com.pluralsight.kafka.consumer.model;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class PreferredProduct {

    public PreferredProduct(String color, String type, String designType) {
        this.color = color;
        this.type = type;
        this.designType = designType;
    }

    private String color;

    private String type;

    private String designType;

    public String getColor() {
        return color;
    }

    public void setColor(String color) {
        this.color = color;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getDesignType() {
        return designType;
    }

    public void setDesignType(String designType) {
        this.designType = designType;
    }
}
