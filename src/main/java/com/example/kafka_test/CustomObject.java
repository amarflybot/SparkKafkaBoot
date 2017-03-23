package com.example.kafka_test;

/**
 * Created by Amarendra Kumar on 11/25/2016.
 */
public class CustomObject {

    private String name;
    private Integer roll;

    public CustomObject() {
    }

    public CustomObject(String name, Integer roll) {
        this.name = name;
        this.roll = roll;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Integer getRoll() {
        return roll;
    }

    public void setRoll(Integer roll) {
        this.roll = roll;
    }

    @Override
    public String toString() {
        return "CustomObject{" +
                "name='" + name + '\'' +
                ", roll=" + roll +
                '}';
    }
}
