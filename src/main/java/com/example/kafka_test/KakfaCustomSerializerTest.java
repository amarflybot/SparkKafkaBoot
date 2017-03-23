package com.example.kafka_test;

/**
 * Created by Amarendra Kumar on 11/25/2016.
 */
public class KakfaCustomSerializerTest {

    public static void main(String[] args) {

        String brokers = "192.168.2.4:9092,localhost:9093,localhost:9094";
        String groupId = "custObj01";
        String topic = "my-topics";

        if (args != null && args.length == 3) {
            brokers = args[0];
            groupId = args[1];
            topic = args[2];
        }

        CustomProducerThread producerThread = new CustomProducerThread(brokers,topic);
        new Thread(producerThread).start();

        CustomConsumerThread consumerThread = new CustomConsumerThread(brokers, groupId, topic);
        new Thread(consumerThread).start();

        try {
            Thread.sleep(100000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
