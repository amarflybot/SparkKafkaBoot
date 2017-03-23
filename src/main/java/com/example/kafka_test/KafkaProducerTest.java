package com.example.kafka_test;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

/**
 * Created by Amarendra Kumar on 11/21/2016.
 */
public class KafkaProducerTest {

    public static void main(String[] args) {

        Properties properties = new Properties();
        properties.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG , "192.168.2.4:9093");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG , "org.apache.kafka.common.serialization.StringSerializer");
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG , "org.apache.kafka.common.serialization.StringSerializer");
        //properties.put(ProducerConfig.ACKS_CONFIG, "all");
        properties.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, CommonClientConfigs.DEFAULT_SECURITY_PROTOCOL);

        KafkaProducer<String,String> kafkaProducer = new KafkaProducer(properties);

        String topic = "my-topics";

        //System.out.println(kafkaProducer.metrics());
        System.out.println(kafkaProducer.partitionsFor(topic));
          for(int i=0;i<100;i++){
              kafkaProducer.send(new ProducerRecord("my-topics","Value-"+ Integer.toString(i)));
          }
        kafkaProducer.close();
    }

}
