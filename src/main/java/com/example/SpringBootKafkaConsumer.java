package com.example;


import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;

import javax.annotation.PostConstruct;
import java.util.Properties;
import java.util.concurrent.ExecutionException;


/**
 * Created by amarendra on 17/03/17.
 */
@Configuration
public class SpringBootKafkaConsumer {

    @Value("${brokerList}")
    private String brokerList;

    @Value("${sync}")
    private String sync;

    @Value("${recievertopic}")
    private String recievertopic;

    private Consumer<String, String> consumer;

    public SpringBootKafkaConsumer() {
    }

    @PostConstruct
    public void initIt() {
        Properties kafkaProps = new Properties();

        kafkaProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);

        kafkaProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringDeserializer");
        kafkaProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringDeserializer");

        consumer = new KafkaConsumer<>(kafkaProps);

    }

    public void send(String value) throws ExecutionException,
            InterruptedException {
        if ("sync".equalsIgnoreCase(sync)) {
            sendSync(value);
        } else {
            sendAsync(value);
        }
    }

    private void sendSync(String value) throws ExecutionException,
            InterruptedException {
        ProducerRecord<String, String> record = new ProducerRecord<>(recievertopic, value);
        consumer.send(record).get();

    }

    private void sendAsync(String value) {
        ProducerRecord<String, String> record = new ProducerRecord<>(recievertopic, value);

        producer.send(record, (RecordMetadata recordMetadata, Exception e) -> {
            if (e != null) {
                e.printStackTrace();
            }
        });
    }
}
