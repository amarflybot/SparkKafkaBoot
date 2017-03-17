package com.example;


import org.apache.kafka.clients.producer.*;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;

import javax.annotation.PostConstruct;
import java.util.Properties;
import java.util.concurrent.ExecutionException;


/**
 * Created by amarendra on 17/03/17.
 */
@Configuration
public class SpringBootKafkaProducer {

    @Value("${brokerList}")
    private String brokerList;

    @Value("${sync}")
    private String sync;

    @Value("${sendertopic}")
    private String sendertopic;

    private Producer<String, String> producer;

    public SpringBootKafkaProducer() {
    }

    @PostConstruct
    public void initIt() {
        Properties kafkaProps = new Properties();

        kafkaProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);

        kafkaProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringSerializer");
        kafkaProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringSerializer");
        kafkaProps.put(ProducerConfig.ACKS_CONFIG, "1");

        kafkaProps.put(ProducerConfig.RETRIES_CONFIG, "1");
        kafkaProps.put(ProducerConfig.LINGER_MS_CONFIG, 5);

        producer = new KafkaProducer<>(kafkaProps);

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
        ProducerRecord<String, String> record = new ProducerRecord<>(sendertopic, value);
        producer.send(record).get();

    }

    private void sendAsync(String value) {
        ProducerRecord<String, String> record = new ProducerRecord<>(sendertopic, value);

        producer.send(record, (RecordMetadata recordMetadata, Exception e) -> {
            if (e != null) {
                e.printStackTrace();
            }
        });
    }
}
