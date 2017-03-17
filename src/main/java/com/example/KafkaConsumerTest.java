package com.example;


import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by amarendra on 17/3/17.
 */
@SpringBootApplication
public class KafkaConsumerTest {

    public static final Logger LOGGER = Logger.getLogger(KafkaConsumerTest.class);

    public static void main(String[] args) throws InterruptedException {

        SpringApplication.run(KafkaConsumerTest.class, args);

        Map<String, Object> kafkaConsParams = new HashMap<>();
        kafkaConsParams.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        kafkaConsParams.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        kafkaConsParams.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        kafkaConsParams.put(ConsumerConfig.GROUP_ID_CONFIG, "groupA");
        kafkaConsParams.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        kafkaConsParams.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, java.lang.Boolean.TRUE);
        kafkaConsParams.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        kafkaConsParams.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000");

        final Map<String, Object> kafkaProdParams = new HashMap<>();
        kafkaProdParams.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        kafkaProdParams.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        kafkaProdParams.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        kafkaProdParams.put(ProducerConfig.ACKS_CONFIG, "all");
        kafkaProdParams.put(ProducerConfig.RETRIES_CONFIG, 0);
        kafkaProdParams.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
        kafkaProdParams.put(ProducerConfig.LINGER_MS_CONFIG, 1);
        kafkaProdParams.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);

        Collection<String> topicsToListen = Arrays.asList("producer");
        final String topicToSend = "consumer";

        // Create a local StreamingContext with two working thread and batch interval of 1 second
        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("FirstProgramTest");
        JavaStreamingContext streamingContext = new JavaStreamingContext(conf, Durations.seconds(1));

        final JavaInputDStream<ConsumerRecord<String, String>> directStream =
                KafkaUtils.createDirectStream(
                        streamingContext,
                        LocationStrategies.PreferConsistent(),
                        ConsumerStrategies.<String, String>Subscribe(topicsToListen, kafkaConsParams)
                );


        /*directStream.mapToPair(
                new PairFunction<ConsumerRecord<String, String>, String, String>() {
                    @Override
                    public Tuple2<String, String> call(ConsumerRecord<String, String> record) {
                        return new Tuple2<>(record.key(), record.value());
                    }
                }).print();*/

        directStream.map(new Function<ConsumerRecord<String,String>, Object>() {
            @Override
            public Object call(ConsumerRecord<String, String> v1) throws Exception {
                LOGGER.info(" KEY-> "+ v1.key() + " VALUE-> "+v1.value());
                final KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(kafkaProdParams);
                kafkaProducer.send(new ProducerRecord<String, String>(topicToSend, v1.value().toUpperCase()));
                return null;
            }
        }).print();
        /*directStream.foreachRDD(new VoidFunction2<JavaRDD<ConsumerRecord<String, String>>, Time>() {
            @Override
            public void call(JavaRDD<ConsumerRecord<String, String>> consumerRecordJavaRDD, Time time) throws Exception {
                final OffsetRange[] offsetRanges = ((HasOffsetRanges) consumerRecordJavaRDD.rdd()).offsetRanges();
                consumerRecordJavaRDD.foreachPartition(new VoidFunction<Iterator<ConsumerRecord<String, String>>>() {
                    @Override
                    public void call(Iterator<ConsumerRecord<String, String>> consumerRecords) {
                        OffsetRange o = offsetRanges[TaskContext.get().partitionId()];

                        LOGGER.info(
                                o.topic() + " " + o.partition() + " " + o.fromOffset() + " " + o.untilOffset());
                    }
                });
            }
        });*/


        // Start running the job to receive and transform the data
        streamingContext.start();

        //Allows the current thread to wait for the termination of the context by stop() or by an exception
        streamingContext.awaitTermination();
    }
}
