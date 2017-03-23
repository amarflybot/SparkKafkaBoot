package com.example;


import com.bazaarvoice.jolt.Chainr;
import com.bazaarvoice.jolt.JsonUtils;
import com.mongodb.DB;
import com.mongodb.DBObject;
import com.mongodb.Mongo;
import com.mongodb.util.JSON;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import scala.Tuple2;

import java.util.*;

/**
 * Created by amarendra on 17/3/17.
 */
public class KafkaConsumerTest {

    public static final Logger LOGGER = Logger.getLogger(KafkaConsumerTest.class);

    public static void main(String[] args) throws InterruptedException {

        Map<String, Object> kafkaConsParams = new HashMap<>();
        kafkaConsParams.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.2.10:9092");
        kafkaConsParams.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        kafkaConsParams.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        kafkaConsParams.put(ConsumerConfig.GROUP_ID_CONFIG, "groupA");
        kafkaConsParams.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        kafkaConsParams.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, java.lang.Boolean.TRUE);
        kafkaConsParams.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        kafkaConsParams.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000");

        final Map<String, Object> kafkaProdParams = new HashMap<>();
        kafkaProdParams.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.2.10:9092");
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
        JavaDStream<Object> objectJavaDStream = directStream.flatMap(new FlatMapFunction<ConsumerRecord<String, String>, Object>() {
            @Override
            public Iterable<Object> call(ConsumerRecord<String, String> stringStringConsumerRecord) throws Exception {
                LOGGER.info(" KEY-> " + stringStringConsumerRecord.key() + " VALUE-> " + stringStringConsumerRecord.value());
                Object object = JsonUtils.jsonToObject(stringStringConsumerRecord.value());
                return Arrays.asList(object);
            }
        });

        JavaPairDStream<Object, Object> objectObjectJavaPairDStream = objectJavaDStream.mapToPair(new PairFunction<Object, Object, Object>() {
            @Override
            public Tuple2<Object, Object> call(Object o) throws Exception {
                List<Object> specs = JsonUtils.classpathToList("/testSpec.json");
                Chainr chainr = Chainr.fromSpec(specs);
                Object transformedOutput = chainr.transform(o);
                Tuple2<Object, Object> objectTuple2 = new Tuple2<>(o, transformedOutput);
                LOGGER.info(" Tuple_1-> " + JsonUtils.toPrettyJsonString(objectTuple2._1) +
                        " Tuple_2-> " + JsonUtils.toPrettyJsonString(objectTuple2._2));
                return objectTuple2;
            }
        });
        JavaPairDStream<Object, Object> reduceByKey = objectObjectJavaPairDStream.reduceByKey(new Function2<Object, Object, Object>() {
            @Override
            public Object call(Object v1, Object v2) throws Exception {
                LOGGER.info("reduceByKey  Object1-> " + JsonUtils.toPrettyJsonString(v1) +
                        " Object2-> " + JsonUtils.toPrettyJsonString(v2));
                final KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(kafkaProdParams);
                String prettyJsonStringResult = JsonUtils.toPrettyJsonString(v2);
                kafkaProducer.send(new ProducerRecord<String, String>(topicToSend, prettyJsonStringResult));
                Mongo mongo = new Mongo("192.168.2.10", 27017);
                DB db = mongo.getDB("messageProcessed");
                DBObject dbObject = (DBObject) JSON.parse(prettyJsonStringResult);
                db.getCollection("test").insert(dbObject);
                return prettyJsonStringResult;
            }
        });

        reduceByKey.print();
        /*objectObjectJavaPairDStream.foreachRDD(new VoidFunction<JavaPairRDD<Object, Object>>() {
            @Override
            public void call(JavaPairRDD<Object, Object> objectObjectJavaPairRDD) throws Exception {
                objectObjectJavaPairRDD.foreach(new VoidFunction<Tuple2<Object, Object>>() {
                    @Override
                    public void call(Tuple2<Object, Object> objectObjectTuple2) throws Exception {
                        System.out.println(objectObjectTuple2);
                    }
                });
            }
        });*/
        /*directStream.map(new Function<ConsumerRecord<String, String>, Object>() {
            @Override
            public Object call(ConsumerRecord<String, String> v1) throws Exception {
                LOGGER.info(" KEY-> " + v1.key() + " VALUE-> " + v1.value());
                Object object = JsonUtils.jsonToObject(v1.value());
                //final KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(kafkaProdParams);
                //kafkaProducer.send(new ProducerRecord<String, String>(topicToSend, v1.value().toUpperCase()));
                return object;
            }
        }).reduce(new Function2<Object, Object, Object>() {
            @Override
            public Object call(Object v1, Object v2) throws Exception {
                List<Object> specs = JsonUtils.classpathToList("/testSpec.json");
                Chainr chainr = Chainr.fromSpec(specs);
                Object transformedOutput = chainr.transform(v2);
                return transformedOutput;
            }
        }).print();*//*.foreachRDD(new VoidFunction<JavaRDD<Object>>() {
            @Override
            public void call(JavaRDD<Object> objectJavaRDD) throws Exception {

                objectJavaRDD.foreach(new VoidFunction<Object>() {
                    @Override
                    public void call(Object transformedOutput) throws Exception {
                        final KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(kafkaProdParams);
                        String prettyJsonStringResult = JsonUtils.toPrettyJsonString(transformedOutput);
                        kafkaProducer.send(new ProducerRecord<String, String>(topicToSend, prettyJsonStringResult));
                    }
                });
            }
        })*/
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
