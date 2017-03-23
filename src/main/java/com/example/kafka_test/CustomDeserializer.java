package com.example.kafka_test;


import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.IOException;
import java.util.Map;

/**
 * Created by Amarendra Kumar on 11/25/2016.
 */
public class CustomDeserializer implements Deserializer<CustomObject> {

    /**
     * Configure this class.
     *
     * @param configs configs in key/value pairs
     * @param isKey   whether is for key or value
     */
    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {

    }

    /**
     * Deserialize a record value from a bytearray into a value or object.
     *
     * @param topic topic associated with the data
     * @param data  serialized bytes; may be null; implementations are recommended to handle null by returning a value or null rather than throwing an exception.
     * @return deserialized typed data; may be null
     */
    @Override
    public CustomObject deserialize(String topic, byte[] data) {
        CustomObject customObject = null;
        ObjectMapper objectMapper = new ObjectMapper();
        try {
            customObject = objectMapper.readValue(data, CustomObject.class);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return customObject;
    }

    @Override
    public void close() {

    }
}
