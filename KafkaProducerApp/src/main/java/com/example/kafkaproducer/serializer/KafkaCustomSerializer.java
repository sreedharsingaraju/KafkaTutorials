package com.example.kafkaproducer.serializer;

import ch.qos.logback.classic.Logger;
import com.example.kafkaproducer.datamodel.CustomData;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Serializer;
import org.slf4j.LoggerFactory;

public class KafkaCustomSerializer implements Serializer<CustomData> {

    ObjectMapper objectConverter = new ObjectMapper();
    Logger logger= (ch.qos.logback.classic.Logger) LoggerFactory.getLogger(KafkaCustomSerializer.class);

    @Override
    public byte[] serialize(String topic, CustomData customData) {

        logger.info("Custom Serializer invoked");
        try {
            logger.info("Converted Custom Object to Bytes and returning");
           return objectConverter.writeValueAsBytes(customData);
        } catch (JsonProcessingException e) {
            logger.error("Encountered error while serializing. Reason is : {}",e.getMessage());
            return  null;
        }
    }
}
