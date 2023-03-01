package com.sre.teaching.kafka.KafkaConsumerApp.deserializer;

import ch.qos.logback.classic.Logger;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sre.teaching.kafka.KafkaConsumerApp.datamodel.CustomData;
import org.apache.kafka.common.serialization.Deserializer;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class KafkaConsumerCustomDeserializer implements Deserializer<CustomData> {
    ObjectMapper custommapper = new ObjectMapper();
    Logger logger= (ch.qos.logback.classic.Logger)LoggerFactory.getLogger(KafkaConsumerCustomDeserializer.class);

    @Override
    public CustomData deserialize(String topicName, byte[] bytes) {

        CustomData customData= null;
        try {
            customData = custommapper.readValue(bytes, CustomData.class);
        } catch (IOException e) {
            logger.error("Failed to deserialize. Reason : {}",e.getMessage());
        }

        logger.info("Successfully Deserialized to Object");
        logger.info(String.valueOf(customData));

        return customData;
    }
}
