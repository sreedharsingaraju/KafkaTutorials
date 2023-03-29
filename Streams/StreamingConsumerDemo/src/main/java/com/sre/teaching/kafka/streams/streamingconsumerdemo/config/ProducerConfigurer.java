package com.sre.teaching.kafka.streams.streamingconsumerdemo.config;

import ch.qos.logback.classic.Logger;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsConfig;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public class ProducerConfigurer {
      Logger logger= ( Logger) LoggerFactory.getLogger(ProducerConfigurer.class);

    public  Map<String, Object> Configure(Boolean isInt) {

        logger.info("Configuring Kafka Producer");

        Map<String, Object> kafkaconfig = new HashMap<>();

        kafkaconfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");//, localhost:9093, localhost:9094");


        kafkaconfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        if(isInt)
            kafkaconfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName());
        else
            kafkaconfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());


        kafkaconfig.put(ProducerConfig.ACKS_CONFIG, "all");

        kafkaconfig.put(ProducerConfig.RETRIES_CONFIG, 3);

        kafkaconfig.put(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, 1000);

        kafkaconfig.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG,"true");


        return kafkaconfig;
    }

}
