package com.sre.teaching.kafka.microservices.producer.config;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Configurable;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.ProducerListener;


import java.util.HashMap;
import java.util.Map;

@Configuration
@Slf4j
public class ProducerConfig {


    @Value("${spring.kafka.producer.bootstrap-servers}")
    private String bootStrapServers;

    @Value("${spring.kafka.template.default-topic}")
    private String defaultTopic;
    @Value("${spring.kafka.producer.properties.acks}")
    private String acks;

    @Value("${spring.kafka.producer.properties.retries}")
    private Integer retries;
    @Value("${spring.kafka.producer.properties.retry.backoff.ms}")
    private Integer retryBackoffMs;



    //1. create the configuration

    public Map<String, Object> producerConfig() {

        log.info("Configuring Kafka Producer");

        Map<String, Object> kafkaconfig = new HashMap<>();

        kafkaconfig.put(org.apache.kafka.clients.producer.ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                                bootStrapServers);//, localhost:9093, localhost:9094");

        kafkaconfig.put(org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                                IntegerSerializer.class.getName());

        kafkaconfig.put(org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                                StringSerializer.class.getName());
        kafkaconfig.put(org.apache.kafka.clients.producer.ProducerConfig.ACKS_CONFIG, acks);

        kafkaconfig.put(org.apache.kafka.clients.producer.ProducerConfig.RETRIES_CONFIG, retries);


        kafkaconfig.put(org.apache.kafka.clients.producer.ProducerConfig.RETRY_BACKOFF_MS_CONFIG,
                                                retryBackoffMs);

        kafkaconfig.put(org.apache.kafka.clients.producer.ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 1000);

        kafkaconfig.put(org.apache.kafka.clients.producer.ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 2000);



        return kafkaconfig;
    }

    //2. create producer factory and pass configuration created
   @Bean
    public ProducerFactory<Integer, String> producerFactory()
    {
        return  new DefaultKafkaProducerFactory<>(producerConfig());
    }

    //3. create kafkatemplate using the producer factory

    @Bean
    KafkaTemplate<Integer, String> kafkaTemplate(ProducerFactory<Integer, String> producerFactory)
    {
        KafkaTemplate<Integer, String> kafkaTemplate= new KafkaTemplate<>(producerFactory);
        kafkaTemplate.setDefaultTopic(defaultTopic);

        kafkaTemplate.setProducerListener(new ProducerListener<>() {
            @Override
            public void onSuccess(ProducerRecord<Integer, String> producerRecord, RecordMetadata recordMetadata) {
                log.info("Produced record successfully {}", producerRecord);

                log.info("Produced record successfully at partition {}, Offset {}",
                        recordMetadata.partition(),
                        recordMetadata.offset());
            }

            @Override
            public void onError(ProducerRecord<Integer, String> producerRecord, RecordMetadata recordMetadata,
                                Exception exception)
            {
                log.info("Failure occurred win sending  record {}", producerRecord);
                log.info("Exception Reason is {}",exception.getMessage());
            }
        });

        return  kafkaTemplate;
    }
}
