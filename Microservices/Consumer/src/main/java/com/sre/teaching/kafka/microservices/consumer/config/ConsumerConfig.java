package com.sre.teaching.kafka.microservices.consumer.config;

import com.sre.teaching.kafka.microservices.consumer.config.plugins.CustomErrorHandler;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.kafka.ConcurrentKafkaListenerContainerFactoryConfigurer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.listener.ContainerProperties;

@Configuration
@Slf4j
@EnableKafka
public class ConsumerConfig {


    @Autowired
    CustomErrorHandler customErrorHandler;


    //this function helps create and configure custom error handlers


    //Manual commit offset : overriding to configuring manual commit of the offsets
    //enable this bean if need to override the configs
    @Bean
    ConcurrentKafkaListenerContainerFactory<?, ?> kafkaListenerContainerFactory(
            ConcurrentKafkaListenerContainerFactoryConfigurer configurer,
            ConsumerFactory<Object, Object> kafkaConsumerFactory) {

        ConcurrentKafkaListenerContainerFactory<Object, Object> factory =
                new ConcurrentKafkaListenerContainerFactory<>();


        configurer.configure(factory, kafkaConsumerFactory);


        //configure the custom error handling hook
        factory.setCommonErrorHandler(customErrorHandler.createCustomHandler());

        //enable this code if want manual commit of offsets
        //make sure the listener DeviceEventsManualConsumer is enabled to process
        // and commit manually offsets
         factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);

        //enable this code if want batch mode commit of offsets
        //make sure the DeviceEvwntsBatchProcessing listener is enabled to process
        //factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.BATCH);
        //ContainerProperties.AckMode ackMode = factory.getContainerProperties().getAckMode();
        //factory.setBatchListener(true);
        //ackMode = factory.getContainerProperties().getAckMode();

        //enable thus code if configuring to scale the consumer service within a process
        //factory.setConcurrency(4);

        return factory;
    }
}
