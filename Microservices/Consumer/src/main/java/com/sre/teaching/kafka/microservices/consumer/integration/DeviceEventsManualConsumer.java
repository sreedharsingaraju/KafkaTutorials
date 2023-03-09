package com.sre.teaching.kafka.microservices.consumer.integration;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.listener.AcknowledgingMessageListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Service;


@Service
@Slf4j
public class DeviceEventsManualConsumer implements AcknowledgingMessageListener<Integer, String> {

    @Autowired
    CommonProcessing messageProcessor;

    //enable this if need manual commit behaviour while
    //consuming and processing messages
    @Override
    //@KafkaListener(topics = {"devices-topic"})
    public void onMessage(ConsumerRecord<Integer, String> record, Acknowledgment acknowledgment) {

        log.info("Message Received in Manual commit call back!!!!!!!!!!!!");

        if(!messageProcessor.ProcessMessage(record, true))
        {
            log.error("Failed to process the event message so not commiting offset");
            return;
        }

        log.info("Acknowledging the record processing");
        acknowledgment.acknowledge();
        log.info("manually committed the offset");

        log.info("Message Processed !!!!!!!!!!!!");
    }

}
