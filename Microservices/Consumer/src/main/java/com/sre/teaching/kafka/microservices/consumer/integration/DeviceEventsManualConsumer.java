package com.sre.teaching.kafka.microservices.consumer.integration;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sre.teaching.kafka.microservices.consumer.entities.DeviceData;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.RecoverableDataAccessException;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.listener.AcknowledgingMessageListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Service;


@Service
@Slf4j
public class DeviceEventsManualConsumer implements AcknowledgingMessageListener<Integer, String> {

    @Autowired
    CommonProcessing messageProcessor;

    @Autowired
    ObjectMapper objectMapper;

    //enable this if need manual commit behaviour while
    //consuming and processing messages
    @Override
    @KafkaListener(topics = {"${mytopics.main}"}, groupId = "main-messages-group")
    public void onMessage(ConsumerRecord<Integer, String> record, Acknowledgment acknowledgment) {

        DeviceData deviceData = null;

        try {
            deviceData = objectMapper.readValue(record.value(), DeviceData.class);
        } catch (JsonProcessingException e) {
            log.info("exception during conversion, value : {}",record.value());
            return;
        }

        log.info("Message Received in Manual commit call back for Device {} Name {}!!!!!!!!!!!!",
                deviceData.getDeviceID(),
                deviceData.getDeviceName());


        try {
            if (!messageProcessor.ProcessMessage(record, true)) {
                log.error("Failed to process the event message so not commiting offset");
                return;
            }
        } catch (RecoverableDataAccessException ex) {
            log.error("Exception occurred while main queue processing," +
                    "Committing the offset as the retry will be taken care durinf fail recover");
            throw ex;
        } finally {
            acknowledgment.acknowledge();
        }

        log.info("Acknowledging the record processing");
        acknowledgment.acknowledge();
        log.info("manually committed the offset");

        log.info("Message Processed !!!!!!!!!!!!");
    }
}
