package com.sre.teaching.kafka.microservices.consumer.integration;


import com.fasterxml.jackson.databind.ObjectMapper;
import com.sre.teaching.kafka.microservices.consumer.datamodel.MessageHeader;
import com.sre.teaching.kafka.microservices.consumer.service.DeviceDataPersistentService;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import java.io.IOException;

@Service
@Slf4j
public class DeviceEventsConsumer {
    @Autowired
    DeviceDataPersistentService dataPersistentService;

    @KafkaListener(topics = {"devices-topic"})
    public void OnMessage(ConsumerRecord<Integer, String> record) {
        log.info("MEssage Received !!!!!!!!!!!!");

        ObjectMapper objectMapper = new ObjectMapper();

        log.info((" Full Consumer Record is : {}"), record);

        log.info(" Headers values {}", record.headers());

        for (Header recordHeader : record.headers()) {
            try {
                MessageHeader messageHeader =
                        objectMapper.readValue(recordHeader.value(),
                                MessageHeader.class);

                log.info("Header Key {} Header value {}", recordHeader.key(), messageHeader);

            } catch (IOException e) {
                log.warn("Exception encountered while converting the header value");
            }

        }
        log.info("Thread {} Received data in Consumer : key {} value {}  partition {}  offset {}",
                Thread.currentThread().threadId(),
                record.key(),
                record.value(),
                record.partition(),
                record.offset());


        log.info("Saving the record to DB");

        if (false == dataPersistentService.Save(record.value())) {
            log.error("Failed to save");
        } else
            log.info("Successfully saved the received message");


        log.info("Message Processed !!!!!!!!!!!!");


    }
}
