package com.sre.teaching.kafka.microservices.consumer.integration;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sre.teaching.kafka.microservices.consumer.datamodel.MessageHeader;
import com.sre.teaching.kafka.microservices.consumer.service.DeviceDataPersistentService;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.IOException;


// this is the common class which implements the common processing logic across different types of listening
// implementations

@Component
@Slf4j
public class CommonProcessing {
    @Autowired
    DeviceDataPersistentService dataPersistentService;

    public Boolean ProcessMessage(ConsumerRecord<Integer, String> record, Boolean saveMessage) {

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
                return false;
            }

        }
        log.info("Thread {} Received data in Consumer : key {} value {}  partition {}  offset {}",
                Thread.currentThread().threadId(),
                record.key(),
                record.value(),
                record.partition(),
                record.offset());


        if(saveMessage) {
            log.info("Saving the record to DB");

            try {
                if (false == dataPersistentService.Save(record.value())) {
                    log.error("Failed to save");
                    return  false;
                } else
                    log.info("Successfully saved the received message");
            }
            catch (Exception ex)
            {
                log.error("Exception during saving the record . reason {}",ex.getMessage());
                return  false;
            }
        }


        return  true;

    }

}
