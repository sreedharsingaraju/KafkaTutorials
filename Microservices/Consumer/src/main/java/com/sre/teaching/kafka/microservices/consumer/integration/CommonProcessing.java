package com.sre.teaching.kafka.microservices.consumer.integration;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sre.teaching.kafka.microservices.consumer.datamodel.RetryMessageHeader;
import com.sre.teaching.kafka.microservices.consumer.service.DeviceDataPersistentService;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.dao.RecoverableDataAccessException;
import org.springframework.stereotype.Component;

import java.io.IOException;


// this is the common class which implements the common processing logic across different types of listening
// implementations

@Component
@Slf4j
public class CommonProcessing {
    @Autowired
    DeviceDataPersistentService dataPersistentService;

    @Value("${recover.retry.key}")
    String RECOVERY_RETRY_COUNT_KEY;
    @Value("${max.recover.retry.count}")
    Integer MAX_RECOVERY_RETRY_COUNT;

    @Autowired
    ObjectMapper objectMapper;

    public  boolean isRecoveryRetryLimit(ConsumerRecord<Integer, String> record) {
        log.info(" Headers values {}", record.headers());

        for (Header recordHeader : record.headers()) {
            if (recordHeader.key().equals(RECOVERY_RETRY_COUNT_KEY)) {

                try {

                    RetryMessageHeader retryMessageHeader =
                            objectMapper.readValue(recordHeader.value(),
                                    RetryMessageHeader.class);

                    log.info("Header Key {} Header value {}", recordHeader.key(), retryMessageHeader);

                    if (retryMessageHeader != null && retryMessageHeader.getRetryCount() >= MAX_RECOVERY_RETRY_COUNT) {
                        //this should be fine as the record would have been written to DLT by the
                        //failrecovery implementation
                        log.info("Max recovery retries reached so skipping this message {}", record.value());
                        return true;
                    }
                } catch (IOException e) {
                    log.warn("Exception encountered while converting the header value");
                    return false;
                }
            }
        }

        return false;
    }


    public Boolean ProcessMessage(ConsumerRecord<Integer, String> record, Boolean saveMessage) {

        ObjectMapper objectMapper = new ObjectMapper();

        log.info((" Full Consumer Record is : {}"), record);

        log.info(" Headers values {}", record.headers());


        if(isRecoveryRetryLimit(record))
        {
            return true;
        }

        log.info("Thread {} Received data in Consumer : key {} value {}  partition {}  offset {}",
                Thread.currentThread().threadId(),
                record.key(),
                record.value(),
                record.partition(),
                record.offset());

        if (saveMessage) {
            log.info("Saving the record to DB");
            if (!dataPersistentService.Save(record.value())) {
                log.error("Failed to save");
                throw new RecoverableDataAccessException("Unable to save, retry this operation");
            } else
                log.info("Successfully saved the received message");
        }
        return true;
    }
}


