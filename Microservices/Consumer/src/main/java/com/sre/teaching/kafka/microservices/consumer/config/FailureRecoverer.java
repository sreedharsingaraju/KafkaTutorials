package com.sre.teaching.kafka.microservices.consumer.config;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sre.teaching.kafka.microservices.consumer.datamodel.RetryMessageHeader;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.dao.RecoverableDataAccessException;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.stereotype.Component;

import java.io.IOException;


@Component
@Slf4j
public  class FailureRecoverer {

    @Autowired
    KafkaTemplate kafkaTemplate;

    @Value("${mytopics.retry}")
    String retrytopic;

    @Value("${mytopics.dlt}")
    String dtltopic;

    //following variables for obtaining reteycount from header
    @Autowired
    ObjectMapper objectMapper;

    @Value("${recover.retry.key}")
    String RECOVERY_RETRY_COUNT_KEY;

    @Value("${max.recover.retry.count}")
    Integer MAX_RECOVERY_RETRY_COUNT;



    //this function helps us implement the recovery pattern when all retries are exhausted
    //we sre posting the message to retry topic if it is a recoverable exception
    //otherwise to dlt
    public DeadLetterPublishingRecoverer FailRecoverer() {

        DeadLetterPublishingRecoverer recoverer = null;

        recoverer = new DeadLetterPublishingRecoverer(kafkaTemplate,
                (record, ex) -> {
                    RetryMessageHeader retryMessageHeader = null;
                    log.info("Fail Recovery strategy trigerred");

                    //get the header for checking how many times the message had been retried already
                    retryMessageHeader = readRetryCountFromMessageHeader(record);

                    if ((retryMessageHeader != null) &&
                            (retryMessageHeader.getRetryCount() < MAX_RECOVERY_RETRY_COUNT)
                            && ex.getCause() instanceof RecoverableDataAccessException) {

                        log.error("Recoverable exception hence posting to retry topic");



                        Header header = null;
                        try {

                            log.info("Incrementing the recoveryretry count and adding to record before posting");

                            retryMessageHeader.setRetryCount(retryMessageHeader.getRetryCount() + 1);

                            header = new RecordHeader(RECOVERY_RETRY_COUNT_KEY,
                                    objectMapper.writeValueAsBytes(retryMessageHeader));

                        } catch (JsonProcessingException e) {
                            throw new RuntimeException(e);
                        }

                        record.headers().add(header);

                        return new TopicPartition(retrytopic, record.partition());

                    } else {
                        log.error("IR-Recoverable exception hence posting to dlt topic");
                        return new TopicPartition(dtltopic, record.partition());
                    }
                });


        return recoverer;
    }


    private RetryMessageHeader readRetryCountFromMessageHeader(ConsumerRecord<?, ?> record) {

        RetryMessageHeader retryMessageHeader = null;

        for (Header header : record.headers()) {
            if (header.key().equals(RECOVERY_RETRY_COUNT_KEY)) {
                try {
                    retryMessageHeader = objectMapper.readValue(header.value(),
                            RetryMessageHeader.class);
                } catch (IOException e) {
                    log.error("Exception during conversion while retrying");
                    return null;
                }
            }
        }
        return retryMessageHeader;
    }
}
