package com.sre.teaching.kafka.microservices.consumer.config.plugins;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sre.teaching.kafka.microservices.consumer.datamodel.RestoreRetryMessageHeader;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.dao.RecoverableDataAccessException;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.function.BiFunction;

@Component
@Slf4j
public class FailureRecoveryProcessor {

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


    public FailureRecoveryProcessor()
    {

    }


    private RestoreRetryMessageHeader readRetryCountFromMessageHeader(ConsumerRecord<?, ?> record) {

        RestoreRetryMessageHeader retryMessageHeader = null;

        for (Header header : record.headers()) {
            if (header.key().equals(RECOVERY_RETRY_COUNT_KEY)) {
                try {
                    retryMessageHeader = objectMapper.readValue(header.value(),
                            RestoreRetryMessageHeader.class);
                } catch (IOException e) {
                    log.error("Exception during conversion while retrying value {}",header.value());
                    return null;
                }
            }
        }
        return retryMessageHeader;
    }

    public BiFunction<ConsumerRecord<?, ?>, Exception, TopicPartition> destinationResolver=
            (record, ex) -> {


                log.info("Fail Recovery strategy triggered");

                if (ex.getCause() instanceof RecoverableDataAccessException) {


                    if(!TryAddRecoveryRetryHeader(record))
                    {
                        log.error("Exhausted all recovery attempts. Hence posting to DLT.");

                        return new TopicPartition(dtltopic, record.partition());
                    }
                    else {

                        log.error("Recoverable exception hence posting to retry topic.");

                        return new TopicPartition(retrytopic, record.partition());
                    }
                }
                else {

                    log.error("Irrecoverable exception hence posting to dlt topic");

                    return new TopicPartition(dtltopic, record.partition());
                }
            };

    private boolean TryAddRecoveryRetryHeader(ConsumerRecord<?,?> record) {


        boolean succeeded=false;

        RestoreRetryMessageHeader restoreRetryMessageHeader;

        //get the header for checking how many times the message had been retried already
        restoreRetryMessageHeader = readRetryCountFromMessageHeader(record);

        Header header = null;

        if (restoreRetryMessageHeader == null) {

            restoreRetryMessageHeader=new RestoreRetryMessageHeader();

            restoreRetryMessageHeader.setRetryCount(1);

            try {
                header = new RecordHeader(RECOVERY_RETRY_COUNT_KEY,
                        objectMapper.writeValueAsBytes(restoreRetryMessageHeader));
            } catch (JsonProcessingException e) {
                log.error("Exception during conversion. value is {}",restoreRetryMessageHeader);
            }

            succeeded=true;

        } else if (restoreRetryMessageHeader.getRetryCount() < MAX_RECOVERY_RETRY_COUNT) {

            log.info("Incrementing the recovery retry count and adding to record before posting");

            restoreRetryMessageHeader.setRetryCount(restoreRetryMessageHeader.getRetryCount() + 1);


            try {

                header = new RecordHeader(RECOVERY_RETRY_COUNT_KEY,
                        objectMapper.writeValueAsBytes(restoreRetryMessageHeader));

            } catch (JsonProcessingException e) {

                log.error("Exception in Recovery retry during converting tge message header. reason:{}", e.getMessage());

                throw new RuntimeException(e);
            }

            succeeded=true;
        }

        log.info("Adding the recovery retry header : {}",restoreRetryMessageHeader);

        if(header!=null) {
            record.headers().add(header);
        }


        log.info("Recovery Attempt: Max recovery retry count: {}, Attempted Count: {}",
                        MAX_RECOVERY_RETRY_COUNT,
                        restoreRetryMessageHeader.getRetryCount());


        return succeeded;
    }
}
