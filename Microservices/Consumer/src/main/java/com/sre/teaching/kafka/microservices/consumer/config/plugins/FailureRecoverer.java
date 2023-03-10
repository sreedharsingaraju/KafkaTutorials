package com.sre.teaching.kafka.microservices.consumer.config.plugins;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.stereotype.Component;


@Component
@Slf4j
public  class FailureRecoverer {

    @Autowired
    KafkaTemplate kafkaTemplate;

    @Autowired
    FailureRecoveryProcessor failureRecoveryProcessor;


    //this function helps us implement the recovery pattern when all retries are exhausted
    //we sre posting the message to retry topic if it is a recoverable exception
    //otherwise to dlt
    public DeadLetterPublishingRecoverer FailRecoverer() {

        DeadLetterPublishingRecoverer recoverer = null;

        recoverer = new DeadLetterPublishingRecoverer(
                                        kafkaTemplate,
                                        failureRecoveryProcessor.destinationResolver);

        return recoverer;
    }

}
