package com.sre.teaching.kafka.microservices.consumer.integration.retry;


import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.sre.teaching.kafka.microservices.consumer.datamodel.RetryMessageHeader;
import com.sre.teaching.kafka.microservices.consumer.integration.CommonProcessing;
import jakarta.persistence.criteria.CriteriaBuilder;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.listener.BatchMessageListener;
import org.springframework.kafka.listener.MessageListener;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.List;


//This class implements the Retry topic listener
@Service
@Slf4j
public class RetryMessagesListener implements MessageListener<Integer, String> {

    @Autowired
    CommonProcessing messageProcessor;
    @Override
    //enable this if need to get the default behavior
    //default is commit offset after entire batch is processed by the
    //listener factory
    @KafkaListener(topics = {"${mytopics.retry}"},groupId = "recovery-retry-group")
    public void onMessage(ConsumerRecord<Integer, String> record) {

        log.info("Message Received from Retry Queue !!!!!!!!!!!!");

        if(!messageProcessor.ProcessMessage(record, true)){
            log.error("Failed to re-process Event Message");
            return;
        }

        log.info("Message Re-Processed !!!!!!!!!!!!");
    }
}
