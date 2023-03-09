package com.sre.teaching.kafka.microservices.consumer.integration;



import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.listener.MessageListener;
import org.springframework.stereotype.Service;



@Service
@Slf4j
public class DeviceEventsConsumer implements MessageListener<Integer, String> {


    @Autowired
    CommonProcessing messageProcessor;

    @Override
    //enable this if need to get the default behavior
    //default is commit offset after entire batch is processed by the
    //listener factory
    @KafkaListener(topics = {"${mytopics.main}"}, groupId = "main-messages-group")
    public void onMessage(ConsumerRecord<Integer, String> record) {

        log.info("Message Received !!!!!!!!!!!!");

        if(!messageProcessor.ProcessMessage(record, true)){
            log.error("Failed to process Event Message");
        }

        log.info("Message Processed !!!!!!!!!!!!");
    }
}
