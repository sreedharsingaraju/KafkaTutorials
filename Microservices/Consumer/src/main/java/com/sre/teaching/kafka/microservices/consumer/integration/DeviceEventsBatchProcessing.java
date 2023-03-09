package com.sre.teaching.kafka.microservices.consumer.integration;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.listener.BatchMessageListener;
import org.springframework.stereotype.Service;

import java.util.List;


@Service
@Slf4j
public class DeviceEventsBatchProcessing implements BatchMessageListener<Integer, String> {
    @Override
    //enable if need batch processing
    //@KafkaListener(topics = {"devices-topic"})
    public void onMessage(List<ConsumerRecord<Integer, String>> consumerRecords) {

        log.info("Message Received in Batch processing !!!!!!!!!!!!");

        for (ConsumerRecord record : consumerRecords) {
            log.info("Record read is : {} {} Partition: {} offset : {}",
                    record.key(),
                    record.value(),
                    record.partition(),
                    record.offset());
        }
    }
}
