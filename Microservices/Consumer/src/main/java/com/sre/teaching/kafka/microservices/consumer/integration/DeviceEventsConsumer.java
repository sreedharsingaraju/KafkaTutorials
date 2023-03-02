package com.sre.teaching.kafka.microservices.consumer.integration;


import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class DeviceEventsConsumer {
    @KafkaListener(topics = {"devices-topic"})
    public  void OnMessage(ConsumerRecord<Integer,String> record)
    {
        log.info((" Full Consumer Record is : {}"),record);

       log.info("Thread {} Received in Consumer : key {} value {}  partition {}  offset {}",
                    Thread.currentThread().threadId(),
                    record.key(),
                    record.value(),
                    record.partition(),
                    record.offset());

    }
}
