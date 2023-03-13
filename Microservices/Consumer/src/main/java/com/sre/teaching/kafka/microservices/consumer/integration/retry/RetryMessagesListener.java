package com.sre.teaching.kafka.microservices.consumer.integration.retry;


import com.sre.teaching.kafka.microservices.consumer.integration.CommonProcessing;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.RecoverableDataAccessException;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.listener.AcknowledgingMessageListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Service;


//This class implements the Retry topic listener
@Service
@Slf4j
public class RetryMessagesListener implements AcknowledgingMessageListener<Integer, String> {

    @Autowired
    CommonProcessing messageProcessor;

    @Override
    //enable this if need to get the default behavior
    //default is commit offset after entire batch is processed by the
    //listener factory
    //the contaimner bean is a different one which polls after cnfigured interval
    @KafkaListener(topics = {"${mytopics.retry}"}, groupId = "recovery-retry-group",
            containerFactory = "retryMessageDelayConcurrentKafkaListenerContainerFactory"
    )
    public void onMessage(ConsumerRecord<Integer, String> record, Acknowledgment acknowledgment) {

        log.info("Message Received from Retry Queue !!!!!!!!!!!!");

        try {
            if (!messageProcessor.ProcessMessage(record, true)) {
                log.error("Failed to re-process Event Message");
                return;
            }
            log.info("Recovery succeeded");
        }
        catch (RecoverableDataAccessException ex) {
            log.error("Exception occurred while recovery processing," +
                    " Will be retrying again but committing the offset in retry queue");
            throw  ex;
        }
        finally {
            acknowledgment.acknowledge();
        }

        log.info("Message Re-Process finished. check above for status !!!!!!!!!!!!");

    }
}