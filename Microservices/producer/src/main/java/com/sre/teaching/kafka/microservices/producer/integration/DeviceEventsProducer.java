package com.sre.teaching.kafka.microservices.producer.integration;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sre.teaching.kafka.microservices.producer.datamodel.DeviceData;
import com.sre.teaching.kafka.microservices.producer.datamodel.EventType;
import com.sre.teaching.kafka.microservices.producer.datamodel.MessageHeader;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.protocol.types.Field;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

@Service
@Slf4j
public class DeviceEventsProducer {

    @Autowired
    KafkaTemplate<Integer, String> kafkaTemplate;

    @Autowired
    ObjectMapper objectMapper;

  //  @Transactional
    public SendResult<Integer, String> SendDeviceEvent(DeviceData deviceData)
    {
        String value;

        try {
            value=objectMapper.writeValueAsString(deviceData);
        } catch (JsonProcessingException e) {

            log.error("Failed to convert Object to String before posting event to Kafka. Reason {}",
                    e.getMessage());
            throw new RuntimeException(e);
        }

        CompletableFuture<SendResult<Integer, String>> sendResultCompletableFuture
                = kafkaTemplate.sendDefault(deviceData.getDeviceID(), value);

        SendResult<Integer, String> sendResult;
        try {
            sendResult = sendResultCompletableFuture.get();
        } catch (InterruptedException e) {
            log.error("Exception encountered during sending message. Reason : {}",e.getMessage());
            return null;
        } catch (ExecutionException e) {
            log.error("Exception encountered during sending message. Reason : {}",e.getMessage());
            return null;
        }

        log.info("Message Written at partition {}, Offset {}",
                    sendResult.getRecordMetadata().partition(),
                    sendResult.getRecordMetadata().offset());


      /*  sendResultCompletableFuture.thenApply( (sendResult)-> {
            if(sendResult==null){
                log.error("Failed to send");
                return false;
            }
            else {
                log.info("Message Written at partition {}, Offset {}",
                        sendResult.getRecordMetadata().partition(),
                        sendResult.getRecordMetadata().offset());
            }

            return true;
        });*/

        return  sendResult;
    }

    public SendResult<Integer, String> SendDeviceEventWithHeader(
                        DeviceData deviceData,String source, EventType evemtType)
    {
        String value=null;

        try {
            value=objectMapper.writeValueAsString(deviceData);
        } catch (JsonProcessingException e) {

            log.error("Failed to convert Object to String before posting event to Kafka." +
                            " Reason {}", e.getMessage());
            throw new RuntimeException(e);
        }

        ProducerRecord<Integer,String> producerRecord=
                     CreateProducerRecord(kafkaTemplate.getDefaultTopic(),
                                 deviceData.getDeviceID(), value,source, evemtType);

        CompletableFuture<SendResult<Integer, String>> sendResultCompletableFuture =
                                            kafkaTemplate.send(producerRecord);

        SendResult<Integer, String> sendResult;
        try {
            sendResult = sendResultCompletableFuture.get();
        } catch (InterruptedException e) {
            log.error("Exception encountered during sending message. Reason : {}",
                                e.getMessage());
            return null;
        } catch (ExecutionException e) {
            log.error("Exception encountered during sending message. Reason : {}",
                                                e.getMessage());
            return null;
        }
        catch(Exception ex) {
            log.error("Unhandled exception Occurred during sending message. Reason : {}",
                    ex.getMessage());
            return null;
        }

        log.info("Message Written at partition {}, Offset {}",
                            sendResult.getRecordMetadata().partition(),
                            sendResult.getRecordMetadata().offset());

        return sendResult;

    }

    ProducerRecord<Integer, String> CreateProducerRecord(String topicName,
                                                         Integer key,
                                                         String value,
                                                         String source,
                                                         EventType eventType) {

        ObjectMapper objectMapper=new ObjectMapper();


        MessageHeader messageHeader=new MessageHeader();
        messageHeader.setEventSource(source);
        messageHeader.setEventType(eventType);

        List<Header> headers= null;
        try {
            headers = List.of(new RecordHeader("custom-data",
                    objectMapper.writeValueAsBytes(messageHeader)));
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }

        return new ProducerRecord<Integer,String>(topicName,
                            (Integer) null,
                            key,
                            value,
                            headers);
    }
}

