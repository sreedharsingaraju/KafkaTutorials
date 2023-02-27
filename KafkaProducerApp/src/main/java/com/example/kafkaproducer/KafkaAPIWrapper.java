package com.example.kafkaproducer;

import ch.qos.logback.classic.Logger;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutionException;
import java.util.Map;
import java.util.concurrent.Future;

public class KafkaAPIWrapper {

    Logger logger= ( ch.qos.logback.classic.Logger)LoggerFactory.getLogger(KafkaAPIWrapper.class);

    KafkaProducer<String, Object> kafkaProducer;
    final String TOPIC_NAME;

    public  KafkaAPIWrapper(String topic, Map<String, Object> configure)
    {
        kafkaProducer=new KafkaProducer<String, Object>(configure);

        TOPIC_NAME=topic;
    }

    public void SendMessage(String key, Object value)
            throws ExecutionException, InterruptedException {

        ProducerRecord<String, Object> prodRecord = new ProducerRecord<>(TOPIC_NAME, key,
                value);

        RecordMetadata sendStatus = kafkaProducer.send(prodRecord).get();


        logger.info("Offset : " + sendStatus.offset()
                + "\n Partition : " + sendStatus.partition()
                + "\n Topic : " + sendStatus.topic());
    }

    Callback sendNotification = (RecordMetadata sendStatus, Exception ex) -> {

        logger.warn("Send Async notification came");
        if (ex != null) {

            logger.error("Exception Occurred : " + ex.getMessage());

        } else {

            logger.info("Offset : " + sendStatus.offset()
                    + "\n Partition : " + sendStatus.partition()
                    + "\n Topic : " + sendStatus.topic());

        }
    };

    public Future<RecordMetadata> SendAsyncMessage(String key, String value) {
        ProducerRecord<String, Object> prodRecord = new ProducerRecord<>(TOPIC_NAME, key,
                value);

        return kafkaProducer.send(prodRecord, sendNotification);

    }

    public  void Close()
    {
        kafkaProducer.close();
    }

}
