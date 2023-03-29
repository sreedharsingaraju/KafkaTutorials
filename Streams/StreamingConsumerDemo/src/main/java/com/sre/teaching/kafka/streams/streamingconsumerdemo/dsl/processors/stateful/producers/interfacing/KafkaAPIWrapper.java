package com.sre.teaching.kafka.streams.streamingconsumerdemo.dsl.processors.stateful.producers.interfacing;

import ch.qos.logback.classic.Logger;
import com.sre.teaching.kafka.streams.streamingconsumerdemo.config.ProducerConfigurer;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutionException;

public  class KafkaAPIWrapper {

    Logger logger= ( Logger)LoggerFactory.getLogger(KafkaAPIWrapper.class);

    KafkaProducer<String, Object> kafkaProducer;
    final String TOPIC_NAME;


    public KafkaAPIWrapper(String topicName, Boolean isInt)
    {

        ProducerConfigurer configurer = new ProducerConfigurer();
        kafkaProducer=new KafkaProducer<String, Object>(configurer.Configure(isInt));

        TOPIC_NAME=topicName;
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

    public void SendAsyncMessages(String key, String val) throws InterruptedException, ExecutionException {

       kafkaProducer.send(new ProducerRecord<>(
                            TOPIC_NAME,
                            key,
                            val),sendNotification).get();

    }

    public void SendAsyncMessages(String key, Integer val) throws InterruptedException, ExecutionException {

        kafkaProducer.send(new ProducerRecord<String,Object>(
                TOPIC_NAME,
                key,
                val),sendNotification).get();

    }
    public  void Close()
    {
        kafkaProducer.close();
    }

}
