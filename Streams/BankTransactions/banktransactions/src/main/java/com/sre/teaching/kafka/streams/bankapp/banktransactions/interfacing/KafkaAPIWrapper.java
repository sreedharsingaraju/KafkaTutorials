package com.sre.teaching.kafka.streams.bankapp.banktransactions.interfacing;

import ch.qos.logback.classic.Logger;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.protocol.types.Field;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;

public  class KafkaAPIWrapper {

    Logger logger= ( ch.qos.logback.classic.Logger)LoggerFactory.getLogger(KafkaAPIWrapper.class);

    KafkaProducer<String, Object> kafkaProducer;
    final String TOPIC_NAME;


    public KafkaAPIWrapper(String topicName)
    {

        Configurer configurer = new Configurer();
        kafkaProducer=new KafkaProducer<String, Object>(configurer.Configure());

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

    public void SendAsyncMessages() throws InterruptedException {

          for (int count = 0; count < 2; count++) {
              System.out.println("writing batch " + count + " or transactions");
              kafkaProducer.send(getNewRecord("Maruthi"), sendNotification);
              Thread.sleep(2000);
              kafkaProducer.send(getNewRecord("Shruti"), sendNotification);
              Thread.sleep(2000);
          }

    }

    private ProducerRecord<String, Object> getNewRecord(String custName) {

        ObjectNode transaction = JsonNodeFactory.instance.objectNode();
        //generat random new amount for transaction
        Integer amount= ThreadLocalRandom.current().nextInt(10, 100);
        Instant now=Instant.now();
        transaction.put("Name",custName);
        transaction.put("amount",amount);
        transaction.put("timeoftrans",now.toString());

       return new ProducerRecord<String, Object>(
                            TOPIC_NAME,
                            custName,
                            transaction.toString());

    }

    public  void Close()
    {
        kafkaProducer.close();
    }

}
