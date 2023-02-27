package com.sre.teaching.kafka.KafkaConsumerApp;

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.ObjectInputStream;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class MyConsumerRebalanceListener implements ConsumerRebalanceListener {


    KafkaConsumer<String, Object> kafkaConsumer;

    public MyConsumerRebalanceListener(KafkaConsumer<String, Object> kafkaConsumer) {
        this.kafkaConsumer = kafkaConsumer;
    }

    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> collection) {

        System.out.println("Partitions revoked ...");

        collection.forEach((topicPartition -> System.out.println(topicPartition.partition())));


        if (kafkaConsumer != null) {
            kafkaConsumer.commitSync();//Notice
            System.out.println("Committing the Offsets ........");
        } else
            System.out.println("Invalid Kafka consumer object");
    }

    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> collection) {

        System.out.println("Partitions Assigned ...");


        collection.forEach((topicPartition -> System.out.println(topicPartition.partition())));


        kafkaConsumer.seekToBeginning(collection);

     /*   System.out.println("Reading the last offset from where to read from file");
        Map<TopicPartition,OffsetAndMetadata> lastSavedOffsets= KafkaConsumerUtil.readOffsetSerializationFile();

        if(lastSavedOffsets.size()>0) {
            collection.forEach(
                    (partition)-> {
                        if(lastSavedOffsets.get(partition)!=null) {
                            kafkaConsumer.seek(partition, lastSavedOffsets.get(partition));
                            System.out.println("\n\nRead Saved offset for Partition "+partition.partition()
                                    +" Offset "+
                                    lastSavedOffsets.get(partition).offset());
                        }
                    });
                */

    }
}
