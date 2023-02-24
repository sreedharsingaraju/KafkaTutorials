package com.sre.teaching.kafka.KafkaConsumerApp;

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.ObjectInputStream;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class MyConsumerRebalanceListener implements ConsumerRebalanceListener {
    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> collection) {

        System.out.println("Partitions revoked ...");

        collection.forEach((topicPartition -> System.out.println(topicPartition.partition())));
    }

    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> collection) {

        System.out.println("Partitions Assigned ...");

        collection.forEach((topicPartition -> System.out.println(topicPartition.partition())));

    }


    private static Map<TopicPartition, OffsetAndMetadata> readOffsetSerializationFile() {
        Map<TopicPartition, OffsetAndMetadata> offsetsMapFromPath = new HashMap<>();
        FileInputStream fileInputStream = null;
        BufferedInputStream bufferedInputStream = null;
        ObjectInputStream objectInputStream = null;
        try {
            String serialiaziedFilePath = ".\\PartitionOffsets.scr";
            fileInputStream = new FileInputStream(serialiaziedFilePath);
            bufferedInputStream = new BufferedInputStream(fileInputStream);
            objectInputStream = new ObjectInputStream(bufferedInputStream);
            offsetsMapFromPath = (Map<TopicPartition, OffsetAndMetadata>) objectInputStream.readObject();
            System.out.println("Offset Map read from the path is :  " + offsetsMapFromPath);
        } catch (Exception e) {
            System.out.println("Exception Occurred while reading the file : " + e);
        } finally {
            try {
                if (objectInputStream != null)
                    objectInputStream.close();
                if (fileInputStream != null)
                    fileInputStream.close();
                if (bufferedInputStream != null)
                    bufferedInputStream.close();
            } catch (Exception e) {
                System.out.println("Exception Occurred in closing the exception : " + e);
            }

        }
        return offsetsMapFromPath;
    }
}
