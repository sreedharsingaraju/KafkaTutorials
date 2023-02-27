package com.sre.teaching.kafka.KafkaConsumerApp;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.io.*;
import java.util.HashMap;
import java.util.Map;

public class KafkaConsumerUtil {

    static  final String SERIALIZED_FILE_PATH=".\\PartitionOffsets.scr";
    public static void writeOffsetsMapToPath(Map<TopicPartition, OffsetAndMetadata> offsetsMap) throws IOException {

        FileOutputStream fout = null;
        ObjectOutputStream oos = null;
        String serialiaziedFilePath=SERIALIZED_FILE_PATH;

        try {
            fout = new FileOutputStream(serialiaziedFilePath);
            oos = new ObjectOutputStream(fout);
            oos.writeObject(offsetsMap);
            System.out.println("Offsets Written Successfully!");
        } catch (Exception ex) {
            System.out.println("Exception Occurred while writing the file : " + ex);
        } finally {
            if(fout!=null)
                fout.close();
            if(oos!=null)
                oos.close();
        }
    }

    public static Map<TopicPartition, OffsetAndMetadata> readOffsetSerializationFile() {
        Map<TopicPartition, OffsetAndMetadata> offsetsMapFromPath = new HashMap<>();
        FileInputStream fileInputStream = null;
        BufferedInputStream bufferedInputStream = null;
        ObjectInputStream objectInputStream = null;
        try {
            String serialiaziedFilePath = SERIALIZED_FILE_PATH;
            fileInputStream = new FileInputStream(serialiaziedFilePath);
            bufferedInputStream = new BufferedInputStream(fileInputStream);
            objectInputStream = new ObjectInputStream(bufferedInputStream);
            offsetsMapFromPath = (Map<TopicPartition, OffsetAndMetadata>) objectInputStream.readObject();
            System.out.println("Offset Map read from the path is :  " + offsetsMapFromPath+"\n");
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
