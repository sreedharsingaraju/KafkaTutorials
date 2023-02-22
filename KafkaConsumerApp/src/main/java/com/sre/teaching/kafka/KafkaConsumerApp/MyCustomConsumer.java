package com.sre.teaching.kafka.KafkaConsumerApp;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.*;


public class MyCustomConsumer {

        KafkaConsumer<String,Object> kafkaConsumer;
        public static final String TOPIC_NAME="replicatedtopic1";


        public MyCustomConsumer(Map<String,Object> config)
        {
            kafkaConsumer=new KafkaConsumer<>(config);
        }

        void PrintCommitedOffsets()
        {
                TopicPartition topicPartition1 = new TopicPartition(TOPIC_NAME, 0);
                TopicPartition topicPartition2 = new TopicPartition(TOPIC_NAME, 1);
                TopicPartition topicPartition3 = new TopicPartition(TOPIC_NAME, 2);
                TopicPartition topicPartition4 = new TopicPartition(TOPIC_NAME, 3);

                Map<TopicPartition, OffsetAndMetadata> committed = kafkaConsumer
                        .committed(new HashSet<>(Arrays.asList(topicPartition1,topicPartition2,topicPartition3,topicPartition4)));

                OffsetAndMetadata offsetAndMetadata1 = committed.get(topicPartition1);
                OffsetAndMetadata offsetAndMetadata2 = committed.get(topicPartition2);
                OffsetAndMetadata offsetAndMetadata3 = committed.get(topicPartition3);
                OffsetAndMetadata offsetAndMetadata4 = committed.get(topicPartition4);



                //long position = kafkaConsumer.position(topicPartition1);

                System.out.printf("Partition 0 : Committed: %s\n\n", offsetAndMetadata1 == null ? null : offsetAndMetadata1
                        .offset() );
                System.out.printf("Partition 2 : Committed: %s\n\n", offsetAndMetadata2 == null ? null : offsetAndMetadata2
                        .offset() );
                System.out.printf("Partition 3 : Committed: %s\n\n", offsetAndMetadata3 == null ? null : offsetAndMetadata3
                        .offset() );
                System.out.printf("Partition 4 : Committed: %s\n\n", offsetAndMetadata4 == null ? null : offsetAndMetadata4
                        .offset() );
        }
        public  void PollKafka()
        {

                kafkaConsumer.subscribe(List.of(TOPIC_NAME));
                Duration time=Duration.of(100, ChronoUnit.MILLIS);

                try {


                        while(true) {
                                System.out.println("Waiting for messages from Queue");
                                PrintCommitedOffsets();
                                ConsumerRecords<String, Object> consumedrecords = kafkaConsumer.poll(time);

                                if(consumedrecords.count()==0)
                                {
                                        System.out.println("No records fetched in this poll");

                                        continue;
                                }

                                consumedrecords.forEach((record) -> System.out.println("\n Read Message key = " + record.key() +
                                                " Value = " + record.value()+
                                                " Partition = "+record.partition()+
                                                " Offset = "+record.offset())
                                );


                                // Thread.sleep(10000);


                        }
                }
                catch(Exception e)
                {

                        System.out.println("Exception occurred :"+ e.getMessage());

                }
                finally {

                        kafkaConsumer.close();
                }


        }

}
