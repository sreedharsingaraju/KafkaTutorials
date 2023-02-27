package com.sre.teaching.kafka.KafkaConsumerApp;
import org.apache.kafka.clients.consumer.CommitFailedException;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.*;


public class MyCustomConsumer {

        KafkaConsumer<String,Object> kafkaConsumer;
        MyConsumerRebalanceListener rebalanceListener;
        public static  String TOPIC_NAME;

        Logger logger = LoggerFactory.getLogger(MyCustomConsumer.class);

        public MyCustomConsumer(String topic, Map<String,Object> config) {

                TOPIC_NAME=topic;
                kafkaConsumer=new KafkaConsumer<>(config);
                rebalanceListener=new MyConsumerRebalanceListener(kafkaConsumer);
        }

        void PrintCommitedOffsets()
        {
                TopicPartition topicPartition1 = new TopicPartition(TOPIC_NAME, 0);
                TopicPartition topicPartition2 = new TopicPartition(TOPIC_NAME, 1);
                TopicPartition topicPartition3 = new TopicPartition(TOPIC_NAME, 2);
                TopicPartition topicPartition4 = new TopicPartition(TOPIC_NAME, 3);

                Map<TopicPartition, OffsetAndMetadata> committed = kafkaConsumer
                                                                        .committed(new HashSet<>(Arrays.asList(topicPartition1,
                                                                                topicPartition2,
                                                                                topicPartition3,
                                                                                topicPartition4)));

                OffsetAndMetadata offsetAndMetadata1 = committed.get(topicPartition1);
                OffsetAndMetadata offsetAndMetadata2 = committed.get(topicPartition2);
                OffsetAndMetadata offsetAndMetadata3 = committed.get(topicPartition3);
                OffsetAndMetadata offsetAndMetadata4 = committed.get(topicPartition4);



                logger.info("Partition 0 : Committed: %s \n\n", offsetAndMetadata1 == null ? null : offsetAndMetadata1
                        .offset() );
                logger.info("Partition 1 : Committed: %s \n\n", offsetAndMetadata2 == null ? null : offsetAndMetadata2
                        .offset() );
                logger.info("Partition 2 : Committed: %s \n\n", offsetAndMetadata3 == null ? null : offsetAndMetadata3
                        .offset() );
                logger.info("Partition 3 : Committed: %s \n\n", offsetAndMetadata4 == null ? null : offsetAndMetadata4
                        .offset() );
        }
        public  void PollKafka()
        {
                kafkaConsumer.subscribe(List.of(TOPIC_NAME),rebalanceListener);
                Duration time=Duration.of(100, ChronoUnit.MILLIS);

                PrintCommitedOffsets();
                try {

                        //intentionally kept infite loop as this is Kafka messages listener for its entire lifetime
                        while(true) {

                                ConsumerRecords<String, Object> consumedrecords = kafkaConsumer.poll(time);

                                if(consumedrecords.count()==0)
                                {
                                        continue;
                                }

                                //loop for processing the poll returned message record batch
                                consumedrecords.forEach((record) -> {
                                                logger.info("\n Read Message key = " + record.key() +
                                                        " Value = " + record.value() +
                                                        " Partition = " + record.partition() +
                                                        " Offset = " + record.offset());

                                        //Commit the offset
                                        // System.out.print("Commiting the Offset " + record.offset()
                                        //       + " at partition " + record.partition());

                                        TopicPartition topicPartition = new TopicPartition(TOPIC_NAME, record.partition());

                                        OffsetAndMetadata offsetMetadata = new OffsetAndMetadata(record.offset() + 1);

                                        Map<TopicPartition, OffsetAndMetadata> commitOffset = new HashMap<>();

                                        commitOffset.put(topicPartition, offsetMetadata);

                                        //Strategy for using external source
                                      /*  try {
                                                KafkaConsumerUtil.writeOffsetsMapToPath(commitOffset);
                                        } catch (IOException e) {
                                                System.out.println("Exception during commiting the offset to the file : "+e.getMessage());
                                        }
                                */
                                        //strategy for saving a given offset based on the processed record
                                        //kafkaConsumer.commitSync(commitOffset);

                                        //full commit based on polled offset, this does not cater well for failed processing of meesages
                                        kafkaConsumer.commitSync();
                                        logger.info("Committed the offsets");

                                        }
                                );


                               // Thread.sleep(10000);
                        }

                }
                catch(CommitFailedException commitfail) {
                        logger.error("Failed to commit the offset. Reason = "+commitfail.getMessage());
                }
                catch(Exception e) {
                        logger.error("Exception occurred :"+ e.getMessage());
                }
                finally {
                        kafkaConsumer.close();
                }
        }


}
