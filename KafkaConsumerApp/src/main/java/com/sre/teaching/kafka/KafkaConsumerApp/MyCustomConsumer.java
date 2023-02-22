package com.sre.teaching.kafka.KafkaConsumerApp;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Map;



public class MyCustomConsumer {

        KafkaConsumer<String,Object> kafkaConsumer;
        public static final String TOPIC_NAME="replicatedtopic1";


        public MyCustomConsumer(Map<String,Object> config)
        {
            kafkaConsumer=new KafkaConsumer<String,Object>(config);
        }

        public  void PollKafka()
        {

                kafkaConsumer.subscribe(List.of(TOPIC_NAME));
                Duration time=Duration.of(100, ChronoUnit.MILLIS);

                try {
                        while(true) {
                                ConsumerRecords<String, Object> consumedrecords = kafkaConsumer.poll(time);

                                consumedrecords.forEach((record) -> {
                                        System.out.println("\n Read Message key = " + record.key() +
                                                " Value = " + record.value()+
                                                " Partition = "+record.partition()+
                                                " Offset = "+record.offset());
                                });
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
