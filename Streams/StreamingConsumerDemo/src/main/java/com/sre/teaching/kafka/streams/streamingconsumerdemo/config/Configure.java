package com.sre.teaching.kafka.streams.streamingconsumerdemo.config;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;

import java.util.Properties;


@Configuration
public class Configure {

    String SOURCE_TOPICNAME="words-stream";

    String SINK_TOPICNAME="word-count-stream";

    public  String SourceTopic()
    {
        return  SOURCE_TOPICNAME;
    }

    public  String SinkTopic()
    {
        return  SINK_TOPICNAME;
    }

    public static Properties ConfigureKafka(String appId)
    {
        Properties properties=new Properties();
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092");
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG,appId);
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
        properties.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG,0);
        properties.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG,0);
        properties.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);



        //properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
       //properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        //properties.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG,60000);


//StreamsConfig.NUM_STREAM_THREADS_CONFIG

        return properties;
    }

    public static  void RegisterShutDownHook(KafkaStreams kafkaStreams)
    {
        //Add this so that streams are gracefully closed during App exits
        Runtime.getRuntime().addShutdownHook(new Thread(()->{
            System.out.println("Shutting down hence cleaning up");
            kafkaStreams.close();
        }));

    }
}
