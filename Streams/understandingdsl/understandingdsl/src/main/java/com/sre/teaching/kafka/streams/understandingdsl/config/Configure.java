package com.sre.teaching.kafka.streams.understandingdsl.config;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.springframework.context.annotation.Configuration;

import java.util.Properties;


@Configuration
public class Configure {

    static String SOURCE_TOPICNAME="ktable-input";

    static String SINK_TOPICNAME="ktable-input";

    static  String STATE_STORE_NAME="ktable-state-store";

    public static String SourceTopic()
    {
        return  SOURCE_TOPICNAME;
    }

    public static String StateStoreName()
    {
        return  STATE_STORE_NAME;
    }

    public static String SinkTopic()
    {
        return  SINK_TOPICNAME;
    }

    public static Properties ConfigureKafka()
    {
        Properties properties=new Properties();
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092");
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG,"ktable.understanding.appid-1");
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        properties.put(StreamsConfig.STATE_DIR_CONFIG,"/tmp/state-store");
        properties.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 0);
        properties.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);

        //properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
       //properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        //properties.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG,60000);


//StreamsConfig.NUM_STREAM_THREADS_CONFIG

        return properties;
    }
}
