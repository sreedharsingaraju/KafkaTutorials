package com.sre.teaching.kafka.streams.streamingconsumerdemo.util;

import com.sre.teaching.kafka.streams.streamingconsumerdemo.config.Configure;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.Topology;

public class RunTopology {
    public static   void RunTopology(Topology topology, String appid)
    {
        KafkaStreams kafkaStreams=new KafkaStreams(topology,
                Configure.ConfigureKafka(appid));

        kafkaStreams.start();

        System.out.println("Started the topology ");
        System.out.println(kafkaStreams.toString());

        //Add this so that streams are gracefully closed during App exits
        Runtime.getRuntime().addShutdownHook(new Thread(()->{
            System.out.println("Shutting down hence cleaning up");
            kafkaStreams.close();
        }));
    }

}
