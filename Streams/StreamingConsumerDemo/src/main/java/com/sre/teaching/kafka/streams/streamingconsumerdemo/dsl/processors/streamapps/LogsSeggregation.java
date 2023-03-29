package com.sre.teaching.kafka.streams.streamingconsumerdemo.dsl.processors.streamapps;

import com.sre.teaching.kafka.streams.streamingconsumerdemo.config.Configure;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Branched;
import org.apache.kafka.streams.kstream.KStream;

//tbd: this is only giving the technical hints and to be used to complete full implementation
//to handle log categories
public class LogsSeggregation {

    public static void main(String [] args)
    {
        StreamsBuilder streamsBuilder=new StreamsBuilder();

        KStream kStream=streamsBuilder.stream("logs-input");

        kStream.split()
                .branch((type, message) -> type.equals("info"), Branched.withConsumer(kStream1 -> kStream1.to("info-logs")))
                .branch((type, message) -> type.equals("error"), Branched.withConsumer(kStream1 -> kStream1.to("error-logs")));


        Topology topology=streamsBuilder.build();

        KafkaStreams kafkaStreams=new KafkaStreams(topology, Configure.ConfigureKafka("log-streams-app-1"));

        kafkaStreams.start();

    }
}
