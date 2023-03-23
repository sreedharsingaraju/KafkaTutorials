package com.sre.teaching.kafka.streams.streamingconsumerdemo;

import com.sre.teaching.kafka.streams.streamingconsumerdemo.config.Configure;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Printed;

import java.util.Arrays;

public class FavouriteColor {

    public static void main(String [] args)
    {
        StreamsBuilder streamsBuilder=new StreamsBuilder();

        //KStream kStream=streamsBuilder.stream("favourite-color");
        KTable<String,String> favouriteColors=streamsBuilder.table("favourite-color");

        favouriteColors.filter((name,color)-> Arrays.asList("red","green","blue").contains(color))
                .groupBy((name,color)->new KeyValue<>(color,color))
                .count()
                .toStream()
                .print(Printed.toSysOut());


        Topology topology=streamsBuilder.build();

        KafkaStreams kafkaStreams=new KafkaStreams(topology, Configure.ConfigureKafka("Favourite-colors-1"));

        kafkaStreams.start();

        Configure.RegisterShutDownHook(kafkaStreams);

    }
}
