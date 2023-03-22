package com.sre.teaching.kafka.streams.streamingconsumerdemo;

import com.sre.teaching.kafka.streams.streamingconsumerdemo.config.Configure;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Printed;

public class UnderstandKStreams {

    public void KStreamsUnderstanding()
    {
        StreamsBuilder streamsBuilder=new StreamsBuilder();

        KStream<String,String> kStream=streamsBuilder.stream("streams-input",
                                    Consumed.with(Serdes.String(), Serdes.String()));

        kStream.print(Printed.toSysOut());

        Topology kstreamUnderstandingTopology=streamsBuilder.build();

        KafkaStreams kafkaStreams=new KafkaStreams(kstreamUnderstandingTopology, Configure.ConfigureKafka());

        kafkaStreams.start();

        //Add this so that streams are gracefully closed during App exits
        Runtime.getRuntime().addShutdownHook(new Thread(()->{
            System.out.println("Shutting down hence cleaning up");
            kafkaStreams.close();
        }));

    }

    public void KTableUnderstanding()
    {
        StreamsBuilder streamsBuilder=new StreamsBuilder();

        KTable<String,String> kTable=streamsBuilder.table("streams-input",
                Consumed.with(Serdes.String(), Serdes.String()));


        kTable.toStream().print(Printed.toSysOut());

        Topology kstreamUnderstandingTopology=streamsBuilder.build();

        KafkaStreams kafkaStreams=new KafkaStreams(kstreamUnderstandingTopology, Configure.ConfigureKafka());

        kafkaStreams.start();

        //Add this so that streams are gracefully closed during App exits
        Runtime.getRuntime().addShutdownHook(new Thread(()->{
            System.out.println("Shutting down hence cleaning up");
            kafkaStreams.close();
        }));

    }



    public static  void main(String [] args)
    {

        UnderstandKStreams understandKStreams=new UnderstandKStreams();

        understandKStreams.KStreamsUnderstanding();

        understandKStreams.KTableUnderstanding();
    }

}
