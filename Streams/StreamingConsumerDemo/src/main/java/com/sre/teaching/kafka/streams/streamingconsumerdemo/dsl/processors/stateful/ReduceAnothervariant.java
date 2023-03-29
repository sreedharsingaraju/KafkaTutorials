package com.sre.teaching.kafka.streams.streamingconsumerdemo.dsl.processors.stateful;

import com.sre.teaching.kafka.streams.streamingconsumerdemo.util.RunTopology;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Printed;

public class ReduceAnothervariant {

    public final String TOPIC="reduceav-topic-in";

    public final String OUT_TOPIC="reduceav-topic-out";

    public Topology ReduceAnotherVariantDSLTopology()
    {
        StreamsBuilder streamsBuilder=new StreamsBuilder();

        KStream<String,Integer> streamOfValues=streamsBuilder.

                stream(TOPIC, Consumed.with(Serdes.String(), Serdes.Integer()));

        streamOfValues.print(Printed.toSysOut());

       streamOfValues
               .filter((key,val)->val%2!=0)
                .groupByKey()
                .reduce((prevval,newval)->
                        prevval+newval)
                .toStream()
                .to(OUT_TOPIC);

        Topology topology=streamsBuilder.build();
        return topology;
    }

    public static void main(String args[])
    {
        ReduceAnothervariant reduceAnothervariant=new ReduceAnothervariant();

        Topology redTopology= reduceAnothervariant.ReduceAnotherVariantDSLTopology();
        RunTopology.RunTopology(redTopology,"reduceav-app-3");

    }
}
