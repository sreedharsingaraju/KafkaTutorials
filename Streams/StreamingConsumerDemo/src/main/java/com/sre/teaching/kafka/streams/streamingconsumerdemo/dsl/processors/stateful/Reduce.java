package com.sre.teaching.kafka.streams.streamingconsumerdemo.dsl.processors.stateful;

import com.sre.teaching.kafka.streams.streamingconsumerdemo.util.RunTopology;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;

public class Reduce {

    public final String TOPIC="reduce-topic-in";

    public final String OUT_TOPIC="reduce-topic-out";

    public Topology  ReduceDSLTopology()
    {
        StreamsBuilder streamsBuilder=new StreamsBuilder();

        KStream<String,String> streamOfValues=streamsBuilder.
                stream(TOPIC,Consumed.with(Serdes.String(), Serdes.String()));

        streamOfValues.print(Printed.toSysOut());

        streamOfValues
                .groupByKey()
                .reduce((prevval,newval)->
                                prevval+":"+newval,Materialized.with(Serdes.String(), Serdes.String()))
                .toStream()
                .to(OUT_TOPIC);

        Topology topology=streamsBuilder.build();
        return topology;
    }

    public static void main(String args[])
    {
        Reduce reduce=new Reduce();

        Topology redTopology= reduce.ReduceDSLTopology();
        RunTopology.RunTopology(redTopology,"reduce-app-3");

    }
}
