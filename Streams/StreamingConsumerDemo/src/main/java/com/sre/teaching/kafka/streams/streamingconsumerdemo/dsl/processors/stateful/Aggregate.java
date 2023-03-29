package com.sre.teaching.kafka.streams.streamingconsumerdemo.dsl.processors.stateful;

import com.sre.teaching.kafka.streams.streamingconsumerdemo.util.RunTopology;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;

public class Aggregate {

    public final String TOPIC="aggregate-topic-in";

    public final String OUT_TOPIC="aggregate-topic-out";

    public Topology  AggregateDSLTopology()
    {
        StreamsBuilder streamsBuilder=new StreamsBuilder();

        KStream<String,String> streamOfValues=streamsBuilder.
                    stream(TOPIC,Consumed.with(Serdes.String(), Serdes.String()));

        streamOfValues.print(Printed.toSysOut());

        streamOfValues
                .groupByKey()
                .aggregate(()->0L,
                        (key,value,prevprocessedval)->
                            Integer.parseInt(value)+prevprocessedval,
                            Materialized.with(Serdes.String(),Serdes.Long()))
                .toStream()
                .to(OUT_TOPIC);

        Topology topology=streamsBuilder.build();
        return topology;
    }

    public static void main(String args[])
    {
        Aggregate aggregate=new Aggregate();

       Topology aggTopology= aggregate.AggregateDSLTopology();
        RunTopology.RunTopology(aggTopology,"aggregate-app-3");
    }
}
