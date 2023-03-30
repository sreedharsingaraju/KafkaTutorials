package com.sre.teaching.kafka.streams.streamingconsumerdemo.dsl.processors.stateful;

import com.sre.teaching.kafka.streams.streamingconsumerdemo.util.RunTopology;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;

import java.time.Duration;

public class Joins {


    public final String TOPIC_LEFT="joins-left-topic-in-1";
    public final String TOPIC_RIGHT="joins-right-topic-in-1";

    public final String OUT_TOPIC="joins-topic-out-1"; //inner joim
    public final String LEFT_JOIN_OUT="left-joins-topic-out-1"; //left join

    public Topology joinsDSLTopology()
    {
        StreamsBuilder streamsBuilder=new StreamsBuilder();

        KStream<String,String> leftStreamOfValues=streamsBuilder.
                stream(TOPIC_LEFT, Consumed.with(Serdes.String(), Serdes.String()));

        KStream<String,String> rightStreamOfValues=streamsBuilder.
                stream(TOPIC_RIGHT, Consumed.with(Serdes.String(), Serdes.String()));


        /* first specify how are left and right values should be treated if col key matches*/
        ValueJoiner<String, String, String> valueJoiner= (leftVal,rightVal)->{
                                                    return leftVal+"  joined  "+rightVal;
        };

        /* first specify how are left and right values should be treated if col key matches*/
        ValueJoiner<String, String, String> leftjoinValueJoiner= (leftVal,rightVal)->{
            /*if(rightVal!=rightVal)
                return leftVal+"  joined  "+rightVal;
            else
                return "left only joined "+leftVal;*/
            return "Left val = "+leftVal + " : "+" rightval = "+ rightVal;
        };


        //Now perform the join
        leftStreamOfValues.join(rightStreamOfValues,
                           valueJoiner,
                           JoinWindows.ofTimeDifferenceWithNoGrace(Duration.ofMinutes(2)))
                            .peek((k,v)-> System.out.println("key "+k+" v="+v))
                            .to(OUT_TOPIC);

        leftStreamOfValues.leftJoin(
                        rightStreamOfValues,
                        leftjoinValueJoiner,
                        JoinWindows.ofTimeDifferenceWithNoGrace(Duration.ofMinutes(2)))
                        .peek((k,v)-> System.out.println("key "+k+" v="+v))
                        .to(LEFT_JOIN_OUT);


        Topology topology=streamsBuilder.build();
        return topology;
    }

    public static void main(String args[])
    {
        Joins joins=new Joins();

        Topology joinsTopology= joins.joinsDSLTopology();
        RunTopology.RunTopology(joinsTopology,"joins-app");

    }
}
