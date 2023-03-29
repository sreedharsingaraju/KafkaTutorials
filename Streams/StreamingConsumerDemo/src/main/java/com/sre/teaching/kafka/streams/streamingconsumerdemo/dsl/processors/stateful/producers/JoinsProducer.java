package com.sre.teaching.kafka.streams.streamingconsumerdemo.dsl.processors.stateful.producers;

import com.sre.teaching.kafka.streams.streamingconsumerdemo.dsl.processors.stateful.producers.interfacing.KafkaAPIWrapper;

import java.util.concurrent.ExecutionException;

public class JoinsProducer {

    public static void main(String[] args) throws InterruptedException, ExecutionException {

        KafkaAPIWrapper leftStreamWriter = new KafkaAPIWrapper("joins-left-topic-in", false);

        KafkaAPIWrapper rightStreamWriter = new KafkaAPIWrapper("joins-right-topic-in", false);

        leftStreamWriter.SendAsyncMessages("key", "leftval1");

        rightStreamWriter.SendAsyncMessages("key", "rightval1");

        leftStreamWriter.SendAsyncMessages("leftkeydiff", "leftval1");

        rightStreamWriter.SendAsyncMessages("rightkey", "rightval1");


        leftStreamWriter.Close();
        rightStreamWriter.Close();
    }
}
