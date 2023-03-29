package com.sre.teaching.kafka.streams.streamingconsumerdemo.dsl.processors.stateful.producers;

import com.sre.teaching.kafka.streams.streamingconsumerdemo.dsl.processors.stateful.producers.interfacing.KafkaAPIWrapper;

import java.util.concurrent.ExecutionException;

public class ReduceProducer {

    public static void main(String[] args) throws ExecutionException, InterruptedException {

        KafkaAPIWrapper leftStreamWriter = new KafkaAPIWrapper("reduceav-topic-in", true);


        leftStreamWriter.SendAsyncMessages("key", 10);

        leftStreamWriter.SendAsyncMessages("key", 20);

        leftStreamWriter.SendAsyncMessages("key1", 30);

        leftStreamWriter.SendAsyncMessages("key4", 40);


        leftStreamWriter.Close();
    }
}
