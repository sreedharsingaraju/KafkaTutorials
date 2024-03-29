package com.sre.teaching.kafka.cloud.streams.ms.consumer;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.stereotype.Component;

import java.util.Arrays;

@Component
@EnableKafkaStreams
@EnableKafka
@ComponentScan
public class Processor {

    final String TOPIC = "ms-cloud-stream-words";
    final String OUT_TOPIC="ms-cloud-streams-wordcount-output";



    @Autowired
    public void process(StreamsBuilder builder) {

        // Serializers/deserializers (serde) for String and Long types
        final Serde<Integer> integerSerde = Serdes.Integer();
        final Serde<String> stringSerde = Serdes.String();
        final Serde<Long> longSerde = Serdes.Long();

        // Construct a `KStream` from the input topic "streams-plaintext-input", where message values
        // represent lines of text (for the sake of this example, we ignore whatever may be stored
        // in the message keys).
        KStream<Integer, String> textLines = builder
                .stream(TOPIC, Consumed.with(integerSerde, stringSerde));

        textLines.peek((key,line)-> System.out.println("Processing line : "+line ));

        KTable<String, Long> wordCounts = textLines
                // Split each text line, by whitespace, into words.  The text lines are the message
                // values, i.e. we can ignore whatever data is in the message keys and thus invoke
                // `flatMapValues` instead of the more generic `flatMap`.
                .flatMapValues(value -> Arrays.asList(value.toLowerCase().split("\\W+")))
                // We use `groupBy` to ensure the words are available as message keys
                .groupBy((key, value) -> value, Grouped.with(stringSerde, stringSerde))
                // Count the occurrences of each word (message key).
                .count(Materialized.as("counts"));

// Convert the `KTable<String, Long>` into a `KStream<String, Long>` and write to the output topic.
        wordCounts.toStream().peek((word,count)->
                System.out.println("Processed and word: "+word+" Coount is "+count))
                .to(OUT_TOPIC, Produced.with(stringSerde, longSerde));

    }
}
