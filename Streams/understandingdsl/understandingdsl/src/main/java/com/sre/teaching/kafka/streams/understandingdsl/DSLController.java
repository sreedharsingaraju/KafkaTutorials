package com.sre.teaching.kafka.streams.understandingdsl;

import com.sre.teaching.kafka.streams.understandingdsl.config.Configure;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Printed;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.ArrayList;
import java.util.List;

@RestController
public class DSLController {

    KafkaStreams kafkaStreams;

    @GetMapping("/ktable")
    public List<KeyValue<String, String>> getKTable() {

        List<KeyValue<String, String>> localResults = new ArrayList<>();
        if(kafkaStreams!=null) {
            ReadOnlyKeyValueStore<String, String> stateStore =
                    kafkaStreams.store(Configure.StateStoreName(),
                            QueryableStoreTypes.keyValueStore());

            stateStore.all().forEachRemaining(localResults::add);
        }

        return localResults;

    }

    @PostMapping("/ktable")
    public ResponseEntity<String> initiateKTableStreaming() {
        StreamsBuilder streamsBuilder = new StreamsBuilder();

        KTable<String, Long> kTable = streamsBuilder.table(Configure.SourceTopic(),
                Consumed.with(Serdes.String(), Serdes.String()), Materialized.as(Configure.StateStoreName()))
                .groupBy((name,color)->new KeyValue<>(color,color))

                .count();



        kTable.toStream().print(Printed.toSysOut());

        Topology kstreamUnderstandingTopology = streamsBuilder.build();

        KafkaStreams kafkaStreams = new KafkaStreams(kstreamUnderstandingTopology,
                Configure.ConfigureKafka());

        this.kafkaStreams=kafkaStreams;

        kafkaStreams.start();

        //Add this so that streams are gracefully closed during App exits
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("Shutting down hence cleaning up");
            kafkaStreams.close();
        }));

        return ResponseEntity.status(HttpStatus.OK).body("Initiated KTable streaming");

    }
}
