package com.sre.teaching.kafka.cloud.streams.ms.producer;

import lombok.AllArgsConstructor;
import org.springframework.boot.context.event.ApplicationStartedEvent;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.event.EventListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ThreadLocalRandom;
import java.util.random.RandomGenerator;

@Component
@ComponentScan
@AllArgsConstructor
public class Producer {

    final String TOPIC = "ms-cloud-stream-words";
    KafkaTemplate<Integer, String> kafkaTemplate; //this will be injected by Spring due to  @AllAegsConstructor

    @EventListener(ApplicationStartedEvent.class)
    public void generateData() throws ExecutionException, InterruptedException {
        String[] sourcetext = {"This text is first in the collection and cand help inject textline",
                "Another text line which can be intected to chect word count",
                "Kafka has so many ways of integrations",
                "quite rich feature set. Si nuxh So that its not just a mere Messaging system"};

        Integer offset=ThreadLocalRandom.current().nextInt(1, (int) Arrays.stream(sourcetext).count());

        kafkaTemplate.send(TOPIC, offset, sourcetext[offset]).get();
    }
}
