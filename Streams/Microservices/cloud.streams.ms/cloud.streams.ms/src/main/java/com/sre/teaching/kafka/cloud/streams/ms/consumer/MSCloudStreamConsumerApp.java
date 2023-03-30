package com.sre.teaching.kafka.cloud.streams.ms.consumer;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.EnableKafkaStreams;

@SpringBootApplication

public class MSCloudStreamConsumerApp {
    public static void main(String[] args) {
        SpringApplication.run(MSCloudStreamConsumerApp.class, args);

    }
}
