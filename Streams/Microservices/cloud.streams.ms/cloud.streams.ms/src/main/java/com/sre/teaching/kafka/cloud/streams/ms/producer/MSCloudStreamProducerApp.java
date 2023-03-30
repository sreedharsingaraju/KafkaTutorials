package com.sre.teaching.kafka.cloud.streams.ms.producer;


import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.EnableKafkaStreams;

import java.util.Collections;

@SpringBootApplication
public class MSCloudStreamProducerApp {

    public static void main(String[] args) {
        SpringApplication app= new SpringApplication(MSCloudStreamProducerApp.class);
        app.setDefaultProperties(Collections
                .singletonMap("server.port", "8083"));
        app.run(args);
   }
}
