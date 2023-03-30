package com.sre.teaching.kafka.cloud.streams.ms.producer;

import com.sre.teaching.kafka.cloud.streams.ms.consumer.MsStreamingConsumerApplication;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;

@SpringBootApplication
@ComponentScan(basePackages = "com.sre.teaching.kafka.streams.ms")
public class MsStreamingProducerApp {

    public static void main(String[] args) {
        SpringApplication.run(MsStreamingConsumerApplication.class, args);
    }


}
