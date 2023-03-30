package com.sre.teaching.kafka.cloud.streams.ms.consumer;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.EnableKafkaStreams;

@SpringBootApplication
@EnableKafkaStreams
public class MsStreamingConsumerApplication {

	public static void main(String[] args) {

		SpringApplication.run(MsStreamingConsumerApplication.class, args);
	}

}
