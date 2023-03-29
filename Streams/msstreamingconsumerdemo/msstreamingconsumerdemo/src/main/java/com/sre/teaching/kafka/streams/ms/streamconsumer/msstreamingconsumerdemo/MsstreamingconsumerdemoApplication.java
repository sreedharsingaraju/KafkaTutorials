package com.sre.teaching.kafka.streams.ms.streamconsumer.msstreamingconsumerdemo;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.EnableKafkaStreams;

@SpringBootApplication
@EnableKafkaStreams
public class MsstreamingconsumerdemoApplication {

	public static void main(String[] args) {

		SpringApplication.run(MsstreamingconsumerdemoApplication.class, args);
	}

}
