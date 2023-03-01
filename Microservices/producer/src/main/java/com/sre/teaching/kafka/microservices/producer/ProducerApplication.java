package com.sre.teaching.kafka.microservices.producer;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;

@SpringBootApplication
@ComponentScan("com.sre.teaching.kafka.microservices.producer")
public class ProducerApplication {
	public static void main(String[] args) {

		SpringApplication.run(ProducerApplication.class, args);
	}

}
