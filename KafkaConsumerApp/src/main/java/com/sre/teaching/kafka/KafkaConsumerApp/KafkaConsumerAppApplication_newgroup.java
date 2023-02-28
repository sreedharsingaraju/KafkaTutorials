package com.sre.teaching.kafka.KafkaConsumerApp;


import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.HashMap;
import java.util.Map;

public class KafkaConsumerAppApplication_newgroup {
	public static final String TOPIC_NAME="replicatedtopic1";

	static Map<String,Object> Configure()
	{
		Map<String,Object> configuration=new HashMap<>();

		configuration.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092, localhost:9093, localhost:9094");
		configuration.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		configuration.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
		configuration.put(ConsumerConfig.GROUP_ID_CONFIG,"NewConsumer.1");
		//configuration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");

		return  configuration;
	}
	public static void main(String[] args) {

		MyCustomConsumer kafkaConsumeMessages=new MyCustomConsumer(TOPIC_NAME,Configure());

		System.out.println("Awaiting Messages ....");
		kafkaConsumeMessages.PollKafka();

	}

}
