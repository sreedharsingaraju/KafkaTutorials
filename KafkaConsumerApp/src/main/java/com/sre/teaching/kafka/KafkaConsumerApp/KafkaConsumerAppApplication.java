package com.sre.teaching.kafka.KafkaConsumerApp;


import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.HashMap;
import java.util.Map;

public class KafkaConsumerAppApplication {

	static Map<String,Object> Configure()
	{
		Map<String,Object> configuration=new HashMap<>();

		configuration.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092, localhost:9093, localhost:9094");
		configuration.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		configuration.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
		configuration.put(ConsumerConfig.GROUP_ID_CONFIG,"Consumer.4");
		configuration.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,false);
		//configuration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
		//configuration.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG,1000);

		return  configuration;
	}
	public static void main(String[] args) {

		MyCustomConsumer kafkaConsumeMessages=new MyCustomConsumer(Configure());

		System.out.println("Awaiting Messages ....");
		kafkaConsumeMessages.PollKafka();

	}

}
