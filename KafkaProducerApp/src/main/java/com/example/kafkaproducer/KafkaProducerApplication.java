package com.example.kafkaproducer;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.LoggerFactory;


import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static java.lang.Thread.*;

public class KafkaProducerApplication {

	private static final ch.qos.logback.classic.Logger  logger= (ch.qos.logback.classic.Logger)LoggerFactory.getLogger(KafkaProducerApplication.class);

	//static  final  String TOPIC_NAME="kafka-topic";
	static final String TOPIC_NAME = "replicatedtopic1";

	KafkaProducer<String, Object> kfkProducer;

	KafkaProducerApplication(Map<String, Object> prodConfig) {

		kfkProducer = new KafkaProducer<String, Object>(prodConfig);

	}

	public static Map<String, Object> Configure() {

		logger.info("Configuring Kafka Producer");

		Map<String, Object> kafkaconfig = new HashMap<>();

		kafkaconfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");//, localhost:9093, localhost:9094");

		kafkaconfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

		kafkaconfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

		kafkaconfig.put(ProducerConfig.ACKS_CONFIG, "all");

		kafkaconfig.put(ProducerConfig.RETRIES_CONFIG, 5);

		kafkaconfig.put(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, 15000);


		return kafkaconfig;
	}

	Callback sendNotification = (RecordMetadata sendStatus, Exception ex) -> {

		logger.warn("Send Async notification came");
		if (ex != null) {

			logger.error("Exception Occurred : " + ex.getMessage());
		} else {
			logger.info("Offset : " + sendStatus.offset()
					+ "\n Partition : " + sendStatus.partition()
					+ "\n Topic : " + sendStatus.topic());
		}

	};

	public Future<RecordMetadata> SendAsyncMessage(String key, String value) {
		ProducerRecord<String, Object> prodRecord = new ProducerRecord<>(TOPIC_NAME, key,
				value);

		return kfkProducer.send(prodRecord, sendNotification);

	}

	public void Close() {
		kfkProducer.close();
	}

	public void SendMessage(String key, Object value)
			throws ExecutionException, InterruptedException {

		ProducerRecord<String, Object> prodRecord = new ProducerRecord<>(TOPIC_NAME, key,
				value);

		RecordMetadata sendStatus = kfkProducer.send(prodRecord).get();


		logger.info("Offset : " + sendStatus.offset()
				+ "\n Partition : " + sendStatus.partition()
				+ "\n Topic : " + sendStatus.topic());
	}

	public static void main(String[] args) throws InterruptedException {

		logger.warn("Producer Application Started");

		KafkaProducerApplication producer = new KafkaProducerApplication(Configure());

		Scanner sc = new Scanner(System.in);

		String keyFromUser = null, valFromUser = null;


		while(true) {

			System.out.println("Enter Key: ");
			keyFromUser = sc.nextLine();
			System.out.println("Enter Value: ");
			valFromUser = sc.nextLine();

			logger.warn("\n key is " + keyFromUser + " and value is " + valFromUser);

			if(keyFromUser.equalsIgnoreCase("exit"))
			{
				break;
			}

			try {
				producer.SendMessage(keyFromUser,valFromUser);
			} catch (ExecutionException ex) {
				System.out.println("Exception occurred during sending " + ex.getMessage());

			} catch (InterruptedException e) {
				System.out.println("Exception occurred during sending " + e.getMessage());
			}

			//Async way of sending
			//Future<RecordMetadata> isComplete = producer.SendAsyncMessage("JavaProgAsync",
			//		"Message Sent in an Async way from a Java Prog");
			//while (!isComplete.isDone()) {
			//	sleep(1000);
			//}
		}
		producer.Close();
	}
}



