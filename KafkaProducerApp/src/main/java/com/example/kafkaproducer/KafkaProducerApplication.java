package com.example.kafkaproducer;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static java.lang.Thread.*;

public class KafkaProducerApplication {

	static  final  String TOPIC_NAME="kafka-topic";

	KafkaProducer<String,Object> kfkProducer;

	KafkaProducerApplication(Map<String,Object> prodConfig)
	{

		kfkProducer=new KafkaProducer<String, Object>(prodConfig);

	}
	public static Map<String,Object> Configure()
	{

		Map<String,Object> kafkaconfig=new HashMap<>();

		kafkaconfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

		kafkaconfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

		kafkaconfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

		return kafkaconfig;
	}

	Callback sendNotification=(RecordMetadata sendStatus, Exception ex)-> {

		System.out.println("Send Async notification came");
		if(ex!=null){

			System.out.println("Exception Occurred : "+ex.getMessage());
		}
		else {
			System.out.println("Offset : "+ sendStatus.offset()
					+"\n Partition : "+sendStatus.partition()
					+"\n Topic : "+sendStatus.topic());
		}

	};
	public Future<RecordMetadata> SendAsyncMessage(String key, String value)
	{
		ProducerRecord<String, Object> prodRecord=new ProducerRecord<>(TOPIC_NAME,key,
				value);

		return kfkProducer.send(prodRecord,sendNotification);

	}

	public void Close()
	{
		kfkProducer.close();
	}
	public void SendMessage(String key, Object value)
								throws ExecutionException, InterruptedException {

		ProducerRecord<String, Object> prodRecord=new ProducerRecord<>(TOPIC_NAME,key,
				value);

		RecordMetadata sendStatus=kfkProducer.send(prodRecord).get();


		System.out.println("Offset : "+ sendStatus.offset()
				+"\n Partition : "+sendStatus.partition()
				+"\n Topic : "+sendStatus.topic());
	}
	public static void main(String [] args) throws InterruptedException {

		KafkaProducerApplication producer = new KafkaProducerApplication(Configure());

		try {
			producer.SendMessage("JavaProg", "String sent from Java Prog Sync");
		}
		catch (ExecutionException ex) {
			System.out.println("Exception occurred during sending "+ex.getMessage());

		} catch (InterruptedException e) {
			System.out.println("Exception occurred during sending "+e.getMessage());
		}

		//Async way of sending
		Future<RecordMetadata> isComplete=producer.SendAsyncMessage("JavaProgAsync",
				                                      "Message Sent in an Async way from a Java Prog");
		while(!isComplete.isDone())
		{
			sleep(1000);
		}

		producer.Close();

	}


}

