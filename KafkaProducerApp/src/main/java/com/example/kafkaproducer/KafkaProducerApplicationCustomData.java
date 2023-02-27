package com.example.kafkaproducer;

import ch.qos.logback.classic.Logger;
import com.example.kafkaproducer.datamodel.CustomData;
import com.example.kafkaproducer.serializer.KafkaCustomSerializer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;
import java.util.concurrent.ExecutionException;

public class KafkaProducerApplicationCustomData {

	private static final Logger  logger= (Logger)LoggerFactory.getLogger(KafkaProducerApplicationCustomData.class);

	//static  final  String TOPIC_NAME="kafka-topic";
	private static final String TOPIC_NAME = "customdatatopic";

	private static  KafkaAPIWrapper kafkaAPIWrapper;


	public KafkaProducerApplicationCustomData(Map<String, Object> prodConfig) {

		kafkaAPIWrapper = new KafkaAPIWrapper(TOPIC_NAME,prodConfig);

	}

	public static Map<String, Object> Configure() {

		logger.info("Configuring Kafka Producer");

		Map<String, Object> kafkaconfig = new HashMap<>();

		kafkaconfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");//, localhost:9093, localhost:9094");

		kafkaconfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

		kafkaconfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaCustomSerializer.class.getName());

		kafkaconfig.put(ProducerConfig.ACKS_CONFIG, "all");

		kafkaconfig.put(ProducerConfig.RETRIES_CONFIG, 5);

		kafkaconfig.put(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, 15000);


		return kafkaconfig;
	}


	public void Close() {

		kafkaAPIWrapper.Close();
	}



	public static void main(String[] args) throws InterruptedException {

		logger.warn("Custom Data Producer Application Started");

		KafkaProducerApplicationCustomData producer = new KafkaProducerApplicationCustomData(Configure());

		Scanner sc = new Scanner(System.in);

		String keyFromUser = null, nameFromUser = null, ageFromUser=null, salaryFromUser=null, experienceFromUser=null;


		while(true) {

			System.out.println("Enter Key: ");
			keyFromUser = sc.nextLine();

			System.out.println("Enter Name: ");
			nameFromUser = sc.nextLine();

			System.out.println("Enter Salary: ");
			salaryFromUser = sc.nextLine();

			System.out.println("Enter Experience: ");
			experienceFromUser = sc.nextLine();

			if(keyFromUser.equalsIgnoreCase("exit"))
			{
				break;
			}

			CustomData customRecord=new CustomData();
			if(!customRecord.SetRecord(keyFromUser,nameFromUser,salaryFromUser,experienceFromUser)) {
				logger.error("Failed to set the record!!!!!!");
			}


			try {
				producer.kafkaAPIWrapper.SendMessage(keyFromUser,customRecord);
			} catch (ExecutionException ex) {
				logger.error("Exception occurred during sending " + ex.getMessage());

			} catch (InterruptedException e) {
				logger.error("Exception occurred during sending " + e.getMessage());
			}

			logger.warn("Succeeded in sending Custom Record");
			logger.warn(customRecord.toString());

			//Async way of sending
			/*Future<RecordMetadata> isComplete = producer.kafkaAPIWrapper.SendAsyncMessage("JavaProgAsync",
					"Message Sent in an Async way from a Java Prog");
			while (!isComplete.isDone()) {
				sleep(1000);
			}*/

		}
		producer.Close();
	}
}



