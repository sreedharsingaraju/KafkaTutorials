package com.sre.teaching.kafka.streams.streamingconsumerdemo;

import com.sre.teaching.kafka.streams.streamingconsumerdemo.config.Configure;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;

import java.util.Properties;

//@SpringBootApplication
public class StreamingConsumerDemoApplication {

	public static void main(String[] args) {

		StreamsBuilder streamsBuilder=new StreamsBuilder();

		KStream<String,String> kstream=streamsBuilder.stream("streams-input");

		kstream.foreach((k,v)-> System.out.println("Key : "+k+" Value: "+v));

		Topology topology=streamsBuilder.build();


		KafkaStreams kstreams= new KafkaStreams(topology, Configure.ConfigureKafka());

		kstreams.start();

		Runtime.getRuntime().addShutdownHook(new Thread(()->
		{
			System.out.println("Shutting down stream");
			kstreams.close();
		}));

	}

}
