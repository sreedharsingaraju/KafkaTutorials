package com.sre.teaching.kafka.streams.streamingconsumerdemo;

import com.sre.teaching.kafka.streams.streamingconsumerdemo.config.Configure;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Printed;

import java.util.Properties;

//@SpringBootApplication
public class StreamingConsumerDemoApplication {

	public static void main(String[] args) {

		StreamsBuilder streamsBuilder=new StreamsBuilder();

		KStream<String,String> kstream=streamsBuilder.stream("streams-input");

		//kstream.foreach((k,v)-> System.out.println("Key : "+k+" Value: "+v));

		kstream.print(Printed.toSysOut());

		Topology topology=streamsBuilder.build();


		KafkaStreams kstreams= new KafkaStreams(topology, Configure.ConfigureKafka("streaming-app-demo-1"));

		kstreams.start();

		Runtime.getRuntime().addShutdownHook(new Thread(()->
		{
			System.out.println("Shutting down stream");
			kstreams.close();
		}));

	}

}
