package com.sre.teaching.kafka.streams.streamingconsumerdemo;

import com.sre.teaching.kafka.streams.streamingconsumerdemo.config.Configure;
import org.apache.kafka.common.protocol.types.ArrayOf;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.ComponentScan;

import java.util.Arrays;

@ComponentScan("com.sre.teaching.kafka.streams.streamingconsumerdemo")
public class StreamsWordCountApp {

    Configure configurer=new Configure();


    public  Topology CreateWordCountTopology()
    {
        StreamsBuilder streamsBuilder=new StreamsBuilder();

        //1. StreamBuilder : Stream data from topic example "This iS a teXt\n this is another text"
        KStream<String,String> streamOfText=streamsBuilder.stream(configurer.SourceTopic());

        //2. mapvalue: convert to same case, in this eg to lower case "this is a text\n this is another text"

       KTable<String, Long> wordsCountTable=streamOfText.mapValues(text -> text.toLowerCase())

                //3. flatmapValues: make every word in the strea data as seperate message
                .flatMapValues(lowercasetext -> Arrays.asList(lowercasetext.split(" ")))

                //4. Setkey: now make the keys same as value
                .selectKey((emptykey, word) -> word)
                //5. Group by based on key
                .groupByKey()
                //6. Get count of such grouped key based messaes
                .count();

            KStream printStream=    wordsCountTable.toStream();

            printStream.foreach((word,count)-> System.out.println("Word "+word+" Count "+count));

         //7. Write the words counted to destination topic
        wordsCountTable.toStream().to(configurer.SinkTopic());

        Topology wordsountTopology=streamsBuilder.build();

        return wordsountTopology;
    }

    public  void RunTopology(Topology wordsountTopology)
    {
        KafkaStreams kafkaStreams=new KafkaStreams(wordsountTopology, Configure.ConfigureKafka());

        kafkaStreams.start();

        System.out.println("Started the topology ");
        System.out.println(kafkaStreams.toString());

        //Add this so that streams are gracefully closed during App exits
        Runtime.getRuntime().addShutdownHook(new Thread(()->{
            System.out.println("Shutting down hence cleaning up");
            kafkaStreams.close();
        }));
    }
    public static void main1(String [] args)
    {
        StreamsWordCountApp streamsWordCountApp=new StreamsWordCountApp();

        Topology wordsountTopology=streamsWordCountApp.CreateWordCountTopology();

        streamsWordCountApp.RunTopology(wordsountTopology);
    }
}
