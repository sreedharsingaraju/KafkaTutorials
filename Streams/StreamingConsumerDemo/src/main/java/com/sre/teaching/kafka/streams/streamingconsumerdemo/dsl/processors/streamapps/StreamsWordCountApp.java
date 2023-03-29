package com.sre.teaching.kafka.streams.streamingconsumerdemo.dsl.processors.streamapps;

import com.sre.teaching.kafka.streams.streamingconsumerdemo.config.Configure;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Printed;

import java.util.Arrays;


public class StreamsWordCountApp {

    Configure configurer=new Configure();


    public  Topology CreateWordCountTopology()
    {
        StreamsBuilder streamsBuilder=new StreamsBuilder();

        //1. StreamBuilder : Stream data from topic example "This iS a teXt\n this is another text"
        KStream<String,String> streamOfText=streamsBuilder.stream(configurer.SourceTopic());

        //2. mapvalue: convert to same case, in this eg to lower case "this is a text\n this is another text"
       KTable<String, Long> wordsCountTable=

                 streamOfText.mapValues(text -> text.toLowerCase())
                //3. flatmapValues: make every word in the strea data as seperate message
                .flatMapValues(lowercasetext -> Arrays.asList(lowercasetext.split(" ")))
                //4. Setkey: now make the keys same as valu
                .selectKey((emptykey, word) -> word)
                //5. Group by based on key
                .groupByKey()
                //.groupBy((emptykey,word)->word)
                //6. Get count of such grouped key based messages
                .count();

            KStream printStream=wordsCountTable.toStream();
            // printStream.foreach((word,count)-> System.out.println("Word "+word+" Count "+count));
            //recommend use print as it will use peek and not commit offsets
            printStream.print(Printed.toSysOut());

         //7. Write the words counted to destination topic
        wordsCountTable.toStream().to(configurer.SinkTopic());

        Topology wordsountTopology=streamsBuilder.build();

        return wordsountTopology;
    }

    public  void RunTopology(Topology wordsountTopology)
    {
        KafkaStreams kafkaStreams=new KafkaStreams(wordsountTopology,
                                        Configure.ConfigureKafka("word-count-1"));

        kafkaStreams.start();

        System.out.println("Started the topology ");
        System.out.println(kafkaStreams.toString());

        //Add this so that streams are gracefully closed during App exits
        Runtime.getRuntime().addShutdownHook(new Thread(()->{
            System.out.println("Shutting down hence cleaning up");
            kafkaStreams.close();
        }));
    }
    public static void main(String [] args)
    {
        StreamsWordCountApp streamsWordCountApp=new StreamsWordCountApp();

        Topology wordsountTopology=streamsWordCountApp.CreateWordCountTopology();

        streamsWordCountApp.RunTopology(wordsountTopology);
    }
}
