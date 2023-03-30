package com.sre.teaching.kafka.cloud.streams.ms.producer;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.stereotype.Component;

@Component
@ComponentScan
public class TopicsCreator {

    @Bean
    NewTopic InputText()
    {
        return TopicBuilder.name("ms-cloud-stream-words")
                            .partitions(3)
                            .replicas(2)
                            .build();
    }

    @Bean
    NewTopic ProcessedWordCount()
    {
        return TopicBuilder.name("ms-cloud-streams-wordcount-output")
                .partitions(3)
                .replicas(2)
                .build();
    }

}
