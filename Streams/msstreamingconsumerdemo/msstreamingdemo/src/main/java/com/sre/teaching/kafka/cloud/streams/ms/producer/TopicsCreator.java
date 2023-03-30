package com.sre.teaching.kafka.cloud.streams.ms.producer;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.stereotype.Component;

@Component
public class TopicsCreator {

    @Bean
    NewTopic MSCloudStream()
    {
        return TopicBuilder.name("ms-cloud-stream")
                            .partitions(3)
                            .replicas(2)
                            .build();
    }

}
