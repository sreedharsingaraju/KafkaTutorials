package com.sre.teaching.kafka.microservices.consumer.datamodel;

import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
public class MessageHeader {

    String eventSource;
    EventType eventType;
}
