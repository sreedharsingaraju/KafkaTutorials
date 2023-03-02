package com.sre.teaching.kafka.microservices.producer.datamodel;

import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
public class ResponseData {
    String topicName;
    Integer partition;
    Long offset;
    DeviceData deviceData;

}
