package com.sre.teaching.kafka.microservices.producer.datamodel;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class ResponseData {
    String topicName;
    Integer partition;
    Long offset;
    DeviceData deviceData;

}
