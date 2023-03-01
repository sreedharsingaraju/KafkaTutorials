package com.sre.teaching.kafka.microservices.producer.datamodel;

import lombok.Data;
import lombok.NoArgsConstructor;


@Data
@NoArgsConstructor
public class DeviceData {
    Integer deviceID;
    String deviceName;
    DeviceType deviceType;
}

