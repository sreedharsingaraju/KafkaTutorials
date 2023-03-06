package com.sre.teaching.kafka.microservices.producer.datamodel;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;



@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class DeviceData {
    Integer deviceID;
    String deviceName;
    DeviceType deviceType;
}

