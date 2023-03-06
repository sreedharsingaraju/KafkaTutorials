package com.sre.teaching.kafka.microservices.consumer.entities;

import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Id;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;


@AllArgsConstructor
@NoArgsConstructor
@Data
@Builder
@Entity
public class DeviceData {
    @Id
    Integer deviceID;
    String deviceName;

    @Enumerated(EnumType.STRING)
    DeviceType deviceType;
}

