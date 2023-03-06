package com.sre.teaching.kafka.microservices.consumer.entities;


import jakarta.persistence.Entity;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;



public enum DeviceType {
    TEMPERATURE_READER,
    MOTION_SENSOR

}