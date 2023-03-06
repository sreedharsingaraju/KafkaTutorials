package com.sre.teaching.kafka.microservices.consumer.repository;

import com.sre.teaching.kafka.microservices.consumer.entities.DeviceData;
import org.springframework.data.repository.CrudRepository;

import java.util.Optional;

public interface DeviceDataRepositoty extends CrudRepository<DeviceData, Integer> {

}
