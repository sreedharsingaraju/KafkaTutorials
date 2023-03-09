package com.sre.teaching.kafka.microservices.consumer.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sre.teaching.kafka.microservices.consumer.entities.DeviceData;
import com.sre.teaching.kafka.microservices.consumer.repository.DeviceDataRepositoty;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.RecoverableDataAccessException;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class DeviceDataPersistentService {

    @Autowired
    DeviceDataRepositoty deviceDataRepositoty;

    @Autowired
    ObjectMapper objectMapper;

    public boolean Save(String record)
    {
        DeviceData deviceData= null;
        try {
            deviceData = objectMapper.readValue(record, DeviceData.class);
            if(deviceData.getDeviceName().isEmpty())
            {
                throw new IllegalArgumentException("Device Name cannot be blank");
            }

            //enable this code which helps demonstrate recoverable exception scenario
            // so whn this exception is raised the fail retry will be atte,pted
            if(deviceData.getDeviceName().contains("India"))
            {
                throw  new RecoverableDataAccessException(
                        "Exception !!!! But this has met a recoverable possibility ");
            }

        } catch (JsonProcessingException e) {
            log.error("Failed to conver the value to object before saving. {}",e.getMessage());
            return false;
        }


        if(null==deviceDataRepositoty.save(deviceData)){
            throw new RecoverableDataAccessException("Unable to save message at this time");
        }

        return true;
    }
}
