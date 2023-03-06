package com.sre.teaching.kafka.microservices.consumer.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sre.teaching.kafka.microservices.consumer.entities.DeviceData;
import com.sre.teaching.kafka.microservices.consumer.repository.DeviceDataRepositoty;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
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
        } catch (JsonProcessingException e) {
            log.error("Failed to conver the value to object before saving. {}",e.getMessage());
            return false;
        }
        catch (Exception ex)
        {
            log.error("Failed to conver the value to object before saving. {}", ex.getMessage());
            return false;
        }

        if(null==deviceDataRepositoty.save(deviceData)){
            return false;
        }

        return true;
    }
}
