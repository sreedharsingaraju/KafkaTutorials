package com.sre.teaching.kafka.microservices.producer.controller;

import com.sre.teaching.kafka.microservices.producer.datamodel.DeviceData;
import com.sre.teaching.kafka.microservices.producer.datamodel.DeviceType;
import com.sre.teaching.kafka.microservices.producer.datamodel.EventType;
import com.sre.teaching.kafka.microservices.producer.datamodel.ResponseData;
import com.sre.teaching.kafka.microservices.producer.integration.DeviceEventsProducer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;
import java.util.Random;


@RestController
public class ProducerController {


    @Autowired
    KafkaTemplate<Integer, String> kafkaTemplate;

    static final String SOURCE="device-scannerr";


    @Autowired
    DeviceEventsProducer eventsProducer;
    Random random=new Random();

    @GetMapping("/test")
    DeviceData Test()
    {
        DeviceData deviceData = new DeviceData();
        deviceData.setDeviceID(100);
        deviceData.setDeviceType(DeviceType.MOTION_SENSOR);
        deviceData.setDeviceName("Motionoio");

        return  deviceData;
    }
    @PostMapping("/Device")
   ResponseEntity<ResponseData> AddDevice(@RequestBody DeviceData deviceData)
    {
         //produce kafka event notifying addition of a device

        Integer id=random.nextInt();
        if(id<-1) id*=-1;

        deviceData.setDeviceID(id);

        //option 1
        /*
        SendResult<Integer,String> result=eventsProducer.SendDeviceEvent(deviceData);
        if(result==null) {
            return  ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(null);
        }
        */

        //option2
        SendResult<Integer,String> result=
                eventsProducer.SendDeviceEventWithHeader(deviceData,SOURCE,EventType.NEWDEVICE);

        if(result==null) {
            return  ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(null);
        }

        ResponseData response = new ResponseData();

        response.setTopicName(kafkaTemplate.getDefaultTopic());
        response.setDeviceData(deviceData);
        response.setPartition(result.getRecordMetadata().partition());
        response.setOffset(result.getRecordMetadata().offset());

        return  ResponseEntity.status(HttpStatus.CREATED).body(response);
    }
}
