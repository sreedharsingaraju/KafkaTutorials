package integration.controller;

import com.sre.teaching.kafka.microservices.producer.datamodel.DeviceData;
import com.sre.teaching.kafka.microservices.producer.datamodel.DeviceType;
import com.sre.teaching.kafka.microservices.producer.datamodel.ResponseData;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.*;
import static  org.junit.jupiter.api.Assertions.assertEquals;


//@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
public class ProducerControllerIntegrationTest {

    @Autowired
    TestRestTemplate testRestTemplate;

    @Test()
    void PostDeviceEvent()
    {

        DeviceData deviceData=new DeviceData();
        deviceData.setDeviceID(566);
        deviceData.setDeviceName("test device");
        deviceData.setDeviceType(DeviceType.MOTION_SENSOR);

        HttpHeaders httpHeaders = new HttpHeaders();
        httpHeaders.set("content-type", MediaType.APPLICATION_JSON.toString());

        HttpEntity<DeviceData> httpEntity=new HttpEntity<>(deviceData,httpHeaders);



        ResponseEntity<ResponseData> response=
                testRestTemplate.exchange("/Device",
                        HttpMethod.POST,
                        httpEntity,
                        ResponseData.class);

        assertEquals(HttpStatus.CREATED, response.getStatusCode());

    }

}
