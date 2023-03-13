package integration.controller;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sre.teaching.kafka.microservices.producer.ProducerApplication;
import com.sre.teaching.kafka.microservices.producer.datamodel.DeviceData;
import com.sre.teaching.kafka.microservices.producer.datamodel.DeviceType;
import com.sre.teaching.kafka.microservices.producer.datamodel.ResponseData;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.*;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.ResultActions;
import scala.Int;

import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.core.StringContains.containsString;
import static org.junit.Assert.*;
import static org.slf4j.MDC.get;
import static org.springframework.kafka.test.utils.KafkaTestUtils.getSingleRecord;
import static org.springframework.test.web.client.match.MockRestRequestMatchers.content;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultHandlers.print;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;


@SpringBootTest(classes = ProducerApplication.class,
        webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@Slf4j
//for testing with embedded kafka
@EmbeddedKafka(topics = {"devices-topic"},partitions = 3)
//configuring embedded kafka ports
@TestPropertySource(properties =
        {"spring.kafka.producer.bootstrap-servers=${spring.embedded.kafka.brokers}"})

public class ProducerApplicationTest {

    final static String TEST_TOPIC_NAME = "devices-topic";

    @Autowired
    TestRestTemplate testRestTemplate;

    @Autowired
    EmbeddedKafkaBroker embeddedKafkaBroker;


    //for interacting with embedded kafka
    Consumer<Integer, String> consumer = null;

    //for embedded kafka
    @BeforeEach
    void Initialize() {
        Map<String, Object> config = new HashMap<>(
                KafkaTestUtils
                        .consumerProps("testgroup",
                                "true",
                                embeddedKafkaBroker)
        );


        consumer = new DefaultKafkaConsumerFactory<>(config,
                new IntegerDeserializer(),
                new StringDeserializer())
                .createConsumer();

        embeddedKafkaBroker.consumeFromAllEmbeddedTopics(consumer);

    }

    //for embedded kafka
    @AfterEach
    void UnInitialize() {
        consumer.close();
    }

    @Test
    void TestAddDevice() {

        DeviceData deviceData = new DeviceData();
        deviceData.setDeviceID(566);
        deviceData.setDeviceName("test device");
        deviceData.setDeviceType(DeviceType.MOTION_SENSOR);

        HttpHeaders httpHeaders = new HttpHeaders();
        httpHeaders.set("content-type", MediaType.APPLICATION_JSON.toString());

        HttpEntity<DeviceData> httpEntity = new HttpEntity<>(deviceData, httpHeaders);

        ResponseEntity<ResponseData> response =
                testRestTemplate.exchange("/Device",
                        HttpMethod.POST,
                        httpEntity,
                        ResponseData.class);


        assertEquals(HttpStatus.CREATED, response.getStatusCode());

    }


    @Test
    void TestAddDeviceWithEmbeddedKafka() {

        DeviceData deviceData = new DeviceData();
        deviceData.setDeviceName("test device");
        deviceData.setDeviceType(DeviceType.MOTION_SENSOR);

        HttpHeaders httpHeaders = new HttpHeaders();
        httpHeaders.set("content-type", MediaType.APPLICATION_JSON.toString());

        HttpEntity<DeviceData> httpEntity = new HttpEntity<>(deviceData, httpHeaders);

        ResponseEntity<ResponseData> response =
                testRestTemplate.exchange("/Device",
                        HttpMethod.POST,
                        httpEntity,
                        ResponseData.class);


        assertEquals(HttpStatus.CREATED, response.getStatusCode());

        //read the records from the topic queue in the embedded Kafka server
        ConsumerRecords<Integer, String> consumerRecords = KafkaTestUtils.getRecords(consumer);

        ResponseData responseData;
        ResponseData expectedResponseData;
        ObjectMapper converter = new ObjectMapper();


        //expecting same record to be read every time
        //preparing the exected response data
        DeviceData expectedDevicedata = DeviceData.builder()
                .deviceID(566)
                .deviceName("test device")
                .deviceType(DeviceType.MOTION_SENSOR)
                .build();

        ResponseData expectedResponse = ResponseData.builder()
                .deviceData(expectedDevicedata)
                .topicName(TEST_TOPIC_NAME)
                .build();
        //--- end of response data expected

        DeviceData testDeviceData = null;

        for (ConsumerRecord<Integer, String> record : consumerRecords) {

            try {
                testDeviceData = converter.readValue(record.value(), DeviceData.class);
            } catch (JsonProcessingException e) {
                log.error("Exception during conversion . {}", record.value());

                return;
            }

            log.info("actual device name {}", testDeviceData.getDeviceName());
            log.info("actual device type {}", testDeviceData.getDeviceType());

            log.info("expected device name{}", expectedResponse.getDeviceData().getDeviceName());
            log.info("expected device type {}", expectedResponse.getDeviceData().getDeviceType());

            if ((testDeviceData.getDeviceName().equals(expectedResponse.getDeviceData().getDeviceName()))) {
                log.info(" device names are matching");
            } else {
                log.info("device names are not matching");
            }

            if ((testDeviceData.getDeviceType().equals(expectedResponse.getDeviceData().getDeviceType()))) {
                log.info(" device types are matching");
            } else {
                log.info("device types are not matching");
            }


            //check if the read record is having same values as that of expected record in each message
            assert (
                    //(testDeviceData.getDeviceID() == expectedResponse.getDeviceData().getDeviceID()) &&
                    (testDeviceData.getDeviceName().equals(expectedResponse.getDeviceData().getDeviceName())) &&
                            (testDeviceData.getDeviceType().equals(expectedResponse.getDeviceData().getDeviceType())));

        }

    }
}
