package com.sre.teaching.kafka.cloud.streams.ms.producer;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatusCode;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.concurrent.ExecutionException;

@RestController
public class Controller {

    @Autowired
    Producer producer;

    @PostMapping("/generate")
    public String Generate() throws ExecutionException, InterruptedException {

        producer.generateData();

        return "Generated";
    }
}
