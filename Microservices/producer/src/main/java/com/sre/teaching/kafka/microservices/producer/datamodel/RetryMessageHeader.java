package com.sre.teaching.kafka.microservices.producer.datamodel;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class RetryMessageHeader {
    Integer retryCount;
}
