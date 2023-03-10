package com.sre.teaching.kafka.microservices.consumer.datamodel;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class RestoreRetryMessageHeader {
    Integer retryCount;
}
