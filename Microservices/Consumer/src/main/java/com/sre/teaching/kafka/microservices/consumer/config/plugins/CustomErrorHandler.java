package com.sre.teaching.kafka.microservices.consumer.config.plugins;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sre.teaching.kafka.microservices.consumer.entities.DeviceData;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.listener.RetryListener;
import org.springframework.stereotype.Component;
import org.springframework.util.backoff.FixedBackOff;
import org.springframework.beans.factory.annotation.Value;

@Component
@Slf4j
public class CustomErrorHandler {

    @Autowired
    FailureRecoverer failRecoverer;


    @Value("${max.retry.backoff.ms}")
    Integer MAX_RETRY_BACKOFF_MS;

    @Value("${max.retry.count}")
    Long MAX_RETRY_COUNT;

    public  CustomErrorHandler()
    {
        log.info("Max Retry Count: {} Max Retry Backoff MS: {} ",
                                 MAX_RETRY_COUNT,MAX_RETRY_BACKOFF_MS);
    }

    RetryListener retryListener=(record, ex, deliveryAttempt) -> {
        log.info("Came in Retry listener handler ");

        ObjectMapper objectMapper=new ObjectMapper();

        DeviceData deviceData=null;

        try {
            deviceData=objectMapper.readValue(record.value().toString(),DeviceData.class);
        } catch (JsonProcessingException e) {
            log.error("exception in conversion");
            return;
        }


        log.error("The exception occurred for record device id{} name {} Retry attempt {}",
                deviceData.getDeviceID(),
                deviceData.getDeviceName(),
                deliveryAttempt
                );
    };

    public DefaultErrorHandler createCustomHandler() {

        //enable this code for fixed backoff strategy
        FixedBackOff fixedBackOff = new FixedBackOff(MAX_RETRY_BACKOFF_MS,MAX_RETRY_COUNT);

        //enable this code for exponential back off strategy
        /*ExponentialBackOff exponentialBackOff = new ExponentialBackOff();
        exponentialBackOff.setInitialInterval(2000L);
        exponentialBackOff.setMultiplier(2);
        exponentialBackOff.setMaxInterval(10*1000L);
        */

        //this code registers retry listener, and we can handle any customisation
        //of anything which we want when the retries are happening
        DefaultErrorHandler defaultErrorHandler = new DefaultErrorHandler(
                failRecoverer.FailRecoverer(),//this argument plugs in the recovery strategy custom implementation
                fixedBackOff);

        //following code allows us define those exceptions which are never going to be
        //corrected even if we retry, in tgis case if the payload is incorrect
        // there is no point to retry
        defaultErrorHandler.addNotRetryableExceptions(IllegalArgumentException.class);

        //plugging in Retry listener
        defaultErrorHandler.setRetryListeners(retryListener);

        return defaultErrorHandler;
    }
}
