package com.harvicom.kafkastreams;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.streams.errors.ProductionExceptionHandler.ProductionExceptionHandlerResponse;
import org.junit.Test;
import org.junit.jupiter.api.BeforeEach;
import static org.assertj.core.api.Assertions.assertThat;

import com.harvicom.kafkastreams.processor.LogAndContinueProductionExceptionHandler;

public class LogAndContinueProductionExceptionHandlerTest {

    LogAndContinueProductionExceptionHandler logAndContinueProductionExceptionHandler;

    @BeforeEach
    void setUp() {
        logAndContinueProductionExceptionHandler = new LogAndContinueProductionExceptionHandler();
    }

    @Test
    public void testHandler_callHandlerAndTestResponse() {

        Exception exception =  new Exception();

        ProductionExceptionHandlerResponse response=logAndContinueProductionExceptionHandler.handle(new ProducerRecord<byte[],byte[]>("topic","value".getBytes()),exception);
        assertThat(response.equals(ProductionExceptionHandlerResponse.CONTINUE));
    }
}
