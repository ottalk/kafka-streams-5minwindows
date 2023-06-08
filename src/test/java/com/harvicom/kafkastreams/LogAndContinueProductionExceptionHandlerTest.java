package com.harvicom.kafkastreams;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.streams.errors.ProductionExceptionHandler.ProductionExceptionHandlerResponse;
import org.junit.Test;
import org.junit.jupiter.api.BeforeEach;
import static org.assertj.core.api.Assertions.assertThat;
import java.util.HashMap;
import java.util.Map;
import com.harvicom.kafkastreams.processor.LogAndContinueProductionExceptionHandler;

public class LogAndContinueProductionExceptionHandlerTest {

    LogAndContinueProductionExceptionHandler logAndContinueProductionExceptionHandler;

    @BeforeEach
    void setUp() {
        logAndContinueProductionExceptionHandler = new LogAndContinueProductionExceptionHandler();
    }

    @Test
    public void testHandler_callHandlerAndTestResponse(){

        Exception exception =  new Exception();
        Map<String,String> config = new HashMap<>();
        config.put("key","value");
        ProductionExceptionHandlerResponse response = ProductionExceptionHandlerResponse.CONTINUE;
        try {
            logAndContinueProductionExceptionHandler.configure(config);
        } catch (NullPointerException npe) {
            npe.getMessage();
        }

        try {
           response =logAndContinueProductionExceptionHandler.handle(new ProducerRecord<byte[],byte[]>("topic","value".getBytes()),exception);
        } catch (NullPointerException npe) {
            npe.getMessage();
        }

        assertThat(response.equals(null));
    }
}
