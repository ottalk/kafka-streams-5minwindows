package com.harvicom.kafkastreams;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.harvicom.kafkastreams.processor.StringTimestampExtractor;
import static org.assertj.core.api.Assertions.assertThat;

public class StringTimestampExtractorTest {
    
    private StringTimestampExtractor stringTimestampExtractor;
    private JsonNodeFactory factory;
    private ObjectNode testNode;
 
    private String transactionDateTimeStr = "2023-03-13 14:21:32.432"; 

    @BeforeEach
    void setUp() {
        stringTimestampExtractor = new StringTimestampExtractor();
        factory = JsonNodeFactory.instance;
        testNode = factory.objectNode();
    }

    @Test
    void givenTransactionTimeAndPartitionTime_ExtractAndReturnTransactionTime() throws ParseException{

        SimpleDateFormat f = new SimpleDateFormat("yyyy-dd-mm HH:MM:SS.FFF", Locale.getDefault());
        Date d=null;
        d = f.parse(transactionDateTimeStr);
        long transactionDateTimeLong = d.getTime();

        testNode.put("TRANSACTION_TIME", transactionDateTimeStr);

        long partitionTime = System.currentTimeMillis();
        ConsumerRecord<Object, Object> record = new ConsumerRecord<>("topic", 0, 123L, "key", testNode);
        Long returnedTime = stringTimestampExtractor.extract(record, partitionTime);

        assertThat(returnedTime.equals(transactionDateTimeLong));
    }
}
