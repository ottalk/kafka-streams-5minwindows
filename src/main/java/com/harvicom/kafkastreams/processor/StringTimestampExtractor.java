package com.harvicom.kafkastreams.processor;

import org.apache.kafka.streams.processor.TimestampExtractor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;
import java.util.Optional;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class StringTimestampExtractor implements TimestampExtractor {

  private static final Logger logger = LogManager.getLogger();

  @Override
  public long extract(ConsumerRecord<Object, Object> record, long partitionTime) {

    SimpleDateFormat f = new SimpleDateFormat("yyyy-dd-mm hh:mm:ss.fff", Locale.getDefault());
    Date d = new Date(partitionTime);
    ObjectNode node = (ObjectNode) record.value();
    if (node != null && node.get("TRANSACTION_TIME").asText() != null) {
      String timestamp = node.get("TRANSACTION_TIME").asText(); 
      try {
          d = f.parse(timestamp);
      } catch (ParseException e) {
          logger.error(e.getMessage());
      }
    }
    return Optional.of(d.getTime()).orElse(partitionTime);
  }
}