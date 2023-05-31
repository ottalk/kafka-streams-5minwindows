package com.harvicom.kafkastreams.processor;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.Windowed;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import java.text.DecimalFormat;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.connect.json.JsonDeserializer;
import org.apache.kafka.connect.json.JsonSerializer;
import org.apache.kafka.streams.kstream.TimeWindowedSerializer;
import org.apache.kafka.streams.kstream.TimeWindowedDeserializer;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.KeyValue;

@Component
public class FiveMinWindowEventStreamProcessor {

    @Value("${fiveminwindows.inputTopicName}")
    public String inputTopicName;

    @Value("${fiveminwindows.outputTopicName}")
    public String outputTopicName;

    DecimalFormat df = new DecimalFormat("##########");

    private String getNodeValue(ObjectNode node, String nodeValue) {
        String result = "";
        if (node.has(nodeValue)) {
            result = node.get(nodeValue).asText();
        }
        return result;
    }

    private Double getNodeValueDouble(ObjectNode node, String nodeValue) {
        Double result = 0.0;
        if (node.has(nodeValue)) {
            try {
                return Double.parseDouble(node.get(nodeValue).asText());
            } catch (NumberFormatException nfe) {
                System.out.println("ERROR: nodeValue does not contain double value - " + nfe.getMessage());
                return 0.0;
            }
        }
        return result;
    }

    @Autowired
    public void buildPipeline(StreamsBuilder streamsBuilder) {

        Deserializer<JsonNode> jsonDeserializer = new JsonDeserializer();
        Serializer<JsonNode> jsonSerializer = new JsonSerializer();
        Serde<JsonNode> jsonSerde = Serdes.serdeFrom(jsonSerializer, jsonDeserializer);

        StringSerializer stringSerializer = new StringSerializer();
        StringDeserializer stringDeserializer = new StringDeserializer();
        Serde<String> stringSerde = Serdes.serdeFrom(stringSerializer, stringDeserializer);

        TimeWindowedSerializer<String> windowedSerializer = new TimeWindowedSerializer<>(stringSerializer);
        TimeWindowedDeserializer<String> windowedDeserializer = new TimeWindowedDeserializer<>();
        Serde<Windowed<String>> windowedSerde = Serdes.serdeFrom(windowedSerializer, windowedDeserializer);

        // Consumed<String, JsonNode> consumerOptions = Consumed.with(Serdes.String(),
        // jsonSerde).withTimestampExtractor(new StringTimestampExtractor());
        // KStream<String, JsonNode> kStream = streamsBuilder.stream("streams-test-1",
        // consumerOptions);
        // KStream<String, JsonNode> kStream =
        // streamsBuilder.stream(fiveMinWindowProperties.getTopicName(),
        // Consumed.with(Serdes.String(), jsonSerde));
        KStream<String, JsonNode> kStream = streamsBuilder.stream(inputTopicName,
                Consumed.with(Serdes.String(), jsonSerde));

        // kStream.filter((key, value) -> value.startsWith("Message_")).mapValues((k, v)
        // -> v.toUpperCase()).peek((k, v) -> System.out.println("Key : " + k + " Value
        // : " + v)).to("streams-test-2", Produced.with(Serdes.String(),
        // Serdes.String()));
        kStream.map(new KeyValueMapper<String, JsonNode, KeyValue<String, JsonNode>>() {
            @Override
            public KeyValue<String, JsonNode> apply(String key, JsonNode value) {
                ObjectNode node = (ObjectNode) value;
                // node.put("TRANSACTION_TIME",node.get("TRANSACTION_TIME").asText()+"TEST");
                // String
                // compositeKey=node.get("MERCHANT_NO").asText()+":"+node.get("DIV").asText()+":"+node.get("COUNTRY").asText();
                String compositeKey = getNodeValue(node, "MERCHANT_NO") + getNodeValue(node, "DIV")
                        + getNodeValue(node, "COUNTRY") + getNodeValue(node, "CRAP");
                node.put("RESPONSE_TIME_CALC_DOUBLE", getNodeValueDouble(node, "RESPONSE_TIME") + 5.5);
                return new KeyValue<>(compositeKey, node);
            }
        }).to(outputTopicName, Produced.with(Serdes.String(), jsonSerde));
    }
}
