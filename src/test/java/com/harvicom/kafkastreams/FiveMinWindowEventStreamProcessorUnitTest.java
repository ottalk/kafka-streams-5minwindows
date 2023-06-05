package com.harvicom.kafkastreams;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Properties;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.connect.json.JsonDeserializer;
import org.apache.kafka.connect.json.JsonSerializer;
import com.harvicom.kafkastreams.processor.FiveMinWindowEventStreamProcessor;

class FiveMinWindowEventStreamProcessorUnitTest {

    private FiveMinWindowEventStreamProcessor fiveMinWindowEventStreamProcessor;

    Deserializer<JsonNode> jsonDeserializer = new JsonDeserializer();
    Serializer<JsonNode> jsonSerializer = new JsonSerializer();
    Serde<JsonNode> jsonSerde = Serdes.serdeFrom(jsonSerializer, jsonDeserializer);

    StringSerializer stringSerializer = new StringSerializer();
    StringDeserializer stringDeserializer = new StringDeserializer();
    Serde<String> stringSerde = Serdes.serdeFrom(stringSerializer, stringDeserializer);

    @BeforeEach
    void setUp() {
        fiveMinWindowEventStreamProcessor = new FiveMinWindowEventStreamProcessor();
    }

    @Test
    @SuppressWarnings("unchecked")
    void givenInputMessages_whenProcessed_thenWordCountIsProduced() {
        StreamsBuilder streamsBuilder = new StreamsBuilder();
        fiveMinWindowEventStreamProcessor.inputTopicName="input-topic";
        fiveMinWindowEventStreamProcessor.outputTopicName="output-topic";
        fiveMinWindowEventStreamProcessor.buildPipeline(streamsBuilder);
        Topology topology = streamsBuilder.build();

        try (TopologyTestDriver topologyTestDriver = new TopologyTestDriver(topology, new Properties())) {

            TestInputTopic<String, JsonNode> inputTopic = topologyTestDriver
                .createInputTopic("input-topic", new StringSerializer(), new JsonSerializer());

            TestOutputTopic<String, JsonNode> outputTopic = topologyTestDriver
                .createOutputTopic("output-topic", new StringDeserializer(), new JsonDeserializer());


                ObjectMapper mapper = new ObjectMapper();
                JsonNode jsonNode = null;
                try {
                        jsonNode = mapper.readTree("{\"MERCHANT_NO\":\"12300002\",\"DIV\":\"0001\",\"COUNTRY\":\"UK\",\"RESPONSE_TIME\":\"0.95\"}");
                } catch (JsonProcessingException e) {
                        e.printStackTrace();
                }    

            inputTopic.pipeInput("key", jsonNode);

            ObjectMapper mapper2 = new ObjectMapper();
            JsonNode expectedJsonNode = null;

            try {
                expectedJsonNode = mapper2.readTree("{\"MERCHANT_NO\":\"12300002\",\"DIV\":\"0001\",\"COUNTRY\":\"UK\",\"RESPONSE_TIME\":\"0.95\",\"RESPONSE_TIME_CALC_DOUBLE\":6.45}");
            } catch (JsonProcessingException e) {
                e.printStackTrace();
            } 

            assertThat(outputTopic.readKeyValuesToList()).containsExactly(KeyValue.pair("123000020001UK", expectedJsonNode));
        }
    }

}