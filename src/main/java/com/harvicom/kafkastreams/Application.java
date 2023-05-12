package com.harvicom.kafkastreams;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.EnableKafkaStreams;

import com.harvicom.kafkastreams.processor.FiveMinWindowProperties;

//import org.springframework.boot.context.properties.ConfigurationPropertiesScan;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.PropertySource;

@SpringBootApplication
@EnableKafka
@EnableKafkaStreams
//@ConfigurationPropertiesScan("com.harvicom.kafkastreams")
//@PropertySource("classpath:application.properties")
@EnableConfigurationProperties(FiveMinWindowProperties.class)
public class Application {

	public static void main(String[] args) {
		SpringApplication.run(Application.class, args);
	}

}
