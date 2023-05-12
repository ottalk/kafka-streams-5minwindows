package com.harvicom.kafkastreams.processor;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;

@Configuration
//@Profile("one")
@PropertySource("classpath:application.properties")
@ConfigurationProperties(prefix = "fiveminwindows")
//@ConfigurationPropertiesScan
public class FiveMinWindowProperties {
    @Value("${fiveminwindows.topicName}")
    private String topicName;

    public String getTopicName() {
        return topicName;
    }

    public void setTopicName(String topicName) {
        this.topicName=topicName;
    }
}
