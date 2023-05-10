package com.harvicom.kafkastreams.processor;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;

@Configuration
//@Profile("one")
//@PropertySource("application.properties")
@ConfigurationProperties(prefix = "fiveminwindows")
public class FiveMinWindowProperties {
    private String topicName;

    public String getTopicName() {
        return topicName;
    }

    public void setTopicName(String topicName) {
        this.topicName=topicName;
    }
}
