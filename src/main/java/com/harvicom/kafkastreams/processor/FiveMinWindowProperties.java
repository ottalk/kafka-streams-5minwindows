package com.harvicom.kafkastreams.processor;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Configuration
//@Profile("one")
//@PropertySource("file:/config/application.properties")
@ConfigurationProperties(prefix = "5minwindows")
public class FiveMinWindowProperties {
    private String topicName;

    public String getTopicName() {
        return topicName;
    }

    public void setTopicName(String topicName) {
        this.topicName=topicName;
    }
}
