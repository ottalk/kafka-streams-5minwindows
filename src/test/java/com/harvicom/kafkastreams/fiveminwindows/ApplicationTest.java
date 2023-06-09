package com.harvicom.kafkastreams.fiveminwindows;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import com.harvicom.kafkastreams.fiveminwindows.processor.FiveMinWindowEventStreamProcessor;

@SpringBootTest
class ApplicationTest {

	@Autowired
    FiveMinWindowEventStreamProcessor fiveMinWindowEventStreamProcessor;

	@Test
	void contextLoads() {
	}

}