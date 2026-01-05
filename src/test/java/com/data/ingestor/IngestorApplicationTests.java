package com.data.ingestor;

import com.data.ingestor.config.AppProperties;
import org.junit.jupiter.api.Test;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.test.context.ActiveProfiles;

@SpringBootTest
@EnableConfigurationProperties(AppProperties.class)
@ActiveProfiles("test")
class IngestorApplicationTests {

	@Test
	void contextLoads() {
	}

}
