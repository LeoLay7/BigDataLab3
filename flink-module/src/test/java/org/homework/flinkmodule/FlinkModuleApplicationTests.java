package org.homework.flinkmodule;

import org.junit.jupiter.api.Test;
import java.util.Objects;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class FlinkModuleApplicationTests {

	@Test
	void shouldLoadDefaultConfigValues() {
		FlinkModuleApplication.AppConfig config = FlinkModuleApplication.AppConfig.load();

		assertNotNull(config);
		assertNotNull(config.kafkaBootstrapServers);
		assertNotNull(config.kafkaTopic);
		assertNotNull(config.jdbcUrl);
		assertEquals(true, !Objects.requireNonNull(config.kafkaTopic).isBlank());
	}

}
