package dk.danskebank.markets.kafka.monitoring;

import com.github.charithe.kafka.EphemeralKafkaBroker;
import lombok.val;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Properties;

import static org.mockito.Mockito.*;

class KafkaClusterMonitoringTest {

	private static final int PORT = 17073;

	private KafkaClusterMonitoring instance;

	private KafkaClusterStateListener listener;

	@BeforeEach void setup() {
		var props = new Properties();
		props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:" + PORT);
		instance  = new KafkaClusterMonitoring(props, 1, 400);
		listener  = mock(KafkaClusterStateListener.class);
		instance.register(listener);
	}

	@Test void isDown() {
		instance.start();
		verify(listener, timeout(500)).isDown();
		instance.shutdown();
	}

	@Test void isUp() throws Exception {
		val broker = EphemeralKafkaBroker.create(PORT);
		broker.start().get();

		instance.start();
		verify(listener, timeout(500)).isUp();

		instance.shutdown();
		broker.stop();
	}

	@Test void isUpButGoesDown() throws Exception {
		val broker = EphemeralKafkaBroker.create(PORT);
		broker.start().get();

		instance.start();
		verify(listener, timeout(500)).isUp();

		broker.stop();
		verify(listener, timeout(500)).isDown();

		instance.shutdown();
	}

	@Test void isDownButGoesUp() throws Exception {
		instance.start();
		verify(listener, timeout(500)).isDown();

		val broker = EphemeralKafkaBroker.create(PORT);
		broker.start().get();

		verify(listener, timeout(500)).isUp();

		broker.stop();
		instance.shutdown();
	}
}
