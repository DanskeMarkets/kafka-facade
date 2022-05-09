package dk.danskebank.markets.kafka.consumer.replay.cache;

import com.github.charithe.kafka.KafkaHelper;
import com.github.charithe.kafka.KafkaJunitExtension;
import com.github.charithe.kafka.KafkaJunitExtensionConfig;
import com.github.charithe.kafka.StartupMode;
import com.google.gson.reflect.TypeToken;
import dk.danskebank.markets.kafka.consumer.ExceptionHandler;
import dk.danskebank.markets.kafka.consumer.KafkaSubscriber;
import lombok.val;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.Map;
import java.util.Properties;

import static dk.danskebank.markets.kafka.helper.Helper.mapOf;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.*;

@ExtendWith(KafkaJunitExtension.class)
@KafkaJunitExtensionConfig(startupMode = StartupMode.WAIT_FOR_STARTUP)
@DisplayName("A KafkaCache")
class KafkaCacheTest {

	private static final String TOPIC            = "TestTopic";
	private static final int CONSUMPTION_TIMEOUT = 2_000;

	private final StringDeserializer stringDeserializer = new StringDeserializer();

	private KafkaCache<String, String> cache;

	@BeforeEach void setup(KafkaHelper kafkaHelper) {
		val brokerAddress = "localhost:" + kafkaHelper.kafkaPort();
		val props = new Properties();
		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerAddress);
		cache = new KafkaCache.Builder<>(TOPIC, props, stringDeserializer, new TypeToken<String>(){}).build();
	}

	@AfterEach void shutdown() {
		cache.shutdown();
	}

	@Test @DisplayName("returns an empty map when Kafka topic contains no records")
	void empty() {
		cache.start();
		cache.awaitTopicReplayed();

		assertEquals(Map.of(), cache.getCacheCopy());
	}

	@Test @DisplayName("throws an IllegalStateException if replay is not done when getting a cache copy")
	void throwsIfNotReplayed() {
		val exception = assertThrows(IllegalStateException.class, () -> cache.getCacheCopy());
		assertEquals("Cache is not replayed yet.", exception.getMessage());
	}

	@Test @DisplayName("should invoke replay exception handler method when Kafka Cluster is down")
	void kafkaClusterDown(KafkaHelper kafkaHelper) {
		val wrongBrokerAddress = "localhost:" + (kafkaHelper.kafkaPort() + 171);
		val props = new Properties();
		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,      wrongBrokerAddress);
		props.put(ConsumerConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, 10);
		cache = new KafkaCache.Builder<>(TOPIC, props, stringDeserializer, new TypeToken<String>(){}).build();

		val exceptionHandler = mock(ExceptionHandler.class);
		cache.setExceptionHandler(exceptionHandler);
		cache.start();

		verify(exceptionHandler, timeout(CONSUMPTION_TIMEOUT)).handleReplayException(
				eq("TestTopic"),
				eq("Exception while replaying."),
				argThat(exception -> "Timeout expired while fetching topic metadata".equals(exception.getMessage())));
	}

	@SuppressWarnings("unchecked")
	@Test @DisplayName("returns a map with all key/values present in the Kafka topic (and ignore deleted records)")
	void withRecords(KafkaHelper kafkaHelper) {
		val subscriber = (KafkaSubscriber<String, String>) mock(KafkaSubscriber.class);
		val producer   = kafkaHelper.createStringProducer();

		cache.register(subscriber);

		kafkaHelper.produce(TOPIC, producer,
			mapOf(
				"k1", "v1",
				"k2", "v2",
				"k3", null
			)
		);

		cache.start();
		cache.awaitTopicReplayed();

		assertEquals(Map.of(
				"k1", "v1",
				"k2", "v2"), cache.getCacheCopy());

		kafkaHelper.produce(TOPIC, producer,
			mapOf(
				"k3", "v3",
				"k4", "v4",
				"k5", null
			)
		);

		verify(subscriber, timeout(CONSUMPTION_TIMEOUT)).onNewRecord("k3", "v3");
		verify(subscriber, timeout(CONSUMPTION_TIMEOUT)).onNewRecord("k4", "v4");
		verify(subscriber, timeout(CONSUMPTION_TIMEOUT)).onDeletedRecord("k5");

		assertEquals(Map.of(
				"k1", "v1",
				"k2", "v2",
				"k3", "v3",
				"k4", "v4"), cache.getCacheCopy());

		cache.takeReadLock();
		try {
			kafkaHelper.produce(TOPIC, producer, Map.of("k5", "v5"));

			assertEquals(Map.of(
					"k1", "v1",
					"k2", "v2",
					"k3", "v3",
					"k4", "v4"), cache.getCacheCopy());
		}
		finally {
			cache.releaseReadLock();
		}

		verify(subscriber, timeout(CONSUMPTION_TIMEOUT)).onNewRecord("k5", "v5");

		assertEquals(Map.of(
				"k1", "v1",
				"k2", "v2",
				"k3", "v3",
				"k4", "v4",
				"k5", "v5"), cache.getCacheCopy());

		assertEquals("v1", cache.get("k1"));
	}
}
