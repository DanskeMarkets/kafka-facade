package dk.danskebank.markets.kafka.consumer.replay;

import com.github.charithe.kafka.KafkaHelper;
import com.github.charithe.kafka.KafkaJunitExtension;
import com.github.charithe.kafka.KafkaJunitExtensionConfig;
import com.github.charithe.kafka.StartupMode;
import com.google.gson.reflect.TypeToken;
import dk.danskebank.markets.kafka.consumer.ExceptionHandler;
import dk.danskebank.markets.kafka.consumer.KafkaSubscriber;
import dk.danskebank.markets.kafka.helper.Helper;
import lombok.val;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mockito;

import java.util.Map;
import java.util.Properties;

import static org.mockito.Mockito.*;

@ExtendWith(KafkaJunitExtension.class)
@KafkaJunitExtensionConfig(startupMode = StartupMode.WAIT_FOR_STARTUP)
@DisplayName("A KafkaReplayConsumer")
class KafkaReplayConsumerTest {

	private static final String TOPIC            = "TestTopic";
	private static final int CONSUMPTION_TIMEOUT = 2_000;

	private final StringDeserializer stringDeserializer = new StringDeserializer();

	private KafkaReplayConsumer<String, String> replayConsumer;

	@BeforeEach void setup(KafkaHelper kafkaHelper) {
		val brokerAddress = "localhost:" + kafkaHelper.kafkaPort();
		val props = new Properties();
		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerAddress);
		replayConsumer = new KafkaReplayConsumer.Builder<>(TOPIC, props, stringDeserializer, new TypeToken<String>(){}).build();
	}

	@AfterEach void shutdown() {
		replayConsumer.shutdown();
	}

	@SuppressWarnings("unchecked")
	@Test @DisplayName("invokes only the replayDone() callback when the Kafka topic contains no records")
	void empty() {
		val subscriber = (KafkaSubscriber<String, String>) mock(KafkaSubscriber.class);
		replayConsumer.register(subscriber);
		replayConsumer.start();

		verify(subscriber, timeout(CONSUMPTION_TIMEOUT)).onReplayDone();
		verify(subscriber, never()).onNewRecord(any(), any());
		verify(subscriber, never()).onDeletedRecord(any());
	}

	@Test @DisplayName("should invoke replay exception handler method when Kafka Cluster is down")
	void kafkaClusterDown(KafkaHelper kafkaHelper) {
		val wrongBrokerAddress = "localhost:" + (kafkaHelper.kafkaPort() + 171);
		val props = new Properties();
		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,      wrongBrokerAddress);
		props.put(ConsumerConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, 10);
		replayConsumer = new KafkaReplayConsumer.Builder<>(TOPIC, props, stringDeserializer, new TypeToken<String>(){}).build();

		val exceptionHandler = mock(ExceptionHandler.class);
		replayConsumer.setExceptionHandler(exceptionHandler);
		replayConsumer.start();

		verify(exceptionHandler, timeout(CONSUMPTION_TIMEOUT)).handleReplayException(
				eq("TestTopic"),
				eq("Exception while replaying."),
				argThat(exception -> "Timeout expired while fetching topic metadata".equals(exception.getMessage())));
	}

	@SuppressWarnings("unchecked")
	@Test @DisplayName("invokes onNewRecord, replayDone and onDeletedRecord as records are read")
	void withRecords(KafkaHelper kafkaHelper) {
		val subscriber = (KafkaSubscriber<String, String>) Mockito.mock(KafkaSubscriber.class);
		val ordering   = inOrder(subscriber);
		val producer   = kafkaHelper.createStringProducer();

		replayConsumer.register(subscriber);

		kafkaHelper.produce(TOPIC, producer,
			Helper.mapOf(
				"k1", "v1",
				"k2", "v2",
				"k3", null
			)
		);

		replayConsumer.start();

		ordering.verify(subscriber, timeout(CONSUMPTION_TIMEOUT)).onNewRecord("k1", "v1");
		ordering.verify(subscriber, timeout(CONSUMPTION_TIMEOUT)).onNewRecord("k2", "v2");
		ordering.verify(subscriber, timeout(CONSUMPTION_TIMEOUT)).onDeletedRecord("k3");
		ordering.verify(subscriber, timeout(CONSUMPTION_TIMEOUT)).onReplayDone();

		kafkaHelper.produce(TOPIC, producer,
			Helper.mapOf(
				"k3", "v3",
				"k4", "v4",
				"k5", null
			)
		);

		ordering.verify(subscriber, timeout(CONSUMPTION_TIMEOUT)).onNewRecord("k3", "v3");
		ordering.verify(subscriber, timeout(CONSUMPTION_TIMEOUT)).onNewRecord("k4", "v4");
		ordering.verify(subscriber, timeout(CONSUMPTION_TIMEOUT)).onDeletedRecord("k5");

		kafkaHelper.produce(TOPIC, producer, Map.of("k5", "v5"));
		verify(subscriber, timeout(CONSUMPTION_TIMEOUT)).onNewRecord("k5", "v5");
	}
}
