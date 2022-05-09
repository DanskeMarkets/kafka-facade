package dk.danskebank.markets.kafka.consumer;

import com.github.charithe.kafka.KafkaHelper;
import com.github.charithe.kafka.KafkaJunitExtension;
import com.github.charithe.kafka.KafkaJunitExtensionConfig;
import com.github.charithe.kafka.StartupMode;
import com.google.gson.reflect.TypeToken;
import dk.danskebank.markets.kafka.helper.Helper;
import lombok.val;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.ArgumentMatcher;
import org.mockito.Mockito;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Objects;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.*;

@ExtendWith(KafkaJunitExtension.class)
@KafkaJunitExtensionConfig(startupMode = StartupMode.WAIT_FOR_STARTUP)
@DisplayName("A KafkaStreamingConsumer")
class KafkaStreamingConsumerTest {

	private static final String TOPIC             = "TestTopic";
	private static final String CONSUMER_GROUP_ID = "TestConsumer";
	private static final int CONSUMPTION_TIMEOUT  = 2_000;
	private static final int SUBSCRIPTION_TIMEOUT = 10_000;

	private final StringDeserializer stringDeserializer = new StringDeserializer();

	private KafkaStreamingConsumer<String, String> streamingConsumer;

	@AfterEach
	void tearDown() {
		if (streamingConsumer != null) {
			streamingConsumer.shutdown();
			streamingConsumer = null;
		}
	}

	@SuppressWarnings("unchecked")
	@Test @DisplayName("must start from the earliest offset if configured accordingly")
	void mustStartFromEarliestOffsetIfConfigured(KafkaHelper kafkaHelper) {
		// Produce a few records to the topic before starting the streaming consumer
		val producer = kafkaHelper.createStringProducer();
		kafkaHelper.produce(TOPIC, producer,
				Helper.mapOf(
						"k1", "v1",
						"k2", "v2",
						"k3", null
				)
		);

		// Set up a streaming consumer which should start consuming from the earliest offset
		val brokerAddress = "localhost:" + kafkaHelper.kafkaPort();
		val props = new Properties();
		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerAddress);

		streamingConsumer = new KafkaStreamingConsumer
				.Builder<>(TOPIC, props, stringDeserializer, new TypeToken<String>(){})
				.with(ConsumerStartPoint.EARLIEST_OFFSET)
				.build();

		val subscriber = (KafkaSubscriber<String, String>) Mockito.mock(KafkaSubscriber.class);
		val ordering   = inOrder(subscriber);

		streamingConsumer.register(subscriber);

		// Start the streaming consumer
		streamingConsumer.start();

		// Verify that the streaming consumer consumes the pre-existing records
		ordering.verify(subscriber, timeout(CONSUMPTION_TIMEOUT)).onNewRecord("k1", "v1");
		ordering.verify(subscriber, timeout(CONSUMPTION_TIMEOUT)).onNewRecord("k2", "v2");
		ordering.verify(subscriber, timeout(CONSUMPTION_TIMEOUT)).onDeletedRecord("k3");
		ordering.verifyNoMoreInteractions();

		// Produce more records to the topic
		kafkaHelper.produce(TOPIC, producer,
				Helper.mapOf(
						"k2", "v3",
						"k1", null
				)
		);

		// Verify that the streaming consumer consumes the new records
		ordering.verify(subscriber, timeout(CONSUMPTION_TIMEOUT)).onNewRecord("k2", "v3");
		ordering.verify(subscriber, timeout(CONSUMPTION_TIMEOUT)).onDeletedRecord("k1");
		ordering.verifyNoMoreInteractions();

		// Shut down the streaming consumer
		streamingConsumer.shutdown();

		// Clear invocations to avoid interfering with subsequent verifications
		clearInvocations(subscriber);

		// Produce more records to the topic
		kafkaHelper.produce(TOPIC, producer,
				Helper.mapOf(
						"k1", "v4",
						"k4", "v5"
				)
		);

		// Set up a new streaming consumer with the same settings as before and start it
		streamingConsumer = new KafkaStreamingConsumer
				.Builder<>(TOPIC, props, stringDeserializer, new TypeToken<String>(){})
				.with(ConsumerStartPoint.EARLIEST_OFFSET)
				.build();
		streamingConsumer.register(subscriber);
		streamingConsumer.start();

		// Verify that the streaming consumer consumes all the records starting from the earliest offset
		ordering.verify(subscriber, timeout(CONSUMPTION_TIMEOUT)).onNewRecord("k1", "v1");
		ordering.verify(subscriber, timeout(CONSUMPTION_TIMEOUT)).onNewRecord("k2", "v2");
		ordering.verify(subscriber, timeout(CONSUMPTION_TIMEOUT)).onDeletedRecord("k3");
		ordering.verify(subscriber, timeout(CONSUMPTION_TIMEOUT)).onNewRecord("k2", "v3");
		ordering.verify(subscriber, timeout(CONSUMPTION_TIMEOUT)).onDeletedRecord("k1");
		ordering.verify(subscriber, timeout(CONSUMPTION_TIMEOUT)).onNewRecord("k1", "v4");
		ordering.verify(subscriber, timeout(CONSUMPTION_TIMEOUT)).onNewRecord("k4", "v5");
		ordering.verifyNoMoreInteractions();
	}

	@SuppressWarnings("unchecked")
	@Test @DisplayName("must start from the latest offset if configured accordingly")
	void mustStartFromLatestOffsetIfConfigured(KafkaHelper kafkaHelper) {
		// Produce a few records to the topic before starting the streaming consumer
		val producer = kafkaHelper.createStringProducer();
		kafkaHelper.produce(TOPIC, producer,
				Helper.mapOf(
						"k1", "v1",
						"k2", "v2",
						"k3", null
				)
		);

		// Set up a streaming consumer which should start consuming from the latest offset
		val brokerAddress = "localhost:" + kafkaHelper.kafkaPort();
		val props = new Properties();
		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerAddress);

		streamingConsumer = new KafkaStreamingConsumer
				.Builder<>(TOPIC, props, stringDeserializer, new TypeToken<String>(){})
				.with(ConsumerStartPoint.LATEST_OFFSET)
				.build();

		val subscriber = (KafkaSubscriber<String, String>) Mockito.mock(KafkaSubscriber.class);
		val ordering   = inOrder(subscriber);

		streamingConsumer.register(subscriber);

		// Start the streaming consumer
		streamingConsumer.start();

		// Verify that the streaming consumer does not consume the pre-existing records
		verify(subscriber, after(CONSUMPTION_TIMEOUT).never()).onNewRecord(anyString(), anyString());
		ordering.verifyNoMoreInteractions();

		// Produce more records to the topic
		kafkaHelper.produce(TOPIC, producer,
				Helper.mapOf(
						"k2", "v3",
						"k1", null
				)
		);

		// Verify that the streaming consumer consumes the new records
		ordering.verify(subscriber, timeout(CONSUMPTION_TIMEOUT)).onNewRecord("k2", "v3");
		ordering.verify(subscriber, timeout(CONSUMPTION_TIMEOUT)).onDeletedRecord("k1");
		ordering.verifyNoMoreInteractions();

		// Shut down the streaming consumer
		streamingConsumer.shutdown();

		// Clear invocations to avoid interfering with subsequent verifications
		clearInvocations(subscriber);

		// Produce more records to the topic
		kafkaHelper.produce(TOPIC, producer,
				Helper.mapOf(
						"k1", "v4",
						"k4", "v5"
				)
		);

		// Set up a new streaming consumer with the same settings as before and start it
		streamingConsumer = new KafkaStreamingConsumer
				.Builder<>(TOPIC, props, stringDeserializer, new TypeToken<String>(){})
				.with(ConsumerStartPoint.LATEST_OFFSET)
				.build();
		streamingConsumer.register(subscriber);
		streamingConsumer.start();

		// Verify that the streaming does not consume any of the records produced so far
		verify(subscriber, after(CONSUMPTION_TIMEOUT).never()).onNewRecord(anyString(), anyString());
		ordering.verifyNoMoreInteractions();
	}

	@SuppressWarnings("unchecked")
	@Test @DisplayName("must start from a reference timestamp if configured accordingly")
	void mustStartFromReferenceTimestampIfConfigured(KafkaHelper kafkaHelper) {
		// Produce a few records to the topic before starting the streaming consumer, where the timestamps of the first
		// two are earlier than the reference timestamp
		val producer           = kafkaHelper.createStringProducer();
		val referenceTimestamp = Instant.now().minus(1, ChronoUnit.DAYS);
		val timestamp1         = referenceTimestamp.minus(2, ChronoUnit.DAYS);
		producer.send(new ProducerRecord<>(
				TOPIC, 0, timestamp1.toEpochMilli(), "k1", "v1"));
		val timestamp2         = referenceTimestamp.minus(30, ChronoUnit.MINUTES);
		producer.send(new ProducerRecord<>(
				TOPIC, 0, timestamp2.toEpochMilli(), "k2", "v2"));
		val timestamp3         = referenceTimestamp.plusSeconds(1);
		producer.send(new ProducerRecord<String, String>(
				TOPIC, 0, timestamp3.toEpochMilli(), "k3", null));
		producer.flush();

		// Set up a streaming consumer which should start consuming from the reference timestamp
		val brokerAddress = "localhost:" + kafkaHelper.kafkaPort();
		val props = new Properties();
		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerAddress);

		streamingConsumer = new KafkaStreamingConsumer
				.Builder<>(TOPIC, props, stringDeserializer, new TypeToken<String>(){})
				.with(ConsumerStartPoint.fromReferenceTimestamp(referenceTimestamp))
				.build();

		val subscriber = (KafkaSubscriber<String, String>) Mockito.mock(KafkaSubscriber.class);
		val ordering   = inOrder(subscriber);

		streamingConsumer.register(subscriber);

		// Start the streaming consumer
		streamingConsumer.start();

		// Verify that the streaming consumer only consumed the record with timestamp later than the reference timestamp
		ordering.verify(subscriber, timeout(CONSUMPTION_TIMEOUT)).onDeletedRecord("k3");
		ordering.verifyNoMoreInteractions();

		// Produce more records to the topic
		kafkaHelper.produce(TOPIC, producer,
				Helper.mapOf(
						"k2", "v3",
						"k1", null
				)
		);

		// Verify that the streaming consumer consumes the new records
		ordering.verify(subscriber, timeout(CONSUMPTION_TIMEOUT)).onNewRecord("k2", "v3");
		ordering.verify(subscriber, timeout(CONSUMPTION_TIMEOUT)).onDeletedRecord("k1");
		ordering.verifyNoMoreInteractions();

		// Shut down the streaming consumer
		streamingConsumer.shutdown();

		// Clear invocations to avoid interfering with subsequent verifications
		clearInvocations(subscriber);

		// Produce more records to the topic
		kafkaHelper.produce(TOPIC, producer,
				Helper.mapOf(
						"k1", "v4",
						"k4", "v5"
				)
		);

		// Set up a new streaming consumer with the same settings as before and start it
		streamingConsumer = new KafkaStreamingConsumer
				.Builder<>(TOPIC, props, stringDeserializer, new TypeToken<String>(){})
				.with(ConsumerStartPoint.fromReferenceTimestamp(referenceTimestamp))
				.build();
		streamingConsumer.register(subscriber);
		streamingConsumer.start();

		// Verify that the streaming consumer consumes the records starting from the first one with a timestamp later
		// than the reference timestamp
		ordering.verify(subscriber, timeout(CONSUMPTION_TIMEOUT)).onDeletedRecord("k3");
		ordering.verify(subscriber, timeout(CONSUMPTION_TIMEOUT)).onNewRecord("k2", "v3");
		ordering.verify(subscriber, timeout(CONSUMPTION_TIMEOUT)).onDeletedRecord("k1");
		ordering.verify(subscriber, timeout(CONSUMPTION_TIMEOUT)).onNewRecord("k1", "v4");
		ordering.verify(subscriber, timeout(CONSUMPTION_TIMEOUT)).onNewRecord("k4", "v5");
		ordering.verifyNoMoreInteractions();
	}

	@SuppressWarnings("unchecked")
	@Test @DisplayName("must work with offset tracking and automatic offset commits if configured accordingly")
	void mustWorkWithOffsetTrackingAndAutoOffsetCommitsIfConfigured(KafkaHelper kafkaHelper) {
		// Produce a few records to the topic before starting the streaming consumer
		val producer = kafkaHelper.createStringProducer();
		kafkaHelper.produce(TOPIC, producer,
				Helper.mapOf(
						"k1", "v1",
						"k2", "v2",
						"k3", null
				)
		);

		// Set up a streaming consumer which should use the default Kafka offset tracking semantics and start from the
		// latest offset if the consumer group does not have a committed offset yet
		val brokerAddress = "localhost:" + kafkaHelper.kafkaPort();
		val props = new Properties();
		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerAddress);
		props.put(ConsumerConfig.GROUP_ID_CONFIG, CONSUMER_GROUP_ID);
		props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
		props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, 100);
		props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");

		streamingConsumer = new KafkaStreamingConsumer
				.Builder<>(TOPIC, props, stringDeserializer, new TypeToken<String>(){})
				.with(Duration.ofMillis(100))
				.with(ConsumerStartPoint.KAFKA_DEFAULT)
				.build();

		val subscriber = (KafkaSubscriber<String, String>) Mockito.mock(KafkaSubscriber.class);
		val ordering   = inOrder(subscriber);

		streamingConsumer.register(subscriber);

		// Start the streaming consumer
		streamingConsumer.start();

		// Verify that the streaming consumer does not consume the pre-existing records
		verify(subscriber, after(SUBSCRIPTION_TIMEOUT + CONSUMPTION_TIMEOUT).never())
				.onNewRecord(anyString(), anyString());
		ordering.verifyNoMoreInteractions();

		// Produce more records to the topic
		kafkaHelper.produce(TOPIC, producer,
				Helper.mapOf(
						"k2", "v3",
						"k1", null
				)
		);

		// Verify that the streaming consumer consumes the new records
		ordering.verify(subscriber, timeout(CONSUMPTION_TIMEOUT)).onNewRecord("k2", "v3");
		ordering.verify(subscriber, timeout(CONSUMPTION_TIMEOUT)).onDeletedRecord("k1");
		ordering.verifyNoMoreInteractions();

		// Create an artificial pause to allow automatic offset commit to kick in
		verify(subscriber, after(CONSUMPTION_TIMEOUT).times(1))
				.onNewRecord(anyString(), anyString());

		// Shut down the streaming consumer
		streamingConsumer.shutdown();

		// Clear invocations to avoid interfering with subsequent verifications
		clearInvocations(subscriber);

		// Produce more records to the topic
		kafkaHelper.produce(TOPIC, producer,
				Helper.mapOf(
						"k1", "v4",
						"k4", "v5"
				)
		);

		// Set up a new streaming consumer with the same settings as before and start it
		streamingConsumer = new KafkaStreamingConsumer
				.Builder<>(TOPIC, props, stringDeserializer, new TypeToken<String>(){})
				.with(Duration.ofMillis(100))
				.with(ConsumerStartPoint.KAFKA_DEFAULT)
				.build();
		streamingConsumer.register(subscriber);
		streamingConsumer.start();

		// Verify that the streaming consumer only consumes the records which were not consumed earlier
		verify(subscriber, after(SUBSCRIPTION_TIMEOUT + CONSUMPTION_TIMEOUT).never()).onDeletedRecord("k1");
		ordering.verify(subscriber, timeout(CONSUMPTION_TIMEOUT)).onNewRecord("k1", "v4");
		ordering.verify(subscriber, timeout(CONSUMPTION_TIMEOUT)).onNewRecord("k4", "v5");
		ordering.verifyNoMoreInteractions();
	}

	@SuppressWarnings("unchecked")
	@Test @DisplayName("must work with offset tracking and manual offset commits if configured accordingly")
	void mustWorkWithOffsetTrackingAndManualOffsetCommitsIfConfigured(KafkaHelper kafkaHelper) {
		// Produce a few records to the topic before starting the streaming consumer
		val producer = kafkaHelper.createStringProducer();
		kafkaHelper.produce(TOPIC, producer,
				Helper.mapOf(
						"k1", "v1",
						"k2", "v2",
						"k3", null
				)
		);

		// Set up a streaming consumer which should use the default Kafka offset tracking semantics and start from the
		// earliest offset if the consumer group does not have a committed offset yet
		val brokerAddress = "localhost:" + kafkaHelper.kafkaPort();
		val props = new Properties();
		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerAddress);
		props.put(ConsumerConfig.GROUP_ID_CONFIG, CONSUMER_GROUP_ID);
		props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
		props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

		streamingConsumer = new KafkaStreamingConsumer
				.Builder<>(TOPIC, props, stringDeserializer, new TypeToken<String>(){})
				.with(ConsumerStartPoint.KAFKA_DEFAULT)
				.build();

		val subscriber = (KafkaMetadataAwareSubscriber<String, String>) Mockito.mock(KafkaMetadataAwareSubscriber.class);
		val ordering   = inOrder(subscriber);

		streamingConsumer.register(subscriber);

		// Start the streaming consumer
		streamingConsumer.start();

		// Verify that the streaming consumer consumes the pre-existing records
		ordering.verify(subscriber, timeout(SUBSCRIPTION_TIMEOUT + CONSUMPTION_TIMEOUT))
				.onNewRecord(eq("k1"), eq("v1"), any(RecordMetadata.class));
		ordering.verify(subscriber, timeout(CONSUMPTION_TIMEOUT))
				.onNewRecord(eq("k2"), eq("v2"), any(RecordMetadata.class));
		ordering.verify(subscriber, timeout(CONSUMPTION_TIMEOUT))
				.onDeletedRecord(eq("k3"), any(RecordMetadata.class));
		ordering.verifyNoMoreInteractions();

		// Produce more records to the topic
		kafkaHelper.produce(TOPIC, producer,
				Helper.mapOf(
						"k2", "v3",
						"k1", null
				)
		);

		// Verify that the streaming consumer consumes the new records, and capture the metadata of the first one
		val metadataCaptor = ArgumentCaptor.forClass(RecordMetadata.class);
		ordering.verify(subscriber, timeout(CONSUMPTION_TIMEOUT))
				.onNewRecord(eq("k2"), eq("v3"), metadataCaptor.capture());
		ordering.verify(subscriber, timeout(CONSUMPTION_TIMEOUT))
				.onDeletedRecord(eq("k1"), any(RecordMetadata.class));
		ordering.verifyNoMoreInteractions();

		// Commit the offset from the captured record metadata
		val recordMetadata = metadataCaptor.getValue();
		assertNotNull(recordMetadata);
		streamingConsumer.commitSync(recordMetadata);

		// Shut down the streaming consumer
		streamingConsumer.shutdown();

		// Clear invocations to avoid interfering with subsequent verifications
		clearInvocations(subscriber);

		// Produce more records to the topic
		kafkaHelper.produce(TOPIC, producer,
				Helper.mapOf(
						"k1", "v4",
						"k4", "v5"
				)
		);

		// Set up a new streaming consumer with the same settings as before and start it
		streamingConsumer = new KafkaStreamingConsumer
				.Builder<>(TOPIC, props, stringDeserializer, new TypeToken<String>(){})
				.with(ConsumerStartPoint.KAFKA_DEFAULT)
				.build();
		streamingConsumer.register(subscriber);
		streamingConsumer.start();

		// Verify that the streaming consumer only consumes the records past the manually committed offset
		verify(subscriber, after(SUBSCRIPTION_TIMEOUT + CONSUMPTION_TIMEOUT).never())
				.onNewRecord(eq("k2"), eq("v3"), any(RecordMetadata.class));
		ordering.verify(subscriber, timeout(CONSUMPTION_TIMEOUT))
				.onDeletedRecord(eq("k1"), any(RecordMetadata.class));
		ordering.verify(subscriber, timeout(CONSUMPTION_TIMEOUT))
				.onNewRecord(eq("k1"), eq("v4"), any(RecordMetadata.class));
		ordering.verify(subscriber, timeout(CONSUMPTION_TIMEOUT))
				.onNewRecord(eq("k4"), eq("v5"), any(RecordMetadata.class));
		ordering.verifyNoMoreInteractions();
	}

	@SuppressWarnings("unchecked")
	@Test @DisplayName("must propagate record metadata to metadata-aware subscribers")
	void mustPropagateRecordMetadataToMetadataAwareSubscribers(KafkaHelper kafkaHelper) {
		// Produce a few records to the topic before starting the streaming consumer and timestamp them with
		// pre-determined values
		val producer           = kafkaHelper.createStringProducer();
		val referenceTimestamp = Instant.now();
		val timestamp1         = referenceTimestamp.minus(2, ChronoUnit.MINUTES);
		producer.send(new ProducerRecord<>(
				TOPIC, 0, timestamp1.toEpochMilli(), "k1", "v1"));
		val timestamp2         = referenceTimestamp.minus(30, ChronoUnit.SECONDS);
		producer.send(new ProducerRecord<>(
				TOPIC, 0, timestamp2.toEpochMilli(), "k2", "v2"));
		val timestamp3         = referenceTimestamp.minusSeconds(1);
		producer.send(new ProducerRecord<String, String>(
				TOPIC, 0, timestamp3.toEpochMilli(), "k3", null));
		producer.flush();

		// Set up a streaming consumer which should start consuming from the earliest offset
		val brokerAddress = "localhost:" + kafkaHelper.kafkaPort();
		val props = new Properties();
		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerAddress);

		streamingConsumer = new KafkaStreamingConsumer
				.Builder<>(TOPIC, props, stringDeserializer, new TypeToken<String>(){})
				.with(ConsumerStartPoint.EARLIEST_OFFSET)
				.build();

		// Register a metadata-aware subscriber
		val subscriber = (KafkaMetadataAwareSubscriber<String, String>) Mockito.mock(KafkaMetadataAwareSubscriber.class);
		val ordering   = inOrder(subscriber);

		streamingConsumer.register(subscriber);

		// Start the streaming consumer
		streamingConsumer.start();

		// Verify that the streaming consumer consumes the pre-existing records and propagates the expected metadata
		// to the subscriber
		ordering.verify(subscriber, timeout(CONSUMPTION_TIMEOUT))
				.onNewRecord(eq("k1"), eq("v1"), argThat(matchesMetadata(0L, timestamp1)));
		ordering.verify(subscriber, timeout(CONSUMPTION_TIMEOUT))
				.onNewRecord(eq("k2"), eq("v2"), argThat(matchesMetadata(1L, timestamp2)));
		ordering.verify(subscriber, timeout(CONSUMPTION_TIMEOUT))
				.onDeletedRecord(eq("k3"), argThat(matchesMetadata(2L, timestamp3)));
		ordering.verifyNoMoreInteractions();

		// Produce more records to the topic
		val timestamp4 = referenceTimestamp.minusMillis(75);
		producer.send(new ProducerRecord<>(
				TOPIC, 0, timestamp4.toEpochMilli(), "k2", "v3"));
		val timestamp5  = referenceTimestamp.plusMillis(100);
		producer.send(new ProducerRecord<String, String>(
				TOPIC, 0, timestamp5.toEpochMilli(), "k1", null));
		producer.flush();

		// Verify that the streaming consumer consumes the newly produced records and propagated the expected metadata
		// to the subscriber
		ordering.verify(subscriber, timeout(CONSUMPTION_TIMEOUT))
				.onNewRecord(eq("k2"), eq("v3"), argThat(matchesMetadata(3L, timestamp4)));
		ordering.verify(subscriber, timeout(CONSUMPTION_TIMEOUT))
				.onDeletedRecord(eq("k1"), argThat(matchesMetadata(4L, timestamp5)));
		ordering.verifyNoMoreInteractions();
	}

	private static ArgumentMatcher<RecordMetadata> matchesMetadata(long offset, Instant timestamp) {
		return a -> Objects.equals(a.getTopic(), TOPIC)
					&& a.getPartition() == 0
					&& a.getOffset() == offset
					&& a.getTimestamp() == timestamp.toEpochMilli()
					&& a.getTimestampType() == TimestampType.CREATE_TIME;
	}
}
