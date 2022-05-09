package dk.danskebank.markets.kafka.consumer.replay;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import dk.danskebank.markets.kafka.consumer.*;
import dk.danskebank.markets.kafka.serialization.GsonDeserializer;
import dk.danskebank.markets.lifecycle.Lifecycle;
import lombok.NonNull;
import lombok.extern.log4j.Log4j2;
import lombok.val;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.Deserializer;

import java.time.Duration;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import static java.util.Objects.requireNonNullElseGet;
import static java.util.stream.Collectors.toMap;
import static java.util.stream.Collectors.toSet;

/**
 * <p>Consumer that replays from the beginning of a topic.
 * <p>It can be used for replaying all records and get a callback when the mode changes from replay to streaming.
 * <p>The default {@link ExceptionHandler} will log any Throwable during replay and streaming and will exit the JVM.
 * If this behaviour is not desired, set a custom {@link ExceptionHandler}.
 * <p>Note, offsets are not committed by the consumer.</p>
 * @param <K> The type of the key.
 * @param <V> The type of the value.
 */
@Log4j2
public class KafkaReplayConsumer<K, V> extends KafkaStreamingConsumer<K, V> implements Lifecycle {

	/**
	 * Creates a {@link KafkaReplayConsumer} for the specified topic.
	 *
	 * @param topicName The name of the topic.
	 * @param consumer The {@link KafkaConsumer} which will be used to consume records from Kafka.
	 * @param pollDuration The poll duration timeout.
	 */
	private KafkaReplayConsumer(
			@NonNull String topicName,
			@NonNull KafkaConsumer<K, V> consumer,
			@NonNull Duration pollDuration) {
		super(topicName, consumer, pollDuration, ConsumerStartPoint.EARLIEST_OFFSET);
	}

	@Override
	protected void run() {
		assignPartitions();
		replayTopic();
		signalTopicReplayed();
		streamTopic();
	}

	private void replayTopic() {
		if (!running.get()) return;

		final Set<Integer> remainingPartitions;
		final Map<Integer, Long> endOffsets;
		try {
			val topicPartitions  = getTopicPartitions();
			val beginningOffsets = consumer.beginningOffsets(topicPartitions);
			endOffsets           = consumer.endOffsets(topicPartitions).entrySet().stream()
					.collect(toMap(kv -> kv.getKey().partition(), Map.Entry::getValue));
			remainingPartitions  = topicPartitions.stream()
					.filter(tp -> beginningOffsets.get(tp) < endOffsets.get(tp.partition()))
					.map(TopicPartition::partition)
					.collect(toSet());
		} catch (WakeupException e) {
			if (running.get()) { // Ignore unless we should be running.
				exceptionHandlerReference.get().handleReplayException(topicName, "Unexpected wakeup exception.", e);
			}
			return;
		} catch (Throwable t) {
			exceptionHandlerReference.get().handleReplayException(topicName, "Exception while replaying.", t);
			return;
		}

		while (running.get() && !remainingPartitions.isEmpty()) {
			try {
				handlePendingCommitRequests();

				val records = consumer.poll(pollDuration);
				for (val record: records) {
					int partition = record.partition();
					if (remainingPartitions.contains(partition)) {
						val endOffset = endOffsets.get(partition);
						if (record.offset() >= endOffset - 1) {
							remainingPartitions.remove(partition);
						}
					}
					publishToSubscribers(record);
				}
			} catch (WakeupException e) {
				if (running.get()) { // Ignore unless we should be running.
					exceptionHandlerReference.get().handleReplayException(topicName, "Unexpected wakeup exception.", e);
				}
			} catch (Throwable t) {
				exceptionHandlerReference.get().handleReplayException(topicName, "Exception while replaying.", t);
			}
		}
	}

	private void signalTopicReplayed() {
		if (!running.get()) return;

		try {
			subscribers.forEach(KafkaSubscriber::onReplayDone);
			metadataAwareSubscribers.forEach(KafkaMetadataAwareSubscriber::onReplayDone);
		} catch (Throwable t) {
			exceptionHandlerReference.get().handleReplayException(topicName, "Exception while signaling topic replayed.", t);
		}
	}

	/**
	 * A builder class used to construct {@link KafkaReplayConsumer} instances.
	 *
	 * @param <K> The type of the key.
	 * @param <V> The type of the value.
	 */
	public static class Builder<K, V> {
		public static final Duration DEFAULT_POLL_DURATION = Duration.ofMillis(1_000);

		private final String topicName;

		private Gson gson;
		private Duration pollDuration;
		private Properties properties;

		private KafkaConsumer<K, V> consumer;

		private Deserializer<K> keyDeserializer;
		private Deserializer<V> valueDeserializer;
		private TypeToken<V> valueType;

		/**
		 * Creates a {@link KafkaReplayConsumer} builder for the specified topic using a preexisting
		 * {@link KafkaConsumer} instance.
		 *
		 * @param topicName The name of the topic.
		 * @param consumer The {@link KafkaConsumer} which will be used to consume records from Kafka.
		 */
		public Builder(@NonNull String topicName, @NonNull KafkaConsumer<K, V> consumer) {
			this.topicName    = topicName;
			this.pollDuration = DEFAULT_POLL_DURATION;
			this.consumer     = consumer;
		}

		/**
		 * Creates a {@link KafkaReplayConsumer} builder for the specified topic with a custom deserializer for the
		 * value part of the record.
		 *
		 * @param topicName The name of the topic.
		 * @param props The properties for the {@link KafkaConsumer}.
		 * @param keyDeserializer The deserializer for the records' keys.
		 * @param valueDeserializer The deserializer for the records' values.
		 */
		public Builder(
				@NonNull String topicName,
				@NonNull Properties props,
				@NonNull Deserializer<K> keyDeserializer,
				@NonNull Deserializer<V> valueDeserializer) {
			this.topicName         = topicName;
			this.properties        = props;
			this.pollDuration      = DEFAULT_POLL_DURATION;
			this.keyDeserializer   = keyDeserializer;
			this.valueDeserializer = valueDeserializer;
		}

		/**
		 * Creates a {@link KafkaReplayConsumer} builder for the specified topic with a generic {@link GsonDeserializer}
		 * for the value part of the record. The {@link GsonDeserializer}'s {@link Gson} instance can be set using
		 * {@link #with(Gson)}.
		 *
		 * @param topicName The name of the topic.
		 * @param props The properties for the {@link KafkaConsumer}.
		 * @param keyDeserializer The deserializer for the records' keys.
		 * @param valueType The type of the records' value.
		 */
		public Builder(
				@NonNull String topicName,
				@NonNull Properties props,
				@NonNull Deserializer<K> keyDeserializer,
				@NonNull TypeToken<V> valueType) {
			this.topicName         = topicName;
			this.properties        = props;
			this.pollDuration      = DEFAULT_POLL_DURATION;
			this.keyDeserializer   = keyDeserializer;
			this.valueType         = valueType;
		}

		/**
		 * Overrides the {@link Gson} instance used by the {@link GsonDeserializer} for the value part of the record.
		 * This method may only be invoked if the builder was created using the constructor
		 * {@link #Builder(String, Properties, Deserializer, TypeToken)}.
		 *
		 * @param gson The {@link Gson} instance to use for deserializing values.
		 * @return The same builder.
		 */
		public Builder<K, V> with(@NonNull Gson gson) {
			if (valueType == null) {
				throw new IllegalStateException(
						"A Gson instance can only be provided if the KafkaReplayConsumer.Builder was created with "
								+ "a value type token.");
			}
			this.gson = gson;
			return this;
		}

		/**
		 * Overrides the poll duration for the {@link KafkaReplayConsumer}.
		 *
		 * @param pollDuration The custom poll duration.
		 * @return The same builder.
		 */
		public Builder<K, V> with(@NonNull Duration pollDuration) {
			this.pollDuration = pollDuration;
			return this;
		}

		/**
		 * Creates a {@link KafkaReplayConsumer} instance with the parameters configured in the builder.
		 *
		 * @return The created {@link KafkaReplayConsumer} instance.
		 */
		public KafkaReplayConsumer<K, V> build() {
			if (consumer != null) {
				return new KafkaReplayConsumer<>(topicName, consumer, pollDuration);
			}

			if (valueDeserializer == null) {
				valueDeserializer = new GsonDeserializer<>(valueType, requireNonNullElseGet(gson, Gson::new));
			}
			return new KafkaReplayConsumer<>(
					topicName, new KafkaConsumer<>(properties, keyDeserializer, valueDeserializer), pollDuration);
		}
	}
}
