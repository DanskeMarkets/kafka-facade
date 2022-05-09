package dk.danskebank.markets.kafka.consumer.replay.cache;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import dk.danskebank.markets.kafka.consumer.ExceptionHandler;
import dk.danskebank.markets.kafka.consumer.KafkaSubscriber;
import dk.danskebank.markets.kafka.consumer.replay.KafkaReplayConsumer;
import dk.danskebank.markets.kafka.serialization.GsonDeserializer;
import dk.danskebank.markets.lifecycle.Lifecycle;
import lombok.NonNull;
import lombok.extern.log4j.Log4j2;
import lombok.val;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.Deserializer;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

/**
 * <p>Cache for a topic.</p>
 * <p>It can be used for both getting the latest value for each key (replay of all historic records)
 * and for getting new records after the initial replay as a callback - hence registered subscribers will only be
 * called after replay is done.
 * </p>
 * <p>The default {@link ExceptionHandler} will log any Throwable during replay and streaming and will exit the JVM.
 * If this behaviour is not desired, set a custom {@link ExceptionHandler}.</p>
 * <p>Note, offsets are not committed by the consumer.</p>
 * @param <K> The type of the key.
 * @param <V> The type of the value.
 */
@Log4j2
public class KafkaCache<K, V> implements Lifecycle {
	private final Map<K, V> cache                         = new HashMap<>();
	private final List<KafkaSubscriber<K, V>> subscribers = new CopyOnWriteArrayList<>();

	private final CountDownLatch replayDoneSignal         = new CountDownLatch(1);

	/** Synchronizes concurrent reads and writes of the cache. */
	private final ReentrantReadWriteLock lock             = new ReentrantReadWriteLock();

	private final KafkaReplayConsumer<K, V> replayConsumer;

	/**
	 * Creates a {@link KafkaCache} using the provided {@link KafkaReplayConsumer}.
	 *
	 * @param replayConsumer The underlying {@link KafkaReplayConsumer} instance which will be used to consume records
	 *                          from Kafka.
	 */
	private KafkaCache(KafkaReplayConsumer<K, V> replayConsumer) {
		this.replayConsumer = replayConsumer;
		this.replayConsumer.register(new Subscriber());
	}

	/**
	 * Creates a {@link KafkaCache} for the specified topic and a {@link GsonDeserializer} for the value part
	 * of the record.
	 *
	 * @param topicName The name of the topic.
	 * @param kafkaProps The properties for the {@code KafkaConsumer}.
	 * @param keyDeserializer The deserializer for the records' keys.
	 * @param valueType The type of the records' value.
	 * @param gson The Gson instance to use for deserializing values.
	 * @param pollDuration The poll duration timeout.
	 */
	private KafkaCache(
			String topicName,
			Properties kafkaProps,
			@NonNull Deserializer<K> keyDeserializer,
			TypeToken<V> valueType,
			Gson gson,
			Duration pollDuration) {
		this.replayConsumer =
				new KafkaReplayConsumer.Builder<>(topicName, kafkaProps, keyDeserializer, valueType)
					.with(gson)
					.with(pollDuration)
					.build();
		this.replayConsumer.register(new Subscriber());
	}

	private class Subscriber implements KafkaSubscriber<K, V> {
		@Override public void onNewRecord(K key, V value) {
			lock.writeLock().lock();
			try {
				cache.put(key, value);
			}
			finally {
				lock.writeLock().unlock();
			}
			if (isReplayDone()) {
				subscribers.forEach(s -> s.onNewRecord(key, value));
			}
		}

		@Override public void onReplayDone() {
			try {
				takeReadLock();
				signalTopicReplayed();
				logInitialStateInDebugMode();
			} finally {
				releaseReadLock();
			}
		}

		@Override public void onDeletedRecord(K key) {
			lock.writeLock().lock();
			try {
				cache.remove(key);
			}
			finally {
				lock.writeLock().unlock();
			}
			if (isReplayDone()) {
				subscribers.forEach(s -> s.onDeletedRecord(key));
			}
		}
	}

	public void setExceptionHandler(ExceptionHandler handler) {
		replayConsumer.setExceptionHandler(handler);
	}

	public void takeReadLock() {
		lock.readLock().lock();
	}

	public void releaseReadLock() {
		lock.readLock().unlock();
	}

	/**
	 * Registers a lambda which will be invoked for each streaming update record. For a record deletion, the second
	 * argument passed to the lambda will be {@code null}.
	 * @param onNewRecord The lambda which will be invoked for each streaming update record
	 */
	public void register(BiConsumer<K, V> onNewRecord) {
		register(KafkaSubscriber.create(onNewRecord));
	}

	/**
	 * Registers a pair of lambdas, one of which will be invoked for each streaming update record, depending on whether
	 * its value is {@code null} or not.
	 * @param onNewRecord The lambda which will be invoked for each streaming update record with value other than
	 *                    {@code null}
	 * @param onDeletedRecord The lambda which will be invoked for each streaming record deletion, i.e. records whose
	 *                        value is {@code null}
	 */
	public void register(BiConsumer<K, V> onNewRecord, Consumer<K> onDeletedRecord) {
		register(KafkaSubscriber.create(onNewRecord, onDeletedRecord));
	}

	/**
	 * Registers a subscriber for streaming update records.
	 * @param subscriber The subscriber which should receive streaming update records
	 */
	public void register(KafkaSubscriber<K, V> subscriber) {
		this.subscribers.add(subscriber);
	}

	public void awaitTopicReplayed() {
		try {
			replayDoneSignal.await();
		} catch (InterruptedException e) {
			log.error("Interrupted while waiting", e);
		}
	}

	public boolean isReplayDone() {
		return replayDoneSignal.getCount() == 0;
	}

	/**
	 * Get a cache copy. Thread safe.
	 * @return A copy of the cache.
	 */
	public Map<K, V> getCacheCopy() {
		if (!isReplayDone()) throw new IllegalStateException("Cache is not replayed yet.");

		takeReadLock();
		try {
			return new HashMap<>(cache);
		}
		finally {
			releaseReadLock();
		}
	}

	/**
	 * Returns the value to which the specified key is mapped,
	 * or {@code null} if this map contains no mapping for the key.
	 *
	 * Thread safe.
	 *
	 * @param key
	 * @return value to which the specified key is mapped
	 */
	public V get(K key){
		takeReadLock();
		try {
			return cache.get(key);
		} finally {
			releaseReadLock();
		}
	}

	@Override public void start() {
		replayConsumer.start();
	}

	/** Shuts down this {@code KafkaCache}. Once shut down, it cannot be restarted. */
	@Override public void shutdown() {
		replayConsumer.shutdown();
	}

	private void logInitialStateInDebugMode() {
		if (log.isDebugEnabled()) {
			val cache         = getCacheCopy();
			val stringBuilder = new StringBuilder("Initial records for topic ")
					.append(replayConsumer.getTopicName())
					.append("\n");
			for (val entry: cache.entrySet()) {
				stringBuilder
						.append("[key=").append(entry.getKey())
						.append(", ")
						.append("value=").append(entry.getValue())
						.append("]\n");
			}
			log.debug(stringBuilder.toString());
		}
	}

	private void signalTopicReplayed() {
		replayDoneSignal.countDown();
	}

	/**
	 * A builder class used to construct {@link KafkaCache} instances.
	 *
	 * @param <K> The type of the key.
	 * @param <V> The type of the value.
	 */
	public static class Builder<K, V> {

		private final KafkaReplayConsumer.Builder<K, V> kafkaReplayConsumerBuilder;

		/**
		 * Creates a {@link KafkaCache} builder for the specified topic using a preexisting {@link KafkaConsumer}
		 * instance.
		 *
		 * @param topicName The name of the topic.
		 * @param consumer The {@link KafkaConsumer} to cache records from.
		 */
		public Builder(String topicName, KafkaConsumer<K, V> consumer) {
			kafkaReplayConsumerBuilder = new KafkaReplayConsumer.Builder<>(topicName, consumer);
		}

		/**
		 * Creates a {@link KafkaCache} builder for the specified topic with a custom deserializer for the value part of
		 * the record.
		 *
		 * @param topicName The name of the topic.
		 * @param props The properties for the {@link KafkaConsumer}.
		 * @param keyDeserializer The deserializer for the records' keys.
		 * @param valueDeserializer The deserializer for the records' values.
		 */
		public Builder(
				String topicName,
				Properties props,
				Deserializer<K> keyDeserializer,
				Deserializer<V> valueDeserializer) {
			kafkaReplayConsumerBuilder = new KafkaReplayConsumer.Builder<>(
					topicName, props, keyDeserializer, valueDeserializer);
		}

		/**
		 * Creates a {@link KafkaCache} builder for the specified topic with a generic {@link GsonDeserializer} for the
		 * value part of the record. The {@link GsonDeserializer}'s {@link Gson} instance can be set using
		 * {@link #with(Gson)}.
		 *
		 * @param topicName The name of the topic.
		 * @param props The properties for the {@link KafkaConsumer}.
		 * @param keyDeserializer The deserializer for the records' keys.
		 * @param valueType The type of the records' value.
		 */
		public Builder(String topicName, Properties props, Deserializer<K> keyDeserializer, TypeToken<V> valueType) {
			kafkaReplayConsumerBuilder = new KafkaReplayConsumer.Builder<>(
					topicName, props, keyDeserializer, valueType);
		}

		/**
		 * Overrides the {@link Gson} instance used by the {@link GsonDeserializer} for the value part of the record.
		 * This method may only be invoked if the builder was created using the constructor
		 * {@link #Builder(String, Properties, Deserializer, TypeToken)}.
		 *
		 * @param gson The {@link Gson} instance to use for deserializing values.
		 * @return The same builder.
		 */
		public Builder<K, V> with(Gson gson) {
			kafkaReplayConsumerBuilder.with(gson);
			return this;
		}

		/**
		 * Overrides the poll duration for the {@link KafkaReplayConsumer}.
		 *
		 * @param pollDuration The custom poll duration.
		 * @return The same builder.
		 */
		public Builder<K, V> with(Duration pollDuration) {
			kafkaReplayConsumerBuilder.with(pollDuration);
			return this;
		}

		/**
		 * Creates a {@link KafkaCache} instance with the parameters configured in the builder.
		 *
		 * @return The created {@link KafkaCache} instance.
		 */
		public KafkaCache<K, V> build() {
			return new KafkaCache<>(kafkaReplayConsumerBuilder.build());
		}
	}
}
