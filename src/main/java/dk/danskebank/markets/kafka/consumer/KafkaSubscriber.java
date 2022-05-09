package dk.danskebank.markets.kafka.consumer;

import dk.danskebank.markets.kafka.consumer.replay.KafkaReplayConsumer;

import java.util.function.BiConsumer;
import java.util.function.Consumer;

/**
 * Contract for Kafka consumer subscribers.
 *
 * @param <K> The type of the key.
 * @param <V> The type of the value.
 */
public interface KafkaSubscriber<K, V> {

	/**
	 * Invoked whenever a new record is produced.
	 *
	 * @param key   The key of the new record.
	 * @param value The value of the new record.
	 */
	void onNewRecord(K key, V value);

	/**
	 * Invoked whenever a record is deleted on Kafka.
	 *
	 * @param key The key of the deleted record.
	 */
	void onDeletedRecord(K key);

	/**
	 * Invoked when the last record is replayed and all future invocations of
	 * {@link #onNewRecord(K, V)} and {@link #onDeletedRecord(K)} are streamed - only invoked
	 * for subscribers of type {@link KafkaReplayConsumer}.
	 */
	default void onReplayDone() { /* Do nothing. */ }

	/**
	 * Creates a {@link KafkaSubscriber} which invokes the supplied lambda for each incoming record. For a record
	 * deletion, the second argument passed to the lambda will be {@code null}.
	 *
	 * @param onNewRecord The lambda which will be invoked for each incoming record.
	 * @param <K>         The record's key type.
	 * @param <V>         The record's value type.
	 * @return A {@link KafkaSubscriber} instance wrapping the supplied lambda.
	 */
	static <K, V> KafkaSubscriber<K, V> create(BiConsumer<K, V> onNewRecord) {
		return new KafkaSubscriber<>() {

			@Override
			public void onNewRecord(K key, V value) {
				onNewRecord.accept(key, value);
			}

			@Override
			public void onDeletedRecord(K key) {
				onNewRecord.accept(key, null);
			}
		};
	}

	/**
	 * Creates a {@link KafkaSubscriber} which invokes one of the supplied lambdas for each incoming record, depending
	 * on whether the record's value is {@code null} or not.
	 *
	 * @param onNewRecord     The lambda which will be invoked for each incoming record with value other than
	 *                        {@code null}.
	 * @param onDeletedRecord The lambda which will be invoked for each incoming record deletion, i.e. records whose
	 *                        value is {@code null}.
	 * @param <K>             The record's key type.
	 * @param <V>             The record's value type.
	 * @return A {@link KafkaSubscriber} instance wrapping the supplied lambdas.
	 */
	static <K, V> KafkaSubscriber<K, V> create(BiConsumer<K, V> onNewRecord, Consumer<K> onDeletedRecord) {
		return new KafkaSubscriber<>() {

			@Override
			public void onNewRecord(K key, V value) {
				onNewRecord.accept(key, value);
			}

			@Override
			public void onDeletedRecord(K key) {
				onDeletedRecord.accept(key);
			}
		};
	}
}
