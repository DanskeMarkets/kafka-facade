package dk.danskebank.markets.kafka.consumer;

import dk.danskebank.markets.kafka.consumer.replay.KafkaReplayConsumer;

import java.util.function.BiConsumer;

/**
 * Contract for Kafka consumer subscribers which need access to the consumed record's {@link RecordMetadata}.
 *
 * @param <K> The type of the key.
 * @param <V> The type of the value.
 */
public interface KafkaMetadataAwareSubscriber<K, V> {

	/**
	 * Invoked whenever a new record is produced.
	 *
	 * @param key      The key of the new record.
	 * @param value    The value of the new record.
	 * @param metadata The record's metadata.
	 */
	void onNewRecord(K key, V value, RecordMetadata metadata);

	/**
	 * Invoked whenever a record is deleted on Kafka.
	 *
	 * @param key      The key of the deleted record.
	 * @param metadata The record's metadata.
	 */
	void onDeletedRecord(K key, RecordMetadata metadata);

	/**
	 * Invoked when the last record is replayed and all future invocations of
	 * {@link #onNewRecord(Object, Object, RecordMetadata) onNewRecord(K, V, RecordMetaData)} and
	 * {@link #onDeletedRecord(Object, RecordMetadata) onDeletedRecord(K, RecordMetadata)} are streamed - only
	 * invoked for subscribers of type {@link KafkaReplayConsumer}.
	 */
	default void onReplayDone() { /* Do nothing. */ }

	/**
	 * Creates a {@link KafkaMetadataAwareSubscriber} which invokes the supplied lambda for each incoming record. For a
	 * record deletion, the second argument passed to the lambda will be {@code null}.
	 *
	 * @param onNewRecord The lambda which will be invoked for each incoming record.
	 * @param <K>         The record's key type.
	 * @param <V>         The record's value type.
	 * @return A {@link KafkaMetadataAwareSubscriber} instance wrapping the supplied lambda.
	 */
	static <K, V> KafkaMetadataAwareSubscriber<K, V> create(RecordWithMetadataHandler<K, V> onNewRecord) {
		return new KafkaMetadataAwareSubscriber<>() {

			@Override
			public void onNewRecord(K key, V value, RecordMetadata metadata) {
				onNewRecord.accept(key, value, metadata);
			}

			@Override
			public void onDeletedRecord(K key, RecordMetadata metadata) {
				onNewRecord.accept(key, null, metadata);
			}
		};
	}

	/**
	 * Creates a {@link KafkaMetadataAwareSubscriber} which invokes one of the supplied lambdas for each incoming
	 * record, depending on whether the record's value is {@code null} or not.
	 *
	 * @param onNewRecord     The lambda which will be invoked for each incoming record with value other than
	 *                        {@code null}.
	 * @param onDeletedRecord The lambda which will be invoked for each incoming record deletion, i.e. records whose
	 *                        value is {@code null}.
	 * @param <K>             The record's key type.
	 * @param <V>             The record's value type.
	 * @return A {@link KafkaMetadataAwareSubscriber} instance wrapping the supplied lambdas.
	 */
	static <K, V> KafkaMetadataAwareSubscriber<K, V> create(
			RecordWithMetadataHandler<K, V> onNewRecord,
			BiConsumer<K, RecordMetadata> onDeletedRecord) {
		return new KafkaMetadataAwareSubscriber<>() {

			@Override
			public void onNewRecord(K key, V value, RecordMetadata metadata) {
				onNewRecord.accept(key, value, metadata);
			}

			@Override
			public void onDeletedRecord(K key, RecordMetadata metadata) {
				onDeletedRecord.accept(key, metadata);
			}
		};
	}

}
