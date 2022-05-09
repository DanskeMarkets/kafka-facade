package dk.danskebank.markets.kafka.consumer;

/**
 * A functional interface for functions which should be invoked to process incoming Kafka records along with their
 * metadata.
 *
 * @param <K> The type of the key.
 * @param <V> The type of the value.
 */
public interface RecordWithMetadataHandler<K, V> {
	void accept(K key, V value, RecordMetadata metadata);
}
