package dk.danskebank.markets.kafka.producer;

import dk.danskebank.markets.lifecycle.Lifecycle;

/**
 * A contract for publishers of key/value pair messages. Such publishers are assumed to implement {@link Lifecycle}.
 *
 * @param <K> The type of the key.
 * @param <V> The type of the value.
 */
public interface MessagePublisher<K, V> extends Lifecycle {
    /**
     * Publish a message with the provided key and value.
     *
     * @param key   The key of the message to be published.
     * @param value The value of the message to be published.
     */
    void publish(K key, V value);
}
