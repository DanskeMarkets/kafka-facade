package dk.danskebank.markets.kafka.producer;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j2;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.logging.log4j.Level;

import java.util.Properties;

/**
 * Default implementation of a message publisher to Kafka. It will automatically log any error which occurs when
 * producing a Kafka record. This behavior can be changed by deriving the class and overriding the method
 * {@link #onSendFailure(Object, Object, RecordMetadata, Exception)}.
 *
 * @param <K> The type of the key.
 * @param <V> The type of the value.
 */
@RequiredArgsConstructor @Log4j2
public class KafkaMessagePublisher<K, V> implements MessagePublisher<K, V> {

    private final KafkaProducer<K, V> kafkaProducer;
    private final String topic;
    private final Level publicationLogLevel;

    /**
     * Creates a {@link KafkaMessagePublisher} which will produce records to the specified topic using the provided
     * {@link KafkaProducer}. Produced records will also be logged in INFO level log events.
     *
     * @param topic         The topic to which records will be produced.
     * @param kafkaProducer The {@link KafkaProducer} which will be used to produce records to Kafka.
     */
    public KafkaMessagePublisher(@NonNull String topic, @NonNull KafkaProducer<K, V> kafkaProducer) {
        this(topic, kafkaProducer, Level.INFO);
    }

    /**
     * Creates a {@link KafkaMessagePublisher} which will produce records to the specified topic using the provided
     * {@link KafkaProducer}. Produced records will also be logged with the specified log level.
     *
     * @param topic               The topic to which records will be produced.
     * @param kafkaProducer       The {@link KafkaProducer} which will be used to produce records to Kafka.
     * @param publicationLogLevel The level of the log event which will be logged when producing each record.
     *                            Setting this to {@link Level#OFF} will disable logging.
     */
    public KafkaMessagePublisher(
            @NonNull String topic,
            @NonNull KafkaProducer<K, V> kafkaProducer,
            @NonNull Level publicationLogLevel) {
        validateLogLevel(publicationLogLevel);

        this.topic               = topic;
        this.kafkaProducer       = kafkaProducer;
        this.publicationLogLevel = publicationLogLevel;
    }

    /**
     * Creates a {@link KafkaMessagePublisher} which will produce records to the specified topic using a
     * {@link KafkaProducer} configured with the provided properties and serializers. Produced records will also be
     * logged in INFO level log events.
     *
     * @param topic           The topic to which records will be produced.
     * @param props           The properties for the {@link KafkaProducer}.
     * @param keySerializer   The serializer for the records' keys.
     * @param valueSerializer The serializer for the records' values.
     */
    public KafkaMessagePublisher(
            @NonNull String topic,
            @NonNull Properties props,
            @NonNull Serializer<K> keySerializer,
            @NonNull Serializer<V> valueSerializer) {
        this(topic, new KafkaProducer<>(props, keySerializer, valueSerializer), Level.INFO);
    }

    /**
     * Creates a {@link KafkaMessagePublisher} which will produce records to the specified topic using a
     * {@link KafkaProducer} configured with the provided properties and serializers. Produced records will also be
     * logged with the specified log level.
     *
     * @param topic               The topic to which records will be produced.
     * @param props               The properties for the {@link KafkaProducer}.
     * @param keySerializer       The serializer for the records' keys.
     * @param valueSerializer     The serializer for the records' values.
     * @param publicationLogLevel The level of the log event which will be logged when producing each record.
     *                            Setting this to {@link Level#OFF} will disable logging.
     */
    public KafkaMessagePublisher(
            @NonNull String topic,
            @NonNull Properties props,
            @NonNull Serializer<K> keySerializer,
            @NonNull Serializer<V> valueSerializer,
            @NonNull Level publicationLogLevel) {
        this(topic, new KafkaProducer<>(props, keySerializer, valueSerializer), publicationLogLevel);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void publish(K key, V value) {
        var kafkaRecord = new ProducerRecord<>(topic, key, value);
        kafkaProducer.send(kafkaRecord, new SendResultCallback(key, value));
        if (publicationLogLevel != Level.OFF) {
            log.log(publicationLogLevel, "Published: {}, {}, {}", key, value, topic);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void start() {
        // Do Nothing
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void shutdown() {
        kafkaProducer.close();
    }

    /**
     * Invoked if an error occurs when producing a record to Kafka. The default implementation logs an error and
     * suppresses the exception. Override this method to extend or replace the default behavior.
     *
     * @param key       The key of the record.
     * @param value     The value of the record.
     * @param metadata  The metadata associated with the record.
     * @param exception The exception which was thrown.
     */
    protected void onSendFailure(K key, V value, RecordMetadata metadata, Exception exception) {
        log.error("Exception when sending key: {}, value: {} to topic: {}, due to: {}",
                key, value, topic, exception);
    }

    private static void validateLogLevel(Level logLevel) {
        if (!logLevel.isInRange(Level.OFF, Level.TRACE)) {
            throw new IllegalArgumentException("Invalid log level: " + logLevel);
        }
    }

    @RequiredArgsConstructor
    public class SendResultCallback implements Callback {

        private final K key;
        private final V value;

        @Override public void onCompletion(RecordMetadata metadata, Exception exception) {
            if (exception == null) return;
            KafkaMessagePublisher.this.onSendFailure(key, value, metadata, exception);
        }
    }
}
