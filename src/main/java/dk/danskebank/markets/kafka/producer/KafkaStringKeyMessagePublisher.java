package dk.danskebank.markets.kafka.producer;

import lombok.NonNull;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.logging.log4j.Level;

import java.util.Properties;

/**
 * A specialized version of {@link KafkaMessagePublisher} which produces Kafka records with {@link String} keys.
 *
 * @param <V> The type of the value.
 */
public class KafkaStringKeyMessagePublisher<V> extends KafkaMessagePublisher<String, V> {

	/**
	 * Creates a {@link KafkaStringKeyMessagePublisher} which will produce records to the specified topic using the
	 * provided {@link KafkaProducer}. Produced records will also be logged in INFO level log events.
	 *
	 * @param topic         The topic to which records will be produced.
	 * @param kafkaProducer The {@link KafkaProducer} which will be used to produce records to Kafka.
	 */
	public KafkaStringKeyMessagePublisher(@NonNull String topic, @NonNull KafkaProducer<String, V> kafkaProducer) {
		super(topic, kafkaProducer, Level.INFO);
	}

	/**
	 * Creates a {@link KafkaStringKeyMessagePublisher} which will produce records to the specified topic using the
	 * provided {@link KafkaProducer}. Produced records will also be logged with the specified log level.
	 *
	 * @param topic               The topic to which records will be produced.
	 * @param kafkaProducer       The {@link KafkaProducer} which will be used to produce records to Kafka.
	 * @param publicationLogLevel The level of the log event which will be logged when producing each record.
	 *                            Setting this to {@link Level#OFF} will disable logging.
	 */
	public KafkaStringKeyMessagePublisher(
			@NonNull String topic,
			@NonNull KafkaProducer<String, V> kafkaProducer,
			@NonNull Level publicationLogLevel) {
		super(topic, kafkaProducer, publicationLogLevel);
	}

	/**
	 * Creates a {@link KafkaStringKeyMessagePublisher} which will produce records to the specified topic using a
	 * {@link KafkaProducer} configured with the provided properties and value serializer. Produced records will also be
	 * logged in INFO level log events.
	 *
	 * @param topic           The topic to which records will be produced.
	 * @param props           The properties for the {@link KafkaProducer}.
	 * @param valueSerializer The serializer for the records' values.
	 */
	public KafkaStringKeyMessagePublisher(
			@NonNull String topic,
			@NonNull Properties props,
			@NonNull Serializer<V> valueSerializer) {
		super(topic, props, new StringSerializer(), valueSerializer, Level.INFO);
	}

	/**
	 * Creates a {@link KafkaStringKeyMessagePublisher} which will produce records to the specified topic using a
	 * {@link KafkaProducer} configured with the provided properties and value serializer. Produced records will also be
	 * logged with the specified log level.
	 *
	 * @param topic               The topic to which records will be produced.
	 * @param props               The properties for the {@link KafkaProducer}.
	 * @param valueSerializer     The serializer for the records' values.
	 * @param publicationLogLevel The level of the log event which will be logged when producing each record.
	 *                            Setting this to {@link Level#OFF} will disable logging.
	 */
	public KafkaStringKeyMessagePublisher(
			@NonNull String topic,
			@NonNull Properties props,
			@NonNull Serializer<V> valueSerializer,
			@NonNull Level publicationLogLevel) {
		super(topic, props, new StringSerializer(), valueSerializer, publicationLogLevel);
	}
}
