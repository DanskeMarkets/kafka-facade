package dk.danskebank.markets.kafka.consumer;

import lombok.AccessLevel;
import lombok.Data;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.time.Instant;

/**
 * Consumer start point configuration for a {@link KafkaStreamingConsumer}.
 */
@Data @RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public class ConsumerStartPoint {

	/**
	 * The consumer start point's type.
	 */
	private final Type type;

	/**
	 * The reference timestamp for a {@link Type#REFERENCE_TIMESTAMP} consumer start point. This will be {@code null}
	 * for other consumer start point types.
	 */
	private final Instant timestamp;

	/**
	 * Indicates that the consumer should start from the default start point controlled by the Kafka broker. The
	 * starting offset will be determined by the broker based on the configured {@link ConsumerConfig#GROUP_ID_CONFIG}
	 * and {@link ConsumerConfig#AUTO_OFFSET_RESET_CONFIG} values.
	 */
	public static final ConsumerStartPoint KAFKA_DEFAULT   = new ConsumerStartPoint(Type.KAFKA_DEFAULT, null);

	/**
	 * Indicates that the consumer should start from the earliest available offset on each partition of the topic.
	 */
	public static final ConsumerStartPoint EARLIEST_OFFSET = new ConsumerStartPoint(Type.EARLIEST_OFFSET, null);

	/**
	 * Indicates that the consumer should start from the offset immediately after the latest available offset on each
	 * partition of the topic. Effectively, this means that only records produced after the consumer starts up will be
	 * consumed.
	 */
	public static final ConsumerStartPoint LATEST_OFFSET   = new ConsumerStartPoint(Type.LATEST_OFFSET, null);

	/**
	 * Creates a consumer starting point indicating that the consumer should start from the earliest offset with a
	 * timestamp newer than the specified reference timestamp.
	 * @param timestamp that the consumer start point should be at.
	 * @return The consumer start point.
	 */
	public static ConsumerStartPoint fromReferenceTimestamp(@NonNull Instant timestamp) {
		return new ConsumerStartPoint(Type.REFERENCE_TIMESTAMP, timestamp);
	}

	/**
	 * Consumer start point type.
	 */
	public enum Type {

		/**
		 * The consumer should start from the default start point controlled by the Kafka broker. The starting offset
		 * will be determined by the broker based on the configured {@link ConsumerConfig#GROUP_ID_CONFIG} and
		 * {@link ConsumerConfig#AUTO_OFFSET_RESET_CONFIG} values.
		 */
		KAFKA_DEFAULT,

		/**
		 * The consumer should start from the earliest available offset on each partition of the topic.
		 */
		EARLIEST_OFFSET,

		/**
		 * The consumer should start from the offset immediately after the latest available offset on each partition of
		 * the topic.
		 */
		LATEST_OFFSET,

		/**
		 * The consumer should start from the earliest offset with a timestamp newer than a specified reference
		 * timestamp.
		 */
		REFERENCE_TIMESTAMP
	}
}
