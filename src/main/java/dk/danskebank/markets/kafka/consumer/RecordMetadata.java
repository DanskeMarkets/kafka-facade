package dk.danskebank.markets.kafka.consumer;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Data;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.record.TimestampType;

/**
 * Kafka record metadata.
 */
@Data @AllArgsConstructor(access = AccessLevel.PRIVATE)
public class RecordMetadata {

	/**
	 * The topic on which the record was produced.
	 */
	private final String topic;

	/**
	 * The topic partition id on which the record was produced.
	 */
	private final int partition;

	/**
	 * The unique offset of the record.
	 */
	private final long offset;

	/**
	 * The record's timestamp. Check {@link RecordMetadata#timestampType} to determine the type of the timestamp.
	 */
	private final long timestamp;

	/**
	 * The record timestamp's type:
	 * <ul>
	 *     <li>{@link TimestampType#CREATE_TIME}: the timestamp was issued by the producer;</li>
	 *     <li>{@link TimestampType#LOG_APPEND_TIME}: the timestamp was issued by the broker when appending the record
	 *     to the partition;</li>
	 *     <li>{@link TimestampType#NO_TIMESTAMP_TYPE}: the record's timestamp type is unknown.</li>
	 * </ul>
	 */
	private final TimestampType timestampType;

	/**
	 * Initializes a {@link RecordMetadata} value based on the provided {@link ConsumerRecord}.
	 *
	 * @param consumerRecord The original {@link ConsumerRecord} object.
	 * @return The corresponding {@link RecordMetadata} object.
	 */
	static RecordMetadata from(ConsumerRecord<?, ?> consumerRecord) {
		return new RecordMetadata(
				consumerRecord.topic(),
				consumerRecord.partition(),
				consumerRecord.offset(),
				consumerRecord.timestamp(),
				consumerRecord.timestampType());
	}
}
