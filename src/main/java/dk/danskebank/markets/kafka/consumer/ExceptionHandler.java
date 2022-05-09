package dk.danskebank.markets.kafka.consumer;

import dk.danskebank.markets.kafka.consumer.replay.KafkaReplayConsumer;
import dk.danskebank.markets.kafka.consumer.replay.cache.KafkaCache;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.logging.log4j.LogManager;

import java.util.Map;

import static dk.danskebank.markets.kafka.internal.ThreadUtil.invokeSystemExitOnNewThread;

public interface ExceptionHandler {

	/**
	 * <p>Callback in case there's an exception occurring during the consumer being started.
	 * <p>Note, the callback must be allowed to return in order for the {@link KafkaStreamingConsumer},
	 * {@link KafkaReplayConsumer} or {@link KafkaCache} to properly shut down.
	 * <p>The default implementation logs the error and triggers a shutdown of the JVM on a new thread to avoid breaking
	 * compatibility with the previous version of the library.
	 *
	 * @param topic   The topic which should be consumed.
	 * @param message The message of what went wrong.
	 * @param cause   The underlying {@link Exception} or {@link Error}.
	 */
	default void handleStartupException(String topic, String message, Throwable cause) {
		LogManager.getLogger().error(DefaultExceptionHandler.ERROR_FORMAT_STRING, topic, message, cause);
		invokeSystemExitOnNewThread(-1);
	}

	/**
	 * <p>Callback in case there's an exception occurring while replaying.
	 * <p>This callback will only be triggered by {@link KafkaReplayConsumer} or {@link KafkaCache}.
	 * <p>Note, the callback must be allowed to return in order for the {@link KafkaReplayConsumer} or
	 * {@link KafkaCache} to properly shut down.
	 * <p>The default implementation logs the error and triggers a shutdown of the JVM on a new thread, which used to be
	 * the implementation in {@link DefaultExceptionHandler} before this method became optional to implement.
	 *
	 * @param topic   The topic being replayed.
	 * @param message The message of what went wrong.
	 * @param cause   The underlying {@link Exception} or {@link Error}.
	 */
	default void handleReplayException(String topic, String message, Throwable cause) {
		LogManager.getLogger().error(DefaultExceptionHandler.ERROR_FORMAT_STRING, topic, message, cause);
		invokeSystemExitOnNewThread(-1);
	}

	/**
	 * <p>Callback in case there's an exception occurring while streaming.
	 * <p>Note, the callback must be allowed to return in order for the {@link KafkaStreamingConsumer},
	 * {@link KafkaReplayConsumer} or {@link KafkaCache} to properly shut down.
	 *
	 * @param topic   The topic being streamed.
	 * @param message The message of what went wrong.
	 * @param cause   The underlying {@link Exception} or {@link Error}.
	 */
	void handleStreamingException(String topic, String message, Throwable cause);

	/**
	 * <p>Callback in case there's an exception occurring while committing offsets.
	 * <p>The default implementation logs and silences them to avoid breaking compatibility with the previous version of
	 * the library.
	 *
	 * @param offsets   The map of offsets which failed to be committed.
	 * @param exception The {@link Exception} which occurred.
	 */
	default void handleCommitException(Map<TopicPartition, OffsetAndMetadata> offsets, Exception exception) {
		LogManager.getLogger().warn("Commit of offsets {} failed.", offsets, exception);
	}

	/**
	 * <p>Callback in case there's an exception occurring during the consumer being shut down.
	 * <p>Note, the callback must be allowed to return in order for the {@link KafkaStreamingConsumer},
	 * {@link KafkaReplayConsumer} or {@link KafkaCache} to properly shut down.
	 * <p>The default implementation logs the error and triggers a shutdown of the JVM on a new thread to avoid breaking
	 * compatibility with the previous version of the library.
	 *
	 * @param topic   The topic which should be consumed.
	 * @param message The message of what went wrong.
	 * @param cause   The underlying {@link Exception} or {@link Error}.
	 */
	default void handleShutdownException(String topic, String message, Throwable cause) {
		LogManager.getLogger().error(DefaultExceptionHandler.ERROR_FORMAT_STRING, topic, message, cause);
		invokeSystemExitOnNewThread(-1);
	}
}
