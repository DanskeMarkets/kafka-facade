package dk.danskebank.markets.kafka.consumer;

import lombok.extern.log4j.Log4j2;

import static dk.danskebank.markets.kafka.internal.ThreadUtil.invokeSystemExitOnNewThread;

/**
 * This DefaultExceptionHandler logs the exception and exits the JVM.
 */
@Log4j2
public class DefaultExceptionHandler implements ExceptionHandler {
	static final String ERROR_FORMAT_STRING = "{}: {}";

	@Override public void handleStreamingException(String topic, String message, Throwable cause) {
		log.error(ERROR_FORMAT_STRING, topic, message, cause);
		invokeSystemExitOnNewThread(-1);
	}
}
