package dk.danskebank.markets.kafka.monitoring;

import dk.danskebank.markets.lifecycle.Lifecycle;
import lombok.extern.log4j.Log4j2;

import java.util.Properties;

import static dk.danskebank.markets.kafka.internal.ThreadUtil.invokeSystemExitOnNewThread;

/**
 * Default KafkaClusterMonitor which will listen to kafka up and down notifications and
 * shut down the application in case the Kafka cluster is down.
 */
@Log4j2
public class ExitOnKafkaDownMonitor implements Lifecycle {
	public final static int KAFKA_DOWN_EXIT_CODE = 99999;

	private final KafkaClusterMonitoring kafkaMonitor;

	public ExitOnKafkaDownMonitor(Properties kafkaProperties) {
		this.kafkaMonitor = new KafkaClusterMonitoring(kafkaProperties);
		this.kafkaMonitor.register(new KafkaClusterStateListener() {
			@Override public void isUp() {
				log.info("Kafka cluster is up and running.");
			}

			@Override public void isDown() {
				log.fatal("Kafka cluster is not running. Shutting down.");
				invokeSystemExitOnNewThread(KAFKA_DOWN_EXIT_CODE);
			}
		});
	}

	@Override public void start() {
		this.kafkaMonitor.start();
	}

	@Override public void shutdown() {
		this.kafkaMonitor.shutdown();
	}

}
