package dk.danskebank.markets.kafka.monitoring;

import dk.danskebank.markets.lifecycle.Lifecycle;
import lombok.extern.log4j.Log4j2;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DescribeClusterOptions;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.util.Objects.requireNonNull;

@Log4j2
public class KafkaClusterMonitoring implements Lifecycle {
	/** Options used for the admin call to check liveliness of the Kafka Cluster.*/
	private final static DescribeClusterOptions OPTIONS        = new DescribeClusterOptions();

	private enum State {UP, DOWN}

	private static final int DEFAULT_KAFKA_RESPONSE_TIMEOUT_MS = 5_000;
	private static final int DEFAULT_TEST_INTERVAL_MS          = 5_000;

	private final AtomicBoolean shouldRun                      = new AtomicBoolean(true);
	private final List<KafkaClusterStateListener> listeners    = new CopyOnWriteArrayList<>();

	private final Thread thread                                = new Thread(this::run);

	private final AdminClient adminClient;

	private final int testIntervalMillis;
	private final int kafkaResponseTimeoutMs;

	private State state = null;

	/**
	 * Create an instance with {@link #DEFAULT_TEST_INTERVAL_MS} and {@link #DEFAULT_KAFKA_RESPONSE_TIMEOUT_MS}.
	 * @param properties The properties with the {@code bootstrap.servers}.
	 */
	public KafkaClusterMonitoring(Properties properties) {
		requireNonNull(properties);

		this.adminClient            = AdminClient.create(properties);
		this.testIntervalMillis     = DEFAULT_TEST_INTERVAL_MS;
		this.kafkaResponseTimeoutMs = DEFAULT_KAFKA_RESPONSE_TIMEOUT_MS;
	}

	/**
	 * @param properties The properties with the {@code bootstrap.servers}.
	 * @param testIntervalMillis The interval between checks of the cluster's status.
	 * @param kafkaResponseTimeoutMs The time to wait for a response from the Kafka cluster.
	 */
	public KafkaClusterMonitoring(Properties properties, int testIntervalMillis, int kafkaResponseTimeoutMs) {
		requireNonNull(properties);
		if (testIntervalMillis < 0)     throw new IllegalArgumentException("testIntervalMillis must be > 0.");
		if (kafkaResponseTimeoutMs < 0) throw new IllegalArgumentException("kafkaResponseTimeoutMs must be > 0.");

		this.adminClient            = AdminClient.create(properties);
		this.testIntervalMillis     = testIntervalMillis;
		this.kafkaResponseTimeoutMs = kafkaResponseTimeoutMs;
	}

	/** Starts monitoring the Kafka cluster. */
	@Override public void start() {
		thread.start();
	}

	/**
	 * Register a listener.
	 * @param listener The listener invoked when the cluster comes up or goes down.
	 */
	public void register(KafkaClusterStateListener listener) {
		this.listeners.add(listener);
	}

	/** Shuts down this monitoring instance. */
	@Override public void shutdown() {
		shouldRun.set(false);
		thread.interrupt();
		try {
			thread.join(1_000);
		} catch (InterruptedException e) {
			// Do nothing.
		}
	}

	private void run() {
		while (shouldRun.get()) {
			try {
				adminClient.describeCluster(OPTIONS).clusterId().get(kafkaResponseTimeoutMs, TimeUnit.MILLISECONDS);
				setState(State.UP);
				Thread.sleep(testIntervalMillis);
			}
			catch (ExecutionException | TimeoutException ex) {
				setState(State.DOWN);
			} catch (InterruptedException ex) {
				// Do nothing.
			}
			catch (Throwable throwable) {
				log.error("Unexpected throwable", throwable);
			}
		}
	}

	private void setState(State newState) {
		if (state == newState) return;
		state = newState;
		switch (state) {
			case UP:   listeners.forEach(KafkaClusterStateListener::isUp);   break;
			case DOWN: listeners.forEach(KafkaClusterStateListener::isDown); break;
			default: throw new IllegalStateException("Unknown state: "+state);
		}
	}
}
