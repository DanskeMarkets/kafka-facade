package dk.danskebank.markets.kafka.monitoring;

public interface KafkaClusterStateListener {
	/** Invoked when the cluster is up (i.e. at least one broker is up). */
	void isUp();

	/** Invoked when the cluster is detected to be down (i.e. no brokers are up). */
	void isDown();
}
