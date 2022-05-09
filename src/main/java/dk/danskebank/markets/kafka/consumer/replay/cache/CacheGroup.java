package dk.danskebank.markets.kafka.consumer.replay.cache;

import dk.danskebank.markets.kafka.consumer.ExceptionHandler;
import dk.danskebank.markets.lifecycle.Lifecycle;

import java.util.Set;

public class CacheGroup implements Lifecycle {
	private final Set<KafkaCache> caches;

	private CacheGroup(KafkaCache... caches) {
		this.caches = Set.of(caches);
	}

	public static CacheGroup of(KafkaCache... caches) {
		return new CacheGroup(caches);
	}

	@Override public void start() {
		caches.forEach(KafkaCache::start);
	}

	public void awaitReplay() {
		caches.forEach(KafkaCache::awaitTopicReplayed);
	}

	public void startAndAwaitReplay() {
		start();
		awaitReplay();
	}

	public void setExceptionHandler(ExceptionHandler handler) {
		caches.forEach(cache -> cache.setExceptionHandler(handler));
	}

	@Override public void shutdown() {
		caches.forEach(KafkaCache::shutdown);
	}

	public void takeReadLock() {
		caches.forEach(KafkaCache::takeReadLock);
	}

	public void releaseReadLock() {
		caches.forEach(KafkaCache::releaseReadLock);
	}
}
