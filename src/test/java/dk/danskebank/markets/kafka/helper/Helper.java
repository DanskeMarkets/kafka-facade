package dk.danskebank.markets.kafka.helper;

import lombok.val;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Defines helper methods for creating maps that preserve insertion order and allow values to be null.
 * ({@link Map#of()} does not).
 */
public class Helper {

	public static <K, V> Map<K, V> mapOf(K k1, V v1, K k2, V v2) {
		val map = new LinkedHashMap<K, V>();
		map.put(k1, v1);
		map.put(k2, v2);
		return map;
	}

	public static <K, V> Map<K, V> mapOf(K k1, V v1, K k2, V v2, K k3, V v3) {
		val map = new LinkedHashMap<K, V>();
		map.put(k1, v1);
		map.put(k2, v2);
		map.put(k3, v3);
		return map;
	}
}
