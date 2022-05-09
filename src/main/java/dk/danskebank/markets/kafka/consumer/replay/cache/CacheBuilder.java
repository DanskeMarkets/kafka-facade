package dk.danskebank.markets.kafka.consumer.replay.cache;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.reflect.TypeToken;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Properties;
import java.util.function.Function;

import static java.util.Objects.requireNonNull;

public class CacheBuilder {
	private final Gson gson;
	private final Properties kafkaProperties;

	public CacheBuilder(Properties kafkaProps, Gson gson) {
		this.kafkaProperties = requireNonNull(kafkaProps);
		this.gson            = requireNonNull(gson);
	}

	public static class Builder {
		private final GsonBuilder gsonBuilder = new GsonBuilder();
		private final Properties kafkaProps;

		public Builder(Properties kafkaProps) {
			this.kafkaProps = kafkaProps;
		}

		public <E> Builder register(Class<E> clazz, Function<JsonElement, ? extends E> inflator) {
			gsonBuilder.registerTypeAdapter(clazz, (JsonDeserializer<E>) (json, typeOfT, context) -> inflator.apply(json));
			return this;
		}

		public CacheBuilder build() {
			return new CacheBuilder(kafkaProps, gsonBuilder.create());
		}
	}

	public <K, V> KafkaCache<K, V> createCache(String topic, Deserializer<K> keyDeserializer, TypeToken<V> valueType) {
		return new KafkaCache.Builder<>(topic, kafkaProperties, keyDeserializer, valueType).with(gson).build();
	}
}
