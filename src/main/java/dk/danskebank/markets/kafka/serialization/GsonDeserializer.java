package dk.danskebank.markets.kafka.serialization;

import com.google.gson.Gson;
import com.google.gson.JsonParseException;
import com.google.gson.reflect.TypeToken;
import lombok.extern.log4j.Log4j2;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

import java.lang.reflect.Type;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

@Log4j2
public class GsonDeserializer<T> implements Deserializer<T> {
	private static final Charset CHARSET = StandardCharsets.UTF_8;

	private final Type type;
	private final Gson gson;

	public GsonDeserializer(TypeToken<T> typeToken, Gson gson) {
		this.type = typeToken.getType();
		this.gson = gson;
	}

	public GsonDeserializer(TypeToken<T> typeToken) {
		this(typeToken, new Gson());
	}

	public GsonDeserializer(Class<T> type, Gson gson) {
		this.type = type;
		this.gson = gson;
	}

	public GsonDeserializer(Class<T> type) {
		this(type, new Gson());
	}

	@Override public T deserialize(String topic, byte[] data) {
		if (data == null) return null;
		final String str = new String(data, CHARSET);
		try {
			final T deserializedData = gson.fromJson(str, type);
			if (log.isTraceEnabled()) {
				log.trace("Deserialized data '{}' on topic '{}': {}", str, topic, deserializedData);
			}
			return deserializedData;
		} catch (JsonParseException e) {
			throw new SerializationException("Error deserializing byte[] for topic '"+topic+"' Probably wrongly formatted data was published on Kafka.", e);
		}
	}
}
