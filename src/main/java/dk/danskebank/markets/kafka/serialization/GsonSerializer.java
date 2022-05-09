package dk.danskebank.markets.kafka.serialization;

import com.google.gson.Gson;
import lombok.extern.log4j.Log4j2;
import org.apache.kafka.common.serialization.Serializer;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

@Log4j2
public class GsonSerializer<T> implements Serializer<T> {
	private static final Charset CHARSET = StandardCharsets.UTF_8;
	private final Gson gson;

	public GsonSerializer(Gson gson) {
		this.gson = gson;
	}

	public GsonSerializer() {
		this(new Gson());
	}

	@Override public byte[] serialize(String topic, T data) {
		if(data == null) return null;

		final String str = gson.toJson(data);
		if (log.isTraceEnabled()) {
			log.trace("Serialized data '{}' on topic '{}': {}", data, topic, str);
		}
		return str.getBytes(CHARSET);
	}
}
