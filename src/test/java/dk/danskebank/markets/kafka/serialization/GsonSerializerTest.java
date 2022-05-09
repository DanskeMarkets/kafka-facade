package dk.danskebank.markets.kafka.serialization;

import com.google.common.io.Resources;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import com.google.gson.stream.JsonReader;
import lombok.val;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

@DisplayName("A GsonSerializer with properly set up type adapters")
class GsonSerializerTest {

	private GsonSerializer<TestData> gsonSerializer;

	@BeforeEach
	void setup() {
		val gson = new GsonBuilder().create();
		gsonSerializer = new GsonSerializer<>(gson);
	}

	@Test @DisplayName("must serialize date and time values correctly")
	void mustSerializeDateAndTimeValues() throws IOException {
		val data = TestDataProvider.getData();

		val serializedBytes = gsonSerializer.serialize(null, data);
		assertNotNull(serializedBytes);

		val serializedString  = new String(serializedBytes, StandardCharsets.UTF_8);
		val serializedElement = JsonParser.parseString(serializedString);
		val expectedElement   = loadJsonElementFromResource("serialization-test-data.json");
		assertEquals(expectedElement, serializedElement);
	}

	@SuppressWarnings({"UnstableApiUsage", "SameParameterValue"})
	private static JsonElement loadJsonElementFromResource(String resourcePath) throws IOException {
		try (val stream = Resources.getResource(resourcePath).openStream();
			 val reader = new JsonReader(new InputStreamReader(stream, StandardCharsets.UTF_8))) {
			return JsonParser.parseReader(reader);
		}
	}
}
