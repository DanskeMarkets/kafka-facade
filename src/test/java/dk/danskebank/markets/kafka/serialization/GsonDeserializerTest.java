package dk.danskebank.markets.kafka.serialization;

import com.google.common.io.Resources;
import com.google.gson.GsonBuilder;
import lombok.val;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

@DisplayName("A GsonDeserializer with properly set up type adapters")
class GsonDeserializerTest {

	private GsonDeserializer<TestData> gsonDeserializer;

	@BeforeEach
	void setup() {
		val gson = new GsonBuilder().create();
		gsonDeserializer = new GsonDeserializer<>(TestData.class, gson);
	}


	@Test
	@DisplayName("must deserialize date and time values correctly")
	void mustDeserializeDateAndTimeValues() throws IOException {
		val serializedBytes = loadBytesFromResource("serialization-test-data.json");

		val deserializedData = gsonDeserializer.deserialize(null, serializedBytes);
		assertNotNull(deserializedData);

		val expectedData = TestDataProvider.getData();
		assertEquals(expectedData, deserializedData);
	}

	@SuppressWarnings({"UnstableApiUsage", "SameParameterValue"})
	private static byte[] loadBytesFromResource(String resourcePath) throws IOException {
		try (val stream = Resources.getResource(resourcePath).openStream()) {
			return stream.readAllBytes();
		}

	}
}
