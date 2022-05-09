package dk.danskebank.markets.kafka.publication;

import com.github.charithe.kafka.KafkaHelper;
import com.github.charithe.kafka.KafkaJunitExtension;
import com.github.charithe.kafka.KafkaJunitExtensionConfig;
import com.github.charithe.kafka.StartupMode;
import lombok.val;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.IOException;
import java.net.URISyntaxException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.function.BiConsumer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

@ExtendWith(KafkaJunitExtension.class)
@KafkaJunitExtensionConfig(startupMode = StartupMode.WAIT_FOR_STARTUP)
@DisplayName("A RecordsFile")
class RecordsFileTest {

	private static final String TOPIC = "test";

	private RecordsFile recordsFile;
	private BiConsumer<String, String> biConsumer;

	@SuppressWarnings("unchecked")
	@BeforeEach void setup() throws URISyntaxException {
		recordsFile = RecordsFile.fromClasspath("example.txt");
		biConsumer = (BiConsumer<String, String>) mock(BiConsumer.class);
	}

	@Test @DisplayName("should stream the key/value pairs in the .txt file.")
	void testStreaming() throws IOException {
		recordsFile.streamRecords(biConsumer);

		verify(biConsumer).accept("EUR/USD", "{rate=1.1232}");
		verify(biConsumer).accept("EUR/DKK", "{rate=7.4732}");
		verify(biConsumer).accept("EUR/ARS", null);
	}

	@Test @DisplayName("should stream the key/value pairs in the .txt file with a different separator.")
	void testStreamingDifferentSepartor() throws IOException, URISyntaxException {
		recordsFile = RecordsFile.fromClasspath("example-different-separator.txt", ";");
		recordsFile.streamRecords(biConsumer);

		verify(biConsumer).accept("EUR/USD", "{rate=1.1232}");
		verify(biConsumer).accept("EUR/DKK", "{rate=7.4732}");
		verify(biConsumer).accept("EUR/ARS", null);
	}

	@Test @DisplayName("should publish the key/value pairs in the .txt file.")
	void testPublishing(KafkaHelper kafkaHelper) throws IOException {
		val brokerAddress = "localhost:" + kafkaHelper.kafkaPort();
		var props = new Properties();
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,      brokerAddress);
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,   StringSerializer.class.getName());
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

		recordsFile.publishTo(TOPIC, props);

		val consumer = kafkaHelper.createStringConsumer(props);
		consumer.assign(List.of(new TopicPartition(TOPIC, 0)));
		val records = consumer.poll(Duration.ofMillis(10_000));

		assertEquals(3, records.count());

		List<ConsumerRecord<String, String>> recordList = new ArrayList<>();
		records.iterator().forEachRemaining(recordList::add);

		val records1 = recordList.get(0);
		assertEquals("EUR/USD",       records1.key());
		assertEquals("{rate=1.1232}", records1.value());

		val records2 = recordList.get(1);
		assertEquals("EUR/DKK",       records2.key());
		assertEquals("{rate=7.4732}", records2.value());

		val records3 = recordList.get(2);
		assertEquals("EUR/ARS",       records3.key());
		assertNull(records3.value());
	}
}
