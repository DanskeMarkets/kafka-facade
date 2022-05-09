package dk.danskebank.markets.kafka.publication;

import lombok.val;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Properties;
import java.util.function.BiConsumer;
import java.util.stream.Stream;

import static java.util.Objects.requireNonNull;

public class RecordsFile {
	public static final String DEFAULT_KEY_VALUE_SEPARATOR = ",";
	private final Path path;
	private final String keyValueSeparator;

	public static RecordsFile fromClasspath(String filename, String keyValueSeparator) throws URISyntaxException {
		val uri  = ClassLoader.getSystemResource(filename).toURI();
		val path = Paths.get(uri);
		return from(path, keyValueSeparator);
	}

	public static RecordsFile from(Path path, String keyValueSeparator) {
		return new RecordsFile(path, keyValueSeparator);
	}

	/**
	 * Creates a {@code RecordsFile} with the default key/value separator {@link #DEFAULT_KEY_VALUE_SEPARATOR}.
	 * @param filename The name of the file to load on the classpath.
	 * @return The {@code RecordsFile} for the parsed file of records.
	 * @throws URISyntaxException If the filename is wrong.
	 */
	public static RecordsFile fromClasspath(String filename) throws URISyntaxException {
		val uri  = ClassLoader.getSystemResource(filename).toURI();
		val path = Paths.get(uri);
		return from(path);
	}

	/**
	 * Creates a {@code RecordsFile} with the default key/value separator {@link #DEFAULT_KEY_VALUE_SEPARATOR}.
	 * @param path The path to the file with records.
	 * @return The {@code RecordsFile} for the parsed file of records.
	 */
	public static RecordsFile from(Path path) {
		return new RecordsFile(path, DEFAULT_KEY_VALUE_SEPARATOR);
	}

	private RecordsFile(Path path, String keyValueSeparator) {
		this.path              = requireNonNull(path);
		this.keyValueSeparator = requireNonNull(keyValueSeparator);
	}

	public void publishTo(String topic, Properties kafkaProperties) throws IOException {
		try (val producer = new KafkaProducer<String, String>(kafkaProperties)) {
			publishTo(topic, producer);
		}
	}

	public void streamRecords(BiConsumer<String, String> consumer) throws IOException {
		lines().forEach(line -> {
			if (hasData(line)) {
				val splitAt = line.indexOf(keyValueSeparator);
				if (splitAt == -1) {
					throw new IllegalFormatException("Wrong format in "+path+" in line:\n"+line);
				}
				// To allow keys starting with escaped #, replace "\#" with "#" before publishing.
				val key        = line.substring(0, splitAt).trim().replace("\\#", "#");
				val value      = line.substring(splitAt + 1).trim();
				// Blank values effectively means deleting the record which is done by publishing a null value in Kafka.
				val kafkaValue = value.isBlank() ? null : value;

				consumer.accept(key, kafkaValue);
			}
		});
	}

	private void publishTo(String topic, KafkaProducer<String, String> producer) throws IOException {
		streamRecords((key, value) -> {
			producer.send(new ProducerRecord<>(topic, key, value));
		});
	}

	public static boolean hasData(String line) {
		return !(line.isBlank() || isComment(line));
	}

	private static boolean isComment(String line) {
		return line.trim().startsWith("#");
	}

	private Stream<String> lines() throws IOException {
		return Files.lines(path);
	}
}
