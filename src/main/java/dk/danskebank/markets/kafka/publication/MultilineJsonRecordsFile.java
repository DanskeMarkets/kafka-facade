package dk.danskebank.markets.kafka.publication;

import lombok.val;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;

/**
 * <p>Utility for reading a file with a record spanning multiple lines. Typically, used for large JSON objects
 * where it needs to be formatted.
 *
 * <p> Example:
 * <pre>{@code
 * EUR/DKK, {
 *     rate=7.4732
 *     # Comments and blank lines (next line) are ok inside multiline JSON object.
 *
 * }
 * # Deleted key:
 * EUR/ARS,
 * }</pre>
 *
 * <p><b>NOTE: Every record must span multiple lines! I.e. the closing bracket "}" must be on a separate line with
 * no leading whitespace. Otherwise, the behaviour is undefined.</b>
 */
public class MultilineJsonRecordsFile {

	public static RecordsFile fromClasspath(String filename, String keyValueSeparator) throws URISyntaxException, IOException {
		val uri  = ClassLoader.getSystemResource(filename).toURI();
		val path = Paths.get(uri);
		return from(path, keyValueSeparator);
	}

	public static RecordsFile from(Path path, String keyValueSeparator) throws IOException, URISyntaxException {
		val pathOfTmpFile = prepareMultilineJsonFile(path, keyValueSeparator);
		return RecordsFile.from(pathOfTmpFile, keyValueSeparator);
	}

	/**
	 * Creates a {@code RecordsFile} with the default key/value separator {@link RecordsFile#DEFAULT_KEY_VALUE_SEPARATOR}.
	 * @param filename The name of the file to load on the classpath.
	 * @return The {@code RecordsFile} for the parsed file of records.
	 * @throws URISyntaxException If the filename is wrong.
	 */
	public static RecordsFile fromClasspath(String filename) throws URISyntaxException, IOException {
		val uri  = ClassLoader.getSystemResource(filename).toURI();
		val path = Paths.get(uri);
		return from(path);
	}

	/**
	 * Creates a {@code RecordsFile} with the default key/value separator {@link RecordsFile#DEFAULT_KEY_VALUE_SEPARATOR}.
	 * @param path The path to the file with records.
	 * @return The {@code RecordsFile} for the parsed file of records.
	 */
	public static RecordsFile from(Path path) throws IOException, URISyntaxException {
		return from(path, RecordsFile.DEFAULT_KEY_VALUE_SEPARATOR);
	}

	private static Path prepareMultilineJsonFile(Path file, String keyValueSeparator) throws IOException {
		// For readability, a JSON object can be split over many lines.
		// But the JSON object is part of a single record so must be put on a single line before publishing.
		val lines      = Files.readAllLines(file, Charset.defaultCharset());
		val records    = new ArrayList<String>();
		var nextRecord = new StringBuilder();
		for (val line: lines) {
			if (RecordsFile.hasData(line)) {
				nextRecord.append(line.strip());
				if (isEndOfRecord(line, keyValueSeparator)) {
					records.add(nextRecord.toString());
					nextRecord = new StringBuilder();
				}
			}
		}
		val tmpFile = Files.createTempFile("kafka-multiline-json","txt");
		Files.write(tmpFile, records);
		return tmpFile;
	}

	private static boolean isEndOfRecord(String line, String keyValueSeparator) {
		// Json object must be indented with whitespace until the last closing bracket '}'.
		return
			(// Case 1: Deletion of key (blank value)
				line.contains(keyValueSeparator) &&
				noLeadingWhiteSpace(line)        &&
				!line.contains("{")
			)
			||
			(// Case 2: JSON object is ended on this line.
				!line.contains(keyValueSeparator) && // Records start with a key and a separator for the value.
				!line.isBlank()                   && // Blank lines inside the object is ok.
				noLeadingWhiteSpace(line)            // The closing '}' has no leading whitespace.
			);
	}

	private static boolean noLeadingWhiteSpace(String line) {
		return line.stripLeading() == line;
	}
}
