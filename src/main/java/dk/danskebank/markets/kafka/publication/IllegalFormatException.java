package dk.danskebank.markets.kafka.publication;

public class IllegalFormatException extends RuntimeException {
	public IllegalFormatException(String message) {
		super(message);
	}
}
