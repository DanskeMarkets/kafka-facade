package dk.danskebank.markets.kafka.internal;

public class ThreadUtil {
	public static void invokeSystemExitOnNewThread(int status) {
		new Thread(() -> System.exit(status)).start();
	}
}
