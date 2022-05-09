package dk.danskebank.markets.kafka.consumer;


import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import dk.danskebank.markets.kafka.serialization.GsonDeserializer;
import dk.danskebank.markets.lifecycle.Lifecycle;
import lombok.Data;
import lombok.Getter;
import lombok.NonNull;
import lombok.extern.log4j.Log4j2;
import lombok.val;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.Deserializer;

import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNullElseGet;
import static java.util.stream.Collectors.toMap;

@Log4j2
public class KafkaStreamingConsumer<K, V> implements Lifecycle {

    private static final long SHUTDOWN_TIMEOUT_MS = 1000;

    @Getter
    protected final String topicName;

    protected final KafkaConsumer<K, V> consumer;

    protected final Duration pollDuration;

    protected final ConsumerStartPoint startPoint;

    protected final List<KafkaSubscriber<K, V>> subscribers                           = new CopyOnWriteArrayList<>();
    protected final List<KafkaMetadataAwareSubscriber<K, V>> metadataAwareSubscribers = new CopyOnWriteArrayList<>();

    protected final AtomicBoolean running                                             = new AtomicBoolean(true);

    protected final AtomicReference<ExceptionHandler> exceptionHandlerReference =
            new AtomicReference<>(new DefaultExceptionHandler());

    protected final Queue<CommitRequest> pendingCommitRequests                        = new ConcurrentLinkedQueue<>();

    private final Thread readerThread;

    private boolean hasMetadataAwareSubscribers = false;

    protected KafkaStreamingConsumer(
            @NonNull String topicName,
            @NonNull KafkaConsumer<K, V> consumer,
            @NonNull Duration pollDuration,
            @NonNull ConsumerStartPoint startPoint) {
        this.topicName    = topicName;
        this.consumer     = consumer;
        this.pollDuration = pollDuration;
        this.startPoint   = startPoint;

        readerThread = new Thread(this::run);
        readerThread.setName(topicName+".Reader");
    }

    protected KafkaStreamingConsumer(
            @NonNull String topicName,
            @NonNull KafkaConsumer<K, V> consumer,
            @NonNull Duration pollDuration,
            boolean assignLatest) {
        this(topicName, consumer, pollDuration,
                assignLatest ? ConsumerStartPoint.LATEST_OFFSET : ConsumerStartPoint.KAFKA_DEFAULT);
    }

    @Override public void start() {
        readerThread.start();
    }

    /**
     * Shuts down this {@link KafkaStreamingConsumer}. Once shut down, it cannot be restarted.
     */
    @Override public void shutdown() {
        running.set(false);
        consumer.wakeup();
        try {
            readerThread.join(SHUTDOWN_TIMEOUT_MS);
        } catch (InterruptedException e) {
            exceptionHandlerReference.get().handleShutdownException(topicName, "Interrupted while shutting down.", e);
            Thread.currentThread().interrupt();
        }
    }

    public void setExceptionHandler(@NonNull ExceptionHandler handler) {
        exceptionHandlerReference.set(handler);
    }

    /**
     * Registers a lambda which will be invoked for each consumed record. For a record deletion, the second argument
     * passed to the lambda will be {@code null}.
     *
     * @param onNewRecord The lambda which will be invoked for each consumed record.
     */
    public void register(BiConsumer<K, V> onNewRecord) {
        register(KafkaSubscriber.create(onNewRecord));
    }

    /**
     * Registers a pair of lambdas, one of which will be invoked for each consumed record, depending on whether its
     * value is {@code null} or not.
     *
     * @param onNewRecord     The lambda which will be invoked for each consumed record with value other than
     *                        {@code null}.
     * @param onDeletedRecord The lambda which will be invoked for each consumed record deletion, i.e. records whose
     *                        value is {@code null}.
     */
    public void register(BiConsumer<K, V> onNewRecord, Consumer<K> onDeletedRecord) {
        register(KafkaSubscriber.create(onNewRecord, onDeletedRecord));
    }

    /**
     * Registers a subscriber for consumed records.
     *
     * @param subscriber The subscriber which should receive consumed records.
     */
    public void register(KafkaSubscriber<K, V> subscriber) {
        subscribers.add(subscriber);
    }

    /**
     * Registers a lambda which will be invoked for each consumed record. For a record deletion, the second argument
     * passed to the lambda will be {@code null}.
     *
     * @param onNewRecord The lambda which will be invoked for each consumed record.
     */
    public void register(RecordWithMetadataHandler<K, V> onNewRecord) {
        register(KafkaMetadataAwareSubscriber.create(onNewRecord));
    }

    /**
     * Registers a pair of lambdas, one of which will be invoked for each consumed record, depending on whether its
     * value is {@code null} or not.
     *
     * @param onNewRecord     The lambda which will be invoked for each consumed record with value other than
     *                        {@code null}.
     * @param onDeletedRecord The lambda which will be invoked for each consumed record deletion, i.e. records whose
     *                        value is {@code null}.
     */
    public void register(RecordWithMetadataHandler<K, V> onNewRecord, BiConsumer<K, RecordMetadata> onDeletedRecord) {
        register(KafkaMetadataAwareSubscriber.create(onNewRecord, onDeletedRecord));
    }

    /**
     * Registers a metadata-aware subscriber for consumed records.
     *
     * @param subscriber The subscriber which should receive consumed records.
     */
    public void register(KafkaMetadataAwareSubscriber<K, V> subscriber) {
        metadataAwareSubscribers.add(subscriber);
        hasMetadataAwareSubscribers = true;
    }

    /**
     * <p>Commits the specified record offset to the broker. Blocks the thread until the commit is acknowledged.
     * <p>Unlike the underlying kafka client's commit operations, this implementation is thread-safe.
     * <p>A derived class can override {@link #onCommitCompleted(Map, Exception)} to execute logic after the commit
     * operation terminates.
     *
     * @param metadata The metadata of the record to be committed.
     */
    public void commitSync(RecordMetadata metadata) {
        commitSync(getOffsetsToCommit(metadata));
    }

    /**
     * <p>Commits the specified record offsets to the broker. Blocks the thread until the commit is acknowledged.
     * <p>Unlike the underlying kafka client's commit operations, this implementation is thread-safe.
     * <p>A derived class can override {@link #onCommitCompleted(Map, Exception)} to execute logic after the commit
     * operation terminates.
     *
     * @param metadatas The metadata of the records to be committed.
     */
    public void commitSync(Collection<RecordMetadata> metadatas) {
        commitSync(getOffsetsToCommit(metadatas));
    }

    /**
     * <p>Initiates a commit of the specified record offset to the broker without blocking the thread.
     * <p>Unlike the underlying kafka client's commit operations, this implementation is thread-safe.
     * <p>A derived class can override {@link #onCommitCompleted(Map, Exception)} to execute logic after the commit
     * operation terminates.
     *
     * @param metadata The metadata of the record to be committed.
     */
    public void commitAsync(RecordMetadata metadata) {
        commitAsync(getOffsetsToCommit(metadata));
    }

    /**
     * <p>Initiates a commit of the specified record offsets to the broker without blocking the thread.
     * <p>Unlike the underlying kafka client's commit operations, this implementation is thread-safe.
     * <p>A derived class can override {@link #onCommitCompleted(Map, Exception)} to execute logic after the commit
     * operation terminates.
     *
     * @param metadatas The metadata of the records to be committed.
     */
    public void commitAsync(Collection<RecordMetadata> metadatas) {
        commitAsync(getOffsetsToCommit(metadatas));
    }

    // Exposed as package-private for unit testing purposes
    boolean isShutDown() {
        return !running.get() && !readerThread.isAlive();
    }

    protected void run() {
        assignPartitions();
        streamTopic();
        closeSubscriptionIfAny();
    }

    protected void assignPartitions() {
        try {
            switch (startPoint.getType()) {
                case KAFKA_DEFAULT:
                    consumer.subscribe(List.of(topicName));
                    break;
                case EARLIEST_OFFSET:
                    seekToEarliestOffset();
                    break;
                case LATEST_OFFSET:
                    seekToLatestOffset();
                    break;
                case REFERENCE_TIMESTAMP:
                    seekToReferenceTimestamp(startPoint.getTimestamp());
                    break;
                default:
                    throw new IllegalArgumentException("Invalid consumer start point set up: " + startPoint);
            }
        } catch (WakeupException e) {
            if (running.get()) { // Ignore unless we should be running.
                exceptionHandlerReference.get().handleStartupException(topicName, "Unexpected wakeup exception.", e);
            }
        } catch (Throwable t) {
            exceptionHandlerReference.get().handleStartupException(topicName, "Exception during initialization.", t);
        }
    }

    protected Set<TopicPartition> getTopicPartitions() {
        return consumer.partitionsFor(topicName).stream()
                .map(p -> new TopicPartition(topicName, p.partition()))
                .collect(Collectors.toUnmodifiableSet());
    }

    protected void streamTopic() {
        while (running.get()) {
            try {
                handlePendingCommitRequests();

                val records = consumer.poll(pollDuration);
                for (val record: records) {
                    publishToSubscribers(record);
                    if (log.isDebugEnabled()) {
                        log.debug("{}: key={}, value={}, metdata={}", topicName, record.key(), record.value(),
                                RecordMetadata.from(record));
                    }
                }
            } catch (WakeupException e) {
                if (running.get()) { // Ignore unless we should be running.
                    exceptionHandlerReference.get().handleStreamingException(topicName, "Unexpected wakeup exception.", e);
                }
            } catch (Throwable t) {
                exceptionHandlerReference.get().handleStreamingException(topicName, "Exception while streaming.", t);
            }
        }
    }

    protected void handlePendingCommitRequests() {
        var commitRequest = pendingCommitRequests.poll();
        while (running.get() && commitRequest != null) {
            consumer.commitAsync(commitRequest.getOffsets(), commitRequest.getCallback());
            commitRequest = pendingCommitRequests.poll();
        }
    }

    protected void publishToSubscribers(ConsumerRecord<K, V> record) {
        if (isDeleted(record)) {
            subscribers.forEach(s -> s.onDeletedRecord(record.key()));
            if (hasMetadataAwareSubscribers) {
                val metadata = RecordMetadata.from(record);
                metadataAwareSubscribers.forEach(s -> s.onDeletedRecord(record.key(), metadata));
            }
        } else {
            subscribers.forEach(s -> s.onNewRecord(record.key(), record.value()));
            if (hasMetadataAwareSubscribers) {
                val metadata = RecordMetadata.from(record);
                metadataAwareSubscribers.forEach(s -> s.onNewRecord(record.key(), record.value(), metadata));
            }
        }
    }

    protected void closeSubscriptionIfAny() {
        try {
            if (!consumer.subscription().isEmpty()) {
                consumer.unsubscribe();
            }
        } catch (Throwable t) {
            exceptionHandlerReference.get().handleShutdownException(topicName, "Exception while unsubscribing.", t);
        }
    }

    protected void onCommitCompleted(Map<TopicPartition, OffsetAndMetadata> offsets, Exception exception) {
        if (exception == null) {
            log.debug("Commit of offsets {} completed.", offsets);
        } else {
            exceptionHandlerReference.get().handleCommitException(offsets, exception);
        }
    }

    private static Map<TopicPartition, OffsetAndMetadata> getOffsetsToCommit(RecordMetadata metadata) {
        return Map.of(
                new TopicPartition(metadata.getTopic(), metadata.getPartition()),
                new OffsetAndMetadata(metadata.getOffset() + 1));
    }

    private static Map<TopicPartition, OffsetAndMetadata> getOffsetsToCommit(Collection<RecordMetadata> metadatas) {
        return metadatas.stream()
                .collect(toMap(
                        m -> new TopicPartition(m.getTopic(), m.getPartition()),
                        m -> new OffsetAndMetadata(m.getOffset() + 1)));
    }

    private void seekToEarliestOffset() {
        val topicPartitions = getTopicPartitions();
        consumer.assign(topicPartitions);
        consumer.seekToBeginning(topicPartitions);
    }

    private void seekToLatestOffset() {
        val topicPartitions = getTopicPartitions();
        consumer.assign(topicPartitions);
        consumer.seekToEnd(topicPartitions);
    }

    private void seekToReferenceTimestamp(Instant timestamp) {
        val topicPartitions    = getTopicPartitions();
        long epochMs           = timestamp.toEpochMilli();
        val timestampsToSearch = topicPartitions.stream().collect(toMap(tp -> tp, tp -> epochMs));
        val offsets            = consumer.offsetsForTimes(timestampsToSearch);

        consumer.assign(topicPartitions);
        for (val entry: offsets.entrySet()) {
            val topicPartition     = entry.getKey();
            val offsetAndTimestamp = entry.getValue();
            if (offsetAndTimestamp == null) {
                consumer.seekToEnd(List.of(topicPartition));
            } else {
                consumer.seek(topicPartition, offsetAndTimestamp.offset());
            }
        }
    }

    private boolean isDeleted(ConsumerRecord<K, V> record) {
        // Kafka's scheme for deletion on a append-only log is to set null as value.
        return record.value() == null;
    }

    private void awaitSyncCommitCompleted(CommitRequest commitRequest, Future<Exception> exceptionFuture) {
        val offsets = commitRequest.getOffsets();
        final Exception exception;
        try {
            exception = exceptionFuture.get();
        } catch (InterruptedException e) {
            exceptionHandlerReference.get().handleCommitException(offsets, e);
            Thread.currentThread().interrupt();
            return;
        } catch (ExecutionException e) {
            exceptionHandlerReference.get().handleCommitException(offsets, e);
            return;
        }
        onCommitCompleted(offsets, exception);
    }

    private void commitSync(Map<TopicPartition, OffsetAndMetadata> offsets) {
        if (Thread.currentThread() == readerThread) {
            // If the synchronous commit operation is invoked from the consumer's reader thread, we execute it
            // immediately instead of putting it on the pending requests queue. This is essential to avoid a deadlock.
            try {
                consumer.commitSync(offsets);
            } catch (Exception e) {
                onCommitCompleted(offsets, e);
                return;
            }
            onCommitCompleted(offsets, null);
        } else {
            // If the synchronous commit operation is invoked on any other thread, we put it on the pending requests
            // queue and block the current thread until it is processed. The reader thread should eventually pick up the
            // pending request, initiate an asynchronous commit operation on the broker, and signal the blocked thread
            // in the callback.
            val exceptionFuture = new CompletableFuture<Exception>();
            val commitRequest = new CommitRequest(offsets, (__, e) -> exceptionFuture.complete(e));
            pendingCommitRequests.add(commitRequest);
            awaitSyncCommitCompleted(commitRequest, exceptionFuture);
        }
    }

    private void commitAsync(Map<TopicPartition, OffsetAndMetadata> offsets) {
        if (Thread.currentThread() == readerThread) {
            // If the asynchronous commit operation is invoked from the consumer's reader thread, we initiate it
            // immediately instead of putting it on the pending requests queue. This is not strictly necessary but
            // eliminates an unnecessary delay in the operation's initiation.
            consumer.commitAsync(offsets, this::onCommitCompleted);
        } else {
            // If the asynchronous commit operation is invoked on any other thread, we put it on the pending requests
            // queue and return control to the caller immediately. The reader thread should eventually pick up the
            // pending request and initiate an asynchronous commit operation on the broker.
            pendingCommitRequests.add(new CommitRequest(offsets, this::onCommitCompleted));
        }
    }

    /**
     * A builder class used to construct {@link KafkaStreamingConsumer} instances.
     *
     * @param <K> The type of the key.
     * @param <V> The type of the value.
     */
    public static class Builder<K, V> {
        public static final Duration DEFAULT_POLL_DURATION = Duration.ofMillis(1_000);

        private final String topicName;

        private Gson gson;
        private Duration pollDuration;
        private ConsumerStartPoint startPoint;
        private Properties properties;

        private KafkaConsumer<K, V> consumer;

        private Deserializer<K> keyDeserializer;
        private Deserializer<V> valueDeserializer;
        private TypeToken<V> valueType;

        /**
         * Creates a {@link KafkaStreamingConsumer} builder for the specified topic using a preexisting
         * {@link KafkaConsumer} instance.
         *
         * @param topicName The name of the topic.
         * @param consumer  The {@link KafkaConsumer} which will be used to consume records from Kafka.
         */
        public Builder(@NonNull String topicName, @NonNull KafkaConsumer<K, V> consumer) {
            this.topicName    = topicName;
            this.pollDuration = DEFAULT_POLL_DURATION;
            this.startPoint   = ConsumerStartPoint.LATEST_OFFSET;
            this.consumer     = consumer;
        }

        /**
         * Creates a {@link KafkaStreamingConsumer} builder for the specified topic with a custom deserializer for the
         * value part of the record.
         *
         * @param topicName         The name of the topic.
         * @param props             The properties for the {@link KafkaConsumer}.
         * @param keyDeserializer   The deserializer for the records' keys.
         * @param valueDeserializer The deserializer for the records' values.
         */
        public Builder(
                @NonNull String topicName,
                @NonNull Properties props,
                @NonNull Deserializer<K> keyDeserializer,
                @NonNull Deserializer<V> valueDeserializer) {
            this.topicName         = topicName;
            this.properties        = props;
            this.pollDuration      = DEFAULT_POLL_DURATION;
            this.startPoint        = ConsumerStartPoint.LATEST_OFFSET;
            this.keyDeserializer   = keyDeserializer;
            this.valueDeserializer = valueDeserializer;
        }

        /**
         * Creates a {@link KafkaStreamingConsumer} builder for the specified topic with a generic {@link GsonDeserializer}
         * for the value part of the record. The {@link GsonDeserializer}'s {@link Gson} instance can be set using
         * {@link #with(Gson)}.
         *
         * @param topicName       The name of the topic.
         * @param props           The properties for the {@link KafkaConsumer}.
         * @param keyDeserializer The deserializer for the records' keys.
         * @param valueType       The type of the records' value.
         */
        public Builder(
                @NonNull String topicName,
                @NonNull Properties props,
                @NonNull Deserializer<K> keyDeserializer,
                @NonNull TypeToken<V> valueType) {
            this.topicName       = topicName;
            this.properties      = props;
            this.pollDuration    = DEFAULT_POLL_DURATION;
            this.startPoint      = ConsumerStartPoint.LATEST_OFFSET;
            this.keyDeserializer = keyDeserializer;
            this.valueType       = valueType;
        }

        /**
         * Overrides the {@link Gson} instance used by the {@link GsonDeserializer} for the value part of the record.
         * This method may only be invoked if the builder was created using the constructor
         * {@link #Builder(String, Properties, Deserializer, TypeToken)}.
         *
         * @param gson The {@link Gson} instance to use for deserializing values.
         * @return The same builder.
         */
        public KafkaStreamingConsumer.Builder<K, V> with(@NonNull Gson gson) {
            if (valueType == null) {
                throw new IllegalStateException(
                        "A Gson instance can only be provided if the StreamingConsumer.Builder was created with "
                                + "a value type token.");
            }
            this.gson = gson;
            return this;
        }

        /**
         * Overrides the poll duration for the {@link KafkaStreamingConsumer}.
         *
         * @param pollDuration The custom poll duration.
         * @return The same builder.
         */
        public KafkaStreamingConsumer.Builder<K, V> with(@NonNull Duration pollDuration) {
            this.pollDuration = pollDuration;
            return this;
        }

        /**
         * Overrides the start point of the {@link KafkaStreamingConsumer}.
         *
         * @param startPoint The custom start point.
         * @return The same builder.
         */
        public KafkaStreamingConsumer.Builder<K, V> with(@NonNull ConsumerStartPoint startPoint) {
            this.startPoint = startPoint;
            return this;
        }

        /**
         * Creates a {@link KafkaStreamingConsumer} instance with the parameters configured in the builder.
         *
         * @return The created {@link KafkaStreamingConsumer} instance.
         */
        public KafkaStreamingConsumer<K, V> build() {
            if (consumer != null) {
                return new KafkaStreamingConsumer<>(topicName, consumer, pollDuration, true);
            }

            if (valueDeserializer == null) {
                valueDeserializer = new GsonDeserializer<>(valueType, requireNonNullElseGet(gson, Gson::new));
            }
            return new KafkaStreamingConsumer<>(
                    topicName, new KafkaConsumer<>(properties, keyDeserializer, valueDeserializer), pollDuration,
                    startPoint);
        }
    }

    @Data
    private static class CommitRequest {
        private final Map<TopicPartition, OffsetAndMetadata> offsets;
        private final OffsetCommitCallback callback;
    }
}