# Kafka Facade Library

## Introduction

The team who wrote this library within Danske Bank, decided to do so because we became aware that we implemented the
same patterns again and again. That's because using Kafka requires a lot of boilerplate code, and you can use this 
library to reduce that and focus on the business logic.

This library is heavily battle tested and used across many applications in Production.

It contains the following utilities which are ideal for the associated use cases:

1. `KafkaReplayConsumer`: Easy replay and streaming of records.
2. `KafkaCache`: Easily get the initial state of a topic and get callbacks on future changes.
3. `GsonSerializer`/`GsonDeserializer`: Easily write Json serializers and deserializers for Kafka.
4. `RecordsFile`: Load a plain text file with records and publish them to a Kafka topic.
5. `MultilineJsonRecordsFile`: Load a plain text file where each record spans multiple lines (formatting of JSON)
     and publish them to a Kafka topic.
6. `KafkaClusterMonitoring`: A simple utility to get callbacks when the cluster comes up, or it goes down.
7. `ExitOnKafkaDownMonitor`: Default monitor which will shut down application if the cluster is down.
8. `KafkaMessagePublisher`: Default kafka message producer to logs error while publishing messages
9. `MessageCodec`: JSON based kafka object serializer and deserializer
10. `TopicConfiguration` : Util class to set policies on specific Topic

## Project Lifecycle

If Danske Bank stops developing this project, it will be stated in this readme.
While this project is actively developed, reported bugs, that are verified, will be fixed.
Feature requests/pull requests will be accepted/rejected at Danske Bank's sole discretion without guarantees
for any explanation.

## Setup

To use this library, add it as a dependency (replace `x.y.z` with the latest version):

**Maven:**

    <dependency>
      <groupId>dk.danskebank.markets</groupId>
      <artifactId>kafka-facade</artifactId>
      <version>x.y.z</version>
    </dependency>

**Gradle:**

    implementation group: 'dk.danskebank.markets', name: 'kafka-facade', version: 'x.y.z'


## Usage of KafkaStreamingConsumer

### Setup

`KafkaStreamingConsumer`s are built using a builder. There are three variants:

A. Passing the `KafkaConsumer` directly:

    var streamingConsumer = new KafkaStreamingConsumer.Builder<>("topic-name", kafkaConsumer).build();

B. Passing misc. deserializers. The `KafkaStreamingConsumer` will create the `KafkaConsumer`:

    var streamingConsumer = new KafkaStreamingConsumer
            .Builder<>("topic-name", kafkaConsumerProperties, new StringDeserializer(), new MyCustomDeserializer())
            .build();

C. Passing a key deserializer and a type token which will be used to instantiate a `GsonDeserializer<V>` for the value.
The `KafkaStreamingConsumer` will create the `KafkaConsumer`:

    var streamingConsumer = new KafkaStreamingConsumer
            .Builder<>("topic-name", kafkaConsumerProperties, new StringDeserializer(), new TypeToken<MyValue>(){})
            .build();

You can set a custom `Gson` instance for the value deserializer if necessary:

    var streamingConsumer = new KafkaStreamingConsumer
            .Builder<>("topic-name", kafkaConsumerProperties, new StringDeserializer(), new TypeToken<MyValue>(){})
            .with(new GsonBuilder()
                    .registerTypeAdapter(MyEnum.class, new MyEnumAdapter().nullSafe())
                    .create())
            .build();

Furthermore, the builder allows you set a custom polling interval:

    var streamingConsumer = new KafkaStreamingConsumer.Builder<>("topic-name", kafkaConsumer)
            .with(Duration.ofMillis(500))
            .build();

Finally, you can specify a start point from which the streaming consumer should start consuming:

    var streamingConsumer = new KafkaStreamingConsumer.Builder<>("topic-name", kafkaConsumer)
            .with(ConsumerStartPoint.fromReferenceTimestamp(Instant.now().minus(1, ChronoUnit.DAYS))
            .build();

### Picking the Correct Starting Point for the Consumer

`KafkaStreamingConsumer` supports four kinds of consumption start points:
* `ConsumerStartPoint.EARLIEST_OFFSET` causes the consumer to start consuming from the earliest available offset on
  each partition of the topic.
* `ConsumerStartPoint.LATEST_OFFSET` causes the consumer to start consuming from the offset following the latest
  available offset on each partition of the topic. This effectively means that only records produced after the consumer
  starts up will be consumed. This is the default start point in order to preserve compatibility with earlier versions
  of the library.
* `ConsumerStartPoint.fromReferenceTimestamp(Instant timestamp)` can be used to create a start point which will cause
  the consumption to start from the earliest offset on each partition whose timestamp is *newer* than the reference
  timestamp.
* `ConsumerStartPoint.KAFKA_DEFAULT` will prompt the consumer to use the default topic subscription semantics
  implemented by the Kafka broker. This should be used to set up offset-tracking consumers, where consumption should
  continue from the first uncommitted offset on each partition. This applies regardless of whether offsets are committed
  in an automatic or manual fashion.

### Custom Exception Handling

Once the `KafkaStreamingConsumer` is constructed, you can set a `ExceptionHandler` to handle exceptions which occur on
startup, streaming, shutdown, or manual offset commit. In case you don't set one, it uses a default implementation which
logs exceptions and initiates a JVM shutdown for all exceptions except for these which occur on offset commit.

    replayConsumer.setExceptionHandler(new ExceptionHandler() {
        @Override public void handleStartupException(String topic, String message, Throwable cause) {
            // Your handling of the throwable (optional).
        }

        @Override public void handleStreamingException(String topic, String message, Throwable cause) {
            // Your handling of the throwable.
        }

        @Override public void handleCommitException(Map<TopicPartition, OffsetAndMetadata> offsets, Exception exception) {
            // Your handling of the manual commit exception (optional).
        }

        @Override public void handleShutdownException(String topic, String message, Throwable cause) {
            // Your handling of the throwable (optional).
        }
    });

### Registering a Simple Subscriber to Get Data

Once you have a started `KafkaStreamingConsumer`, you register a subscriber which will be invoked whenever a new record
is consumed:

    var mySubscriber = new KafkaSubscriber<>() {
        @Override public void onNewRecord(K key, V value) {
            // Your logic for records whose value is not null.
        }

        @Override public void onDeletedRecord(K key) {
            // Your logic for records that are deleted (value is null).
        }
    };
    streamingConsumer.register(mySubscriber);

Alternatively, you can use one of the overloads of the `register` method which take one or two lambdas:

    streamingConsumer.register(
        (key, value) -> { /* Your logic for any record. */ }
    );

    // -or-

    streamingConsumer.register(
        (key, value) -> { /* Your logic for records whose value is not null. */ },
        key -> { /* Your logic for records that are deleted (value is null). */ }
    );

### Registering a Metadata-aware Subscriber to Get Data

If you application needs to access record metadata (topic name, partition index, offset, timestamp), it can
register a metadata-aware subscriber instead:

    var mySubscriber = new KafkaMetadataAwareSubscriber<>() {
        @Override public void onNewRecord(K key, V value, RecordMetadata metadata) {
            // Your logic for records whose value is not null.
        }

        @Override public void onDeletedRecord(K key, RecordMetadata metadata) {
            // Your logic for records that are deleted (value is null).
        }
    };
    streamingConsumer.register(mySubscriber);

As above, you can also use an overload which takes one or two lambdas instead:

    streamingConsumer.register(
        (key, value, metadata) -> { /* Your logic for any record. */ }
    );

    // -or-

    streamingConsumer.register(
        (key, value, metadata) -> { /* Your logic for records whose value is not null. */ },
        (key, metadata) -> { /* Your logic for records that are deleted (value is null). */ }
    );

### Implementing Manual Offset Commit Semantics

If your application should perform manual commits of the latest processed offset on a topic partition, you need to
* Define a consumer group ID and disable automatic offset commits in the Kafka `Properties`:

        val props = new Properties();
        props.put(ConsumerConfig.GROUP_ID_CONFIG,           "my-consumer-group");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        val streamingConsumer = new KafkaStreamingConsumer
                .Builder<>("topic-name", props, new StringDeserializer(), new MyCustomDeserializer())
                .build();

* Register a metadata-aware subscriber on the streaming consumer;
* Use the `RecordMetadata` which came with a record in order to commit this record back to the Kafka broker. The
streaming consumer supports synchronous and asynchronous commits:
  
        streamingConsumer.register(
            (key, value, metadata) -> {
                val processedSuccessfully = doProcessing(key, value);
                if (processedSuccessfully) {
                    // Will block the consumption until the commit operation is completed
                    streamingConsumer.commitSync(metadata);
                }
            });

        // -or-
        
        streamingConsumer.register(
            (key, value, metadata) -> {
                val processedSuccessfully = doProcessing(key, value);
                if (processedSuccessfully) {
                    // Will initiate a commit operation but allow consumption from the topic to proceed immediately
                    streamingConsumer.commitAsync(metadata);
                }
            });

If you need to execute custom logic after a commit operation terminates (successfully or with an exception), you can
derive `KafkaStreamingSubscriber` and override the method `onCommitCompleted`. This is the only way to execute logic
after an asynchronous commit operation terminates, but it can also be used in a synchronous commit scenario.

The consumer also supports committing a collection of `RecordMetadata` objects, which should contain only a single
object per topic partition. The result of the commit if this condition is violated is undefined.

## Usage of KafkaReplayConsumer

### Setup

`KafkaReplayConsumer`s are built using a builder. There are three variants:

A. Passing the `KafkaConsumer` directly:

    var replayConsumer = new KafkaReplayConsumer.Builder<>("topic-name", kafkaConsumer).build();

B. Passing misc. deserializers. The `KafkaReplayConsumer` will create the `KafkaConsumer`:

    var streamingConsumer = new KafkaReplayConsumer
            .Builder<>("topic-name", kafkaConsumerProperties, new StringDeserializer(), new MyCustomDeserializer())
            .build();

C. Passing a key deserializer and a type token which will be used to instantiate a `GsonDeserializer<V>` for the value.
The `KafkaReplayConsumer` will create the `KafkaConsumer`:

    var streamingConsumer = new KafkaReplayConsumer
            .Builder<>("topic-name", kafkaConsumerProperties, new StringDeserializer(), new TypeToken<MyValue>(){})
            .build();

You can set a custom `Gson` instance for the value deserializer if necessary:

    var streamingConsumer = new KafkaReplayConsumer
            .Builder<>("topic-name", kafkaConsumerProperties, new StringDeserializer(), new TypeToken<MyValue>(){})
            .with(new GsonBuilder()
                    .registerTypeAdapter(MyEnum.class, new MyEnumAdapter().nullSafe())
                    .create())
            .build();

Furthermore, the builder allows you set a custom polling interval:

    var streamingConsumer = new KafkaReplayConsumer.Builder<>("topic-name", kafkaConsumer)
            .with(Duration.ofMillis(500))
            .build();

### Custom Exception Handling

Once the `KafkaReplayConsumer` is constructed, you can set a `ExceptionHandler` to handle exceptions which occur on
startup, replay, streaming, or shutdown. In case you don't set one, it uses a default implementation which logs
exceptions and initiates a JVM shutdown.

    replayConsumer.setExceptionHandler(new ExceptionHandler() {
        @Override public void handleStartupException(String topic, String message, Throwable cause) {
            // Your handling of the throwable (optional).
        }

        @Override public void handleReplayException(String topic, String message, Throwable cause) {
            // Your handling of the throwable (optional).
        }

        @Override public void handleStreamingException(String topic, String message, Throwable cause) {
            // Your handling of the throwable.
        }

        @Override public void handleShutdownException(String topic, String message, Throwable cause) {
            // Your handling of the throwable (optional).
        }
    });

### Registering a Subscriber to Get Data

Once you have a started `KafkaReplayConsumer`, you register a subscriber which will be invoked whenever a new record is
read and when replay is done:

    var mySubscriber = new KafkaSubscriber<>() {
        @Override public void onNewRecord(K key, V value) {
            // Your logic for records whose value is not null.
        }

        @Override public void onDeletedRecord(K key) {
            // Your logic for records that are deleted (value is null).
        }

        @Override public void onReplayDone() {
            // Your logic.
        }
    };
    replayConsumer.register(mySubscriber);

You can also register a metadata-aware subscriber if needed. See the corresponding subsection in the section about
`KafkaStreamingConsumer` for details.

## Usage of KafkaCache

### Setup

`KafkaCache`s are built using a builder. There are three variants:

A. Passing the `KafkaConsumer` directly:

    var cache = new KafkaCache.Builder<>("topic-name", kafkaConsumer).build();

B. Passing misc. deserializers. The `KafkaCache` will create the `KafkaConsumer`:

    var streamingConsumer = new KafkaCache
            .Builder<>("topic-name", kafkaConsumerProperties, new StringDeserializer(), new MyCustomDeserializer())
            .build();

C. Passing a key deserializer and a type token which will be used to instantiate a `GsonDeserializer<V>` for the value.
The `KafkaCache` will create the `KafkaConsumer`:

    var streamingConsumer = new KafkaCache
            .Builder<>("topic-name", kafkaConsumerProperties, new StringDeserializer(), new TypeToken<MyValue>(){})
            .build();

You can set a custom `Gson` instance for the value deserializer if necessary:

    var streamingConsumer = new KafkaCache
            .Builder<>("topic-name", kafkaConsumerProperties, new StringDeserializer(), new TypeToken<MyValue>(){})
            .with(new GsonBuilder()
                    .registerTypeAdapter(MyEnum.class, new MyEnumAdapter().nullSafe())
                    .create())
            .build();

Furthermore, the builder allows you set a custom polling interval:

    var streamingConsumer = new KafkaCache.Builder<>("topic-name", kafkaConsumer)
            .with(Duration.ofMillis(500))
            .build();

### Custom Exception Handling

See the corresponding subsection under the section about `KafkaReplayConsumer` for details.

### Getting Data

Once you have a started `KafkaCache`, you can either get a cache copy (which does not update -
it's a copy of the keys/values in the topic at the time of invocation):

    var cacheCopy = kafkaCache.getCacheCopy();

Or you can register a subscriber which will be invoked whenever a new record is read by the cache after replay:

    var mySubscriber = new KafkaStreamingSubscriber<>() {
        @Override public void onNewRecord(K key, V value) {
            // Your logic for records whose value is not null.
        }

        @Override public void onDeletedRecord(K key) {
            // Your logic for records that are deleted (value is null).
        }
    };
    kafkaCache.register(mySubscriber);

Alternatively, you can use one of the overloads of the `register` method which take one or two lambdas:

    kafkaCache.register(
        (key, value) -> { /* Your logic for any record. */ }
    );

    // -or-

    kafkaCache.register(
        (key, value) -> { /* Your logic for records whose value is not null. */ },
        key -> { /* Your logic for records that are deleted (value is null). */ }
    );

Note that unlike `KafkaStreamingConsumer` and `KafkaReplayConsumer`, `KafkaCache` does not support metadata-aware
subscribers.

### Working with Multiple Caches

Sometimes, you need to work with a group of caches where you join data from different topics.
To make this easier, a `CacheGroup` can be used:

    var cache1 = new KafkaCache.Builder<>("topic-1", kafkaConsumer).build();
    var cache2 = new KafkaCache.Builder<>("topic-2", kafkaConsumer).build();
    var cache3 = new KafkaCache.Builder<>("topic-3", kafkaConsumer).build();

    // Create a group:
    var group  = CacheGroup.of(cache1, cache2, cache3);

    // Start all caches and await replay:
    group.startAndAwaitReplay();

    // Take read lock (to ensure consistency of cache copies):
    group.takeReadLock();
    try {
        var cacheCopy1 = cache1.getCacheCopy();
        var cacheCopy2 = cache2.getCacheCopy();
        var cacheCopy3 = cache3.getCacheCopy();
        // Use the data here.
    }
    finally {
        group.releaseReadLock();
    }

    // Stop all caches:
    group.stop();


## Usage of RecordsFile

Create a new instance and point it to a text file with key/value pairs separated by a comma (','):

    var recordsFile  = RecordsFile.fromClasspath("records.json");
    // or load from an arbitrary path:
    val recordsFile2 = RecordsFile.from(Path.of("records.json"));

A file with records could look like this:

    # List rates from 2020-04-13:
    EUR/USD, {rate=1.1232}
    EUR/DKK, {rate=7.4732}
    # Delete USD/ARS as we don't use it anymore.
    EUR/ARS,

Note, that a key without a value is interpreted as a deletion of that key (EUR/ARS in the above example).

To publish them to a topic, e.g. `my-topic`, just invoke

    recordsFile.publishTo("my-topic", kafkaProperties);

where `kafkaProperties` is the usual Kafka Properties file.

## Usage of MultilineJsonRecordsFile

This works much similar to the RecordsFile described above and the static methods of `MultilineJsonRecordsFile` actually
return `RecordsFile`s:

    var recordsFile  = MultilineJsonRecordsFile.fromClasspath("records.json");
    // or load from an arbitrary path:
    val recordsFile2 = MultilineJsonRecordsFile.from(Path.of("records.json"));

A file with records spanning multiple lines could look like this:

    # List rates from 2020-04-13:
    EUR/USD, {
      rate=1.1232
      # Comments and blank lines ok in multiline JSON.

    }
    EUR/DKK, {
      rate=7.4732
    }
    # Delete USD/ARS as we don't use it anymore.
    EUR/ARS,

Note, that the closing bracket (the end of the JSON object) MUST be on its own line.

## Usage of KafkaClusterMonitoring

Create a `KafkaClusterMonitoring` by passing the properties with the `bootstrap.servers`, register your listener
and start it:

    var props      = new Properties();
    props.put("bootstrap.servers", "server1:9092,server2:9092,server3:9092");
    var monitoring = new KafkaClusterMonitoring(props);
    monitoring.register(new KafkaClusterStateListener() {
        public void isUp() {
            // Invoked when minimum one broker is up. Also invoked initially (if the cluster is up).
        }

        public void isDown() {
            // Invoked if the cluster is down.
        }
    });
    monitoring.start();

When you don't need to monitor the Kafka cluster's state anymore (e.g. at application shutdown), invoke `shutdown`:

    monitoring.shutdown();
    
## Usage of ExitOnKafkaDownMonitor
Create an instance of KafkaClusterMonitor and use it to initialize ExitOnKafkaMonitor. 
ExitOnKafkaMonitor implements LifeCycle, so the monitor shutdown can be controlled by Lifecycle methods.
 
     var kafkaStateListener = new ExitOnKafkaDownMonitor(kafkaClusterMonitoring);
     
 ## Usage of KafkaMessagePublisher
Create kafka producer and pass it in constructor to create KafkaMessagePublisher
 
    var kafkaMessagePublisher = new KafkaMessagePublisher<EventObject>(topic, kafkaProducer);
    
## Usage of MessageCodec
Create a new instance of MessageCodec by specifying the object type which needs 
to be serialized or deserialized. EventObject is the application data model class. Below is 
the example to use MessageCodec as serializer/deserializer.

    var MessageCodec = new MessageCodec<EventObject>(EventObject.class)
    var kafkaProducer = new KafkaProducer(kafkaConfiguration, new StringSerializer(), new MessageCodec<EventObject>(EventObject.class));
    var kafkaConsumer = new KafkaConsumer<>(kafkaConfiguration, new StringDeserializer(), new MessageCodec<EventObject>(EventObject.class));

## Usage of TopicConfiguration
Create a builder from TopicConfiguration.builder() - here you can specify properties of the topic. When done call configure() method.
From here you can call applyOnTopic(topic, props)

    var props = new Properties();
    props.put("bootstrap.servers", "localhost:17073");
    var topicConfig = TopicConfiguration.builder().policy(TopicConfiguration.CleanUpPolicy.DELETE_AND_COMPACT).configure();
    // Set Delete and compact policy on 2 topics:
    topicConfig.applyOnTopic("TestTopic", props)
    topicConfig.applyOnTopic("TestTopic2", props)

## Contributors

Several people inside Danske Bank has contributed to this project:

* Aleksandar Ivanov Dalemski ([musashibg](https://github.com/musashibg))
* Honi Jain ([jainhj](https://github.com/jainhj))
* Nicholas Schultz-Møller ([nicholassm](https://github.com/nicholassm))