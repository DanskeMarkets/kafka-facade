package dk.danskebank.markets.kafka.config;

import com.github.charithe.kafka.KafkaHelper;
import com.github.charithe.kafka.KafkaJunitExtension;
import com.github.charithe.kafka.KafkaJunitExtensionConfig;
import com.github.charithe.kafka.StartupMode;
import lombok.val;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.DescribeConfigsResult;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.TopicConfig;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;

@ExtendWith(KafkaJunitExtension.class)
@KafkaJunitExtensionConfig(startupMode = StartupMode.WAIT_FOR_STARTUP)
class TopicConfigTest {

    private static Properties props;

    @BeforeEach
    public void setup(KafkaHelper kafkaHelper) {
        val brokerAddress = "localhost:" + kafkaHelper.kafkaPort();
        props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, brokerAddress);
    }

    @Test
    void testChangeAttribute() {
        val builder = TopicConfiguration
                .builder()
                .replicationFactor((short) 1);

        try (val adminClient = AdminClient.create(props)) {
            ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, "Test");
            builder.configure().applyOnTopic("Test", props);
            DescribeConfigsResult beforeResult = adminClient.describeConfigs(Collections.singleton(resource));
            val before = extractEntry(beforeResult, TopicConfig.CLEANUP_POLICY_CONFIG);

            assertNotNull(before);
            assertEquals("delete", before.value());

            builder.policy(TopicConfiguration.CleanUpPolicy.DELETE_AND_COMPACT)
                    .configure()
                    .applyOnTopic("Test", props);
            DescribeConfigsResult afterResult = adminClient.describeConfigs(Collections.singleton(resource));
            val after = extractEntry(afterResult, TopicConfig.CLEANUP_POLICY_CONFIG);

            assertNotNull(after);
            assertEquals("compact,delete", after.value());
        } catch (Exception e) {
            fail("Exception happened "+e.getLocalizedMessage());
        }
    }

    @Test
    void removeExistingAttribute() {
        val builder = TopicConfiguration.builder().replicationFactor((short) 1);

        try (val adminClient = AdminClient.create(props)) {
            builder.policy(TopicConfiguration.CleanUpPolicy.DELETE_AND_COMPACT);
            ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, "Test");
            builder.configure().applyOnTopic("Test",props);

            DescribeConfigsResult beforeResult = adminClient.describeConfigs(Collections.singleton(resource));
            val before = extractEntry(beforeResult, TopicConfig.CLEANUP_POLICY_CONFIG);

            assertNotNull(before);
            assertEquals("compact,delete", before.value());

            builder.policy(TopicConfiguration.CleanUpPolicy.NOT_SET).configure().applyOnTopic("Test",props);

            DescribeConfigsResult afterResult = adminClient.describeConfigs(Collections.singleton(resource));
            val after = extractEntry(afterResult, TopicConfig.CLEANUP_POLICY_CONFIG);

            assertNotNull(after);
            assertEquals("delete", after.value());
            assertEquals(ConfigEntry.ConfigSource.DEFAULT_CONFIG, after.source());
        } catch (Exception e) {
            fail("Exception happened "+e.getLocalizedMessage());
        }
    }

    @Test
    void removeExistingAttribute2() {
        // props/topics to builder
        // configure method

        val builder = TopicConfiguration
                .builder()
                .replicationFactor((short) 1);

        try (val adminClient = AdminClient.create(props)) {
            builder.segmentMs(Duration.ofMinutes(2));
            ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, "Test");
            builder.configure().applyOnTopic("Test",props);

            DescribeConfigsResult beforeResult = adminClient.describeConfigs(Collections.singleton(resource));
            val before = extractEntry(beforeResult, TopicConfig.SEGMENT_MS_CONFIG);

            assertNotNull(before);
            assertEquals(Long.toString(Duration.ofMinutes(2).toMillis()), before.value());

            //-2 is the default value for undefined
            builder.segmentMs(Duration.ofMillis(-2)).configure().applyOnTopic("Test",props);

            DescribeConfigsResult afterResult = adminClient.describeConfigs(Collections.singleton(resource));
            val after = extractEntry(afterResult, TopicConfig.SEGMENT_MS_CONFIG);

            assertNotNull(after);
            assertEquals("604800000", after.value());
            assertEquals(ConfigEntry.ConfigSource.DEFAULT_CONFIG, after.source());
        } catch (Exception e) {
            fail("Exception happened "+e.getLocalizedMessage());
        }
    }

    @Test
    void idempotentCreation() {
        val builder = TopicConfiguration.builder().replicationFactor((short) 1);

        try {
            builder.configure().applyOnTopic("Test", props);
            builder.configure().applyOnTopic("Test", props);
        } catch (Exception e) {
            fail("Exception happened "+e.getLocalizedMessage());
        }
    }

    private ConfigEntry extractEntry(DescribeConfigsResult describeConfigsResult, String name) throws Exception {
        return describeConfigsResult.all().get().values().stream()
                .findFirst()
                .map(config -> config.get(name))
                .orElse(null);
    }
}