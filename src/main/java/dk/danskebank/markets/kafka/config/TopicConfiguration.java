package dk.danskebank.markets.kafka.config;

import lombok.Builder;
import lombok.extern.log4j.Log4j2;
import lombok.val;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.TopicConfig;

import java.time.Duration;
import java.util.*;

/**
 * Used for configuring topic specific properties - should be called from producing application to a topic.
 * After an instance is built and {@link #applyOnTopic} is called, the topic will be created (as idempotent operation)
 * and then updated with requested values. There's no protection against multiple producer defining the same topic -
 * only a single producer on each topic should use this class.
 */
@Log4j2
@Builder(buildMethodName = "configure")
public class TopicConfiguration {

    private static final long NOT_DEFINED = -2L;
    private static final String NOT_DEFINED_STR = Long.toString(NOT_DEFINED);

    private final Map<CleanUpPolicy, String> policyMap =
            Map.of(
                    CleanUpPolicy.NOT_SET,"",
                    CleanUpPolicy.DELETE,"delete",
                    CleanUpPolicy.COMPACT,"compact",
                    CleanUpPolicy.DELETE_AND_COMPACT,"compact,delete");

    private final Map<MessageTimestamp, String> timeMap =
            Map.of(
                    MessageTimestamp.NOT_SET,"",
                    MessageTimestamp.CREATE_TIME,"CreateTime",
                    MessageTimestamp.LOG_APPEND_TIME,"LogAppendTime");

    public enum CleanUpPolicy { DELETE, COMPACT, DELETE_AND_COMPACT, NOT_SET}
    public enum MessageTimestamp { CREATE_TIME, LOG_APPEND_TIME, NOT_SET}

    //Config supported
    @Builder.Default
    private MessageTimestamp messageTimestampType = MessageTimestamp.NOT_SET;
    @Builder.Default
    private CleanUpPolicy policy = CleanUpPolicy.NOT_SET;

    @Builder.Default
    private long segmentBytes    = NOT_DEFINED;
    @Builder.Default
    private Duration segmentMs   = Duration.ofMillis(NOT_DEFINED);
    @Builder.Default
    private Duration retentionMs = Duration.ofMillis(NOT_DEFINED);

    /**
     * Default single partition - can be changed
     */
    @Builder.Default
    private int numberPartitions = 1;
    /**
     * Default 2 in replication factor - can be changed
     */
    @Builder.Default
    private short replicationFactor = 2;

    /**
     * Executes the configuration that has been built - always create topic first with @replicationFactor and @numberPartitions
     * Once done the topic configs supported by this class will either be deleted or set
     * @return the result of the config change
     */

    public AlterConfigsResult applyOnTopic(String topic, Properties properties) {
        try (val adminClient = AdminClient.create(properties)) {
            ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, topic);

            val topicCreate = new NewTopic(topic, numberPartitions, replicationFactor);
            adminClient.createTopics(Collections.singleton(topicCreate)).all();

            List<ConfigEntry> configs = new ArrayList<>();
            // create a new entry for updating the retention.ms value on the same topic
            configs.add(new ConfigEntry(TopicConfig.CLEANUP_POLICY_CONFIG, policyMap.get(policy)));
            configs.add(new ConfigEntry(TopicConfig.RETENTION_MS_CONFIG, Long.toString(retentionMs.toMillis())));
            configs.add(new ConfigEntry(TopicConfig.MESSAGE_TIMESTAMP_TYPE_CONFIG, timeMap.get(messageTimestampType)));
            configs.add(new ConfigEntry(TopicConfig.SEGMENT_MS_CONFIG, Long.toString(segmentMs.toMillis())));
            configs.add(new ConfigEntry(TopicConfig.SEGMENT_BYTES_CONFIG, Long.toString(segmentBytes)));

            Map<ConfigResource, Collection<AlterConfigOp>> updateConfig = new HashMap<>();
            List<AlterConfigOp> alterConfigs = new ArrayList<>();
            for (val config : configs) {
                alterConfigs.add(new AlterConfigOp(config, getOpType(config.value())));
            }

            updateConfig.put(resource, alterConfigs);
            return adminClient.incrementalAlterConfigs(updateConfig);
        }
    }

    private AlterConfigOp.OpType getOpType(String value) {
        return value.isEmpty() || NOT_DEFINED_STR.equals(value) ?
                AlterConfigOp.OpType.DELETE : AlterConfigOp.OpType.SET;
    }
}