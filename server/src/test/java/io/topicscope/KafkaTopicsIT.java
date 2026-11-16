package io.topicscope;

import static io.topicscope.McpTestSupport.call;
import static io.topicscope.McpTestSupport.callExpectingProblem;
import static io.topicscope.McpTestSupport.connect;
import static io.topicscope.McpTestSupport.number;
import static io.topicscope.McpTestSupport.row;
import static org.assertj.core.api.Assertions.assertThat;

import io.modelcontextprotocol.client.McpSyncClient;
import io.topicscope.sim.DemoData;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * The check of Phase 3: {@code list_topics} and {@code describe_topic}, asked through MCP, report exactly
 * the topics the simulator seeded. Expected values come from {@link DemoData}, not from this file.
 */
class KafkaTopicsIT {

  static final Instant STARTED = Instant.now().minusSeconds(5);
  static McpSyncClient client;

  @BeforeAll
  static void connectToServer() {
    client = connect();
  }

  @AfterAll
  static void disconnect() {
    if (client != null) client.close();
  }

  @Test
  void listTopicsMatchesWhatTheSimulatorCreated() {
    Map<String, Object> result = call(client, "list_topics", Map.of("source", "local-kafka"));

    assertThat(result).containsEntry("source", "local-kafka").containsEntry("type", "KAFKA").containsEntry("note", "");
    assertThat(number(result.get("matching"))).isEqualTo(DemoData.TOPICS.size());
    assertThat(number(result.get("shown"))).isEqualTo(DemoData.TOPICS.size());
    assertThat(names(result)).containsExactlyElementsOf(DemoData.TOPICS.stream().map(DemoData.TopicSpec::name).sorted().toList());

    for (DemoData.TopicSpec expected : DemoData.TOPICS) {
      Map<String, Object> topic = row(result, "topics", expected.name());
      assertThat(number(topic.get("partitions"))).as(expected.name() + " partitions").isEqualTo(expected.partitions());
      assertThat(number(topic.get("messages"))).as(expected.name() + " messages").isEqualTo(expected.seededTotal());
      assertThat(number(topic.get("replicationFactor"))).isEqualTo(1);
      assertThat(topic).containsEntry("internal", false);
      assertThat(number(topic.get("underReplicatedPartitions"))).isZero();
      assertThat(number(topic.get("offlinePartitions"))).isZero();
    }
  }

  @Test
  void theNewestMessageTimeIsWhenTheSeedWasWrittenAndEmptyForAnEmptyTopic() {
    Map<String, Object> result = call(client, "list_topics", Map.of("source", "local-kafka"));

    Map<String, Object> alarms = row(result, "topics", DemoData.ALARMS.name());
    Instant newest = Instant.parse((String) alarms.get("newestMessageTime"));
    assertThat(newest).isBetween(STARTED.minusSeconds(600), Instant.now().plusSeconds(5));
    assertThat(number(alarms.get("newestMessageAgeSeconds"))).isBetween(0L, 900L);

    Map<String, Object> empty = row(result, "topics", DemoData.EMPTY.name());
    assertThat(empty).containsEntry("newestMessageTime", "");
    assertThat(number(empty.get("newestMessageAgeSeconds"))).isEqualTo(-1);
  }

  @Test
  void theFilterNarrowsAndInternalTopicsShowOnlyOnRequest() {
    Map<String, Object> sensors = call(client, "list_topics", Map.of("source", "local-kafka", "nameContains", "SENSORS"));
    assertThat(names(sensors)).containsExactly(DemoData.TELEMETRY.name());

    Map<String, Object> none = call(client, "list_topics", Map.of("source", "local-kafka", "nameContains", "no-such-thing"));
    assertThat(names(none)).isEmpty();
    assertThat((String) none.get("note")).contains("No topic name contains");

    // The seeded consumer groups made the broker create its offsets topic.
    assertThat(names(call(client, "list_topics", Map.of("source", "local-kafka")))).doesNotContain("__consumer_offsets");
    Map<String, Object> all = call(client, "list_topics", Map.of("source", "local-kafka", "includeInternal", true));
    assertThat(names(all)).contains("__consumer_offsets").containsAll(DemoData.TOPICS.stream().map(DemoData.TopicSpec::name).toList());
    assertThat(row(all, "topics", "__consumer_offsets")).containsEntry("internal", true);
  }

  @Test
  @SuppressWarnings("unchecked")
  void describeTopicShowsEveryPartitionAndTheRetentionSetting() {
    DemoData.TopicSpec positions = DemoData.POSITIONS;
    Map<String, Object> result = call(client, "describe_topic", Map.of("source", "local-kafka", "topic", positions.name()));

    Map<String, Object> topic = (Map<String, Object>) result.get("topic");
    assertThat(topic).containsEntry("name", positions.name());
    assertThat(number(topic.get("messages"))).isEqualTo(positions.seededTotal());

    List<Map<String, Object>> partitions = (List<Map<String, Object>>) result.get("partitions");
    assertThat(partitions).hasSize(positions.partitions());
    for (int p = 0; p < positions.partitions(); p++) {
      Map<String, Object> partition = partitions.get(p);
      assertThat(number(partition.get("partition"))).isEqualTo(p);
      assertThat(number(partition.get("startOffset"))).isZero();
      assertThat(number(partition.get("endOffset"))).isEqualTo(positions.seededPerPartition());
      assertThat(number(partition.get("messages"))).isEqualTo(positions.seededPerPartition());
      assertThat(number(partition.get("leader"))).isGreaterThanOrEqualTo(0);
      assertThat((List<Object>) partition.get("replicas")).hasSize(1).isEqualTo(partition.get("inSyncReplicas"));
      assertThat((String) partition.get("newestMessageTime")).isNotEmpty();
    }

    // The simulator creates its topics with retention.ms = -1; everything else is the broker's default.
    Map<String, Object> retention = row(result, "settings", "retention.ms");
    assertThat(retention).containsEntry("value", "-1").containsEntry("overridden", true);
    assertThat(row(result, "settings", "cleanup.policy")).containsEntry("value", "delete").containsEntry("overridden", false);
    // Set in the broker's own configuration, not on the topic: shown, and not marked as an override.
    assertThat(row(result, "settings", "min.insync.replicas")).containsEntry("overridden", false);
    assertThat(result).containsEntry("note", "");
  }

  @Test
  void aCompactedTopicCarriesTheUpperBoundNoteAndOthersDoNot() {
    DemoData.TopicSpec state = DemoData.DEVICE_STATE;
    Map<String, Object> result = call(client, "describe_topic", Map.of("source", "local-kafka", "topic", state.name()));
    assertThat(row(result, "settings", "cleanup.policy")).containsEntry("value", "compact").containsEntry("overridden", true);
    assertThat((String) result.get("note")).contains("This topic is compacted (cleanup.policy=compact)", "upper bound");
    // Offsets never shrink, so the count from offsets is what was written, whatever compaction has removed since.
    @SuppressWarnings("unchecked")
    Map<String, Object> topic = (Map<String, Object>) result.get("topic");
    assertThat(number(topic.get("messages"))).isEqualTo(state.seededTotal());

    for (DemoData.TopicSpec other : DemoData.TOPICS) {
      if (other.compacted()) continue;
      assertThat(call(client, "describe_topic", Map.of("source", "local-kafka", "topic", other.name()))).as(other.name()).containsEntry("note", "");
    }
  }

  @Test
  void anEmptyTopicIsDescribedWithZeroesAndNoTime() {
    Map<String, Object> result = call(client, "describe_topic", Map.of("source", "local-kafka", "topic", DemoData.EMPTY.name()));
    @SuppressWarnings("unchecked")
    Map<String, Object> partition = ((List<Map<String, Object>>) result.get("partitions")).get(0);
    assertThat(number(partition.get("startOffset"))).isZero();
    assertThat(number(partition.get("endOffset"))).isZero();
    assertThat(partition).containsEntry("newestMessageTime", "");
  }

  @Test
  void wrongNamesAreAnsweredWithTheRightOnes() {
    // Exactly two lines: the problem and the hint, each once.
    assertThat(callExpectingProblem(client, "describe_topic", Map.of("source", "local-kafka", "topic", "positions")).lines().toList())
        .containsExactly("There is no topic named 'positions' on source 'local-kafka'.", "Did you mean: " + DemoData.POSITIONS.name() + ".");
    assertThat(callExpectingProblem(client, "list_topics", Map.of("source", "prod-kafka")))
        .contains("There is no source named 'prod-kafka'", "local-kafka (kafka)", "local-mqtt (mqtt)");
    assertThat(callExpectingProblem(client, "describe_topic", Map.of("source", "local-mqtt", "topic", "demo/sensors")))
        .contains("is an MQTT source");
  }

  @Test
  void anMqttSourceSaysThatItCannotBeListedYet() {
    Map<String, Object> result = call(client, "list_topics", Map.of("source", "local-mqtt"));
    assertThat(result).containsEntry("type", "MQTT");
    assertThat(names(result)).isEmpty();
    assertThat((String) result.get("note")).contains("not available yet");
  }

  @Test
  void aKafkaSourceThatIsDownIsAProblemWithAReasonNotASilentHang() {
    try (McpSyncClient lonely = connect("127.0.0.1:1", McpTestSupport.MQTT_ADDRESS)) {
      long start = System.nanoTime();
      String problem = callExpectingProblem(lonely, "list_topics", Map.of("source", "local-kafka"));
      assertThat(problem).contains("Source 'local-kafka' (127.0.0.1:1) could not be read", "list_sources");
      assertThat((System.nanoTime() - start) / 1_000_000).as("answers within the request timeout, plus margin").isLessThan(25_000);
    }
  }

  @SuppressWarnings("unchecked")
  private static List<String> names(Map<String, Object> result) {
    return ((List<Map<String, Object>>) result.get("topics")).stream().map(t -> (String) t.get("name")).toList();
  }
}
