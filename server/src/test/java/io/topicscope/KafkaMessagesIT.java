package io.topicscope;

import static io.topicscope.McpTestSupport.KAFKA_ADDRESS;
import static io.topicscope.McpTestSupport.call;
import static io.topicscope.McpTestSupport.callExpectingProblem;
import static io.topicscope.McpTestSupport.connect;
import static io.topicscope.McpTestSupport.number;
import static org.assertj.core.api.Assertions.assertThat;

import io.modelcontextprotocol.client.McpSyncClient;
import io.topicscope.sim.DemoData;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.GroupListing;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * The check of Phase 4: {@code recent_messages}, asked through MCP, returns the last records exactly as the
 * simulator published them, and leaves the broker exactly as it was: no consumer group, no committed offset,
 * no new topic. Expected records come from {@link DemoData}.
 */
class KafkaMessagesIT {

  static McpSyncClient client;
  static Admin admin;

  @BeforeAll
  static void connectToServer() {
    client = connect();
    Properties props = new Properties();
    props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_ADDRESS);
    admin = Admin.create(props);
  }

  @AfterAll
  static void disconnect() {
    if (client != null) client.close();
    if (admin != null) admin.close();
  }

  // ---------- the last N records match what was published ----------

  @Test
  void theLastRecordsOfOnePartitionComeBackNewestFirstAndUnchanged() {
    DemoData.TopicSpec topic = DemoData.POSITIONS;
    List<DemoData.SeedRecord> published = DemoData.seedRecords(topic.name(), 1);

    Map<String, Object> result = call(client, "recent_messages", Map.of("source", "local-kafka", "topic", topic.name(), "partition", 1, "limit", 5));

    assertThat(result).containsEntry("source", "local-kafka").containsEntry("topic", topic.name()).containsEntry("note", "");
    assertThat(number(result.get("limit"))).isEqualTo(5);
    assertThat(number(result.get("returned"))).isEqualTo(5);
    List<Map<String, Object>> messages = messages(result);
    assertThat(messages).hasSize(5);
    for (int i = 0; i < 5; i++) {
      long offset = topic.seededPerPartition() - 1 - i;
      Map<String, Object> message = messages.get(i);
      assertThat(number(message.get("partition"))).isEqualTo(1);
      assertThat(number(message.get("offset"))).as("newest first").isEqualTo(offset);
      assertSameAsPublished(message, published.get((int) offset));
      assertThat(Instant.parse((String) message.get("timestamp"))).isBefore(Instant.now().plusSeconds(5));
    }
  }

  @Test
  void overAllPartitionsEveryRecordIsOneOfTheLastOfItsPartition() {
    DemoData.TopicSpec topic = DemoData.POSITIONS;
    Map<String, Object> result = call(client, "recent_messages", Map.of("source", "local-kafka", "topic", topic.name()));

    assertThat(number(result.get("limit"))).as("default limit").isEqualTo(10);
    List<Map<String, Object>> messages = messages(result);
    assertThat(messages).hasSize(10);
    Instant previous = Instant.MAX;
    for (Map<String, Object> message : messages) {
      int partition = (int) number(message.get("partition"));
      long offset = number(message.get("offset"));
      assertThat(offset).as("within the last 10 of partition " + partition).isBetween(topic.seededPerPartition() - 10L, topic.seededPerPartition() - 1L);
      assertSameAsPublished(message, DemoData.seedRecords(topic.name(), partition).get((int) offset));
      Instant time = Instant.parse((String) message.get("timestamp"));
      assertThat(time).as("newest first").isBeforeOrEqualTo(previous);
      previous = time;
    }
  }

  @Test
  void askingForMoreThanThereIsReturnsEverythingWithHeaders() {
    DemoData.TopicSpec topic = DemoData.ALARMS;
    List<DemoData.SeedRecord> published = DemoData.seedRecords(topic.name(), 0);

    List<Map<String, Object>> messages = messages(call(client, "recent_messages", Map.of("source", "local-kafka", "topic", topic.name(), "limit", 50)));

    assertThat(messages).hasSize(topic.seededTotal());
    for (int i = 0; i < messages.size(); i++) {
      int offset = topic.seededTotal() - 1 - i;
      assertThat(number(messages.get(i).get("offset"))).isEqualTo(offset);
      assertSameAsPublished(messages.get(i), published.get(offset));
    }
    assertThat(headers(messages.get(0))).containsEntry("schema", "alarm.v1").containsEntry("origin", "simulator");
  }

  @Test
  void anEmptyTopicIsAnEmptyListWithANote() {
    Map<String, Object> result = call(client, "recent_messages", Map.of("source", "local-kafka", "topic", DemoData.EMPTY.name()));
    assertThat(messages(result)).isEmpty();
    assertThat(result).containsEntry("note", "The topic holds no messages.");
  }

  // ---------- value formatting, caps and redaction, on the awkward records ----------

  @Test
  void theAwkwardRecordsAreShownSafely() {
    List<Map<String, Object>> messages = messages(call(client, "recent_messages", Map.of("source", "local-kafka", "topic", DemoData.EDGE_CASES.name())));
    assertThat(messages).hasSize(DemoData.EDGE_CASES.seededTotal());
    Map<Long, Map<String, Object>> byOffset = new HashMap<>();
    for (Map<String, Object> m : messages) byOffset.put(number(m.get("offset")), m);

    Map<String, Object> json = byOffset.get((long) DemoData.EDGE_JSON);
    assertThat(json).containsEntry("valueFormat", "json").containsEntry("value", "{\"kind\":\"json\",\"nested\":{\"list\":[1,2,3],\"ok\":true}}")
        .containsEntry("truncated", false).containsEntry("redacted", false).containsEntry("key", "json");
    assertThat(headers(json)).containsEntry("content-type", "application/json");

    Map<String, Object> text = byOffset.get((long) DemoData.EDGE_TEXT_NO_KEY);
    assertThat(text).containsEntry("valueFormat", "text").containsEntry("key", "")
        .containsEntry("value", "Wind shear advisory — 활주로 09");

    Map<String, Object> binary = byOffset.get((long) DemoData.EDGE_BINARY);
    assertThat(binary).containsEntry("valueFormat", "binary").containsEntry("truncated", true);
    assertThat(number(binary.get("valueBytes"))).isEqualTo(256);
    assertThat((String) binary.get("value")).startsWith("00 01 02 03 04").endsWith("(first 64 of 256 bytes)");

    Map<String, Object> oversized = byOffset.get((long) DemoData.EDGE_OVERSIZED);
    assertThat(oversized).containsEntry("valueFormat", "json").containsEntry("truncated", true);
    assertThat(number(oversized.get("valueBytes"))).isGreaterThan(DemoData.OVERSIZED_BLOB_CHARS);
    assertThat(((String) oversized.get("value")).length()).as("a 20 kB value is shown as a short one").isLessThan(700);
    assertThat((String) oversized.get("value")).startsWith("{\"kind\":\"oversized\",\"blob\":\"xxxx").contains("more characters)");

    Map<String, Object> secrets = byOffset.get((long) DemoData.EDGE_SECRETS);
    assertThat(secrets).containsEntry("redacted", true)
        .containsEntry("value", "{\"user\":\"operator1\",\"password\":\"[REDACTED]\",\"apiKey\":\"[REDACTED]\",\"token\":\"[REDACTED]\"}");
    assertThat(messages.toString()).doesNotContain("not-a-real-password", "not-a-real-key", "not-a-real-token");

    Map<String, Object> tombstone = byOffset.get((long) DemoData.EDGE_TOMBSTONE);
    assertThat(tombstone).containsEntry("valueFormat", "none").containsEntry("value", "").containsEntry("key", "tombstone");
    assertThat(number(tombstone.get("valueBytes"))).isZero();
  }

  // ---------- looking changes nothing ----------

  @Test
  void readingLeavesNoConsumerGroupNoCommittedOffsetAndNoNewRecord() throws Exception {
    Set<String> groupsBefore = groups();
    Map<String, Map<TopicPartition, Long>> committedBefore = committed();
    Map<TopicPartition, Long> endBefore = endOffsets();

    for (DemoData.TopicSpec topic : DemoData.TOPICS) {
      call(client, "recent_messages", Map.of("source", "local-kafka", "topic", topic.name(), "limit", 100));
      call(client, "recent_messages", Map.of("source", "local-kafka", "topic", topic.name(), "partition", 0));
    }

    assertThat(groups()).as("no consumer group appeared").isEqualTo(groupsBefore)
        .containsExactlyInAnyOrderElementsOf(DemoData.GROUPS.stream().map(DemoData.GroupSpec::group).toList());
    assertThat(committed()).as("no committed offset moved").isEqualTo(committedBefore);
    assertThat(endOffsets()).as("nothing was written").isEqualTo(endBefore);
    for (DemoData.GroupSpec g : DemoData.GROUPS) {
      assertThat(committedBefore.get(g.group()).values()).as(g.group()).containsOnly(g.committedPerPartition());
    }
  }

  @Test
  void askingAboutATopicThatDoesNotExistDoesNotCreateIt() throws Exception {
    // The test broker creates topics on first use for ordinary clients, so this proves the server asks differently.
    String missing = "demo.never-created";
    assertThat(callExpectingProblem(client, "recent_messages", Map.of("source", "local-kafka", "topic", missing)))
        .contains("There is no topic named '" + missing + "'");
    assertThat(callExpectingProblem(client, "describe_topic", Map.of("source", "local-kafka", "topic", missing))).contains("There is no topic named");

    Thread.sleep(1500); // creation, had it been triggered, would show up within this time
    assertThat(admin.listTopics().names().get(30, TimeUnit.SECONDS)).doesNotContain(missing);
  }

  // ---------- wrong input ----------

  @Test
  void wrongInputIsAnsweredWithWhatWouldWork() {
    String topic = DemoData.POSITIONS.name();
    assertThat(callExpectingProblem(client, "recent_messages", Map.of("source", "local-kafka", "topic", topic, "partition", 9)).lines().toList())
        .containsExactly("Topic '" + topic + "' has no partition 9.", "Its partitions are 0 to 2.");
    assertThat(callExpectingProblem(client, "recent_messages", Map.of("source", "local-kafka", "topic", topic, "limit", 0)))
        .contains("'limit' must be at least 1");

    Map<String, Object> capped = call(client, "recent_messages", Map.of("source", "local-kafka", "topic", topic, "limit", 100000));
    assertThat(number(capped.get("limit"))).isEqualTo(100);
    assertThat(messages(capped)).hasSize(100);
    assertThat((String) capped.get("note")).contains("'limit' was lowered from 100000 to 100");

    Map<String, Object> mqtt = call(client, "recent_messages", Map.of("source", "local-mqtt", "topic", "demo/sensors/#"));
    assertThat((String) mqtt.get("note")).contains("not available yet");
  }

  // ---------- helpers ----------

  /** Key, value and headers of a returned message against the record the simulator published at that position. */
  private static void assertSameAsPublished(Map<String, Object> message, DemoData.SeedRecord published) {
    assertThat(message.get("key")).isEqualTo(published.key() == null ? "" : published.key());
    assertThat(message.get("value")).isEqualTo(new String(published.value(), StandardCharsets.UTF_8));
    assertThat(message).containsEntry("valueFormat", "json").containsEntry("truncated", false).containsEntry("redacted", false);
    assertThat(number(message.get("valueBytes"))).isEqualTo(published.value().length);
    assertThat(headers(message)).isEqualTo(published.headers());
  }

  @SuppressWarnings("unchecked")
  private static List<Map<String, Object>> messages(Map<String, Object> result) {
    return (List<Map<String, Object>>) result.get("messages");
  }

  @SuppressWarnings("unchecked")
  private static Map<String, String> headers(Map<String, Object> message) {
    Map<String, String> headers = new HashMap<>();
    for (Map<String, Object> h : (List<Map<String, Object>>) message.get("headers")) headers.put((String) h.get("name"), (String) h.get("value"));
    return headers;
  }

  private static Set<String> groups() throws Exception {
    Set<String> ids = new TreeSet<>();
    for (GroupListing g : admin.listGroups().all().get(30, TimeUnit.SECONDS)) ids.add(g.groupId());
    return ids;
  }

  private static Map<String, Map<TopicPartition, Long>> committed() throws Exception {
    Map<String, Map<TopicPartition, Long>> all = new HashMap<>();
    for (String group : groups()) {
      Map<TopicPartition, Long> offsets = new HashMap<>();
      for (Map.Entry<TopicPartition, OffsetAndMetadata> e : admin.listConsumerGroupOffsets(group).partitionsToOffsetAndMetadata().get(30, TimeUnit.SECONDS).entrySet()) {
        offsets.put(e.getKey(), e.getValue().offset());
      }
      all.put(group, offsets);
    }
    return all;
  }

  private static Map<TopicPartition, Long> endOffsets() throws Exception {
    Map<TopicPartition, OffsetSpec> ask = new HashMap<>();
    for (DemoData.TopicSpec t : DemoData.TOPICS) {
      for (int p = 0; p < t.partitions(); p++) ask.put(new TopicPartition(t.name(), p), OffsetSpec.latest());
    }
    Map<TopicPartition, Long> ends = new HashMap<>();
    admin.listOffsets(ask).all().get(30, TimeUnit.SECONDS).forEach((tp, info) -> ends.put(tp, info.offset()));
    return ends;
  }
}
