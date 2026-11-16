package io.topicscope;

import static io.topicscope.McpTestSupport.KAFKA_ADDRESS;
import static io.topicscope.McpTestSupport.MQTT_ADDRESS;
import static io.topicscope.McpTestSupport.call;
import static io.topicscope.McpTestSupport.callExpectingProblem;
import static io.topicscope.McpTestSupport.connect;
import static io.topicscope.McpTestSupport.number;
import static org.assertj.core.api.Assertions.assertThat;

import io.modelcontextprotocol.client.McpSyncClient;
import io.topicscope.sim.DemoData;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * Part of the check of Phase 7, on the shared seeded brokers: the topic allow and deny lists, and the audit log.
 * Each test starts a server of its own with a configuration file of its own, the way the README describes.
 */
class AccessAndAuditIT {

  static final String HIDDEN = "is not available through this server";

  /** A server whose sources carry the given topic rules, written as YAML list bodies such as {@code "[demo.alarms]"}. */
  private static McpSyncClient serverWithRules(Path auditFile, String kafkaAllow, String kafkaDeny, String mqttDeny) throws Exception {
    Path config = Files.createTempFile("topic-scope-rules", ".yml");
    Files.writeString(config, """
        topic-scope:
          sources:
            - name: local-kafka
              type: kafka
              address: %s
              topics:
                allow: %s
                deny: %s
            - name: local-mqtt
              type: mqtt
              address: %s
              topics:
                deny: %s
        """.formatted(KAFKA_ADDRESS, kafkaAllow, kafkaDeny, MQTT_ADDRESS, mqttDeny));
    return connect(Map.of("TOPIC_SCOPE_AUDIT_FILE", auditFile.toString()), "--spring.config.additional-location=file:" + config);
  }

  private static Path newAuditFile() throws Exception {
    return Files.createTempDirectory("topic-scope-audit").resolve("audit.jsonl");
  }

  // ---------- allow and deny ----------

  @Test
  @SuppressWarnings("unchecked")
  void deniedKafkaTopicsAreHiddenEverywhereAndRefusedByName() throws Exception {
    try (McpSyncClient client = serverWithRules(newAuditFile(), "[]", "[demo.alarms, 'demo.edge*']", "[]")) {
      Map<String, Object> list = call(client, "list_topics", Map.of("source", "local-kafka"));
      List<String> names = ((List<Map<String, Object>>) list.get("topics")).stream().map(t -> (String) t.get("name")).toList();
      assertThat(names).doesNotContain(DemoData.ALARMS.name(), DemoData.EDGE_CASES.name())
          .contains(DemoData.POSITIONS.name(), DemoData.TELEMETRY.name(), DemoData.EMPTY.name(), DemoData.DEVICE_STATE.name());
      assertThat((String) list.get("note")).isEqualTo("2 topic(s) were left out by this server's configuration (topics.allow / topics.deny).");

      for (String tool : List.of("describe_topic", "recent_messages")) {
        assertThat(callExpectingProblem(client, tool, Map.of("source", "local-kafka", "topic", DemoData.ALARMS.name()))).as(tool)
            .contains("Topic '" + DemoData.ALARMS.name() + "' on source 'local-kafka' " + HIDDEN, "list_topics shows what is available");
      }
      assertThat(callExpectingProblem(client, "topic_freshness", Map.of("source", "local-kafka", "topic", DemoData.EDGE_CASES.name()))).contains(HIDDEN);
      // The same answer for a name that matches the rule and does not exist: nothing is told about what is behind it.
      assertThat(callExpectingProblem(client, "describe_topic", Map.of("source", "local-kafka", "topic", "demo.edge-of-nothing"))).contains(HIDDEN);
      // A wrong name is not answered with a hidden topic as a suggestion.
      assertThat(callExpectingProblem(client, "describe_topic", Map.of("source", "local-kafka", "topic", "alarms"))).doesNotContain(DemoData.ALARMS.name());

      Map<String, Object> freshness = call(client, "topic_freshness", Map.of("source", "local-kafka", "expectedIntervalSeconds", 3600));
      assertThat(((List<Map<String, Object>>) freshness.get("topics")).stream().map(t -> (String) t.get("topic")).toList())
          .doesNotContain(DemoData.ALARMS.name(), DemoData.EDGE_CASES.name()).contains(DemoData.POSITIONS.name());

      // A group that reads a hidden topic: its lag on that topic is not shown, and not counted.
      Map<String, Object> notifier = call(client, "consumer_lag", Map.of("source", "local-kafka", "group", DemoData.ALARM_NOTIFIER.group(), "sampleSeconds", 1));
      assertThat((List<Object>) notifier.get("partitions")).isEmpty();
      assertThat(number(notifier.get("lag"))).isZero();
      assertThat((String) notifier.get("note")).contains("1 topic(s) this group reads were left out");
      assertThat(callExpectingProblem(client, "consumer_lag", Map.of("source", "local-kafka", "group", DemoData.ALARM_NOTIFIER.group(), "topic", DemoData.ALARMS.name())))
          .contains(HIDDEN);
      // A group on a topic that is available is untouched.
      assertThat(number(call(client, "consumer_lag", Map.of("source", "local-kafka", "group", DemoData.TRACKER.group(), "sampleSeconds", 1)).get("lag")))
          .isEqualTo(DemoData.TRACKER.seededLagTotal());
    }
  }

  @Test
  @SuppressWarnings("unchecked")
  void anAllowListLeavesOnlyWhatItNames() throws Exception {
    try (McpSyncClient client = serverWithRules(newAuditFile(), "['demo.flights.*', demo.empty]", "[]", "[]")) {
      Map<String, Object> list = call(client, "list_topics", Map.of("source", "local-kafka", "includeInternal", true));
      assertThat(((List<Map<String, Object>>) list.get("topics")).stream().map(t -> (String) t.get("name")).toList())
          .containsExactly(DemoData.EMPTY.name(), DemoData.POSITIONS.name());
      assertThat(callExpectingProblem(client, "recent_messages", Map.of("source", "local-kafka", "topic", DemoData.TELEMETRY.name()))).contains(HIDDEN);
      assertThat((List<Object>) call(client, "recent_messages", Map.of("source", "local-kafka", "topic", DemoData.POSITIONS.name(), "limit", 2)).get("messages")).hasSize(2);
    }
  }

  @Test
  @SuppressWarnings("unchecked")
  void deniedMqttTopicsAreDroppedOnArrivalAndRefusedByName() throws Exception {
    DemoData.Device tower = DemoData.DEVICES.get(0); // twr-01/wind
    try (McpSyncClient client = serverWithRules(newAuditFile(), "[]", "[]", "['demo/sensors/twr-01/*']")) {
      Map<String, Object> messages = call(client, "recent_messages", Map.of("source", "local-mqtt", "topic", "demo/sensors/+/+/status", "windowSeconds", 1));
      List<String> topics = ((List<Map<String, Object>>) messages.get("messages")).stream().map(m -> (String) m.get("topic")).toList();
      assertThat(topics).containsExactlyInAnyOrder(DemoData.DEVICES.get(2).statusTopic(), DemoData.DEVICES.get(3).statusTopic());
      assertThat((String) messages.get("note")).contains("2 message(s) on other topics arrived and were dropped unread");
      assertThat(messages.toString()).doesNotContain("twr-01");

      Map<String, Object> list = call(client, "list_topics", Map.of("source", "local-mqtt", "windowSeconds", 1));
      assertThat(((List<Map<String, Object>>) list.get("observed")).stream().map(t -> (String) t.get("topic")).toList())
          .hasSize(2).noneMatch(t -> t.contains("twr-01"));

      assertThat(callExpectingProblem(client, "recent_messages", Map.of("source", "local-mqtt", "topic", tower.statusTopic()))).contains(HIDDEN);
      assertThat(callExpectingProblem(client, "topic_freshness", Map.of("source", "local-mqtt", "topic", tower.telemetryTopic(), "expectedIntervalSeconds", 1, "windowSeconds", 1)))
          .contains(HIDDEN);
    }
  }

  // ---------- audit log ----------

  @Test
  void everyCallIsRecordedWithItsArgumentsAndWithoutAnyContent() throws Exception {
    Path audit = newAuditFile();
    try (McpSyncClient client = serverWithRules(audit, "[]", "[demo.alarms]", "[]")) {
      call(client, "list_sources", Map.of());
      call(client, "list_topics", Map.of("source", "local-kafka", "nameContains", "flights"));
      call(client, "describe_topic", Map.of("source", "local-kafka", "topic", DemoData.POSITIONS.name()));
      call(client, "recent_messages", Map.of("source", "local-kafka", "topic", DemoData.EDGE_CASES.name(), "limit", 6));
      call(client, "list_consumer_groups", Map.of("source", "local-kafka"));
      call(client, "consumer_lag", Map.of("source", "local-kafka", "group", DemoData.TRACKER.group(), "sampleSeconds", 1));
      call(client, "topic_freshness", Map.of("source", "local-mqtt", "topic", "demo/#", "expectedIntervalSeconds", 5, "windowSeconds", 1));
      callExpectingProblem(client, "recent_messages", Map.of("source", "local-kafka", "topic", DemoData.ALARMS.name()));
      callExpectingProblem(client, "describe_topic", Map.of("source", "nowhere", "topic", "x"));
    }

    List<String> lines = Files.readAllLines(audit);
    assertThat(lines).hasSize(9).allSatisfy(line -> assertThat(line).startsWith("{\"time\":\"20").contains("\"tool\":", "\"arguments\":", "\"outcome\":", "\"millis\":"));
    assertThat(lines).extracting(line -> line.replaceAll(".*\"tool\":\"([a-z_]+)\".*\"outcome\":\"([a-z]+)\".*", "$1 $2")).containsExactly(
        "list_sources ok", "list_topics ok", "describe_topic ok", "recent_messages ok", "list_consumer_groups ok", "consumer_lag ok",
        "topic_freshness ok", "recent_messages refused", "describe_topic refused");
    assertThat(lines.get(1)).contains("\"arguments\":{\"source\":\"local-kafka\",\"nameContains\":\"flights\"}");
    assertThat(lines.get(3)).contains("\"topic\":\"" + DemoData.EDGE_CASES.name() + "\"", "\"limit\":6");
    assertThat(lines.get(7)).contains("\"problem\":\"Topic '" + DemoData.ALARMS.name() + "' on source 'local-kafka' " + HIDDEN);
    assertThat(lines.get(8)).contains("There is no source named 'nowhere'");

    String everything = String.join("\n", lines);
    assertThat(everything).as("no message value, key or header of the six edge-case records").doesNotContain("not-a-real", "operator1", "Wind shear", "REDACTED", "xxxx");
    assertThat(PosixFilePermissions.toString(Files.getPosixFilePermissions(audit))).as("readable by the owner only").isEqualTo("rw-------");
  }

  @Test
  void aCallThatCannotBeRecordedIsRefusedUnlessRecordingIsSwitchedOff() throws Exception {
    Path occupied = Files.createTempFile("topic-scope-not-a-folder", "");
    String unusable = occupied.resolve("audit.jsonl").toString();

    try (McpSyncClient strict = connect(Map.of("TOPIC_SCOPE_AUDIT_FILE", unusable))) {
      for (String tool : List.of("list_sources", "list_topics")) {
        assertThat(callExpectingProblem(strict, tool, tool.equals("list_sources") ? Map.of() : Map.of("source", "local-kafka")))
            .contains("This call was not answered because it could not be recorded in the audit log", "topic-scope.audit.enabled=false");
      }
    }
    try (McpSyncClient relaxed = connect(Map.of("TOPIC_SCOPE_AUDIT_FILE", unusable, "TOPIC_SCOPE_AUDIT", "false"))) {
      assertThat(number(call(relaxed, "list_sources", Map.of()).get("reachable"))).isEqualTo(2);
    }
  }
}
