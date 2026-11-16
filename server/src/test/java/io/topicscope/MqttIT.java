package io.topicscope;

import static io.topicscope.McpTestSupport.MQTT_ADDRESS;
import static io.topicscope.McpTestSupport.call;
import static io.topicscope.McpTestSupport.callExpectingProblem;
import static io.topicscope.McpTestSupport.connect;
import static io.topicscope.McpTestSupport.number;
import static org.assertj.core.api.Assertions.assertThat;

import io.modelcontextprotocol.client.McpSyncClient;
import io.topicscope.sim.DemoData;
import io.topicscope.sim.LiveTraffic;
import io.topicscope.sim.Seeder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.MountableFile;

/**
 * The check of Phase 6: the MQTT side of {@code list_topics}, {@code recent_messages} and
 * {@code topic_freshness}, asked through MCP against a real Mosquitto broker.
 *
 * <p>Two parts, like the lag test. On the shared seeded broker only the four retained status messages exist,
 * so the answers are exact. Then on brokers of its own with live traffic: the sensors that report are
 * {@code FRESH}, and the one the simulator silences turns {@code STALE} and then {@code SILENT}.
 */
class MqttIT {

  static final String STATUS_FILTER = "demo/sensors/+/+/status";

  static McpSyncClient client;

  @BeforeAll
  static void connectToServer() {
    client = connect();
  }

  @AfterAll
  static void disconnect() {
    if (client != null) client.close();
  }

  // ---------- seeded broker: exact ----------

  @Test
  @SuppressWarnings("unchecked")
  void theTopicListOfASeededBrokerIsItsFourRetainedTopics() {
    Map<String, Object> result = call(client, "list_topics", Map.of("source", "local-mqtt", "windowSeconds", 1));

    assertThat(result).containsEntry("type", "MQTT");
    assertThat((List<Object>) result.get("topics")).as("the Kafka list stays empty").isEmpty();
    List<Map<String, Object>> observed = (List<Map<String, Object>>) result.get("observed");
    assertThat(observed).extracting(t -> (String) t.get("topic"))
        .containsExactlyElementsOf(DemoData.DEVICES.stream().map(DemoData.Device::statusTopic).sorted().toList());
    assertThat(observed).allSatisfy(t -> {
      assertThat(t).containsEntry("retained", true).containsEntry("lastMessageTime", "");
      assertThat(number(t.get("messages"))).as("nothing is published on a seeded broker").isZero();
      assertThat(number(t.get("qos"))).isEqualTo(1);
    });
    assertThat(number(result.get("matching"))).isEqualTo(DemoData.DEVICES.size());
    assertThat(((Number) result.get("windowSeconds")).doubleValue()).isBetween(1.0, 2.0);
    assertThat((String) result.get("note")).startsWith("An MQTT broker keeps no list of topics.").contains("'#' does not match topics that start with '$'");

    Map<String, Object> filtered = call(client, "list_topics", Map.of("source", "local-mqtt", "windowSeconds", 1, "filter", "demo/sensors/twr-01/#", "nameContains", "WIND"));
    assertThat((List<Map<String, Object>>) filtered.get("observed")).extracting(t -> (String) t.get("topic")).containsExactly(DemoData.DEVICES.get(0).statusTopic());
  }

  @Test
  void recentMessagesOfASeededBrokerAreTheRetainedStatusMessages() {
    Map<String, Object> result = call(client, "recent_messages", Map.of("source", "local-mqtt", "topic", STATUS_FILTER, "windowSeconds", 1));

    assertThat(result).containsEntry("topic", STATUS_FILTER).containsEntry("type", "MQTT");
    Map<String, String> valueByTopic = new HashMap<>();
    for (Map<String, Object> m : messages(result)) {
      assertThat(m).containsEntry("retained", true).containsEntry("valueFormat", "json").containsEntry("key", "").containsEntry("redacted", false);
      assertThat(number(m.get("partition"))).isEqualTo(-1);
      assertThat(number(m.get("offset"))).isEqualTo(-1);
      assertThat(number(m.get("qos"))).isEqualTo(1);
      valueByTopic.put((String) m.get("topic"), (String) m.get("value"));
    }
    Map<String, String> expected = new HashMap<>();
    for (DemoData.Device d : DemoData.DEVICES) expected.put(d.statusTopic(), d.statusJson());
    assertThat(valueByTopic).isEqualTo(expected);
    assertThat((String) result.get("note")).startsWith("An MQTT broker keeps no history.");

    Map<String, Object> nothing = call(client, "recent_messages", Map.of("source", "local-mqtt", "topic", DemoData.DEVICES.get(0).telemetryTopic(), "windowSeconds", 1));
    assertThat(messages(nothing)).isEmpty();
    assertThat((String) nothing.get("note")).contains("Nothing was heard");
  }

  @Test
  void onASeededBrokerEveryTelemetryTopicIsSilent() {
    String topic = DemoData.DEVICES.get(0).telemetryTopic();
    Map<String, Object> exact = call(client, "topic_freshness", Map.of("source", "local-mqtt", "topic", topic, "expectedIntervalSeconds", 1, "windowSeconds", 2));
    assertThat(rows(exact)).hasSize(1);
    assertThat(rows(exact).get(0)).containsEntry("topic", topic).containsEntry("status", "SILENT").containsEntry("lastMessageTime", "").containsEntry("retained", false);
    assertThat(number(exact.get("silent"))).isEqualTo(1);

    // The status topics hold a retained message, so they are listed; nothing is published on them, so they are silent too.
    Map<String, Object> statuses = call(client, "topic_freshness", Map.of("source", "local-mqtt", "topic", STATUS_FILTER, "expectedIntervalSeconds", 1, "windowSeconds", 2));
    assertThat(rows(statuses)).hasSize(DemoData.DEVICES.size()).allSatisfy(r -> assertThat(r).containsEntry("status", "SILENT").containsEntry("retained", true));
    assertThat(brokerCounters(statuses)).as("Mosquitto reports about itself under $SYS").containsKey("$SYS/broker/clients/connected");
  }

  @Test
  void kafkaFreshnessComesWithoutListening() throws Exception {
    Thread.sleep(2000); // so the seeded records are certainly older than one and a half seconds
    long start = System.nanoTime();
    Map<String, Object> strict = call(client, "topic_freshness", Map.of("source", "local-kafka", "expectedIntervalSeconds", 1));
    assertThat((System.nanoTime() - start) / 1_000_000).as("no window to wait for").isLessThan(5000);

    assertThat(strict).containsEntry("type", "KAFKA").containsEntry("note", "");
    assertThat(rows(strict)).extracting(r -> (String) r.get("topic"))
        .containsExactlyElementsOf(DemoData.TOPICS.stream().map(DemoData.TopicSpec::name).sorted().toList());
    for (Map<String, Object> row : rows(strict)) {
      boolean empty = row.get("topic").equals(DemoData.EMPTY.name());
      assertThat(row).as(row.toString()).containsEntry("status", empty ? "SILENT" : "STALE");
    }

    Map<String, Object> relaxed = call(client, "topic_freshness", Map.of("source", "local-kafka", "topic", DemoData.ALARMS.name(), "expectedIntervalSeconds", 3600));
    assertThat(rows(relaxed)).hasSize(1);
    assertThat(rows(relaxed).get(0)).containsEntry("status", "FRESH");
    assertThat(number(rows(relaxed).get(0).get("ageSeconds"))).isBetween(1L, 900L);
  }

  @Test
  void wrongInputIsAnsweredWithWhatWouldWork() {
    assertThat(callExpectingProblem(client, "recent_messages", Map.of("source", "local-mqtt", "topic", "demo/#/telemetry")))
        .contains("is not a valid MQTT topic filter", "'#' for all remaining levels and only at the end");
    assertThat(callExpectingProblem(client, "list_topics", Map.of("source", "local-mqtt", "windowSeconds", 99))).contains("'windowSeconds' must be between 1 and 30");
    assertThat(callExpectingProblem(client, "topic_freshness", Map.of("source", "local-mqtt", "expectedIntervalSeconds", 0))).contains("'expectedIntervalSeconds' must be between");
  }

  @Test
  void listeningLeavesNothingOnTheBroker() throws Exception {
    for (int i = 0; i < 3; i++) {
      call(client, "list_topics", Map.of("source", "local-mqtt", "windowSeconds", 1));
      call(client, "recent_messages", Map.of("source", "local-mqtt", "topic", "demo/#", "windowSeconds", 1));
      call(client, "topic_freshness", Map.of("source", "local-mqtt", "topic", "demo/#", "expectedIntervalSeconds", 1, "windowSeconds", 1));
    }
    Thread.sleep(6000); // Mosquitto refreshes its counters every 5 seconds (sys_interval in mosquitto.conf)

    // Asked with a client of the test's own, not through the server.
    Map<String, String> heard = listen("#", "$SYS/broker/clients/disconnected", "$SYS/broker/clients/connected", "$SYS/broker/subscriptions/count");
    Map<String, String> retained = new HashMap<>(heard);
    retained.keySet().removeIf(t -> t.startsWith("$SYS"));
    Map<String, String> expected = new HashMap<>();
    for (DemoData.Device d : DemoData.DEVICES) expected.put(d.statusTopic(), d.statusJson());

    assertThat(retained).as("the server published nothing: the retained messages are the seeded ones").isEqualTo(expected);
    assertThat(heard.get("$SYS/broker/clients/disconnected")).as("no session was left behind for a client that is gone").isEqualTo("0");
    // The counter is from the broker's last refresh, up to 5 seconds ago: nobody then, or already this test's own client.
    assertThat(heard.get("$SYS/broker/clients/connected")).as("none of the server's nine connections is still open").isIn("0", "1");
  }

  // ---------- live traffic: behaviour ----------

  @Test
  @SuppressWarnings("unchecked")
  void withLiveTrafficTheSilencedSensorTurnsStaleAndThenSilent() throws Exception {
    // Ten times faster than the defaults: a reading every 0.2 s, and the last sensor falls silent 12 s after the start.
    Duration silentAfter = Duration.ofSeconds(12);
    LiveTraffic.Timing fast = new LiveTraffic.Timing(Duration.ofMillis(50), Duration.ofMillis(200), Duration.ofMillis(200), silentAfter);
    List<String> reporting = DemoData.DEVICES.stream().filter(d -> !d.equals(DemoData.SILENT_DEVICE)).map(DemoData.Device::telemetryTopic).toList();
    String silenced = DemoData.SILENT_DEVICE.telemetryTopic();

    try (KafkaContainer kafka = new KafkaContainer("apache/kafka:4.3.2");
        GenericContainer<?> mosquitto = new GenericContainer<>("eclipse-mosquitto:2.0.22").withExposedPorts(1883)
            .withCopyFileToContainer(MountableFile.forHostPath(Path.of("..", "mosquitto", "mosquitto.conf").toAbsolutePath().normalize()),
                "/mosquitto/config/mosquitto.conf")) {
      kafka.start();
      mosquitto.start();
      String kafkaAddress = kafka.getBootstrapServers();
      String mqttAddress = "tcp://" + mosquitto.getHost() + ":" + mosquitto.getMappedPort(1883);
      Seeder.seed(kafkaAddress, mqttAddress);

      try (McpSyncClient liveClient = connect(kafkaAddress, mqttAddress)) {
        long started = System.nanoTime();
        try (LiveTraffic live = LiveTraffic.start(kafkaAddress, mqttAddress, fast)) {

          // While all four report.
          Map<String, Object> early = call(liveClient, "topic_freshness",
              Map.of("source", "local-mqtt", "topic", DemoData.TELEMETRY_FILTER, "expectedIntervalSeconds", 1, "windowSeconds", 3));
          assertThat(secondsSince(started)).as("this look must end before the sensor is silenced").isLessThan(silentAfter.toSeconds() - 1);
          assertThat(rows(early)).hasSize(DemoData.DEVICES.size()).allSatisfy(r -> {
            assertThat(r).as(r.toString()).containsEntry("status", "FRESH").containsEntry("retained", false);
            assertThat(number(r.get("messagesInWindow"))).isGreaterThan(5);
            assertThat(number(r.get("deliveryDelayMillis"))).as("the readings carry their own time").isBetween(0L, 2000L);
          });

          Map<String, Object> messages = call(liveClient, "recent_messages", Map.of("source", "local-mqtt", "topic", DemoData.TELEMETRY_FILTER, "limit", 5, "windowSeconds", 2));
          assertThat(messages(messages)).hasSize(5).allSatisfy(m -> {
            assertThat(m).containsEntry("retained", false).containsEntry("valueFormat", "json");
            assertThat((String) m.get("topic")).endsWith("/telemetry");
            assertThat((String) m.get("value")).contains("\"device\":", "\"ts\":");
          });
          assertThat((String) messages.get("note")).contains("messages arrived; the newest 5 are shown.");

          // A window that covers the moment the sensor falls silent: it was heard, and its last message is old.
          long wait = silentAfter.toSeconds() - 3 - secondsSince(started);
          if (wait > 0) Thread.sleep(wait * 1000);
          Map<String, Object> across = call(liveClient, "topic_freshness",
              Map.of("source", "local-mqtt", "topic", DemoData.TELEMETRY_FILTER, "expectedIntervalSeconds", 1, "windowSeconds", 8));
          Map<String, String> status = new HashMap<>();
          for (Map<String, Object> r : rows(across)) status.put((String) r.get("topic"), (String) r.get("status"));
          assertThat(status).as("the sensor the simulator silenced").containsEntry(silenced, "STALE");
          for (String topic : reporting) assertThat(status).containsEntry(topic, "FRESH");
          Map<String, Object> staleRow = rows(across).stream().filter(r -> silenced.equals(r.get("topic"))).findFirst().orElseThrow();
          assertThat(number(staleRow.get("ageSeconds"))).as("silent for a few seconds by the end of the window").isGreaterThanOrEqualTo(2);
          assertThat(number(across.get("stale"))).isEqualTo(1);
          assertThat(number(across.get("fresh"))).isEqualTo(3);

          // Later it is not heard at all: named exactly it is SILENT, behind a wildcard it is simply missing.
          Map<String, Object> exact = call(liveClient, "topic_freshness", Map.of("source", "local-mqtt", "topic", silenced, "expectedIntervalSeconds", 1, "windowSeconds", 2));
          assertThat(rows(exact)).hasSize(1);
          assertThat(rows(exact).get(0)).containsEntry("status", "SILENT");
          Map<String, Object> wildcard = call(liveClient, "topic_freshness", Map.of("source", "local-mqtt", "topic", DemoData.TELEMETRY_FILTER, "expectedIntervalSeconds", 1, "windowSeconds", 2));
          assertThat(rows(wildcard)).extracting(r -> (String) r.get("topic")).containsExactlyInAnyOrderElementsOf(reporting);
          assertThat((String) wildcard.get("note")).contains("is not listed at all");

          // The topic list shows the three that report, and all four retained status topics.
          Map<String, Object> listed = call(liveClient, "list_topics", Map.of("source", "local-mqtt", "windowSeconds", 2));
          List<Map<String, Object>> observed = (List<Map<String, Object>>) listed.get("observed");
          assertThat(observed).extracting(t -> (String) t.get("topic")).containsAll(reporting).doesNotContain(silenced)
              .containsAll(DemoData.DEVICES.stream().map(DemoData.Device::statusTopic).toList());
          Map<String, Object> one = observed.stream().filter(t -> reporting.get(0).equals(t.get("topic"))).findFirst().orElseThrow();
          assertThat(number(one.get("messages"))).isGreaterThan(3);
          assertThat((String) one.get("lastMessageTime")).isNotEmpty();
        }
      }
    }
  }

  // ---------- helpers ----------

  @SuppressWarnings("unchecked")
  private static List<Map<String, Object>> messages(Map<String, Object> result) {
    return (List<Map<String, Object>>) result.get("messages");
  }

  @SuppressWarnings("unchecked")
  private static List<Map<String, Object>> rows(Map<String, Object> report) {
    return (List<Map<String, Object>>) report.get("topics");
  }

  @SuppressWarnings("unchecked")
  private static Map<String, String> brokerCounters(Map<String, Object> report) {
    Map<String, String> counters = new HashMap<>();
    for (Map<String, Object> c : (List<Map<String, Object>>) report.get("brokerCounters")) counters.put((String) c.get("name"), (String) c.get("value"));
    return counters;
  }

  private static long secondsSince(long startedNanos) {
    return (System.nanoTime() - startedNanos) / 1_000_000_000L;
  }

  /** What the shared broker holds right now, by topic: retained messages and its own counters. */
  private static Map<String, String> listen(String... filters) throws Exception {
    Map<String, String> heard = new ConcurrentHashMap<>();
    MqttClient own = new MqttClient(MQTT_ADDRESS, "it-check-" + System.nanoTime(), new MemoryPersistence());
    MqttConnectOptions options = new MqttConnectOptions();
    options.setCleanSession(true);
    own.connect(options);
    try {
      for (String filter : filters) {
        own.subscribe(filter, 1, (topic, message) -> heard.put(topic, new String(message.getPayload(), StandardCharsets.UTF_8)));
      }
      Thread.sleep(1500);
    } finally {
      own.disconnect();
      own.close();
    }
    return heard;
  }
}
