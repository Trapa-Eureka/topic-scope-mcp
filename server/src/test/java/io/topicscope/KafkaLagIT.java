package io.topicscope;

import static io.topicscope.McpTestSupport.call;
import static io.topicscope.McpTestSupport.callExpectingProblem;
import static io.topicscope.McpTestSupport.connect;
import static io.topicscope.McpTestSupport.number;
import static org.assertj.core.api.Assertions.assertThat;

import io.modelcontextprotocol.client.McpSyncClient;
import io.topicscope.sim.DemoData;
import io.topicscope.sim.LiveTraffic;
import io.topicscope.sim.Seeder;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.MountableFile;

/**
 * The check of Phase 5: {@code list_consumer_groups} and {@code consumer_lag}, asked through MCP.
 *
 * <p>Two parts. On the shared seeded brokers nothing moves, so every number is exact and comes from
 * {@link DemoData}. Then on brokers of its own with live traffic, where the numbers are not exact but the
 * behaviour is: the slow consumer is {@code LAGGING} and the stopped one is {@code STALLED}.
 */
class KafkaLagIT {

  static McpSyncClient client;

  @BeforeAll
  static void connectToServer() {
    client = connect();
  }

  @AfterAll
  static void disconnect() {
    if (client != null) client.close();
  }

  // ---------- seeded brokers: exact ----------

  @Test
  @SuppressWarnings("unchecked")
  void theSeededGroupsAreListedAsEmpty() {
    Map<String, Object> result = call(client, "list_consumer_groups", Map.of("source", "local-kafka"));

    List<Map<String, Object>> groups = (List<Map<String, Object>>) result.get("groups");
    assertThat(groups).extracting(g -> (String) g.get("group"))
        .containsExactlyElementsOf(DemoData.GROUPS.stream().map(DemoData.GroupSpec::group).sorted().toList());
    assertThat(groups).allSatisfy(g -> {
      assertThat(g).as("committed offsets, nobody connected").containsEntry("state", "EMPTY");
      assertThat(number(g.get("members"))).isZero();
    });
    assertThat(number(result.get("matching"))).isEqualTo(DemoData.GROUPS.size());
    assertThat(result).containsEntry("note", "");

    Map<String, Object> filtered = call(client, "list_consumer_groups", Map.of("source", "local-kafka", "nameContains", "TRACK"));
    assertThat((List<Map<String, Object>>) filtered.get("groups")).extracting(g -> (String) g.get("group")).containsExactly(DemoData.TRACKER.group());
  }

  @Test
  @SuppressWarnings("unchecked")
  void everySeededGroupHasExactlyTheDeclaredLag() {
    for (DemoData.GroupSpec expected : DemoData.GROUPS) {
      Map<String, Object> report = call(client, "consumer_lag", Map.of("source", "local-kafka", "group", expected.group(), "sampleSeconds", 1));

      assertThat(report).as(expected.group()).containsEntry("group", expected.group()).containsEntry("state", "EMPTY").containsEntry("note", "");
      assertThat(number(report.get("members"))).isZero();
      assertThat(number(report.get("lag"))).as(expected.group() + " lag").isEqualTo(expected.seededLagTotal());
      assertThat(number(report.get("lagChange"))).as("nothing moves on a seeded broker").isZero();
      assertThat(((Number) report.get("sampleSeconds")).doubleValue()).isBetween(1.0, 3.0);

      List<Map<String, Object>> partitions = (List<Map<String, Object>>) report.get("partitions");
      assertThat(partitions).hasSize(expected.topic().partitions());
      for (int p = 0; p < partitions.size(); p++) {
        Map<String, Object> partition = partitions.get(p);
        assertThat(partition).containsEntry("topic", expected.topic().name());
        assertThat(number(partition.get("partition"))).isEqualTo(p);
        assertThat(number(partition.get("committedOffset"))).isEqualTo(expected.committedPerPartition());
        assertThat(number(partition.get("endOffset"))).isEqualTo(expected.topic().seededPerPartition());
        assertThat(number(partition.get("lag"))).isEqualTo(expected.seededLagPerPartition());
      }

      Map<String, Object> topic = ((List<Map<String, Object>>) report.get("topics")).get(0);
      assertThat(number(topic.get("consumed"))).isZero();
      assertThat(number(topic.get("arrived"))).isZero();
      if (expected.seededLagTotal() == 0) {
        assertThat(report).containsEntry("status", "OK").containsEntry("summary", "Caught up: nothing is waiting.");
        assertThat(number(topic.get("oldestUnreadAgeSeconds"))).isEqualTo(-1);
      } else {
        // A backlog, no members, nothing arriving: nobody is working on it.
        assertThat(report).containsEntry("status", "NO_CONSUMER");
        assertThat((String) report.get("summary")).startsWith("Nobody is reading: " + expected.seededLagTotal() + " waiting");
        assertThat(number(topic.get("oldestUnreadAgeSeconds"))).as("the seeded records are a few seconds to minutes old").isBetween(0L, 900L);
      }
    }
  }

  @Test
  void wrongNamesAreAnsweredWithTheRightOnes() {
    assertThat(callExpectingProblem(client, "consumer_lag", Map.of("source", "local-kafka", "group", "tracker")).lines().toList())
        .containsExactly("There is no consumer group named 'tracker' on source 'local-kafka'.", "Did you mean: " + DemoData.TRACKER.group() + ".");
    assertThat(callExpectingProblem(client, "consumer_lag", Map.of("source", "local-kafka", "group", DemoData.TRACKER.group(), "topic", DemoData.ALARMS.name())))
        .contains("has no committed offset on a topic named '" + DemoData.ALARMS.name() + "'", "Its topics are: " + DemoData.POSITIONS.name());
    assertThat(callExpectingProblem(client, "consumer_lag", Map.of("source", "local-kafka", "group", DemoData.TRACKER.group(), "sampleSeconds", 99)))
        .contains("'sampleSeconds' must be between 1 and 30");
    assertThat(callExpectingProblem(client, "list_consumer_groups", Map.of("source", "local-mqtt"))).contains("is an MQTT source");
  }

  @Test
  @SuppressWarnings("unchecked")
  void measuringLagDoesNotCreateAGroupOrMoveOne() {
    for (DemoData.GroupSpec g : DemoData.GROUPS) call(client, "consumer_lag", Map.of("source", "local-kafka", "group", g.group(), "sampleSeconds", 1));

    // Asked again afterwards: the same groups, the same committed offsets. The age lookup read records without a group.
    List<Map<String, Object>> groups = (List<Map<String, Object>>) call(client, "list_consumer_groups", Map.of("source", "local-kafka")).get("groups");
    assertThat(groups).extracting(g -> (String) g.get("group"))
        .containsExactlyElementsOf(DemoData.GROUPS.stream().map(DemoData.GroupSpec::group).sorted().toList());
    for (DemoData.GroupSpec g : DemoData.GROUPS) {
      Map<String, Object> report = call(client, "consumer_lag", Map.of("source", "local-kafka", "group", g.group(), "sampleSeconds", 1));
      assertThat(number(report.get("lag"))).isEqualTo(g.seededLagTotal());
    }
  }

  // ---------- live traffic: behaviour ----------

  @Test
  @SuppressWarnings("unchecked")
  void withLiveTrafficTheSlowConsumerIsLaggingAndTheStoppedOneIsStalled() throws Exception {
    // Ten times faster than the defaults, same proportions: 20 positions a second arrive, the tracker reads 5.
    LiveTraffic.Timing fast = new LiveTraffic.Timing(Duration.ofMillis(50), Duration.ofMillis(200), Duration.ofMillis(200), Duration.ofMinutes(10));

    try (KafkaContainer kafka = new KafkaContainer("apache/kafka:4.3.2");
        GenericContainer<?> mosquitto = new GenericContainer<>("eclipse-mosquitto:2.0.22").withExposedPorts(1883)
            .withCopyFileToContainer(MountableFile.forHostPath(Path.of("..", "mosquitto", "open.conf").toAbsolutePath().normalize()),
                "/mosquitto/config/mosquitto.conf")) {
      kafka.start();
      mosquitto.start();
      String kafkaAddress = kafka.getBootstrapServers();
      String mqttAddress = "tcp://" + mosquitto.getHost() + ":" + mosquitto.getMappedPort(1883);
      Seeder.seed(kafkaAddress, mqttAddress);

      try (LiveTraffic live = LiveTraffic.start(kafkaAddress, mqttAddress, fast);
          McpSyncClient liveClient = connect(kafkaAddress, mqttAddress)) {

        // Both consumers have to join their groups before their behaviour means anything.
        Map<String, Object> listed = null;
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(60);
        while (System.nanoTime() < deadline) {
          listed = call(liveClient, "list_consumer_groups", Map.of("source", "local-kafka"));
          if (members(listed, DemoData.TRACKER) == 1 && members(listed, DemoData.ARCHIVER) == 1) break;
          Thread.sleep(500);
        }
        assertThat(groupRow(listed, DemoData.TRACKER)).containsEntry("state", "STABLE");
        assertThat(groupRow(listed, DemoData.REPORTER)).containsEntry("state", "EMPTY");
        Thread.sleep(2000);

        Map<String, Object> archiver = lag(liveClient, DemoData.ARCHIVER);
        assertThat(archiver).as("keeps up: %s", archiver.get("summary")).containsEntry("status", "OK");
        assertThat(number(topic(archiver).get("consumed"))).isGreaterThan(20);

        Map<String, Object> tracker = lag(liveClient, DemoData.TRACKER);
        assertThat(tracker).as("slow consumer: %s", tracker.get("summary")).containsEntry("status", "LAGGING").containsEntry("state", "STABLE");
        assertThat(number(tracker.get("members"))).isEqualTo(1);
        assertThat(number(topic(tracker).get("consumed"))).as("it does read").isGreaterThan(0);
        assertThat(number(topic(tracker).get("arrived"))).as("more arrives than it reads").isGreaterThan(number(topic(tracker).get("consumed")));
        assertThat(number(tracker.get("lagChange"))).as("so it falls further behind").isGreaterThan(0);
        assertThat(number(tracker.get("lag"))).isGreaterThan(DemoData.TRACKER.seededLagTotal());
        assertThat((String) tracker.get("summary")).startsWith("Reading, but behind:").contains("so the lag grew by");
        assertThat(number(topic(tracker).get("oldestUnreadAgeSeconds"))).isGreaterThanOrEqualTo(0);

        Map<String, Object> reporter = lag(liveClient, DemoData.REPORTER);
        assertThat(reporter).as("stopped consumer: %s", reporter.get("summary")).containsEntry("status", "STALLED").containsEntry("state", "EMPTY");
        assertThat(number(topic(reporter).get("consumed"))).as("its committed offsets stand still").isZero();
        assertThat(number(topic(reporter).get("arrived"))).as("while messages keep arriving").isGreaterThan(20);
        assertThat(number(reporter.get("lag"))).isGreaterThan(DemoData.REPORTER.seededLagTotal());
        List<Map<String, Object>> partitions = (List<Map<String, Object>>) reporter.get("partitions");
        assertThat(partitions).allSatisfy(p -> assertThat(number(p.get("committedOffset"))).isEqualTo(DemoData.REPORTER.committedPerPartition()));

        Map<String, Object> notifier = lag(liveClient, DemoData.ALARM_NOTIFIER);
        assertThat(notifier).as("a backlog on a topic where nothing happens").containsEntry("status", "NO_CONSUMER");
        assertThat(number(notifier.get("lag"))).isEqualTo(DemoData.ALARM_NOTIFIER.seededLagTotal());
      }
    }
  }

  // ---------- helpers ----------

  private static Map<String, Object> lag(McpSyncClient c, DemoData.GroupSpec group) {
    return call(c, "consumer_lag", Map.of("source", "local-kafka", "group", group.group(), "sampleSeconds", 3));
  }

  @SuppressWarnings("unchecked")
  private static Map<String, Object> topic(Map<String, Object> report) {
    return ((List<Map<String, Object>>) report.get("topics")).get(0);
  }

  @SuppressWarnings("unchecked")
  private static Map<String, Object> groupRow(Map<String, Object> listed, DemoData.GroupSpec group) {
    return ((List<Map<String, Object>>) listed.get("groups")).stream().filter(g -> group.group().equals(g.get("group"))).findFirst()
        .orElseThrow(() -> new AssertionError("no group " + group.group() + " in " + listed));
  }

  private static long members(Map<String, Object> listed, DemoData.GroupSpec group) {
    return number(groupRow(listed, group).get("members"));
  }
}
