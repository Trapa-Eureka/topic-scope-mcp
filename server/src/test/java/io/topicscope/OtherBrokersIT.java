package io.topicscope;

import static io.topicscope.McpTestSupport.KAFKA_ADDRESS;
import static io.topicscope.McpTestSupport.MQTT_ADDRESS;
import static io.topicscope.McpTestSupport.call;
import static io.topicscope.McpTestSupport.callExpectingProblem;
import static io.topicscope.McpTestSupport.connect;
import static io.topicscope.McpTestSupport.number;
import static io.topicscope.McpTestSupport.row;
import static org.assertj.core.api.Assertions.assertThat;

import io.modelcontextprotocol.client.McpSyncClient;
import io.topicscope.sim.DemoData;
import io.topicscope.sim.Seeder;
import java.net.ServerSocket;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.kafka.KafkaContainer;

/**
 * The same demo data on brokers other than the two the rest of the tests use (Apache Kafka 4.3, Mosquitto 2.0):
 * an older and a newer Apache Kafka, Redpanda, which speaks the Kafka protocol without being Kafka, and two
 * other MQTT brokers, EMQX and HiveMQ Community Edition.
 *
 * <p>Each broker is filled by the same seeder, so the answers must be the ones the other tests expect, and
 * what the server shows of the awkward records must be exactly what it shows on the usual broker.
 */
class OtherBrokersIT {

  static final Instant STARTED = Instant.now();

  /** What must be the same on every broker: where the record is, and how its key and value are shown. */
  static final List<String> COMPARED = List.of("partition", "offset", "key", "value", "valueFormat", "valueBytes", "truncated", "redacted", "headers");

  record KafkaLike(String name, Supplier<GenericContainer<?>> container, int[] hostPort) {
    @Override
    public String toString() {
      return name;
    }
  }

  static Stream<Arguments> kafkaLikeBrokers() {
    int[] redpandaPort = {0};
    return Stream.of(
        Arguments.of(new KafkaLike("Apache Kafka 3.9", () -> new KafkaContainer("apache/kafka:3.9.2"), null)),
        Arguments.of(new KafkaLike("Apache Kafka 4.4", () -> new KafkaContainer("apache/kafka:4.4.0"), null)),
        Arguments.of(new KafkaLike("Redpanda 26.2", () -> redpanda(redpandaPort), redpandaPort)));
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("kafkaLikeBrokers")
  @SuppressWarnings("unchecked")
  void theKafkaToolsGiveTheSameAnswers(KafkaLike broker) throws Exception {
    try (GenericContainer<?> container = broker.container().get()) {
      container.start();
      String address = container instanceof KafkaContainer kafka ? kafka.getBootstrapServers() : "localhost:" + broker.hostPort()[0];
      seed(address, MQTT_ADDRESS);

      try (McpSyncClient usual = connect(); McpSyncClient client = connect(address, MQTT_ADDRESS)) {
        assertThat(row(call(client, "list_sources", Map.of()), "sources", "local-kafka")).containsEntry("reachable", true);

        // The catalog, the sizes, and when each topic was last written to.
        Map<String, Object> topics = call(client, "list_topics", Map.of("source", "local-kafka"));
        assertThat(((List<Map<String, Object>>) topics.get("topics"))).extracting(t -> (String) t.get("name"))
            .containsExactlyElementsOf(DemoData.TOPICS.stream().map(DemoData.TopicSpec::name).sorted().toList());
        for (DemoData.TopicSpec expected : DemoData.TOPICS) {
          Map<String, Object> topic = row(topics, "topics", expected.name());
          assertThat(number(topic.get("partitions"))).as(expected.name()).isEqualTo(expected.partitions());
          assertThat(number(topic.get("messages"))).as(expected.name()).isEqualTo(expected.seededTotal());
          if (expected.seededTotal() == 0) {
            assertThat(topic).as(expected.name()).containsEntry("newestMessageTime", "");
          } else {
            assertThat(Instant.parse((String) topic.get("newestMessageTime"))).as(expected.name() + ": time of the newest message")
                .isBetween(STARTED.minusSeconds(60), Instant.now().plusSeconds(5));
          }
        }

        Map<String, Object> compacted = call(client, "describe_topic", Map.of("source", "local-kafka", "topic", DemoData.DEVICE_STATE.name()));
        assertThat((String) compacted.get("note")).contains("This topic is compacted");
        assertThat(row(compacted, "settings", "cleanup.policy")).containsEntry("value", "compact").containsEntry("overridden", true);
        assertThat(callExpectingProblem(client, "describe_topic", Map.of("source", "local-kafka", "topic", "demo.alarm")))
            .contains("Did you mean: " + DemoData.ALARMS.name());

        // Every awkward record is shown exactly as on the usual broker: binary, oversized, secret-looking, no key, no value.
        for (DemoData.TopicSpec topic : List.of(DemoData.EDGE_CASES, DemoData.ALARMS, DemoData.DEVICE_STATE, DemoData.EMPTY)) {
          Map<String, Object> asked = Map.of("source", "local-kafka", "topic", topic.name(), "limit", 20);
          assertThat(shown(call(client, "recent_messages", asked))).as(topic.name()).isEqualTo(shown(call(usual, "recent_messages", asked)));
        }

        // The groups and how far behind each one is.
        List<Map<String, Object>> groups = (List<Map<String, Object>>) call(client, "list_consumer_groups", Map.of("source", "local-kafka")).get("groups");
        assertThat(groups).extracting(g -> (String) g.get("group"))
            .containsExactlyElementsOf(DemoData.GROUPS.stream().map(DemoData.GroupSpec::group).sorted().toList());
        assertThat(groups).allSatisfy(g -> assertThat(g).containsEntry("state", "EMPTY"));
        for (DemoData.GroupSpec expected : DemoData.GROUPS) {
          Map<String, Object> asked = Map.of("source", "local-kafka", "group", expected.group(), "sampleSeconds", 1);
          Map<String, Object> lag = call(client, "consumer_lag", asked);
          assertThat(number(lag.get("lag"))).as(expected.group()).isEqualTo(expected.seededLagTotal());
          assertThat(lag.get("status")).as(expected.group()).isEqualTo(call(usual, "consumer_lag", asked).get("status"));
        }

        // Fresh or not is decided from the newest message time, which is the answer one of these brokers cannot give directly.
        Map<String, Object> freshness = call(client, "topic_freshness", Map.of("source", "local-kafka", "expectedIntervalSeconds", 3600));
        assertThat(number(freshness.get("fresh"))).isEqualTo(DemoData.TOPICS.stream().filter(t -> t.seededTotal() > 0).count());
        assertThat(number(freshness.get("silent"))).isEqualTo(1);
      }
    }
  }

  @Test
  @SuppressWarnings("unchecked")
  void emqxRefusesTheWidestFilterByDefaultAndTheServerSaysSoInsteadOfReportingSilence() throws Exception {
    try (GenericContainer<?> emqx = new GenericContainer<>("emqx/emqx:6.3.3").withExposedPorts(1883)
        .waitingFor(Wait.forLogMessage(".*EMQX.* is running now!.*", 1))) {
      emqx.start();
      String address = "tcp://" + emqx.getHost() + ":" + emqx.getMappedPort(1883);
      seed(KAFKA_ADDRESS, address);

      try (McpSyncClient client = connect(KAFKA_ADDRESS, address)) {
        // Out of the box EMQX lets a client from another machine listen neither to '#' nor to '$SYS/#'.
        Map<String, Object> source = row(call(client, "list_sources", Map.of()), "sources", "local-mqtt");
        assertThat(source).as("refusing its version topic is not being unreachable").containsEntry("reachable", true);
        assertThat((String) source.get("detail")).startsWith("connected");

        assertThat(callExpectingProblem(client, "list_topics", Map.of("source", "local-mqtt", "windowSeconds", 1)))
            .contains("refused the subscription to '#', so nothing was listened to", "ask for a narrower filter");
        // With the broker's counters asked for as well, the refusal comes back inside a longer answer and must not get lost.
        assertThat(callExpectingProblem(client, "topic_freshness", Map.of("source", "local-mqtt", "expectedIntervalSeconds", 5, "windowSeconds", 1)))
            .contains("refused the subscription to '#'");

        theSeededStatusMessagesAreHeard(client, "demo/#");
        Map<String, Object> freshness = call(client, "topic_freshness",
            Map.of("source", "local-mqtt", "topic", "demo/#", "expectedIntervalSeconds", 5, "windowSeconds", 1));
        assertThat((List<Object>) freshness.get("topics")).hasSize(DemoData.DEVICES.size());
        assertThat((List<Object>) freshness.get("brokerCounters")).as("refused, and the rest of the answer is still given").isEmpty();
      }
    }
  }

  @Test
  void hiveMqAnswersLikeMosquittoExceptThatItSaysNothingAboutItself() throws Exception {
    try (GenericContainer<?> hivemq = new GenericContainer<>("hivemq/hivemq-ce:2026.5").withExposedPorts(1883)
        .waitingFor(Wait.forLogMessage(".*Started HiveMQ in.*", 1))) {
      hivemq.start();
      String address = "tcp://" + hivemq.getHost() + ":" + hivemq.getMappedPort(1883);
      seed(KAFKA_ADDRESS, address);

      try (McpSyncClient client = connect(KAFKA_ADDRESS, address)) {
        Map<String, Object> source = row(call(client, "list_sources", Map.of()), "sources", "local-mqtt");
        assertThat(source).containsEntry("reachable", true);
        assertThat((String) source.get("detail")).as("no $SYS topics").startsWith("connected (the broker does not announce a version");

        theSeededStatusMessagesAreHeard(client, "#");
        Map<String, Object> freshness = call(client, "topic_freshness", Map.of("source", "local-mqtt", "expectedIntervalSeconds", 5, "windowSeconds", 1));
        assertThat((List<?>) freshness.get("topics")).hasSize(DemoData.DEVICES.size());
        assertThat((List<?>) freshness.get("brokerCounters")).isEmpty();
      }
    }
  }

  // ---------- helpers ----------

  @SuppressWarnings("unchecked")
  private static void theSeededStatusMessagesAreHeard(McpSyncClient client, String filter) {
    List<String> statusTopics = DemoData.DEVICES.stream().map(DemoData.Device::statusTopic).sorted().toList();

    List<Map<String, Object>> observed = (List<Map<String, Object>>) call(client, "list_topics",
        Map.of("source", "local-mqtt", "filter", filter, "windowSeconds", 1)).get("observed");
    assertThat(observed).extracting(t -> (String) t.get("topic")).containsExactlyElementsOf(statusTopics);
    assertThat(observed).allSatisfy(t -> assertThat(t).containsEntry("retained", true));

    List<Map<String, Object>> heard = (List<Map<String, Object>>) call(client, "recent_messages",
        Map.of("source", "local-mqtt", "topic", filter, "windowSeconds", 1)).get("messages");
    assertThat(heard).extracting(m -> (String) m.get("topic")).containsExactlyInAnyOrderElementsOf(statusTopics);
    for (DemoData.Device device : DemoData.DEVICES) {
      assertThat(heard).filteredOn(m -> device.statusTopic().equals(m.get("topic"))).singleElement()
          .satisfies(m -> assertThat(m).containsEntry("value", device.statusJson()).containsEntry("valueFormat", "json").containsEntry("retained", true));
    }
  }

  /** The parts of each message that do not depend on when the broker was filled. */
  @SuppressWarnings("unchecked")
  private static List<Map<String, Object>> shown(Map<String, Object> recentMessages) {
    return ((List<Map<String, Object>>) recentMessages.get("messages")).stream().map(m -> {
      Map<String, Object> kept = new java.util.TreeMap<>();
      for (String field : COMPARED) {
        assertThat(m).containsKey(field);
        kept.put(field, m.get(field));
      }
      return (Map<String, Object>) kept;
    }).toList();
  }

  /** A broker that says it has started may still refuse its first requests for a moment. */
  private static void seed(String kafkaAddress, String mqttAddress) throws Exception {
    long deadline = System.nanoTime() + 60_000_000_000L;
    while (true) {
      try {
        Seeder.seed(kafkaAddress, mqttAddress);
        return;
      } catch (IllegalStateException e) {
        throw e; // half seeded: waiting does not help
      } catch (Exception e) {
        if (System.nanoTime() > deadline) throw e;
        Thread.sleep(1000);
      }
    }
  }

  /** Redpanda in its single-node development mode, reachable from the host under one fixed address. */
  private static GenericContainer<?> redpanda(int[] hostPort) {
    try (ServerSocket socket = new ServerSocket(0)) {
      hostPort[0] = socket.getLocalPort();
    } catch (java.io.IOException e) {
      throw new IllegalStateException(e);
    }
    GenericContainer<?> redpanda = new GenericContainer<>("redpandadata/redpanda:v26.2.7")
        .withExposedPorts(9092)
        .withCommand("redpanda", "start", "--mode", "dev-container", "--smp", "1", "--memory", "1G", "--overprovisioned",
            "--kafka-addr", "0.0.0.0:9092", "--advertise-kafka-addr", "localhost:" + hostPort[0])
        .waitingFor(Wait.forLogMessage(".*Successfully started Redpanda!.*", 1));
    redpanda.setPortBindings(List.of("127.0.0.1:" + hostPort[0] + ":9092"));
    return redpanda;
  }
}
