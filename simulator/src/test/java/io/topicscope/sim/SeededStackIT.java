package io.topicscope.sim;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.MountableFile;

/**
 * The check of Phase 1: after seeding, real brokers hold exactly what {@link DemoData} says, and live
 * traffic behaves as documented. Starts Kafka and Mosquitto in Docker, with the images and the Mosquitto
 * configuration that compose.yaml uses.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class SeededStackIT {

  static final String KAFKA_IMAGE = "apache/kafka:4.3.2";
  static final String MOSQUITTO_IMAGE = "eclipse-mosquitto:2.0.22";

  static final KafkaContainer kafka = new KafkaContainer(KAFKA_IMAGE);
  static final GenericContainer<?> mosquitto = new GenericContainer<>(MOSQUITTO_IMAGE)
      .withExposedPorts(1883)
      .withCopyFileToContainer(MountableFile.forHostPath(Path.of("..", "mosquitto", "mosquitto.conf").toAbsolutePath().normalize()),
          "/mosquitto/config/mosquitto.conf");

  static String bootstrap;
  static String mqttUri;
  static Admin admin;

  @BeforeAll
  static void startBrokersAndSeed() throws Exception {
    kafka.start();
    mosquitto.start();
    bootstrap = kafka.getBootstrapServers();
    mqttUri = "tcp://" + mosquitto.getHost() + ":" + mosquitto.getMappedPort(1883);

    Properties props = new Properties();
    props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
    admin = Admin.create(props);

    assertThat(Seeder.seed(bootstrap, mqttUri)).isEqualTo(Seeder.Result.SEEDED);
  }

  @AfterAll
  static void stop() {
    if (admin != null) admin.close();
    mosquitto.stop();
    kafka.stop();
  }

  // ---------- seeded state: exact ----------

  @Test
  @Order(1)
  void topicsExistWithTheDeclaredPartitionsAndRecordCounts() throws Exception {
    Set<String> names = admin.listTopics().names().get();
    assertThat(names).containsExactlyInAnyOrderElementsOf(DemoData.TOPICS.stream().map(DemoData.TopicSpec::name).toList());

    Map<String, TopicDescription> described = admin.describeTopics(names).allTopicNames().get();
    for (DemoData.TopicSpec t : DemoData.TOPICS) {
      assertThat(described.get(t.name()).partitions()).as(t.name()).hasSize(t.partitions());
      for (int p = 0; p < t.partitions(); p++) {
        assertThat(offset(t.name(), p, OffsetSpec.earliest())).as("%s-%d start", t.name(), p).isZero();
        assertThat(offset(t.name(), p, OffsetSpec.latest())).as("%s-%d end", t.name(), p).isEqualTo(t.seededPerPartition());
      }
    }
  }

  @Test
  @Order(2)
  void everyGroupHasTheDeclaredCommittedOffsetsAndLag() throws Exception {
    for (DemoData.GroupSpec g : DemoData.GROUPS) {
      assertThat(committedTotal(g)).as(g.group() + " committed").isEqualTo(g.committedPerPartition() * g.topic().partitions());
      assertThat(lagTotal(g)).as(g.group() + " lag").isEqualTo(g.seededLagTotal());
    }
    assertThat(lagTotal(DemoData.TRACKER)).isEqualTo(120);
    assertThat(lagTotal(DemoData.REPORTER)).isEqualTo(240);
    assertThat(lagTotal(DemoData.ALARM_NOTIFIER)).isEqualTo(5);
  }

  @Test
  @Order(3)
  void recordsReadBackByteForByteInOffsetOrder() {
    for (TopicPartition tp : List.of(new TopicPartition(DemoData.EDGE_CASES.name(), 0), new TopicPartition(DemoData.POSITIONS.name(), 1),
        new TopicPartition(DemoData.ALARMS.name(), 0))) {
      List<DemoData.SeedRecord> expected = DemoData.seedRecords(tp.topic(), tp.partition());
      List<ConsumerRecord<byte[], byte[]>> actual = readAll(tp, expected.size());
      assertThat(actual).as(tp.toString()).hasSameSizeAs(expected);
      for (int i = 0; i < expected.size(); i++) {
        DemoData.SeedRecord e = expected.get(i);
        ConsumerRecord<byte[], byte[]> a = actual.get(i);
        assertThat(a.offset()).isEqualTo(i);
        assertThat(a.key() == null ? null : new String(a.key(), StandardCharsets.UTF_8)).isEqualTo(e.key());
        assertThat(a.value()).as("%s offset %d", tp, i).isEqualTo(e.value());
        Map<String, String> headers = new HashMap<>();
        a.headers().forEach(h -> headers.put(h.key(), new String(h.value(), StandardCharsets.UTF_8)));
        assertThat(headers).isEqualTo(e.headers());
      }
    }
  }

  @Test
  @Order(4)
  void mqttHoldsOneRetainedStatusPerDeviceAndNothingElse() throws Exception {
    Map<String, String> retained = new ConcurrentHashMap<>();
    listen(DemoData.MQTT_FILTER, Duration.ofSeconds(2), (topic, payload, isRetained) -> {
      if (isRetained) retained.put(topic, payload);
      else retained.put("NOT RETAINED " + topic, payload);
    });
    Map<String, String> expected = new HashMap<>();
    for (DemoData.Device d : DemoData.DEVICES) expected.put(d.statusTopic(), d.statusJson());
    assertThat(retained).isEqualTo(expected);
  }

  @Test
  @Order(5)
  void seedingAgainChangesNothing() throws Exception {
    assertThat(Seeder.seed(bootstrap, mqttUri)).isEqualTo(Seeder.Result.ALREADY_SEEDED);
    topicsExistWithTheDeclaredPartitionsAndRecordCounts();
    everyGroupHasTheDeclaredCommittedOffsetsAndLag();
  }

  // ---------- live traffic: behaviour, not exact numbers ----------

  @Test
  @Order(6)
  void liveTrafficMakesOneGroupKeepUpOneFallBehindAndOneStandStill() throws Exception {
    // Ten times faster than the defaults, same proportions: the tracker reads 5 records a second while 20 arrive.
    LiveTraffic.Timing fast = new LiveTraffic.Timing(Duration.ofMillis(50), Duration.ofMillis(200), Duration.ofMillis(200), Duration.ofSeconds(3));
    long reporterCommitted = committedTotal(DemoData.REPORTER);

    try (LiveTraffic live = LiveTraffic.start(bootstrap, mqttUri, fast)) {
      await("positions keep arriving", () -> endTotal(DemoData.POSITIONS) > DemoData.POSITIONS.seededTotal() + 100);
      await("the bridge copies MQTT readings into Kafka", () -> endTotal(DemoData.TELEMETRY) > DemoData.TELEMETRY.seededTotal() + 8);
      await("the tracker moves", () -> committedTotal(DemoData.TRACKER) > DemoData.TRACKER.committedPerPartition() * 3 + 10);
      await("the archiver keeps up", () -> lagTotal(DemoData.ARCHIVER) < 30);

      assertThat(lagTotal(DemoData.TRACKER)).as("slow consumer: moving, and further behind than at the start").isGreaterThan(DemoData.TRACKER.seededLagTotal());
      assertThat(committedTotal(DemoData.REPORTER)).as("stopped consumer: committed offset stands still").isEqualTo(reporterCommitted);
      assertThat(lagTotal(DemoData.REPORTER)).as("stopped consumer: lag grows").isGreaterThan(DemoData.REPORTER.seededLagTotal() + 100);
      assertThat(endTotal(DemoData.ALARMS)).as("the alarm topic never changes").isEqualTo(DemoData.ALARMS.seededTotal());
      assertThat(lagTotal(DemoData.ALARM_NOTIFIER)).isEqualTo(5);

      // After the silent device has stopped, a listener hears the other three and not that one.
      Thread.sleep(3500);
      Set<String> heard = ConcurrentHashMap.newKeySet();
      listen(DemoData.TELEMETRY_FILTER, Duration.ofMillis(1500), (topic, payload, isRetained) -> heard.add(topic));
      List<String> expected = DemoData.DEVICES.stream().filter(d -> !d.equals(DemoData.SILENT_DEVICE)).map(DemoData.Device::telemetryTopic).toList();
      assertThat(heard).containsExactlyInAnyOrderElementsOf(expected);
    }
  }

  // ---------- helpers (read-only, the same way the server will read) ----------

  static long offset(String topic, int partition, OffsetSpec spec) throws Exception {
    TopicPartition tp = new TopicPartition(topic, partition);
    return admin.listOffsets(Map.of(tp, spec)).partitionResult(tp).get().offset();
  }

  static long endTotal(DemoData.TopicSpec t) {
    try {
      long total = 0;
      for (int p = 0; p < t.partitions(); p++) total += offset(t.name(), p, OffsetSpec.latest());
      return total;
    } catch (Exception e) {
      throw new IllegalStateException(e);
    }
  }

  static long committedTotal(DemoData.GroupSpec g) {
    try {
      Map<TopicPartition, OffsetAndMetadata> committed = admin.listConsumerGroupOffsets(g.group()).partitionsToOffsetAndMetadata().get();
      long total = 0;
      for (int p = 0; p < g.topic().partitions(); p++) total += committed.get(new TopicPartition(g.topic().name(), p)).offset();
      return total;
    } catch (Exception e) {
      throw new IllegalStateException(e);
    }
  }

  static long lagTotal(DemoData.GroupSpec g) {
    return endTotal(g.topic()) - committedTotal(g);
  }

  /** Reads a partition from its start with assign + seek: no consumer group, nothing committed. */
  static List<ConsumerRecord<byte[], byte[]>> readAll(TopicPartition tp, int expected) {
    Properties props = new Properties();
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
    List<ConsumerRecord<byte[], byte[]>> out = new ArrayList<>();
    try (KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(props)) {
      consumer.assign(List.of(tp));
      consumer.seekToBeginning(List.of(tp));
      long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(20);
      while (out.size() < expected && System.nanoTime() < deadline) {
        consumer.poll(Duration.ofMillis(500)).forEach(out::add);
      }
    }
    return out;
  }

  interface Heard {
    void message(String topic, String payload, boolean retained);
  }

  /** Subscribes with a throwaway client, listens for the given time, disconnects. */
  static void listen(String filter, Duration window, Heard heard) throws Exception {
    MqttClient client = new MqttClient(mqttUri, "it-listener-" + System.nanoTime(), new MemoryPersistence());
    MqttConnectOptions options = new MqttConnectOptions();
    options.setCleanSession(true);
    client.connect(options);
    try {
      client.subscribe(filter, 1, (topic, message) -> heard.message(topic, new String(message.getPayload(), StandardCharsets.UTF_8), message.isRetained()));
      Thread.sleep(window.toMillis());
    } finally {
      client.disconnect();
      client.close();
    }
  }

  static void await(String what, BooleanSupplier condition) throws InterruptedException {
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(60);
    while (System.nanoTime() < deadline) {
      if (condition.getAsBoolean()) return;
      Thread.sleep(250);
    }
    throw new AssertionError("Timed out waiting until " + what);
  }
}
