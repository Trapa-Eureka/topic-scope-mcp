package io.topicscope;

import static io.topicscope.McpTestSupport.call;
import static io.topicscope.McpTestSupport.connect;
import static io.topicscope.McpTestSupport.number;
import static io.topicscope.McpTestSupport.row;
import static org.assertj.core.api.Assertions.assertThat;

import io.modelcontextprotocol.client.McpSyncClient;
import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;

/**
 * What the topic tools say when a broker is lost, on a real cluster: one controller and two brokers, one of
 * which is stopped during the test. A single-broker stack cannot show this state.
 *
 * <ul>
 *   <li>{@code solo}: its only replica is on the broker that stops, so the partition is left without a leader.</li>
 *   <li>{@code shared}: replicas on both brokers, so it keeps a leader and becomes under-replicated.</li>
 * </ul>
 *
 * The test itself creates topics and publishes; it is the test's own cluster. The server under test only reads.
 */
class KafkaOfflinePartitionIT {

  static final String IMAGE = "apache/kafka:4.3.2";
  static final String CLUSTER_ID = "dG9waWMtc2NvcGUtdGVzdA"; // any base64 UUID, the same on every node
  static final int SURVIVOR = 2;
  static final int VICTIM = 3;
  static final int RECORDS = 5;

  static final Network network = Network.newNetwork();
  static GenericContainer<?> controller;
  static GenericContainer<?> survivor;
  static GenericContainer<?> victim;
  static String bootstrap;
  static McpSyncClient client;

  @BeforeAll
  static void startClusterAndFillIt() throws Exception {
    controller = node(1, "controller")
        .withEnv("KAFKA_PROCESS_ROLES", "controller")
        .withEnv("KAFKA_LISTENERS", "CONTROLLER://:9093");
    controller.start();

    int survivorPort = freePort();
    int victimPort = freePort();
    survivor = broker(SURVIVOR, "broker" + SURVIVOR, survivorPort);
    victim = broker(VICTIM, "broker" + VICTIM, victimPort);
    survivor.start();
    victim.start();
    bootstrap = "localhost:" + survivorPort;

    Properties adminProps = new Properties();
    adminProps.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
    try (Admin admin = Admin.create(adminProps)) {
      awaitBrokers(admin, 2);
      admin.createTopics(List.of(
          new NewTopic("solo", Map.of(0, List.of(VICTIM))),
          new NewTopic("shared", Map.of(0, List.of(SURVIVOR, VICTIM))))).all().get(30, TimeUnit.SECONDS);
    }

    Properties producerProps = new Properties();
    producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
    producerProps.put(ProducerConfig.ACKS_CONFIG, "all");
    producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
    producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
    try (KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(producerProps)) {
      for (String topic : List.of("solo", "shared")) {
        for (int i = 0; i < RECORDS; i++) {
          producer.send(new ProducerRecord<>(topic, 0, null, ("record " + i).getBytes(StandardCharsets.UTF_8))).get(30, TimeUnit.SECONDS);
        }
      }
    }

    client = connect(bootstrap, McpTestSupport.MQTT_ADDRESS);
  }

  @AfterAll
  static void stopCluster() {
    if (client != null) client.close();
    for (GenericContainer<?> c : new GenericContainer<?>[] {victim, survivor, controller}) {
      if (c != null) c.stop();
    }
    network.close();
  }

  @Test
  @SuppressWarnings("unchecked")
  void aLostBrokerShowsAsAnOfflineAndAnUnderReplicatedPartition() throws Exception {
    // Before: both topics are whole.
    Map<String, Object> before = call(client, "list_topics", Map.of("source", "local-kafka"));
    for (String topic : List.of("solo", "shared")) {
      Map<String, Object> row = row(before, "topics", topic);
      assertThat(number(row.get("messages"))).as(topic + " messages before").isEqualTo(RECORDS);
      assertThat(number(row.get("offlinePartitions"))).isZero();
      assertThat(number(row.get("underReplicatedPartitions"))).isZero();
    }
    assertThat(number(row(before, "topics", "shared").get("replicationFactor"))).isEqualTo(2);

    victim.stop();

    // The controller needs a moment to notice (broker session timeout) and to take the broker out of the in-sync sets.
    Map<String, Object> after = null;
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(90);
    while (System.nanoTime() < deadline) {
      after = call(client, "list_topics", Map.of("source", "local-kafka"));
      if (number(row(after, "topics", "solo").get("offlinePartitions")) == 1
          && number(row(after, "topics", "shared").get("underReplicatedPartitions")) == 1) {
        break;
      }
      Thread.sleep(1000);
    }

    Map<String, Object> solo = row(after, "topics", "solo");
    assertThat(number(solo.get("offlinePartitions"))).as("solo: no leader left").isEqualTo(1);
    assertThat(number(solo.get("messages"))).as("solo: records of an offline partition are not counted").isZero();
    assertThat(solo).containsEntry("newestMessageTime", "");

    Map<String, Object> shared = row(after, "topics", "shared");
    assertThat(number(shared.get("offlinePartitions"))).as("shared: still has a leader").isZero();
    assertThat(number(shared.get("underReplicatedPartitions"))).as("shared: one replica is gone").isEqualTo(1);
    assertThat(number(shared.get("messages"))).as("shared: still readable").isEqualTo(RECORDS);

    // describe_topic answers quickly even though one partition cannot be asked for its offsets.
    long start = System.nanoTime();
    Map<String, Object> detail = call(client, "describe_topic", Map.of("source", "local-kafka", "topic", "solo"));
    assertThat((System.nanoTime() - start) / 1_000_000).as("describe_topic does not wait for the dead broker").isLessThan(8000);
    Map<String, Object> partition = ((List<Map<String, Object>>) detail.get("partitions")).get(0);
    assertThat(number(partition.get("leader"))).isEqualTo(-1);
    assertThat(number(partition.get("startOffset"))).isEqualTo(-1);
    assertThat(number(partition.get("endOffset"))).isEqualTo(-1);
    assertThat((List<Object>) partition.get("replicas")).containsExactly(VICTIM);
    assertThat((String) detail.get("note")).contains("1 partition(s) have no leader");

    Map<String, Object> sharedDetail = call(client, "describe_topic", Map.of("source", "local-kafka", "topic", "shared"));
    Map<String, Object> sharedPartition = ((List<Map<String, Object>>) sharedDetail.get("partitions")).get(0);
    assertThat(number(sharedPartition.get("leader"))).isEqualTo(SURVIVOR);
    assertThat((List<Object>) sharedPartition.get("replicas")).containsExactlyInAnyOrder(SURVIVOR, VICTIM);
    assertThat((List<Object>) sharedPartition.get("inSyncReplicas")).containsExactly(SURVIVOR);
    assertThat(number(sharedPartition.get("endOffset"))).isEqualTo(RECORDS);
  }

  // ---------- the cluster ----------

  private static GenericContainer<?> node(int id, String alias) {
    return new GenericContainer<>(IMAGE)
        .withNetwork(network)
        .withNetworkAliases(alias)
        .withEnv("CLUSTER_ID", CLUSTER_ID)
        .withEnv("KAFKA_NODE_ID", String.valueOf(id))
        .withEnv("KAFKA_CONTROLLER_QUORUM_VOTERS", "1@controller:9093")
        .withEnv("KAFKA_CONTROLLER_LISTENER_NAMES", "CONTROLLER")
        .withEnv("KAFKA_LISTENER_SECURITY_PROTOCOL_MAP", "HOST:PLAINTEXT,INTERNAL:PLAINTEXT,CONTROLLER:PLAINTEXT")
        .waitingFor(Wait.forLogMessage(".*Kafka Server started.*", 1));
  }

  /** A broker reachable from the test as localhost:hostPort and from the other broker as alias:19092. */
  private static GenericContainer<?> broker(int id, String alias, int hostPort) {
    GenericContainer<?> broker = node(id, alias)
        .withEnv("KAFKA_PROCESS_ROLES", "broker")
        .withEnv("KAFKA_LISTENERS", "HOST://:9092,INTERNAL://:19092")
        .withEnv("KAFKA_ADVERTISED_LISTENERS", "HOST://localhost:" + hostPort + ",INTERNAL://" + alias + ":19092")
        .withEnv("KAFKA_INTER_BROKER_LISTENER_NAME", "INTERNAL")
        .withEnv("KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR", "1")
        .withEnv("KAFKA_AUTO_CREATE_TOPICS_ENABLE", "false")
        .withExposedPorts(9092);
    // The advertised address must be known before the container starts, so the host port is fixed.
    broker.setPortBindings(List.of("127.0.0.1:" + hostPort + ":9092"));
    return broker;
  }

  private static void awaitBrokers(Admin admin, int expected) throws Exception {
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(60);
    while (System.nanoTime() < deadline) {
      try {
        if (admin.describeCluster().nodes().get(10, TimeUnit.SECONDS).size() == expected) return;
      } catch (Exception e) {
        // not ready yet
      }
      Thread.sleep(500);
    }
    throw new AssertionError("The cluster did not reach " + expected + " brokers");
  }

  private static int freePort() throws Exception {
    try (ServerSocket socket = new ServerSocket(0)) {
      return socket.getLocalPort();
    }
  }
}
