package io.topicscope;

import static io.topicscope.McpTestSupport.call;
import static io.topicscope.McpTestSupport.connect;
import static io.topicscope.McpTestSupport.number;
import static io.topicscope.McpTestSupport.row;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.modelcontextprotocol.client.McpSyncClient;
import io.topicscope.sim.DemoData;
import io.topicscope.sim.Seeder;
import io.topicscope.sim.Stack;
import java.io.Reader;
import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.AuthorizationException;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.MountableFile;
import org.yaml.snakeyaml.Yaml;

/**
 * The check of Phase 7: brokers that ask for a login, set up exactly as the local stack sets them up
 * (the Kafka settings are read from compose.yaml; the Mosquitto files are the ones compose mounts).
 *
 * <p>Two things are shown. With its look-only account the server can do everything it does. And with that
 * same account nothing can be written: the test tries, with the account's own login, and the brokers refuse.
 * The read-only promise then no longer rests on the server's code alone.
 */
class SecuredStackIT {

  static final Stack.Login SIMULATOR = new Stack.Login("simulator", "simulator-local-only");
  static final Stack.Login VIEWER = new Stack.Login("topicscope", "topicscope-local-only");

  static final Network network = Network.newNetwork();
  static GenericContainer<?> kafka;
  static GenericContainer<?> mosquitto;
  static String kafkaAddress;
  static String mqttAddress;
  static McpSyncClient client;

  @BeforeAll
  @SuppressWarnings("unchecked")
  static void startTheStackAsComposeDoes() throws Exception {
    Map<String, Object> compose;
    try (Reader reader = Files.newBufferedReader(Path.of("..", "compose.yaml"))) {
      compose = new Yaml().load(reader);
    }
    Map<String, Object> services = (Map<String, Object>) compose.get("services");
    Map<String, Object> kafkaService = (Map<String, Object>) services.get("kafka");
    Map<String, Object> mosquittoService = (Map<String, Object>) services.get("mosquitto");

    int kafkaPort;
    try (ServerSocket socket = new ServerSocket(0)) {
      kafkaPort = socket.getLocalPort();
    }
    kafka = new GenericContainer<>((String) kafkaService.get("image"))
        .withNetwork(network).withNetworkAliases("kafka")
        .withExposedPorts(9092)
        .waitingFor(Wait.forLogMessage(".*Kafka Server started.*", 1));
    ((Map<String, Object>) kafkaService.get("environment")).forEach((name, value) -> kafka.withEnv(name, String.valueOf(value)));
    // The one difference from compose: the port on this machine is a free one, so the advertised address has to follow it.
    kafka.withEnv("KAFKA_ADVERTISED_LISTENERS", "HOST://localhost:" + kafkaPort + ",INTERNAL://kafka:19092");
    kafka.setPortBindings(List.of("127.0.0.1:" + kafkaPort + ":9092"));
    kafka.start();
    kafkaAddress = "localhost:" + kafkaPort;

    mosquitto = new GenericContainer<>((String) mosquittoService.get("image")).withExposedPorts(1883);
    for (String file : List.of("mosquitto.conf", "passwd", "acl")) {
      mosquitto.withCopyFileToContainer(MountableFile.forHostPath(Path.of("..", "mosquitto", file).toAbsolutePath().normalize()), "/mosquitto/config/" + file);
    }
    mosquitto.start();
    mqttAddress = "tcp://" + mosquitto.getHost() + ":" + mosquitto.getMappedPort(1883);

    // The simulator's account sets everything up, and gives the viewer its look-only rights on Kafka.
    Seeder.Result seeded = null;
    for (int attempt = 0; attempt < 30 && seeded == null; attempt++) {
      try {
        seeded = Seeder.seed(new Stack(kafkaAddress, SIMULATOR, mqttAddress, SIMULATOR), VIEWER.username());
      } catch (ExecutionException | org.apache.kafka.common.KafkaException e) {
        Thread.sleep(1000); // the broker accepts connections a moment before it accepts logins
      }
    }
    assertThat(seeded).isEqualTo(Seeder.Result.SEEDED);

    client = connect(Map.of(
        "TOPIC_SCOPE_KAFKA", kafkaAddress, "TOPIC_SCOPE_KAFKA_USERNAME", VIEWER.username(), "TOPIC_SCOPE_KAFKA_PASSWORD", VIEWER.password(),
        "TOPIC_SCOPE_MQTT", mqttAddress, "TOPIC_SCOPE_MQTT_USERNAME", VIEWER.username(), "TOPIC_SCOPE_MQTT_PASSWORD", VIEWER.password()));
  }

  @AfterAll
  static void stop() {
    if (client != null) client.close();
    if (mosquitto != null) mosquitto.stop();
    if (kafka != null) kafka.stop();
    network.close();
  }

  // ---------- with its look-only account the server can do everything it does ----------

  @Test
  @SuppressWarnings("unchecked")
  void everyToolWorksWithTheLookOnlyAccount() {
    Map<String, Object> sources = call(client, "list_sources", Map.of());
    assertThat(sources).containsEntry("reachable", 2);
    assertThat(sources.toString()).as("the password is nowhere in the answer").doesNotContain(VIEWER.password());

    Map<String, Object> topics = call(client, "list_topics", Map.of("source", "local-kafka"));
    for (DemoData.TopicSpec expected : DemoData.TOPICS) {
      assertThat(number(row(topics, "topics", expected.name()).get("messages"))).as(expected.name()).isEqualTo(expected.seededTotal());
    }
    Map<String, Object> described = call(client, "describe_topic", Map.of("source", "local-kafka", "topic", DemoData.POSITIONS.name()));
    assertThat(row(described, "settings", "retention.ms")).containsEntry("value", "-1");

    List<Map<String, Object>> messages = (List<Map<String, Object>>) call(client, "recent_messages",
        Map.of("source", "local-kafka", "topic", DemoData.ALARMS.name(), "limit", 50)).get("messages");
    assertThat(messages).hasSize(DemoData.ALARMS.seededTotal());

    List<Map<String, Object>> groups = (List<Map<String, Object>>) call(client, "list_consumer_groups", Map.of("source", "local-kafka")).get("groups");
    assertThat(groups).extracting(g -> (String) g.get("group")).containsExactlyElementsOf(DemoData.GROUPS.stream().map(DemoData.GroupSpec::group).sorted().toList());
    for (DemoData.GroupSpec expected : DemoData.GROUPS) {
      Map<String, Object> lag = call(client, "consumer_lag", Map.of("source", "local-kafka", "group", expected.group(), "sampleSeconds", 1));
      assertThat(number(lag.get("lag"))).as(expected.group()).isEqualTo(expected.seededLagTotal());
    }
    assertThat(number(call(client, "topic_freshness", Map.of("source", "local-kafka", "expectedIntervalSeconds", 3600)).get("fresh")))
        .isEqualTo(DemoData.TOPICS.size() - 1);

    List<Map<String, Object>> observed = (List<Map<String, Object>>) call(client, "list_topics", Map.of("source", "local-mqtt", "windowSeconds", 1)).get("observed");
    assertThat(observed).hasSize(DemoData.DEVICES.size());
    assertThat((List<Object>) call(client, "recent_messages", Map.of("source", "local-mqtt", "topic", "demo/#", "windowSeconds", 1)).get("messages"))
        .hasSize(DemoData.DEVICES.size());
    Map<String, Object> freshness = call(client, "topic_freshness", Map.of("source", "local-mqtt", "topic", "demo/#", "expectedIntervalSeconds", 5, "windowSeconds", 1));
    assertThat((List<Object>) freshness.get("brokerCounters")).as("the look-only account may read $SYS").isNotEmpty();
  }

  @Test
  void aWrongPasswordIsARowWithTheReasonAndWithoutThePassword() {
    String wrong = "not-the-password-4711";
    try (McpSyncClient locked = connect(Map.of(
        "TOPIC_SCOPE_KAFKA", kafkaAddress, "TOPIC_SCOPE_KAFKA_USERNAME", VIEWER.username(), "TOPIC_SCOPE_KAFKA_PASSWORD", wrong,
        "TOPIC_SCOPE_MQTT", mqttAddress, "TOPIC_SCOPE_MQTT_USERNAME", VIEWER.username(), "TOPIC_SCOPE_MQTT_PASSWORD", wrong))) {
      Map<String, Object> sources = call(locked, "list_sources", Map.of());
      assertThat(sources).containsEntry("reachable", 0);
      assertThat((String) row(sources, "sources", "local-kafka").get("detail")).contains("Authentication failed");
      assertThat((String) row(sources, "sources", "local-mqtt").get("detail")).contains("Not authorized");
      assertThat(sources.toString()).doesNotContain(wrong);
      assertThat(McpTestSupport.callExpectingProblem(locked, "list_topics", Map.of("source", "local-kafka")))
          .contains("Authentication failed", "check the username and password").doesNotContain(wrong);
    }
  }

  // ---------- with that same account nothing can be written: the brokers refuse ----------

  @Test
  void kafkaRefusesEveryKindOfWriteFromTheLookOnlyAccount() throws Exception {
    Properties viewer = kafkaLogin(VIEWER);
    String topic = DemoData.ALARMS.name();

    viewer.put("key.serializer", ByteArraySerializer.class.getName());
    viewer.put("value.serializer", ByteArraySerializer.class.getName());
    viewer.put("max.block.ms", "15000");
    try (KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(viewer)) {
      refused("publish a record", () -> producer.send(new ProducerRecord<>(topic, "x".getBytes(StandardCharsets.UTF_8))).get(30, TimeUnit.SECONDS));
    }

    try (Admin admin = Admin.create(kafkaLogin(VIEWER))) {
      refused("create a topic", () -> admin.createTopics(List.of(new NewTopic("viewer.made-this", 1, (short) 1))).all().get(30, TimeUnit.SECONDS));
      refused("delete a topic", () -> admin.deleteTopics(List.of(topic)).all().get(30, TimeUnit.SECONDS));
      refused("move a group's offsets", () -> admin.alterConsumerGroupOffsets(DemoData.TRACKER.group(),
          Map.of(new TopicPartition(DemoData.POSITIONS.name(), 0), new OffsetAndMetadata(0))).all().get(30, TimeUnit.SECONDS));
      refused("delete a group", () -> admin.deleteConsumerGroups(List.of(DemoData.REPORTER.group())).all().get(30, TimeUnit.SECONDS));
    }

    Properties member = kafkaLogin(VIEWER);
    member.put("group" + ".id", "viewer-tries-to-join");
    member.put("key.deserializer", ByteArrayDeserializer.class.getName());
    member.put("value.deserializer", ByteArrayDeserializer.class.getName());
    try (KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(member)) {
      consumer.subscribe(List.of(topic));
      refused("join a consumer group", () -> consumer.poll(Duration.ofSeconds(15)));
    }

    // And nothing changed: asked with the account that may see everything.
    try (Admin admin = Admin.create(kafkaLogin(SIMULATOR))) {
      assertThat(admin.listTopics().names().get(30, TimeUnit.SECONDS)).containsExactlyInAnyOrderElementsOf(DemoData.TOPICS.stream().map(DemoData.TopicSpec::name).toList());
      assertThat(admin.listGroups().all().get(30, TimeUnit.SECONDS)).extracting(g -> g.groupId())
          .containsExactlyInAnyOrderElementsOf(DemoData.GROUPS.stream().map(DemoData.GroupSpec::group).toList());
      assertThat(admin.listConsumerGroupOffsets(DemoData.TRACKER.group()).partitionsToOffsetAndMetadata().get(30, TimeUnit.SECONDS)
          .get(new TopicPartition(DemoData.POSITIONS.name(), 0)).offset()).isEqualTo(DemoData.TRACKER.committedPerPartition());
    }
  }

  @Test
  void mqttDropsWhatTheLookOnlyAccountPublishes() throws Exception {
    String topic = "demo/sensors/twr-01/wind/status";
    String intruder = "demo/viewer/made-this";
    Map<String, String> heard = new ConcurrentHashMap<>();

    MqttClient listener = mqttClient(SIMULATOR, "it-listener");
    MqttClient viewer = mqttClient(VIEWER, "it-viewer");
    try {
      listener.subscribe("demo/#", 1, (t, m) -> heard.put(t, new String(m.getPayload(), StandardCharsets.UTF_8)));
      Thread.sleep(500);
      assertThat(heard).as("the seeded retained messages").hasSize(DemoData.DEVICES.size());
      String before = heard.get(topic);

      // An MQTT 3.1.1 broker accepts the packet and says nothing. Whether it was refused shows only in what arrives.
      viewer.publish(topic, retained("overwritten by the viewer"));
      viewer.publish(intruder, retained("made by the viewer"));
      Thread.sleep(1500);

      assertThat(heard.get(topic)).as("the retained message is untouched").isEqualTo(before).isEqualTo(DemoData.DEVICES.get(0).statusJson());
      assertThat(heard).as("the new topic never came to be").doesNotContainKey(intruder).hasSize(DemoData.DEVICES.size());

      // The same two publishes from the account that may write do arrive, so the test can tell refused from lost.
      MqttClient writer = mqttClient(SIMULATOR, "it-writer");
      try {
        writer.publish("demo/it/proof", new MqttMessage("arrives".getBytes(StandardCharsets.UTF_8)));
        Thread.sleep(1000);
        assertThat(heard).containsEntry("demo/it/proof", "arrives");
      } finally {
        writer.disconnect();
        writer.close();
      }
    } finally {
      for (MqttClient c : List.of(viewer, listener)) {
        c.disconnect();
        c.close();
      }
    }
  }

  // ---------- helpers ----------

  private interface Attempt {
    void run() throws Exception;
  }

  /** The attempt must fail, and because the broker said the account is not allowed: not for a timeout or a typo. */
  private static void refused(String what, Attempt attempt) {
    assertThatThrownBy(attempt::run).as("the broker must refuse to " + what)
        .satisfies(e -> {
          Throwable cause = e;
          while (cause != null && !(cause instanceof AuthorizationException)) cause = cause.getCause();
          assertThat(cause).as("refused for lack of rights, but was: " + e).isInstanceOf(AuthorizationException.class);
        });
  }

  private static Properties kafkaLogin(Stack.Login login) {
    Properties props = new Properties();
    props.put("bootstrap.servers", kafkaAddress);
    props.put("security.protocol", "SASL_PLAINTEXT");
    props.put("sasl.mechanism", "PLAIN");
    props.put("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"" + login.username()
        + "\" password=\"" + login.password() + "\";");
    return props;
  }

  private static MqttClient mqttClient(Stack.Login login, String id) throws Exception {
    MqttClient c = new MqttClient(mqttAddress, id + "-" + System.nanoTime(), new MemoryPersistence());
    MqttConnectOptions options = new MqttConnectOptions();
    options.setCleanSession(true);
    options.setUserName(login.username());
    options.setPassword(login.password().toCharArray());
    c.connect(options);
    return c;
  }

  private static MqttMessage retained(String text) {
    MqttMessage message = new MqttMessage(text.getBytes(StandardCharsets.UTF_8));
    message.setQos(1);
    message.setRetained(true);
    return message;
  }
}
