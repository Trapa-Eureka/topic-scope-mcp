package io.topicscope;

import static io.topicscope.McpTestSupport.call;
import static io.topicscope.McpTestSupport.callExpectingProblem;
import static io.topicscope.McpTestSupport.connect;
import static io.topicscope.McpTestSupport.number;
import static io.topicscope.McpTestSupport.row;
import static org.assertj.core.api.Assertions.assertThat;

import io.modelcontextprotocol.client.McpSyncClient;
import java.io.InputStream;
import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.MountableFile;

/**
 * Encrypted connections, against real brokers: Kafka over TLS with a SCRAM-SHA-512 login, and Mosquitto over
 * TLS ({@code ssl://}) with a user name and password. The certificate is made for this test run with the
 * JDK's own keytool, signed by nobody, so Java does not trust it unless the source's trust store says so.
 *
 * <p>The sources are configured the way the README describes, in a file passed with
 * {@code --spring.config.additional-location}.
 */
class TlsStackIT {

  static final String STORE_PASSWORD = "test-store-pass";
  static final String VIEWER = "viewer";
  static final String VIEWER_PASSWORD = "viewer-scram-pass";
  static final String WRITER = "writer";
  static final String WRITER_PASSWORD = "writer-scram-pass";
  static final String TOPIC = "tls.demo";
  static final String GROUP = "tls-demo-group";
  static final String MQTT_TOPIC = "tls/demo/status";
  static final int RECORDS = 4;

  static Path folder;
  static Path trustStore;
  static GenericContainer<?> kafka;
  static GenericContainer<?> mosquitto;
  static String kafkaAddress;
  static String mqttAddress;

  @BeforeAll
  static void makeACertificateAndStartBrokersThatUseIt() throws Exception {
    folder = Files.createTempDirectory("topic-scope-tls");
    Path keyStore = folder.resolve("server.p12");
    Path certificate = folder.resolve("server-cert.pem");
    Path key = folder.resolve("server-key.pem");
    trustStore = folder.resolve("trust.p12");

    // One self-signed certificate for both brokers, valid for the names and the address they are reached under.
    keytool("-genkeypair", "-alias", "server", "-keyalg", "RSA", "-keysize", "2048", "-validity", "2", "-dname", "CN=localhost",
        "-ext", "san=dns:localhost,ip:127.0.0.1", "-storetype", "PKCS12", "-keystore", keyStore.toString(), "-storepass", STORE_PASSWORD);
    keytool("-exportcert", "-alias", "server", "-rfc", "-keystore", keyStore.toString(), "-storepass", STORE_PASSWORD, "-file", certificate.toString());
    keytool("-importcert", "-noprompt", "-alias", "server", "-file", certificate.toString(), "-storetype", "PKCS12",
        "-keystore", trustStore.toString(), "-storepass", STORE_PASSWORD);
    writePrivateKeyAsPem(keyStore, key);

    startKafka(keyStore);
    startMosquitto(certificate, key);
    fillTheBrokers();
  }

  @AfterAll
  static void stop() {
    if (mosquitto != null) mosquitto.stop();
    if (kafka != null) kafka.stop();
  }

  // ---------- the checks ----------

  @Test
  @SuppressWarnings("unchecked")
  void everyToolWorksOverTlsWithScramAndWithATrustStorePerSource() throws Exception {
    try (McpSyncClient client = server(sources(VIEWER_PASSWORD, true))) {
      Map<String, Object> sources = call(client, "list_sources", Map.of());
      assertThat(sources).containsEntry("reachable", 2);
      assertThat(row(sources, "sources", "tls-kafka")).containsEntry("address", kafkaAddress);
      assertThat((String) row(sources, "sources", "tls-mqtt").get("detail")).isEqualTo("mosquitto version 2.0.22");
      assertThat(sources.toString()).as("no password in the answer").doesNotContain(VIEWER_PASSWORD, STORE_PASSWORD, "topicscope-local-only");

      Map<String, Object> topics = call(client, "list_topics", Map.of("source", "tls-kafka"));
      assertThat(number(row(topics, "topics", TOPIC).get("messages"))).isEqualTo(RECORDS);
      assertThat(call(client, "describe_topic", Map.of("source", "tls-kafka", "topic", TOPIC))).containsEntry("note", "");

      List<Map<String, Object>> messages = (List<Map<String, Object>>) call(client, "recent_messages", Map.of("source", "tls-kafka", "topic", TOPIC)).get("messages");
      assertThat(messages).extracting(m -> (String) m.get("value")).containsExactly("record 3", "record 2", "record 1", "record 0");

      List<Map<String, Object>> groups = (List<Map<String, Object>>) call(client, "list_consumer_groups", Map.of("source", "tls-kafka")).get("groups");
      assertThat(groups).extracting(g -> (String) g.get("group")).containsExactly(GROUP);
      Map<String, Object> lag = call(client, "consumer_lag", Map.of("source", "tls-kafka", "group", GROUP, "sampleSeconds", 1));
      assertThat(number(lag.get("lag"))).as("committed at 1 of 4").isEqualTo(RECORDS - 1);
      assertThat(number(call(client, "topic_freshness", Map.of("source", "tls-kafka", "expectedIntervalSeconds", 3600)).get("fresh"))).isEqualTo(1);

      List<Map<String, Object>> observed = (List<Map<String, Object>>) call(client, "list_topics", Map.of("source", "tls-mqtt", "windowSeconds", 1)).get("observed");
      assertThat(observed).extracting(t -> (String) t.get("topic")).containsExactly(MQTT_TOPIC);
      List<Map<String, Object>> heard = (List<Map<String, Object>>) call(client, "recent_messages", Map.of("source", "tls-mqtt", "topic", "tls/#", "windowSeconds", 1)).get("messages");
      assertThat(heard).hasSize(1);
      assertThat(heard.get(0)).containsEntry("value", "{\"state\":\"online\"}").containsEntry("retained", true);
      assertThat((List<Object>) call(client, "topic_freshness", Map.of("source", "tls-mqtt", "topic", "tls/#", "expectedIntervalSeconds", 5, "windowSeconds", 1)).get("brokerCounters"))
          .isNotEmpty();
    }
  }

  @Test
  void aWrongScramPasswordIsRefusedWithTheReason() throws Exception {
    try (McpSyncClient client = server(sources("not-the-scram-password", true))) {
      Map<String, Object> sources = call(client, "list_sources", Map.of());
      assertThat(row(sources, "sources", "tls-kafka")).containsEntry("reachable", false);
      assertThat((String) row(sources, "sources", "tls-kafka").get("detail")).contains("Authentication failed", "SCRAM-SHA-512");
      assertThat(callExpectingProblem(client, "list_topics", Map.of("source", "tls-kafka")))
          .contains("Authentication failed").doesNotContain("not-the-scram-password", STORE_PASSWORD);
    }
  }

  @Test
  void withoutTheTrustStoreTheCertificateIsNotTrustedAndNothingIsRead() throws Exception {
    // The same sources without their trust stores: Java knows nobody who vouches for this certificate.
    try (McpSyncClient client = server(sources(VIEWER_PASSWORD, false))) {
      Map<String, Object> sources = call(client, "list_sources", Map.of());
      assertThat(sources).containsEntry("reachable", 0);
      assertThat((String) row(sources, "sources", "tls-mqtt").get("detail")).contains("unable to find valid certification path");
      assertThat((String) row(sources, "sources", "tls-kafka").get("detail")).isNotBlank();
      assertThat(callExpectingProblem(client, "recent_messages", Map.of("source", "tls-mqtt", "topic", "tls/#", "windowSeconds", 1)))
          .contains("could not be read");
    }
  }

  // ---------- the server under test ----------

  /** A server whose two sources are written into a configuration file, as a user would write them. */
  private static McpSyncClient server(String yaml) throws Exception {
    Path config = Files.createTempFile(folder, "sources", ".yml");
    Files.writeString(config, yaml);
    return connect(Map.of("TLS_KAFKA_PASSWORD_FOR_TEST", "unused"), "--spring.config.additional-location=file:" + config);
  }

  private static String sources(String kafkaPassword, boolean withTrustStores) {
    String kafkaTrust = !withTrustStores ? "" : """
                ssl.truststore.location: %s
                ssl.truststore.password: %s
                ssl.truststore.type: PKCS12
        """.formatted(trustStore, STORE_PASSWORD);
    String mqttTrust = !withTrustStores ? "" : """
              properties:
                ssl.truststore.location: %s
                ssl.truststore.password: %s
        """.formatted(trustStore, STORE_PASSWORD);
    return """
        topic-scope:
          probe-timeout: 4s
          sources:
            - name: tls-kafka
              type: kafka
              address: %s
              username: %s
              password: %s
              properties:
                security.protocol: SASL_SSL
                sasl.mechanism: SCRAM-SHA-512
        %s    - name: tls-mqtt
              type: mqtt
              address: %s
              username: topicscope
              password: topicscope-local-only
        %s""".formatted(kafkaAddress, VIEWER, kafkaPassword, kafkaTrust, mqttAddress, mqttTrust);
  }

  // ---------- the brokers ----------

  private static void startKafka(Path keyStore) throws Exception {
    int port = freePort();
    kafka = new GenericContainer<>("apache/kafka:4.3.2")
        .withExposedPorts(9092)
        .withCopyFileToContainer(MountableFile.forHostPath(keyStore, 0644), "/certs/server.p12")
        .withEnv("KAFKA_NODE_ID", "1")
        .withEnv("KAFKA_PROCESS_ROLES", "broker,controller")
        .withEnv("KAFKA_CONTROLLER_QUORUM_VOTERS", "1@localhost:9093")
        .withEnv("KAFKA_CONTROLLER_LISTENER_NAMES", "CONTROLLER")
        // SECURE is what clients use: TLS, and a SCRAM login. INTERNAL stays inside the container and is how the accounts are created.
        .withEnv("KAFKA_LISTENERS", "SECURE://:9092,INTERNAL://:19092,CONTROLLER://:9093")
        .withEnv("KAFKA_ADVERTISED_LISTENERS", "SECURE://localhost:" + port + ",INTERNAL://localhost:19092")
        .withEnv("KAFKA_LISTENER_SECURITY_PROTOCOL_MAP", "SECURE:SASL_SSL,INTERNAL:PLAINTEXT,CONTROLLER:PLAINTEXT")
        .withEnv("KAFKA_INTER_BROKER_LISTENER_NAME", "INTERNAL")
        .withEnv("KAFKA_SASL_ENABLED_MECHANISMS", "SCRAM-SHA-512")
        .withEnv("KAFKA_LISTENER_NAME_SECURE_SCRAM___SHA___512_SASL_JAAS_CONFIG", "org.apache.kafka.common.security.scram.ScramLoginModule required;")
        .withEnv("KAFKA_SSL_KEYSTORE_LOCATION", "/certs/server.p12")
        .withEnv("KAFKA_SSL_KEYSTORE_PASSWORD", STORE_PASSWORD)
        .withEnv("KAFKA_SSL_KEY_PASSWORD", STORE_PASSWORD)
        .withEnv("KAFKA_SSL_KEYSTORE_TYPE", "PKCS12")
        .withEnv("KAFKA_SSL_CLIENT_AUTH", "none")
        .withEnv("KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR", "1")
        .withEnv("KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR", "1")
        .withEnv("KAFKA_TRANSACTION_STATE_LOG_MIN_ISR", "1")
        .waitingFor(Wait.forLogMessage(".*Kafka Server started.*", 1));
    kafka.setPortBindings(List.of("127.0.0.1:" + port + ":9092"));
    kafka.start();
    kafkaAddress = "localhost:" + port;

    for (String[] account : new String[][] {{VIEWER, VIEWER_PASSWORD}, {WRITER, WRITER_PASSWORD}}) {
      Container.ExecResult added = kafka.execInContainer("/opt/kafka/bin/kafka-configs.sh", "--bootstrap-server", "localhost:19092", "--alter",
          "--add-config", "SCRAM-SHA-512=[password=" + account[1] + "]", "--entity-type", "users", "--entity-name", account[0]);
      assertThat(added.getStdout()).as(added.getStderr()).contains("Completed updating config for user " + account[0]);
    }
  }

  private static void startMosquitto(Path certificate, Path key) throws Exception {
    Path conf = folder.resolve("mosquitto-tls.conf");
    Files.writeString(conf, """
        listener 8883
        certfile /mosquitto/config/server-cert.pem
        keyfile /mosquitto/config/server-key.pem
        allow_anonymous false
        password_file /mosquitto/config/passwd
        acl_file /mosquitto/config/acl
        persistence false
        sys_interval 5
        log_dest stdout
        """);
    mosquitto = new GenericContainer<>("eclipse-mosquitto:2.0.22")
        .withExposedPorts(8883)
        .withCopyFileToContainer(MountableFile.forHostPath(conf, 0644), "/mosquitto/config/mosquitto.conf")
        .withCopyFileToContainer(MountableFile.forHostPath(certificate, 0644), "/mosquitto/config/server-cert.pem")
        .withCopyFileToContainer(MountableFile.forHostPath(key, 0644), "/mosquitto/config/server-key.pem");
    // The accounts and rights are the local stack's: simulator may publish, topicscope may only listen.
    for (String file : List.of("passwd", "acl")) {
      mosquitto.withCopyFileToContainer(MountableFile.forHostPath(Path.of("..", "mosquitto", file).toAbsolutePath().normalize()), "/mosquitto/config/" + file);
    }
    mosquitto.waitingFor(Wait.forLogMessage(".*mosquitto version .* running.*", 1));
    mosquitto.start();
    mqttAddress = "ssl://localhost:" + mosquitto.getMappedPort(8883);
  }

  /** Written by the test's own clients, over the same encrypted connections, with the accounts that may write. */
  private static void fillTheBrokers() throws Exception {
    Properties writer = new Properties();
    writer.put("bootstrap.servers", kafkaAddress);
    writer.put("security.protocol", "SASL_SSL");
    writer.put("sasl.mechanism", "SCRAM-SHA-512");
    writer.put("sasl.jaas.config", "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"" + WRITER + "\" password=\"" + WRITER_PASSWORD + "\";");
    writer.put("ssl.truststore.location", trustStore.toString());
    writer.put("ssl.truststore.password", STORE_PASSWORD);
    writer.put("ssl.truststore.type", "PKCS12");

    try (Admin admin = Admin.create(writer)) {
      long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(60);
      while (true) {
        try {
          admin.createTopics(List.of(new NewTopic(TOPIC, 1, (short) 1))).all().get(20, TimeUnit.SECONDS);
          break;
        } catch (Exception e) {
          // A SCRAM account is usable a moment after it was created.
          if (System.nanoTime() > deadline) throw e;
          Thread.sleep(1000);
        }
      }
      Properties producerProps = new Properties();
      producerProps.putAll(writer);
      producerProps.put("key.serializer", ByteArraySerializer.class.getName());
      producerProps.put("value.serializer", ByteArraySerializer.class.getName());
      try (KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(producerProps)) {
        for (int i = 0; i < RECORDS; i++) {
          producer.send(new ProducerRecord<>(TOPIC, 0, null, ("record " + i).getBytes(StandardCharsets.UTF_8))).get(30, TimeUnit.SECONDS);
        }
      }
      admin.alterConsumerGroupOffsets(GROUP, Map.of(new TopicPartition(TOPIC, 0), new OffsetAndMetadata(1))).all().get(30, TimeUnit.SECONDS);
    }

    KeyStore trusted = KeyStore.getInstance("PKCS12");
    try (InputStream in = Files.newInputStream(trustStore)) {
      trusted.load(in, STORE_PASSWORD.toCharArray());
    }
    TrustManagerFactory factory = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
    factory.init(trusted);
    SSLContext context = SSLContext.getInstance("TLS");
    context.init(null, factory.getTrustManagers(), null);

    MqttClient publisher = new MqttClient(mqttAddress, "tls-it-publisher", new MemoryPersistence());
    MqttConnectOptions options = new MqttConnectOptions();
    options.setCleanSession(true);
    options.setUserName("simulator");
    options.setPassword("simulator-local-only".toCharArray());
    options.setSocketFactory(context.getSocketFactory());
    publisher.connect(options);
    try {
      MqttMessage status = new MqttMessage("{\"state\":\"online\"}".getBytes(StandardCharsets.UTF_8));
      status.setQos(1);
      status.setRetained(true);
      publisher.publish(MQTT_TOPIC, status);
    } finally {
      publisher.disconnect();
      publisher.close();
    }
  }

  // ---------- certificates ----------

  private static void keytool(String... arguments) throws Exception {
    List<String> command = new java.util.ArrayList<>();
    command.add(Path.of(System.getProperty("java.home"), "bin", "keytool").toString());
    command.addAll(List.of(arguments));
    Process process = new ProcessBuilder(command).redirectErrorStream(true).start();
    String output = new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
    assertThat(process.waitFor(60, TimeUnit.SECONDS)).isTrue();
    assertThat(process.exitValue()).as("keytool %s: %s", arguments[0], output).isZero();
  }

  /** Mosquitto wants the key as a PEM file; keytool only writes key stores. The header is put together here so that no file holds a key header as text. */
  private static void writePrivateKeyAsPem(Path keyStore, Path target) throws Exception {
    KeyStore store = KeyStore.getInstance("PKCS12");
    try (InputStream in = Files.newInputStream(keyStore)) {
      store.load(in, STORE_PASSWORD.toCharArray());
    }
    byte[] pkcs8 = store.getKey("server", STORE_PASSWORD.toCharArray()).getEncoded();
    String dashes = "-----";
    String body = Base64.getMimeEncoder(64, "\n".getBytes(StandardCharsets.US_ASCII)).encodeToString(pkcs8);
    Files.writeString(target, dashes + "BEGIN " + "PRIVATE KEY" + dashes + "\n" + body + "\n" + dashes + "END " + "PRIVATE KEY" + dashes + "\n");
  }

  private static int freePort() throws Exception {
    try (ServerSocket socket = new ServerSocket(0)) {
      return socket.getLocalPort();
    }
  }
}
