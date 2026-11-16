package io.topicscope;

import static org.assertj.core.api.Assertions.assertThat;

import io.modelcontextprotocol.client.McpClient;
import io.modelcontextprotocol.client.McpSyncClient;
import io.modelcontextprotocol.client.transport.ServerParameters;
import io.modelcontextprotocol.client.transport.StdioClientTransport;
import io.modelcontextprotocol.json.McpJsonDefaults;
import io.modelcontextprotocol.spec.McpSchema;
import java.io.File;
import java.net.ServerSocket;
import java.nio.file.Files;
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
 * The check of Phase 2, end to end: the packaged jar is started the way an MCP client starts it (a child
 * process talking over stdin/stdout), against real Kafka and Mosquitto brokers in Docker.
 *
 * <p>It runs the jar and not the classes on purpose. One bug only showed in the jar: its nested class
 * loader is not visible to every thread.
 */
class McpServerIT {

  static final KafkaContainer kafka = new KafkaContainer("apache/kafka:4.3.2");
  static final GenericContainer<?> mosquitto = new GenericContainer<>("eclipse-mosquitto:2.0.22")
      .withExposedPorts(1883)
      .withCopyFileToContainer(MountableFile.forHostPath(Path.of("..", "mosquitto", "mosquitto.conf").toAbsolutePath().normalize()),
          "/mosquitto/config/mosquitto.conf");

  static String kafkaAddress;
  static String mqttAddress;

  @BeforeAll
  static void startBrokers() {
    kafka.start();
    mosquitto.start();
    kafkaAddress = kafka.getBootstrapServers();
    mqttAddress = "tcp://" + mosquitto.getHost() + ":" + mosquitto.getMappedPort(1883);
  }

  @AfterAll
  static void stopBrokers() {
    mosquitto.stop();
    kafka.stop();
  }

  @Test
  void aClientSeesTheServerAndItsTools() {
    try (McpSyncClient client = connect(kafkaAddress, mqttAddress)) {
      McpSchema.InitializeResult hello = client.initialize();
      assertThat(hello.serverInfo().name()).isEqualTo("topic-scope");
      assertThat(hello.instructions()).contains("Read-only");

      List<McpSchema.Tool> tools = client.listTools().tools();
      assertThat(tools).extracting(McpSchema.Tool::name).containsExactly("list_sources");
      // Holds for every tool, including the ones later phases add: all of them only read.
      assertThat(tools).allSatisfy(tool -> {
        assertThat(tool.description()).as(tool.name() + " description").isNotBlank();
        assertThat(tool.annotations().readOnlyHint()).as(tool.name() + " readOnlyHint").isTrue();
        assertThat(tool.annotations().destructiveHint()).as(tool.name() + " destructiveHint").isFalse();
        assertThat(tool.outputSchema()).as(tool.name() + " outputSchema").isNotNull();
      });
    }
  }

  @Test
  void listSourcesReportsBothBrokersAsReachable() {
    try (McpSyncClient client = connect(kafkaAddress, mqttAddress)) {
      client.initialize();
      Map<String, Object> result = call(client, "list_sources");

      assertThat(result).containsEntry("total", 2).containsEntry("reachable", 2);
      Map<String, Object> k = row(result, "local-kafka");
      assertThat(k).containsEntry("type", "KAFKA").containsEntry("address", kafkaAddress).containsEntry("reachable", true);
      assertThat((String) k.get("detail")).startsWith("cluster ").endsWith(", 1 broker");
      Map<String, Object> m = row(result, "local-mqtt");
      assertThat(m).containsEntry("type", "MQTT").containsEntry("address", mqttAddress).containsEntry("reachable", true);
      assertThat((String) m.get("detail")).isEqualTo("mosquitto version 2.0.22");
    }
  }

  @Test
  void aBrokerThatIsDownIsARowAndNotAnError() throws Exception {
    int closedPort;
    try (ServerSocket socket = new ServerSocket(0)) {
      closedPort = socket.getLocalPort();
    }
    try (McpSyncClient client = connect(kafkaAddress, "tcp://127.0.0.1:" + closedPort)) {
      client.initialize();
      Map<String, Object> result = call(client, "list_sources");

      assertThat(result).containsEntry("total", 2).containsEntry("reachable", 1);
      assertThat(row(result, "local-kafka")).containsEntry("reachable", true);
      Map<String, Object> m = row(result, "local-mqtt");
      assertThat(m).containsEntry("reachable", false);
      assertThat((String) m.get("detail")).contains("Connection refused");
    }
  }

  @Test
  void theServerExitsWhenItsClientGoesAway() throws Exception {
    ProcessBuilder builder = new ProcessBuilder(javaBinary(), "-jar", serverJar());
    builder.environment().put("TOPIC_SCOPE_KAFKA", kafkaAddress);
    builder.environment().put("TOPIC_SCOPE_MQTT", mqttAddress);
    builder.redirectError(ProcessBuilder.Redirect.DISCARD);
    Process server = builder.start();
    try {
      Thread.sleep(4000); // let it start; closing stdin earlier is a different case
      assertThat(server.isAlive()).as("the server runs while its stdin is open").isTrue();
      server.getOutputStream().close(); // the client closes the pipe
      assertThat(server.waitFor(15, TimeUnit.SECONDS)).as("exits after stdin closes, leaving no orphan process").isTrue();
      assertThat(server.exitValue()).isZero();
    } finally {
      server.destroyForcibly();
    }
  }

  // ---------- helpers ----------

  private static McpSyncClient connect(String kafka, String mqtt) {
    ServerParameters parameters = ServerParameters.builder(javaBinary())
        .args("-jar", serverJar(), "--topic-scope.probe-timeout=3s")
        .env(Map.of("TOPIC_SCOPE_KAFKA", kafka, "TOPIC_SCOPE_MQTT", mqtt))
        .build();
    return McpClient.sync(new StdioClientTransport(parameters, McpJsonDefaults.getMapper()))
        .requestTimeout(Duration.ofSeconds(60))
        .build();
  }

  @SuppressWarnings("unchecked")
  private static Map<String, Object> call(McpSyncClient client, String tool) {
    McpSchema.CallToolResult result = client.callTool(new McpSchema.CallToolRequest(tool, Map.of()));
    assertThat(result.isError()).as("isError of " + tool).isNotEqualTo(Boolean.TRUE);
    assertThat(result.content()).as("text content for clients that do not read structured results").isNotEmpty();
    assertThat(result.structuredContent()).as("structured result of " + tool).isInstanceOf(Map.class);
    return (Map<String, Object>) result.structuredContent();
  }

  @SuppressWarnings("unchecked")
  private static Map<String, Object> row(Map<String, Object> result, String name) {
    return ((List<Map<String, Object>>) result.get("sources")).stream()
        .filter(r -> name.equals(r.get("name"))).findFirst()
        .orElseThrow(() -> new AssertionError("no source named " + name + " in " + result));
  }

  private static String javaBinary() {
    return Path.of(System.getProperty("java.home"), "bin", "java").toString();
  }

  /** The runnable jar built by the package phase; its path comes from the failsafe configuration in pom.xml. */
  private static String serverJar() {
    String jar = System.getProperty("topicscope.server.jar");
    assertThat(jar).as("system property topicscope.server.jar (run through ./mvnw verify)").isNotBlank();
    assertThat(Files.isRegularFile(new File(jar).toPath())).as("the packaged server jar exists: " + jar).isTrue();
    return jar;
  }
}
