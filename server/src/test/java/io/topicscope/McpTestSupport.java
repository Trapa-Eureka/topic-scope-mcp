package io.topicscope;

import static org.assertj.core.api.Assertions.assertThat;

import io.modelcontextprotocol.client.McpClient;
import io.modelcontextprotocol.client.McpSyncClient;
import io.modelcontextprotocol.client.transport.ServerParameters;
import io.modelcontextprotocol.client.transport.StdioClientTransport;
import io.modelcontextprotocol.json.McpJsonDefaults;
import io.modelcontextprotocol.spec.McpSchema;
import io.topicscope.sim.Seeder;
import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.MountableFile;

/**
 * What every end-to-end test shares: real Kafka and Mosquitto brokers in Docker, seeded with the demo data,
 * and the packaged server jar started the way an MCP client starts it (a child process on stdin/stdout).
 *
 * <p>The brokers start once per test run and are seeded only; no live traffic. Their state is exactly what
 * {@code DemoData} describes, so tests compare against its constants. Tests must not change the brokers.
 *
 * <p>It runs the jar and not the classes on purpose. One bug only showed in the jar: its nested class
 * loader is not visible to every thread.
 */
final class McpTestSupport {

  private McpTestSupport() {}

  static final KafkaContainer KAFKA = new KafkaContainer("apache/kafka:4.3.2");
  static final GenericContainer<?> MOSQUITTO = new GenericContainer<>("eclipse-mosquitto:2.0.22")
      .withExposedPorts(1883)
      .withCopyFileToContainer(MountableFile.forHostPath(Path.of("..", "mosquitto", "open.conf").toAbsolutePath().normalize()),
          "/mosquitto/config/mosquitto.conf");

  static final String KAFKA_ADDRESS;
  static final String MQTT_ADDRESS;

  // Started once for all test classes; Testcontainers removes them when the JVM ends.
  static {
    KAFKA.start();
    MOSQUITTO.start();
    KAFKA_ADDRESS = KAFKA.getBootstrapServers();
    MQTT_ADDRESS = "tcp://" + MOSQUITTO.getHost() + ":" + MOSQUITTO.getMappedPort(1883);
    try {
      Seeder.seed(KAFKA_ADDRESS, MQTT_ADDRESS);
    } catch (Exception e) {
      throw new IllegalStateException("The demo data could not be seeded", e);
    }
  }

  /** A client connected to a fresh server process that looks at the test brokers. Close it to end the server. */
  static McpSyncClient connect() {
    return connect(KAFKA_ADDRESS, MQTT_ADDRESS);
  }

  /** Where the servers started by the tests record their calls: a file of this test run, never the developer's own. */
  static final Path AUDIT_FILE = temporaryAuditFile();

  static McpSyncClient connect(String kafka, String mqtt) {
    return connect(Map.of("TOPIC_SCOPE_KAFKA", kafka, "TOPIC_SCOPE_MQTT", mqtt));
  }

  /** A server with the given environment on top of the defaults; extra arguments are Spring settings such as {@code --topic-scope.x=y}. */
  static McpSyncClient connect(Map<String, String> environment, String... extraArguments) {
    Map<String, String> env = new java.util.HashMap<>(Map.of("TOPIC_SCOPE_KAFKA", KAFKA_ADDRESS, "TOPIC_SCOPE_MQTT", MQTT_ADDRESS,
        "TOPIC_SCOPE_AUDIT_FILE", AUDIT_FILE.toString()));
    env.putAll(environment);
    List<String> arguments = new java.util.ArrayList<>(List.of("-jar", serverJar(), "--topic-scope.probe-timeout=3s"));
    arguments.addAll(List.of(extraArguments));
    ServerParameters parameters = ServerParameters.builder(javaBinary()).args(arguments).env(env).build();
    McpSyncClient client = McpClient.sync(new StdioClientTransport(parameters, McpJsonDefaults.getMapper()))
        .requestTimeout(Duration.ofSeconds(60))
        .build();
    client.initialize();
    return client;
  }

  /** Calls a tool that is expected to answer, and returns its structured result. */
  @SuppressWarnings("unchecked")
  static Map<String, Object> call(McpSyncClient client, String tool, Map<String, Object> arguments) {
    McpSchema.CallToolResult result = client.callTool(new McpSchema.CallToolRequest(tool, arguments));
    assertThat(result.isError()).as("isError of %s: %s", tool, result.content()).isNotEqualTo(Boolean.TRUE);
    assertThat(result.content()).as("text content for clients that do not read structured results").isNotEmpty();
    assertThat(result.structuredContent()).as("structured result of " + tool).isInstanceOf(Map.class);
    return (Map<String, Object>) result.structuredContent();
  }

  /** Calls a tool that is expected to refuse, and returns the message the assistant would read. */
  static String callExpectingProblem(McpSyncClient client, String tool, Map<String, Object> arguments) {
    McpSchema.CallToolResult result = client.callTool(new McpSchema.CallToolRequest(tool, arguments));
    assertThat(result.isError()).as("%s should answer with an error result: %s", tool, result).isEqualTo(Boolean.TRUE);
    StringBuilder text = new StringBuilder();
    for (McpSchema.Content c : result.content()) {
      if (c instanceof McpSchema.TextContent t) text.append(t.text());
    }
    return text.toString();
  }

  /** The row of a list result whose {@code name} is the given one. */
  @SuppressWarnings("unchecked")
  static Map<String, Object> row(Map<String, Object> result, String listField, String name) {
    return ((List<Map<String, Object>>) result.get(listField)).stream()
        .filter(r -> name.equals(r.get("name"))).findFirst()
        .orElseThrow(() -> new AssertionError("no row named " + name + " in " + result.get(listField)));
  }

  /** JSON numbers arrive as Integer or Long depending on their size; tests compare them as long. */
  static long number(Object value) {
    return ((Number) value).longValue();
  }

  private static Path temporaryAuditFile() {
    try {
      return Files.createTempDirectory("topic-scope-it").resolve("audit.jsonl");
    } catch (java.io.IOException e) {
      throw new IllegalStateException(e);
    }
  }

  static String javaBinary() {
    return Path.of(System.getProperty("java.home"), "bin", "java").toString();
  }

  /** The runnable jar built by the package phase; its path comes from the failsafe configuration in pom.xml. */
  static String serverJar() {
    String jar = System.getProperty("topicscope.server.jar");
    assertThat(jar).as("system property topicscope.server.jar (run through ./mvnw verify)").isNotBlank();
    assertThat(Files.isRegularFile(new File(jar).toPath())).as("the packaged server jar exists: " + jar).isTrue();
    return jar;
  }
}
