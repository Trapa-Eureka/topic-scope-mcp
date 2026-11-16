package io.topicscope;

import static io.topicscope.McpTestSupport.KAFKA_ADDRESS;
import static io.topicscope.McpTestSupport.MQTT_ADDRESS;
import static io.topicscope.McpTestSupport.call;
import static io.topicscope.McpTestSupport.connect;
import static io.topicscope.McpTestSupport.row;
import static org.assertj.core.api.Assertions.assertThat;

import io.modelcontextprotocol.client.McpSyncClient;
import io.modelcontextprotocol.spec.McpSchema;
import java.net.ServerSocket;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

/** The server as an MCP client meets it: what it announces, {@code list_sources}, and how it starts and stops. */
class McpServerIT {

  @Test
  void aClientSeesTheServerAndItsTools() {
    try (McpSyncClient client = connect()) {
      McpSchema.InitializeResult hello = client.getCurrentInitializationResult();
      assertThat(hello.serverInfo().name()).isEqualTo("topic-scope");
      assertThat(hello.instructions()).contains("Read-only");

      List<McpSchema.Tool> tools = client.listTools().tools();
      assertThat(tools).extracting(McpSchema.Tool::name).containsExactlyInAnyOrder("list_sources", "list_topics", "describe_topic", "recent_messages", "list_consumer_groups", "consumer_lag", "topic_freshness");
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
    try (McpSyncClient client = connect()) {
      Map<String, Object> result = call(client, "list_sources", Map.of());

      assertThat(result).containsEntry("total", 2).containsEntry("reachable", 2);
      Map<String, Object> k = row(result, "sources", "local-kafka");
      assertThat(k).containsEntry("type", "KAFKA").containsEntry("address", KAFKA_ADDRESS).containsEntry("reachable", true);
      assertThat((String) k.get("detail")).startsWith("cluster ").endsWith(", 1 broker");
      Map<String, Object> m = row(result, "sources", "local-mqtt");
      assertThat(m).containsEntry("type", "MQTT").containsEntry("address", MQTT_ADDRESS).containsEntry("reachable", true);
      assertThat((String) m.get("detail")).isEqualTo("mosquitto version 2.0.22");
    }
  }

  @Test
  void aBrokerThatIsDownIsARowAndNotAnError() throws Exception {
    int closedPort;
    try (ServerSocket socket = new ServerSocket(0)) {
      closedPort = socket.getLocalPort();
    }
    try (McpSyncClient client = connect(KAFKA_ADDRESS, "tcp://127.0.0.1:" + closedPort)) {
      Map<String, Object> result = call(client, "list_sources", Map.of());

      assertThat(result).containsEntry("total", 2).containsEntry("reachable", 1);
      assertThat(row(result, "sources", "local-kafka")).containsEntry("reachable", true);
      Map<String, Object> m = row(result, "sources", "local-mqtt");
      assertThat(m).containsEntry("reachable", false);
      assertThat((String) m.get("detail")).contains("Connection refused");
    }
  }

  @Test
  void theServerExitsWhenItsClientGoesAway() throws Exception {
    ProcessBuilder builder = new ProcessBuilder(McpTestSupport.javaBinary(), "-jar", McpTestSupport.serverJar());
    builder.environment().put("TOPIC_SCOPE_KAFKA", KAFKA_ADDRESS);
    builder.environment().put("TOPIC_SCOPE_MQTT", MQTT_ADDRESS);
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
}
