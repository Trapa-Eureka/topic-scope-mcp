package io.topicscope;

import static io.topicscope.McpTestSupport.KAFKA_ADDRESS;
import static io.topicscope.McpTestSupport.MQTT_ADDRESS;
import static io.topicscope.McpTestSupport.call;
import static io.topicscope.McpTestSupport.number;
import static org.assertj.core.api.Assertions.assertThat;

import io.modelcontextprotocol.client.McpClient;
import io.modelcontextprotocol.client.McpSyncClient;
import io.modelcontextprotocol.client.transport.HttpClientStreamableHttpTransport;
import io.modelcontextprotocol.spec.McpSchema;
import io.topicscope.sim.DemoData;
import java.net.ServerSocket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * The server in HTTP mode: the same tools over Streamable HTTP, and nobody gets in without the token.
 * Runs against the shared seeded brokers.
 */
class HttpIT {

  static final String TOKEN = "it-token-" + "0123456789abcdef".repeat(2);
  static final String INITIALIZE = "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"initialize\",\"params\":{\"protocolVersion\":\"2025-06-18\","
      + "\"capabilities\":{},\"clientInfo\":{\"name\":\"it\",\"version\":\"0\"}}}";

  static Process server;
  static int port;
  static Path auditFile;

  @BeforeAll
  static void startInHttpMode() throws Exception {
    try (ServerSocket socket = new ServerSocket(0)) {
      port = socket.getLocalPort();
    }
    auditFile = Files.createTempDirectory("topic-scope-http-it").resolve("audit.jsonl");
    server = start(Map.of("TOPIC_SCOPE_HTTP_TOKEN", TOKEN, "TOPIC_SCOPE_HTTP_PORT", String.valueOf(port)));

    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(60);
    while (System.nanoTime() < deadline) {
      try {
        post(null, null);
        return;
      } catch (Exception e) {
        Thread.sleep(500);
      }
    }
    throw new AssertionError("The server did not start listening on port " + port);
  }

  @AfterAll
  static void stop() {
    if (server != null) server.destroyForcibly();
  }

  private static Process start(Map<String, String> environment) throws Exception {
    ProcessBuilder builder = new ProcessBuilder(McpTestSupport.javaBinary(), "-jar", McpTestSupport.serverJar(), "--spring.profiles.active=http");
    builder.environment().putAll(Map.of("TOPIC_SCOPE_KAFKA", KAFKA_ADDRESS, "TOPIC_SCOPE_MQTT", MQTT_ADDRESS, "TOPIC_SCOPE_AUDIT_FILE", auditFile.toString()));
    builder.environment().remove("TOPIC_SCOPE_HTTP_TOKEN");
    builder.environment().putAll(environment);
    builder.redirectErrorStream(true);
    return builder.start();
  }

  @Test
  void withTheTokenAClientGetsTheSameToolsAsOverStdio() throws Exception {
    HttpClientStreamableHttpTransport transport = HttpClientStreamableHttpTransport.builder("http://127.0.0.1:" + port)
        .endpoint("/mcp")
        .requestBuilder(HttpRequest.newBuilder().header("Authorization", "Bearer " + TOKEN))
        .build();
    try (McpSyncClient client = McpClient.sync(transport).requestTimeout(Duration.ofSeconds(60)).build()) {
      McpSchema.InitializeResult hello = client.initialize();
      assertThat(hello.serverInfo().name()).isEqualTo("topic-scope");
      assertThat(hello.serverInfo().version()).isEqualTo("0.1.1");

      assertThat(client.listTools().tools()).extracting(McpSchema.Tool::name).containsExactlyInAnyOrder(
          "list_sources", "list_topics", "describe_topic", "recent_messages", "list_consumer_groups", "consumer_lag", "topic_freshness");

      assertThat(call(client, "list_sources", Map.of())).containsEntry("reachable", 2);
      Map<String, Object> lag = call(client, "consumer_lag", Map.of("source", "local-kafka", "group", DemoData.TRACKER.group(), "sampleSeconds", 1));
      assertThat(number(lag.get("lag"))).isEqualTo(DemoData.TRACKER.seededLagTotal());
    }
    assertThat(Files.readAllLines(auditFile)).as("calls over HTTP are recorded like any other")
        .anySatisfy(line -> assertThat(line).contains("\"tool\":\"consumer_lag\"", "\"outcome\":\"ok\""));
  }

  @Test
  void withoutTheTokenNothingIsAnswered() throws Exception {
    assertThat(post(null, null).statusCode()).as("no token").isEqualTo(401);
    assertThat(post("Bearer wrong-token-wrong-token-wrong", null).statusCode()).as("a wrong token").isEqualTo(401);
    assertThat(post(TOKEN, null).statusCode()).as("the token without the Bearer scheme").isEqualTo(401);
    assertThat(post("Bearer " + TOKEN + "x", null).statusCode()).as("the token with something appended").isEqualTo(401);

    HttpResponse<String> refused = post(null, null);
    assertThat(refused.headers().firstValue("WWW-Authenticate")).contains("Bearer");
    assertThat(refused.body()).as("nothing about the server or the brokers leaks into a refusal").doesNotContain("topic-scope", "local-kafka", TOKEN);

    assertThat(post("Bearer " + TOKEN, null).statusCode()).as("the right token").isEqualTo(200);
  }

  @Test
  void aWebPageIsRefusedEvenWithTheToken() throws Exception {
    assertThat(post("Bearer " + TOKEN, "http://evil.example").statusCode()).isEqualTo(403);
    assertThat(post("Bearer " + TOKEN, "http://127.0.0.1:" + port).statusCode()).as("no origin is allowed unless it is configured").isEqualTo(403);
  }

  @Test
  void itRefusesToStartWithoutAToken() throws Exception {
    int otherPort;
    try (ServerSocket socket = new ServerSocket(0)) {
      otherPort = socket.getLocalPort();
    }
    for (String token : List.of("", "too-short")) {
      Process open = start(Map.of("TOPIC_SCOPE_HTTP_TOKEN", token, "TOPIC_SCOPE_HTTP_PORT", String.valueOf(otherPort)));
      try {
        String output = new String(open.getInputStream().readAllBytes());
        assertThat(open.waitFor(60, TimeUnit.SECONDS)).as("it gives up instead of serving").isTrue();
        assertThat(open.exitValue()).isNotZero();
        assertThat(output).contains("HTTP mode needs a token of at least 24 characters in TOPIC_SCOPE_HTTP_TOKEN");
      } finally {
        open.destroyForcibly();
      }
    }
  }

  /** One initialize request with the given Authorization and Origin headers; either may be left out. */
  private static HttpResponse<String> post(String authorization, String origin) throws Exception {
    HttpRequest.Builder request = HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + port + "/mcp"))
        .timeout(Duration.ofSeconds(20))
        .header("Content-Type", "application/json")
        .header("Accept", "application/json, text/event-stream")
        .POST(HttpRequest.BodyPublishers.ofString(INITIALIZE));
    if (authorization != null) request.header("Authorization", authorization);
    if (origin != null) request.header("Origin", origin);
    return HttpClient.newHttpClient().send(request.build(), HttpResponse.BodyHandlers.ofString());
  }
}
