package io.topicscope;

import static io.topicscope.McpTestSupport.number;
import static org.assertj.core.api.Assertions.assertThat;

import io.topicscope.sim.DemoData;
import io.topicscope.sim.LiveTraffic;
import io.topicscope.sim.Seeder;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.WebSocket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.MountableFile;
import tools.jackson.core.type.TypeReference;
import tools.jackson.databind.json.JsonMapper;

/**
 * The check of Phase 8: the server in board mode, with live traffic on brokers of its own, watched through
 * one WebSocket connection the way the page watches it. Stopping the slow consumer must change what arrives
 * on that same connection: the page never asks again, and it is never reloaded.
 *
 * <p>What the page then draws was checked by eye, in a browser; this test checks what the page is given.
 */
class BoardIT {

  static final JsonMapper JSON = JsonMapper.builder().build();

  static KafkaContainer kafka;
  static GenericContainer<?> mosquitto;
  static LiveTraffic live;
  static Process board;
  static int port;
  static Path auditFile;

  @BeforeAll
  static void startBrokersTrafficAndTheBoard() throws Exception {
    kafka = new KafkaContainer("apache/kafka:4.3.2");
    mosquitto = new GenericContainer<>("eclipse-mosquitto:2.0.22").withExposedPorts(1883)
        .withCopyFileToContainer(MountableFile.forHostPath(Path.of("..", "mosquitto", "open.conf").toAbsolutePath().normalize()), "/mosquitto/config/mosquitto.conf");
    kafka.start();
    mosquitto.start();
    String kafkaAddress = kafka.getBootstrapServers();
    String mqttAddress = "tcp://" + mosquitto.getHost() + ":" + mosquitto.getMappedPort(1883);
    Seeder.seed(kafkaAddress, mqttAddress);
    // Ten times faster than the defaults: 20 positions a second arrive, the tracker reads 5; a reading every 0.2 s,
    // and the last sensor falls silent after 12 s.
    live = LiveTraffic.start(kafkaAddress, mqttAddress,
        new LiveTraffic.Timing(Duration.ofMillis(50), Duration.ofMillis(200), Duration.ofMillis(200), Duration.ofSeconds(12)));

    try (ServerSocket socket = new ServerSocket(0)) {
      port = socket.getLocalPort();
    }
    auditFile = Files.createTempDirectory("topic-scope-board-it").resolve("audit.jsonl");
    ProcessBuilder builder = new ProcessBuilder(McpTestSupport.javaBinary(), "-jar", McpTestSupport.serverJar(), "--spring.profiles.active=board");
    builder.environment().putAll(Map.of(
        "TOPIC_SCOPE_KAFKA", kafkaAddress, "TOPIC_SCOPE_MQTT", mqttAddress, "TOPIC_SCOPE_AUDIT_FILE", auditFile.toString(),
        "TOPIC_SCOPE_BOARD_PORT", String.valueOf(port), "TOPIC_SCOPE_BOARD_INTERVAL", "3s"));
    builder.redirectOutput(ProcessBuilder.Redirect.DISCARD).redirectError(ProcessBuilder.Redirect.DISCARD);
    board = builder.start();

    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(60);
    while (System.nanoTime() < deadline) {
      try {
        if (!request("127.0.0.1:" + port, "/", List.of()).isEmpty()) return;
      } catch (Exception e) {
        Thread.sleep(500);
      }
    }
    throw new AssertionError("The board did not start listening on port " + port);
  }

  @AfterAll
  static void stop() {
    if (board != null) board.destroyForcibly();
    if (live != null) live.close();
    if (mosquitto != null) mosquitto.stop();
    if (kafka != null) kafka.stop();
  }

  @Test
  void stoppingTheSlowConsumerChangesWhatThePageIsGivenWithoutAskingAgain() throws Exception {
    try (Page page = Page.open(port)) {
      // The first picture of a group cannot say whether it moves; the next ones can.
      Map<String, Object> reading = page.awaitSnapshot("the slow consumer is reported as LAGGING",
          s -> "LAGGING".equals(group(s, DemoData.TRACKER).get("status")));
      assertThat(group(reading, DemoData.TRACKER)).containsEntry("state", "STABLE");
      assertThat(number(group(reading, DemoData.TRACKER).get("members"))).isEqualTo(1);
      assertThat(group(reading, DemoData.ARCHIVER)).containsEntry("status", "OK");
      assertThat(group(reading, DemoData.REPORTER)).containsEntry("status", "STALLED");
      assertThat(group(reading, DemoData.ALARM_NOTIFIER)).containsEntry("status", "NO_CONSUMER");
      assertThat(list(reading.get("sources"))).allSatisfy(s -> assertThat(s).containsEntry("reachable", true));
      assertThat(list(list(reading.get("kafka")).get(0).get("topics"))).extracting(t -> (String) t.get("name"))
          .containsExactlyElementsOf(DemoData.TOPICS.stream().map(DemoData.TopicSpec::name).sorted().toList());

      live.holdTracker(true);

      Map<String, Object> held = page.awaitSnapshot("the held consumer turns STALLED on the same connection",
          s -> "STALLED".equals(group(s, DemoData.TRACKER).get("status")));
      Map<String, Object> tracker = group(held, DemoData.TRACKER);
      assertThat(number(tracker.get("members"))).as("it is still connected; it just does not read").isEqualTo(1);
      assertThat((String) tracker.get("summary")).startsWith("Not reading:").contains("the group has 1 member(s)");
      assertThat(group(held, DemoData.ARCHIVER)).as("the other consumer is not affected").containsEntry("status", "OK");

      live.holdTracker(false);
      page.awaitSnapshot("released, it reads again", s -> "LAGGING".equals(group(s, DemoData.TRACKER).get("status")));
      assertThat(page.sent).as("the page asked for nothing; everything was pushed").isZero();
    }
  }

  @Test
  void aSensorThatFallsSilentIsRememberedAndTurnsStale() throws Exception {
    String silenced = DemoData.SILENT_DEVICE.telemetryTopic();
    try (Page page = Page.open(port)) {
      // A single tool call cannot know a topic that is silent. The board heard it earlier and keeps the time.
      Map<String, Object> snapshot = page.awaitSnapshot("the silenced sensor is STALE", s -> "STALE".equals(mqttTopic(s, silenced).get("status")));
      assertThat(number(mqttTopic(snapshot, silenced).get("ageSeconds"))).isGreaterThanOrEqualTo(7);
      assertThat(mqttTopic(snapshot, DemoData.DEVICES.get(0).telemetryTopic())).containsEntry("status", "FRESH");
      Map<String, Object> status = mqttTopic(snapshot, DemoData.DEVICES.get(0).statusTopic());
      assertThat(status).as("a topic only ever seen as a retained message").containsEntry("status", "SILENT").containsEntry("retained", true);
    }
  }

  @Test
  @SuppressWarnings("unchecked")
  void watchingATopicBringsItsNewestMessagesAndIsRecorded() throws Exception {
    try (Page page = Page.open(port)) {
      page.awaitSnapshot("a first picture", s -> true);
      page.send("{\"type\":\"watch\",\"source\":\"local-kafka\",\"topic\":\"" + DemoData.EDGE_CASES.name() + "\"}");

      Map<String, Object> watch = (Map<String, Object>) page.awaitMessage("the watched topic's messages", m -> m.get("watch") != null).get("watch");
      assertThat(watch).containsEntry("topic", DemoData.EDGE_CASES.name()).containsEntry("error", "");
      List<Map<String, Object>> messages = list(watch.get("messages"));
      assertThat(messages).hasSize(DemoData.EDGE_CASES.seededTotal());
      assertThat(messages.toString()).as("formatted and redacted like recent_messages").contains("[REDACTED]").doesNotContain("not-a-real-password");

      // An MQTT topic that reports every 0.2 s here (every 2 s by default): one short listen could miss it, so the
      // board shows what it has heard on the topic since it started. The retained status message is kept once.
      String telemetry = DemoData.DEVICES.get(0).telemetryTopic();
      page.send("{\"type\":\"watch\",\"source\":\"local-mqtt\",\"topic\":\"" + telemetry + "\"}");
      Map<String, Object> heard = (Map<String, Object>) page.awaitMessage("messages heard on the watched MQTT topic", m -> m.get("watch") != null
          && telemetry.equals(((Map<String, Object>) m.get("watch")).get("topic")) && !list(((Map<String, Object>) m.get("watch")).get("messages")).isEmpty()).get("watch");
      assertThat(list(heard.get("messages"))).hasSizeLessThanOrEqualTo(10).allSatisfy(m -> {
        assertThat(m).containsEntry("topic", telemetry).containsEntry("retained", false).containsEntry("valueFormat", "json");
        assertThat((String) m.get("value")).contains("\"device\":\"twr-01/wind\"", "\"ts\":");
      });
      assertThat((String) heard.get("note")).contains("since it started listening");

      String status = DemoData.DEVICES.get(0).statusTopic();
      page.send("{\"type\":\"watch\",\"source\":\"local-mqtt\",\"topic\":\"" + status + "\"}");
      Map<String, Object> retained = (Map<String, Object>) page.awaitMessage("the retained message of a status topic", m -> m.get("watch") != null
          && status.equals(((Map<String, Object>) m.get("watch")).get("topic")) && !list(((Map<String, Object>) m.get("watch")).get("messages")).isEmpty()).get("watch");
      assertThat(list(retained.get("messages"))).as("a retained message arrives with every look and is shown once").hasSize(1)
          .allSatisfy(m -> assertThat(m).containsEntry("retained", true).containsEntry("value", DemoData.DEVICES.get(0).statusJson()));

      page.send("{\"type\":\"watch\",\"source\":\"local-kafka\",\"topic\":\"no.such.topic\"}");
      Map<String, Object> refused = (Map<String, Object>) page.awaitMessage("a problem instead of messages",
          m -> m.get("watch") != null && "no.such.topic".equals(((Map<String, Object>) m.get("watch")).get("topic"))).get("watch");
      assertThat((String) refused.get("error")).contains("There is no topic named 'no.such.topic'");
    }
    List<String> audit = Files.readAllLines(auditFile);
    assertThat(audit).anySatisfy(line -> assertThat(line).contains("\"tool\":\"board_open\""));
    assertThat(audit).anySatisfy(line -> assertThat(line).contains("\"tool\":\"board_watch\"", "\"topic\":\"" + DemoData.EDGE_CASES.name() + "\"", "\"outcome\":\"ok\""));
    assertThat(String.join("\n", audit)).as("no message content in the record").doesNotContain("operator1", "Wind shear");
  }

  @Test
  void onlyThisMachinesOwnPagesGetIn() throws Exception {
    String local = "127.0.0.1:" + port;
    List<String> page = request(local, "/", List.of());
    assertThat(page).as("this machine's own name").anyMatch(line -> line.equalsIgnoreCase("X-Frame-Options: DENY"))
        .anyMatch(line -> line.toLowerCase().startsWith("content-security-policy: frame-ancestors 'none'"));

    assertThat(request("evil.example", "/", List.of()).get(0)).as("a foreign name pointed at this machine").contains(" 403");

    List<String> handshake = List.of("Upgrade: websocket", "Connection: Upgrade", "Sec-WebSocket-Version: 13", "Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==");
    List<String> own = new ArrayList<>(handshake);
    own.add("Origin: http://" + local);
    assertThat(request(local, "/ws", own).get(0)).as("a page that came from the board itself").contains(" 101");

    List<String> foreign = new ArrayList<>(handshake);
    foreign.add("Origin: http://evil.example");
    assertThat(request(local, "/ws", foreign).get(0)).as("a page from another site cannot open the WebSocket").contains(" 403");
  }

  // ---------- reading a snapshot ----------

  @SuppressWarnings("unchecked")
  private static List<Map<String, Object>> list(Object value) {
    return (List<Map<String, Object>>) value;
  }

  private static Map<String, Object> group(Map<String, Object> snapshot, DemoData.GroupSpec group) {
    return list(list(snapshot.get("kafka")).get(0).get("groups")).stream().filter(g -> group.group().equals(g.get("group"))).findFirst().orElse(Map.of());
  }

  private static Map<String, Object> mqttTopic(Map<String, Object> snapshot, String topic) {
    return list(list(snapshot.get("mqtt")).get(0).get("topics")).stream().filter(t -> topic.equals(t.get("topic"))).findFirst().orElse(Map.of());
  }

  /**
   * One raw HTTP request, so the Host and Origin headers can be anything (the JDK's HTTP client does not let them be set).
   *
   * @return the status line and the header lines of the answer
   */
  private static List<String> request(String host, String path, List<String> headers) throws Exception {
    try (Socket socket = new Socket("127.0.0.1", port)) {
      socket.setSoTimeout(10_000);
      StringBuilder out = new StringBuilder("GET " + path + " HTTP/1.1\r\nHost: " + host + "\r\n");
      for (String header : headers) out.append(header).append("\r\n");
      if (headers.isEmpty()) out.append("Connection: close\r\n");
      out.append("\r\n");
      socket.getOutputStream().write(out.toString().getBytes(StandardCharsets.US_ASCII));
      BufferedReader reader = new BufferedReader(new InputStreamReader(socket.getInputStream(), StandardCharsets.US_ASCII));
      List<String> lines = new ArrayList<>();
      for (String line = reader.readLine(); line != null && !line.isEmpty(); line = reader.readLine()) lines.add(line);
      return lines;
    }
  }

  /** One open page: a WebSocket connection that collects what the server pushes. */
  private static final class Page implements AutoCloseable, WebSocket.Listener {
    private final BlockingQueue<Map<String, Object>> received = new LinkedBlockingQueue<>();
    private final StringBuilder partial = new StringBuilder();
    private WebSocket socket;
    int sent;

    static Page open(int port) throws Exception {
      Page page = new Page();
      page.socket = HttpClient.newHttpClient().newWebSocketBuilder().buildAsync(URI.create("ws://127.0.0.1:" + port + "/ws"), page).get(30, TimeUnit.SECONDS);
      return page;
    }

    void send(String text) throws Exception {
      sent++;
      socket.sendText(text, true).get(10, TimeUnit.SECONDS);
    }

    @Override
    public CompletionStage<?> onText(WebSocket webSocket, CharSequence data, boolean last) {
      partial.append(data);
      if (last) {
        received.add(JSON.readValue(partial.toString(), new TypeReference<Map<String, Object>>() {}));
        partial.setLength(0);
      }
      webSocket.request(1);
      return null;
    }

    /** Waits for a pushed message that satisfies the condition; earlier ones are dropped, as a page drops an old picture. */
    Map<String, Object> awaitMessage(String what, Predicate<Map<String, Object>> condition) throws InterruptedException {
      long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(90);
      Map<String, Object> last = null;
      while (System.nanoTime() < deadline) {
        Map<String, Object> message = received.poll(1, TimeUnit.SECONDS);
        if (message == null) continue;
        last = message;
        if (condition.test(message)) return message;
      }
      throw new AssertionError("Timed out waiting until " + what + ". The last message was: " + last);
    }

    @SuppressWarnings("unchecked")
    Map<String, Object> awaitSnapshot(String what, Predicate<Map<String, Object>> condition) throws InterruptedException {
      return (Map<String, Object>) awaitMessage(what, m -> condition.test((Map<String, Object>) m.get("snapshot"))).get("snapshot");
    }

    @Override
    public void close() {
      socket.abort();
    }
  }
}
