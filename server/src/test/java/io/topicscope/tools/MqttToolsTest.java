package io.topicscope.tools;

import static io.topicscope.tools.TopicServiceTest.ONE_MINUTE_AGO;
import static io.topicscope.tools.TopicServiceTest.healthy;
import static io.topicscope.tools.TopicServiceTest.problem;
import static org.assertj.core.api.Assertions.assertThat;

import io.topicscope.config.TopicScopeProperties;
import io.topicscope.config.TopicScopeProperties.Source;
import io.topicscope.config.TopicScopeProperties.SourceType;
import io.topicscope.kafka.KafkaRecordReader;
import io.topicscope.tools.FreshnessService.Freshness;
import io.topicscope.tools.FreshnessService.FreshnessRow;
import io.topicscope.tools.TopicServiceTest.PretendCluster;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.time.ZoneOffset;
import java.util.List;
import org.junit.jupiter.api.Test;

/** How what was heard on an MQTT broker becomes a topic list, recent messages and freshness. The broker here is pretend; MqttIT uses a real one. */
class MqttToolsTest {

  static final TopicScopeProperties PROPERTIES = new TopicScopeProperties(List.of(
      Source.open("k", SourceType.KAFKA, "kafka:9092"), Source.open("m", SourceType.MQTT, "tcp://mqtt:1883")), null, null, null, TopicScopeProperties.Audit.off());

  final PretendMqtt broker = new PretendMqtt();
  final PretendCluster cluster = new PretendCluster();
  final Sources sources = new Sources(PROPERTIES);
  final MqttService mqtt = new MqttService(sources, broker, PROPERTIES);
  final TopicService topics = new TopicService(sources, cluster, mqtt, PROPERTIES, Clock.fixed(TopicServiceTest.NOW, ZoneOffset.UTC));
  final MessageService messages = new MessageService(sources, topics, new KafkaRecordReader(), mqtt, PROPERTIES);
  final FreshnessService freshness = new FreshnessService(sources, topics, cluster, mqtt, PROPERTIES, Clock.fixed(TopicServiceTest.NOW, ZoneOffset.UTC));

  // ---------- topic list ----------

  @Test
  void theTopicListIsWhatWasHeardAndSaysSo() {
    broker.retained("site/a/status", "{\"state\":\"online\"}")
        .live("site/a/telemetry", "1", 500).live("site/a/telemetry", "2", 2500)
        .live("site/b/telemetry", "x", 1000);

    TopicService.TopicList list = topics.listTopics("m", null, null, null, null);

    assertThat(list.type()).isEqualTo("MQTT");
    assertThat(list.topics()).as("the Kafka list stays empty").isEmpty();
    assertThat(list.observed()).containsExactly(
        new TopicService.ObservedTopic("site/a/status", 0, true, 1, ""),
        new TopicService.ObservedTopic("site/a/telemetry", 2, false, 1, "2026-11-16T12:00:02Z"),
        new TopicService.ObservedTopic("site/b/telemetry", 1, false, 1, "2026-11-16T12:00:01Z"));
    assertThat(list.matching()).isEqualTo(3);
    assertThat(list.windowSeconds()).isEqualTo(3.0);
    assertThat(list.note()).startsWith("An MQTT broker keeps no list of topics. These are the topics heard on the filter '#' during 3.0 s")
        .contains("'#' does not match topics that start with '$'");
    assertThat(broker.askedFilters).containsExactly("#");
    assertThat(broker.askedWindow).isEqualTo(Duration.ofSeconds(3));
  }

  @Test
  void theFilterTheNameFilterAndTheWindowArePassedOn() {
    broker.live("site/a/telemetry", "1", 100).live("site/b/telemetry", "1", 100);

    TopicService.TopicList list = topics.listTopics("m", "/B/", null, " site/+/telemetry ", 10);

    assertThat(broker.askedFilters).containsExactly("site/+/telemetry");
    assertThat(broker.askedWindow).isEqualTo(Duration.ofSeconds(10));
    assertThat(list.observed()).extracting(TopicService.ObservedTopic::topic).containsExactly("site/b/telemetry");
    assertThat(list.note()).doesNotContain("'#' does not match");

    assertThat(problem(() -> topics.listTopics("m", null, null, null, 0))).startsWith("'windowSeconds' must be between 1 and 30, and it was 0.");
    assertThat(problem(() -> topics.listTopics("m", null, null, "a/#/b", null)))
        .startsWith("'a/#/b' is not a valid MQTT topic filter").contains("'+' stands for exactly one level");
  }

  // ---------- recent messages ----------

  @Test
  void recentMessagesAreTheRetainedOnesAndWhatArrivedNewestFirst() {
    broker.retained("site/a/status", "{\"state\":\"online\",\"password\":\"p\"}")
        .live("site/a/telemetry", "{\"seq\":1}", 500)
        .live("site/a/telemetry", "{\"seq\":2}", 2500);

    MessageService.MessageList list = messages.recentMessages("m", "site/#", null, null, null);

    assertThat(list.topic()).as("the filter that was listened on").isEqualTo("site/#");
    assertThat(list.windowSeconds()).isEqualTo(3.0);
    assertThat(list.messages()).containsExactly(
        new MessageService.MessageRow("site/a/telemetry", -1, -1, "2026-11-16T12:00:02.500Z", "", List.of(), "{\"seq\":2}", "json", 9, false, false, 1, false),
        new MessageService.MessageRow("site/a/telemetry", -1, -1, "2026-11-16T12:00:00.500Z", "", List.of(), "{\"seq\":1}", "json", 9, false, false, 1, false),
        new MessageService.MessageRow("site/a/status", -1, -1, "2026-11-16T12:00:00Z", "", List.of(),
            "{\"state\":\"online\",\"password\":\"[REDACTED]\"}", "json", 33, false, true, 1, true));
    assertThat(list.note()).startsWith("An MQTT broker keeps no history.").contains("during the 3.0 s of listening");
  }

  @Test
  void onlyTheNewestAreKeptAndTheNoteSaysHowManyArrived() {
    for (int i = 0; i < 30; i++) broker.live("t", "m" + i, i * 10L);

    MessageService.MessageList list = messages.recentMessages("m", "t", 5, null, 2);

    assertThat(list.messages()).extracting(MessageService.MessageRow::value).containsExactly("m29", "m28", "m27", "m26", "m25");
    assertThat(list.note()).contains("30 messages arrived; the newest 5 are shown.");

    broker.messages.clear(); // the pretend broker plays back whatever it holds, whichever filter is asked
    assertThat(messages.recentMessages("m", "nothing/here", null, null, 1).note())
        .contains("Nothing was heard: no retained message, and nothing was published during the window.");
  }

  // ---------- freshness ----------

  @Test
  void theRule() {
    assertThat(Freshness.judge(-1, 2)).isEqualTo(Freshness.SILENT);
    assertThat(Freshness.judge(0, 2)).isEqualTo(Freshness.FRESH);
    assertThat(Freshness.judge(3000, 2)).as("one and a half intervals is still fresh").isEqualTo(Freshness.FRESH);
    assertThat(Freshness.judge(3001, 2)).isEqualTo(Freshness.STALE);
  }

  @Test
  void aSensorThatStoppedDuringTheWindowIsStaleAndOneThatKeepsGoingIsFresh() {
    // Expected every 2 s, listened for 10 s. "going" reports to the end; "stopped" was last heard at 3 s.
    for (int at = 0; at <= 10_000; at += 2000) broker.live("s/going", "{\"ts\":" + (PretendMqtt.START + at - 40) + "}", at);
    broker.live("s/stopped", "{\"v\":1}", 1000).live("s/stopped", "{\"v\":2}", 3000);
    broker.retained("s/stopped/status", "{\"state\":\"online\"}");
    broker.retained("$SYS/broker/clients/connected", "4");

    FreshnessService.FreshnessReport report = freshness.freshness("m", "s/#", 2, 10);

    assertThat(report.type()).isEqualTo("MQTT");
    assertThat(report.expectedIntervalSeconds()).isEqualTo(2);
    assertThat(report.windowSeconds()).isEqualTo(10.0);
    assertThat(report.topics()).containsExactly(
        new FreshnessRow("s/going", "FRESH", "2026-11-16T12:00:10Z", 0, 6, false, 40),
        new FreshnessRow("s/stopped", "STALE", "2026-11-16T12:00:03Z", 7, 2, false, -1),
        new FreshnessRow("s/stopped/status", "SILENT", "", -1, 0, true, -1));
    assertThat(report).extracting(FreshnessService.FreshnessReport::fresh, FreshnessService.FreshnessReport::stale, FreshnessService.FreshnessReport::silent)
        .containsExactly(1, 1, 1);
    assertThat(report.brokerCounters()).containsExactly(new MqttService.BrokerCounter("$SYS/broker/clients/connected", "4"));
    assertThat(report.note()).contains("with a wildcard filter, a topic that was silent and has no retained message is not listed at all");
    assertThat(broker.askedFilters).startsWith("s/#").contains("$SYS/broker/clients/connected");
  }

  @Test
  void aTopicNamedExactlyIsSilentWhenNothingIsHeardOnIt() {
    FreshnessService.FreshnessReport report = freshness.freshness("m", "s/gone/telemetry", 2, 5);
    assertThat(report.topics()).containsExactly(new FreshnessRow("s/gone/telemetry", "SILENT", "", -1, 0, false, -1));
    assertThat(report.note()).contains("SILENT means nothing was published on the topic in that time.").doesNotContain("wildcard");
  }

  @Test
  void aRetainedMessageCountsOnlyIfItsPayloadSaysWhenItWasMade() {
    broker.retained("s/last-reading", "{\"ts\":" + (PretendMqtt.START - 60_000) + "}");
    FreshnessRow row = freshness.freshness("m", "s/#", 10, 5).topics().get(0);
    assertThat(row).isEqualTo(new FreshnessRow("s/last-reading", "STALE", "2026-11-16T11:59:00Z", 65, 0, true, -1));
  }

  @Test
  void theWindowDefaultsToTwoIntervalsAndWarnsWhenItIsTooShortToJudge() {
    freshness.freshness("m", null, 2, null);
    assertThat(broker.askedWindow).as("at least five seconds").isEqualTo(Duration.ofSeconds(5));
    freshness.freshness("m", null, 10, null);
    assertThat(broker.askedWindow).isEqualTo(Duration.ofSeconds(21));
    freshness.freshness("m", null, 600, null);
    assertThat(broker.askedWindow).as("at most a minute").isEqualTo(Duration.ofSeconds(60));

    assertThat(freshness.freshness("m", null, 600, null).note())
        .contains("The window (60 s) is shorter than one and a half expected intervals (600 s each)");
    assertThat(freshness.freshness("m", null, 2, 5).note()).doesNotContain("shorter than");

    assertThat(problem(() -> freshness.freshness("m", null, 0, null))).startsWith("'expectedIntervalSeconds' must be between 1 and 86400");
    assertThat(problem(() -> freshness.freshness("m", null, 2, 61))).startsWith("'windowSeconds' must be between 1 and 60");
  }

  @Test
  void kafkaFreshnessComesFromTheTimeOfTheNewestRecord() {
    cluster.topic("busy", false, healthy(0, 0, 10, ONE_MINUTE_AGO + 59_000))
        .topic("quiet", false, healthy(0, 0, 10, ONE_MINUTE_AGO))
        .topic("empty", false, healthy(0, 0, 0, -1))
        .topic("__consumer_offsets", true, healthy(0, 0, 5, ONE_MINUTE_AGO));

    FreshnessService.FreshnessReport all = freshness.freshness("k", null, 10, null);
    assertThat(all.type()).isEqualTo("KAFKA");
    assertThat(all.windowSeconds()).isZero();
    assertThat(all.brokerCounters()).isEmpty();
    assertThat(all.note()).isEmpty();
    assertThat(all.topics()).containsExactly(
        new FreshnessRow("busy", "FRESH", "2026-11-16T11:59:59Z", 1, -1, false, -1),
        new FreshnessRow("empty", "SILENT", "", -1, -1, false, -1),
        new FreshnessRow("quiet", "STALE", "2026-11-16T11:59:00Z", 60, -1, false, -1));
    assertThat(broker.askedFilters).as("a Kafka source is not listened to").isNull();

    assertThat(freshness.freshness("k", "quiet", 3600, null).topics()).extracting(FreshnessRow::status).containsExactly("FRESH");
    assertThat(problem(() -> freshness.freshness("k", "quite", 10, null))).startsWith("There is no topic named 'quite'");
  }

  // ---------- the time inside a payload ----------

  @Test
  void payloadTimesAreFoundInTheUsualPlaces() {
    assertThat(PayloadTime.of(bytes("{\"ts\":1794830400123}"))).isEqualTo(1_794_830_400_123L);
    assertThat(PayloadTime.of(bytes("{\"timestamp\":1794830400}"))).as("seconds").isEqualTo(1_794_830_400_000L);
    assertThat(PayloadTime.of(bytes("{\"time\":\"2026-11-16T12:00:00Z\"}"))).isEqualTo(1_794_830_400_000L);
    assertThat(PayloadTime.of(bytes("{\"ts\":\"yesterday\",\"timestamp\":1794830400123}"))).as("the next field is tried").isEqualTo(1_794_830_400_123L);
    for (String none : new String[] {"{\"seq\":7}", "{\"ts\":42}", "[1794830400123]", "1794830400123", "{broken", ""}) {
      assertThat(PayloadTime.of(bytes(none))).as(none).isEqualTo(-1);
    }
    assertThat(PayloadTime.of(null)).isEqualTo(-1);
  }

  private static byte[] bytes(String s) {
    return s.getBytes(StandardCharsets.UTF_8);
  }
}
