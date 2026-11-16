package io.topicscope.tools;

import io.topicscope.config.TopicScopeProperties;
import io.topicscope.config.TopicScopeProperties.Source;
import io.topicscope.config.TopicScopeProperties.SourceType;
import io.topicscope.kafka.KafkaTopicReader;
import io.topicscope.kafka.KafkaTopicReader.TopicInfo;
import io.topicscope.kafka.KafkaTopicReader.TopicName;
import io.topicscope.tools.MqttService.BrokerCounter;
import io.topicscope.tools.MqttService.TopicActivity;
import java.time.Clock;
import java.util.ArrayList;
import java.util.List;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

/**
 * Answers "is anything still arriving?" for topics that are expected to carry a message every so often.
 * For an MQTT source this stands in for consumer lag, which MQTT does not have.
 */
@Service
public class FreshnessService {

  static final int DEFAULT_EXPECTED_SECONDS = 60;
  static final int MAX_EXPECTED_SECONDS = 86_400;
  static final int MAX_WINDOW_SECONDS = 60;
  static final int MAX_TOPICS = 200;

  /** A topic is stale once its newest message is older than this many expected intervals. One interval alone would flap. */
  static final double GRACE = 1.5;

  /** The verdict on one topic. */
  public enum Freshness {
    /** The newest message is no older than one and a half expected intervals. */
    FRESH,
    /** There is a newest message, and it is older than that. */
    STALE,
    /** No message at all: an empty Kafka topic, or an MQTT topic on which nothing was heard. */
    SILENT;

    /** @param ageMillis age of the newest message; negative when there is none */
    public static Freshness judge(long ageMillis, int expectedIntervalSeconds) {
      if (ageMillis < 0) return SILENT;
      return ageMillis > expectedIntervalSeconds * 1000L * GRACE ? STALE : FRESH;
    }
  }

  /**
   * One row of {@code topic_freshness}. No field is ever null.
   *
   * @param status {@code FRESH}, {@code STALE} or {@code SILENT}
   * @param lastMessageTime time of the newest message, ISO-8601 in UTC; empty when there is none
   * @param ageSeconds seconds since that message; -1 when there is none
   * @param messagesInWindow MQTT: messages published while listening. Kafka: -1
   * @param retained MQTT: whether the broker holds a retained message for the topic. Kafka: false
   * @param deliveryDelayMillis MQTT: arrival minus the time inside the newest payload, when the payload carries one. Otherwise -1
   */
  public record FreshnessRow(String topic, String status, String lastMessageTime, long ageSeconds, int messagesInWindow, boolean retained,
                             long deliveryDelayMillis) {}

  /**
   * The result of {@code topic_freshness}.
   *
   * @param windowSeconds how long an MQTT source was listened to; 0 for a Kafka source
   * @param brokerCounters MQTT: what the broker reports about itself under $SYS, when it does; otherwise empty
   * @param note anything the reader must know to read the list correctly; empty when there is nothing
   */
  public record FreshnessReport(String source, String type, int expectedIntervalSeconds, double windowSeconds, List<FreshnessRow> topics,
                                int fresh, int stale, int silent, List<BrokerCounter> brokerCounters, String note) {}

  private final Sources sources;
  private final TopicService topics;
  private final KafkaTopicReader kafka;
  private final MqttService mqtt;
  private final TopicScopeProperties properties;
  private final Clock clock;

  @Autowired
  public FreshnessService(Sources sources, TopicService topics, KafkaTopicReader kafka, MqttService mqtt, TopicScopeProperties properties) {
    this(sources, topics, kafka, mqtt, properties, Clock.systemUTC());
  }

  FreshnessService(Sources sources, TopicService topics, KafkaTopicReader kafka, MqttService mqtt, TopicScopeProperties properties, Clock clock) {
    this.sources = sources;
    this.topics = topics;
    this.kafka = kafka;
    this.mqtt = mqtt;
    this.properties = properties;
    this.clock = clock;
  }

  public FreshnessReport freshness(String sourceName, String topic, Integer expectedIntervalSeconds, Integer windowSeconds) {
    Source source = sources.require(sourceName);
    int expected = expectedIntervalSeconds == null ? DEFAULT_EXPECTED_SECONDS : expectedIntervalSeconds;
    if (expected < 1 || expected > MAX_EXPECTED_SECONDS) {
      throw new ToolProblem("'expectedIntervalSeconds' must be between 1 and " + MAX_EXPECTED_SECONDS + ", and it was " + expected + ".",
          "Give the longest gap between two messages that is still normal for the topic. The default is " + DEFAULT_EXPECTED_SECONDS + ".");
    }
    return source.type() == SourceType.KAFKA ? kafkaFreshness(source, topic, expected) : mqttFreshness(source, topic, expected, windowSeconds);
  }

  // ---------- Kafka: the broker knows the time of the newest record ----------

  private FreshnessReport kafkaFreshness(Source source, String topic, int expected) {
    return sources.ask(source, () -> {
      List<TopicInfo> infos;
      List<String> notes = new ArrayList<>();
      if (topic == null || topic.isBlank()) {
        List<String> names = kafka.listNames(source.address(), properties.requestTimeout()).stream().filter(t -> !t.internal()).map(TopicName::name).toList();
        if (names.size() > MAX_TOPICS) notes.add("Showing the first " + MAX_TOPICS + " of " + names.size() + " topics by name. Name one topic to check it alone.");
        infos = kafka.describe(source.address(), names.stream().limit(MAX_TOPICS).toList(), properties.requestTimeout());
      } else {
        infos = List.of(topics.requireTopic(source, topic.trim()));
      }
      long now = clock.millis();
      List<FreshnessRow> rows = new ArrayList<>();
      for (TopicInfo info : infos) {
        long newest = info.newestTimestamp();
        long age = newest < 0 ? -1 : Math.max(0, now - newest);
        rows.add(new FreshnessRow(info.name(), Freshness.judge(age, expected).name(), TopicService.time(newest), age < 0 ? -1 : age / 1000, -1, false, -1));
      }
      return report(source, expected, 0, rows, List.of(), notes);
    });
  }

  // ---------- MQTT: only listening tells ----------

  private FreshnessReport mqttFreshness(Source source, String topic, int expected, Integer windowSeconds) {
    String filter = MqttService.filter(topic);
    // Long enough to hear two messages of a healthy topic, within the bounds.
    int fallback = Math.max(5, Math.min(MAX_WINDOW_SECONDS, 2 * expected + 1));
    int seconds = MqttService.windowSeconds(windowSeconds, fallback, MAX_WINDOW_SECONDS);
    MqttService.Heard heard = mqtt.listen(source, filter, seconds, 0, true);
    long end = heard.window().endedAtMillis();

    List<FreshnessRow> rows = new ArrayList<>();
    for (TopicActivity t : heard.topics()) {
      // A message published while listening has an arrival time. A retained one only has a time if its payload carries it.
      long last = t.lastLiveAtMillis() >= 0 ? t.lastLiveAtMillis() : t.lastPayloadTimeMillis();
      long age = last < 0 ? -1 : Math.max(0, end - last);
      rows.add(new FreshnessRow(t.topic(), Freshness.judge(age, expected).name(), TopicService.time(last), age < 0 ? -1 : age / 1000,
          t.messages(), t.retained(), t.lastDelayMillis()));
    }
    boolean exact = !filter.contains("+") && !filter.contains("#");
    if (exact && rows.isEmpty()) {
      rows.add(new FreshnessRow(filter, Freshness.SILENT.name(), "", -1, 0, false, -1));
    }
    List<FreshnessRow> shown = rows.stream().limit(MAX_TOPICS).toList();

    List<String> notes = new ArrayList<>();
    notes.add("An MQTT broker keeps no history, so this is what was heard on '" + filter + "' during " + heard.window().seconds() + " s. "
        + "SILENT means nothing was published on the topic in that time"
        + (exact ? "." : "; with a wildcard filter, a topic that was silent and has no retained message is not listed at all. Name it exactly to check it."));
    if (seconds < expected * GRACE) {
      notes.add("The window (" + seconds + " s) is shorter than one and a half expected intervals (" + expected + " s each), so a healthy topic "
          + "may not have had the chance to be heard. Use a longer windowSeconds before trusting SILENT.");
    }
    if (rows.size() > shown.size()) notes.add("Showing the first " + shown.size() + " of " + rows.size() + " topics by name.");
    return report(source, expected, heard.window().seconds(), shown, heard.brokerCounters(), notes);
  }

  private static FreshnessReport report(Source source, int expected, double windowSeconds, List<FreshnessRow> rows, List<BrokerCounter> counters, List<String> notes) {
    int fresh = count(rows, Freshness.FRESH), stale = count(rows, Freshness.STALE), silent = count(rows, Freshness.SILENT);
    return new FreshnessReport(source.name(), source.type().name(), expected, windowSeconds, List.copyOf(rows), fresh, stale, silent, counters, String.join(" ", notes));
  }

  private static int count(List<FreshnessRow> rows, Freshness status) {
    return (int) rows.stream().filter(r -> r.status().equals(status.name())).count();
  }
}
