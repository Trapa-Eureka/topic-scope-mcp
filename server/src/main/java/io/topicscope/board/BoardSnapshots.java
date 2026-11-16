package io.topicscope.board;

import io.topicscope.config.TopicScopeProperties;
import io.topicscope.config.TopicScopeProperties.Source;
import io.topicscope.config.TopicScopeProperties.SourceType;
import io.topicscope.kafka.KafkaGroupReader;
import io.topicscope.kafka.KafkaGroupReader.Sample;
import io.topicscope.tools.FreshnessService.Freshness;
import io.topicscope.tools.GroupService;
import io.topicscope.tools.GroupService.LagReport;
import io.topicscope.tools.GroupService.TopicLag;
import io.topicscope.tools.MessageService;
import io.topicscope.tools.MessageService.MessageRow;
import io.topicscope.tools.MqttService;
import io.topicscope.tools.MqttService.BrokerCounter;
import io.topicscope.tools.MqttService.TopicActivity;
import io.topicscope.tools.SourceService;
import io.topicscope.tools.SourceService.SourceStatus;
import io.topicscope.tools.Sources;
import io.topicscope.tools.ToolProblem;
import io.topicscope.tools.TopicService;
import io.topicscope.tools.TopicService.TopicRow;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import io.topicscope.mqtt.MqttObserver.Seen;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

/**
 * Takes the picture the board shows: every source, its topics, its consumer groups with their lag, its MQTT
 * topics with their freshness. It asks through the same services the MCP tools use, so the same rules apply:
 * read-only, topic allow and deny lists, redaction and caps.
 *
 * <p>Unlike a tool call, the board looks again and again, and it remembers the look before. That gives it two
 * things a single call does not have: lag is judged from one look to the next without waiting, and an MQTT
 * topic that fell silent is still known, with the time it was last heard.
 */
@Component
@ConditionalOnProperty(name = "topic-scope.board.enabled", havingValue = "true")
public class BoardSnapshots {

  /** Shown for a consumer group until there is an earlier look to compare with. */
  static final String MEASURING = "MEASURING";

  static final int MAX_MQTT_TOPICS = 200;
  static final int WATCHED_MESSAGES = 10;

  /** How many of the newest MQTT messages per source are kept in memory, so that a watched topic has something to show. */
  static final int REMEMBERED_MQTT_MESSAGES = 300;

  /** One consumer group on the board. */
  public record GroupView(String group, String state, int members, long lag, long lagChange, String status, String summary, List<TopicLag> topics) {}

  /** A Kafka source on the board. {@code error} is empty unless the source could not be read. */
  public record KafkaView(String source, List<TopicRow> topics, List<GroupView> groups, String note, String error) {}

  /** One MQTT topic on the board. {@code ageSeconds} is -1 when it has only ever been seen as a retained message. */
  public record MqttTopicView(String topic, String status, String lastSeen, long ageSeconds, int messages, boolean retained, long deliveryDelayMillis) {}

  /** An MQTT source on the board. */
  public record MqttView(String source, List<MqttTopicView> topics, List<BrokerCounter> counters, String note, String error) {}

  /** The newest messages of the topic one page is watching. */
  public record WatchView(String source, String topic, List<MessageRow> messages, String note, String error) {}

  /** Everything every page shows. */
  public record Snapshot(String time, int intervalSeconds, List<SourceStatus> sources, List<KafkaView> kafka, List<MqttView> mqtt) {}

  /** What is remembered of one MQTT topic between looks. */
  private static final class Remembered {
    long lastLiveAtMillis = -1;
    boolean retained;
    int lastMessages;
    long lastDelayMillis = -1;
  }

  private record GroupLook(Map<String, Sample> samples, long takenAtNanos) {}

  private final TopicScopeProperties properties;
  private final BoardProperties board;
  private final Sources sources;
  private final SourceService sourceService;
  private final TopicService topics;
  private final GroupService groups;
  private final KafkaGroupReader groupReader;
  private final MqttService mqtt;
  private final MessageService messages;

  private final Map<String, GroupLook> previousGroups = new HashMap<>();
  private final Map<String, Map<String, Remembered>> mqttMemory = new HashMap<>();
  /** Per MQTT source, the newest messages heard, newest first. A broker keeps no history; this is the board's own. */
  private final Map<String, Deque<Seen>> mqttHeard = new HashMap<>();

  public BoardSnapshots(TopicScopeProperties properties, BoardProperties board, Sources sources, SourceService sourceService, TopicService topics,
                        GroupService groups, KafkaGroupReader groupReader, MqttService mqtt, MessageService messages) {
    this.properties = properties;
    this.board = board;
    this.sources = sources;
    this.sourceService = sourceService;
    this.topics = topics;
    this.groups = groups;
    this.groupReader = groupReader;
    this.mqtt = mqtt;
    this.messages = messages;
  }

  /** One look at everything. Called by one thread at a time. */
  public synchronized Snapshot take() {
    List<SourceStatus> statuses = sourceService.listSources().sources();
    List<KafkaView> kafka = new ArrayList<>();
    List<MqttView> mqttViews = new ArrayList<>();
    for (Source source : properties.sources()) {
      boolean reachable = statuses.stream().anyMatch(s -> s.name().equals(source.name()) && s.reachable());
      if (source.type() == SourceType.KAFKA) kafka.add(reachable ? kafka(source) : new KafkaView(source.name(), List.of(), List.of(), "", "The source does not answer."));
      else mqttViews.add(reachable ? mqtt(source) : new MqttView(source.name(), List.of(), List.of(), "", "The source does not answer."));
    }
    return new Snapshot(Instant.now().truncatedTo(ChronoUnit.SECONDS).toString(), (int) board.interval().toSeconds(), statuses, kafka, mqttViews);
  }

  private KafkaView kafka(Source source) {
    try {
      TopicService.TopicList list = topics.listTopics(source.name(), null, false, null, null);

      Map<String, Sample> now = sources.ask(source, () -> groupReader.sampleAll(source, properties.requestTimeout()));
      long takenAt = System.nanoTime();
      GroupLook before = previousGroups.put(source.name(), new GroupLook(now, takenAt));
      List<GroupView> views = new ArrayList<>();
      for (Map.Entry<String, Sample> e : now.entrySet()) {
        Sample earlier = before == null ? null : before.samples().get(e.getKey());
        views.add(earlier == null ? measuring(source, e.getKey(), e.getValue())
            : view(groups.lagBetween(source, e.getKey(), earlier, e.getValue(), Duration.ofNanos(takenAt - before.takenAtNanos()))));
      }
      return new KafkaView(source.name(), list.topics(), views, list.note(), "");
    } catch (ToolProblem e) {
      previousGroups.remove(source.name());
      return new KafkaView(source.name(), List.of(), List.of(), "", e.text());
    }
  }

  /** The first look at a group: its lag is known, whether it moves is not yet. */
  private GroupView measuring(Source source, String group, Sample sample) {
    LagReport still = groups.lagBetween(source, group, sample, sample, Duration.ZERO);
    return new GroupView(group, still.state(), still.members(), still.lag(), 0, MEASURING,
        "First look: " + still.lag() + " waiting. Whether the group is reading shows with the next look.", List.of());
  }

  private static GroupView view(LagReport r) {
    return new GroupView(r.group(), r.state(), r.members(), r.lag(), r.lagChange(), r.status(), r.summary(), r.topics());
  }

  private MqttView mqtt(Source source) {
    try {
      MqttService.Heard heard = mqtt.listen(source, "#", board.mqttListenSeconds(), REMEMBERED_MQTT_MESSAGES, true);
      remember(source.name(), heard.newest());
      long now = heard.window().endedAtMillis();
      Map<String, Remembered> memory = mqttMemory.computeIfAbsent(source.name(), k -> new TreeMap<>());
      memory.values().forEach(r -> r.lastMessages = 0);
      for (TopicActivity t : heard.topics()) {
        if (!memory.containsKey(t.topic()) && memory.size() >= MAX_MQTT_TOPICS) continue;
        Remembered r = memory.computeIfAbsent(t.topic(), k -> new Remembered());
        r.retained |= t.retained();
        r.lastMessages = t.messages();
        if (t.lastLiveAtMillis() >= 0) {
          r.lastLiveAtMillis = t.lastLiveAtMillis();
          r.lastDelayMillis = t.lastDelayMillis();
        }
      }
      List<MqttTopicView> rows = new ArrayList<>();
      memory.forEach((topic, r) -> {
        long age = r.lastLiveAtMillis < 0 ? -1 : Math.max(0, now - r.lastLiveAtMillis);
        rows.add(new MqttTopicView(topic, Freshness.judge(age, board.expectedIntervalSeconds()).name(),
            r.lastLiveAtMillis < 0 ? "" : Instant.ofEpochMilli(r.lastLiveAtMillis).truncatedTo(ChronoUnit.SECONDS).toString(),
            age < 0 ? -1 : age / 1000, r.lastMessages, r.retained, r.lastDelayMillis));
      });
      String note = "Topics heard since the board started. Listened to for " + board.mqttListenSeconds() + " s of every "
          + board.interval().toSeconds() + " s; a topic counts as stale after " + (board.expectedIntervalSeconds() * 1.5) + " s without a message."
          + (memory.size() >= MAX_MQTT_TOPICS ? " Only the first " + MAX_MQTT_TOPICS + " topics are tracked." : "");
      return new MqttView(source.name(), rows, heard.brokerCounters(), (note + " " + heard.hiddenNote()).trim(), "");
    } catch (ToolProblem e) {
      return new MqttView(source.name(), List.of(), List.of(), "", e.text());
    }
  }

  /** Adds what one look heard to the memory of a source. A retained message arrives again with every look; only its latest copy is kept. */
  private void remember(String source, List<Seen> newestFirst) {
    Deque<Seen> memory = mqttHeard.computeIfAbsent(source, k -> new ArrayDeque<>());
    for (int i = newestFirst.size() - 1; i >= 0; i--) {
      Seen seen = newestFirst.get(i);
      if (seen.retained()) memory.removeIf(old -> old.retained() && old.topic().equals(seen.topic()));
      memory.addFirst(seen);
    }
    while (memory.size() > REMEMBERED_MQTT_MESSAGES) memory.removeLast();
  }

  /**
   * The newest messages of one topic, for the page that asked to watch it. Formatted, capped and redacted like {@code recent_messages}.
   *
   * <p>A Kafka topic is read on the spot. An MQTT topic cannot be: listening for a moment would miss a sensor
   * that reports every few seconds. So the board shows what it has heard on that topic since it started.
   */
  public synchronized WatchView watch(String source, String topic) {
    try {
      Source configured = sources.require(source);
      if (configured.type() == SourceType.MQTT) {
        List<Seen> onTopic = mqttHeard.getOrDefault(source, new ArrayDeque<>()).stream().filter(s -> s.topic().equals(topic)).limit(WATCHED_MESSAGES).toList();
        MessageService.MessageList heard = messages.heardEarlier(configured, topic, onTopic,
            onTopic.isEmpty() ? "Nothing has been heard on this topic since the board started listening. An MQTT broker keeps no history."
                : "The newest messages the board has heard on this topic since it started listening. An MQTT broker keeps no history of its own.");
        return new WatchView(source, topic, heard.messages(), heard.note(), "");
      }
      MessageService.MessageList list = messages.recentMessages(source, topic, WATCHED_MESSAGES, null, null);
      return new WatchView(source, topic, list.messages(), list.note(), "");
    } catch (ToolProblem e) {
      return new WatchView(source, topic, List.of(), "", e.text());
    }
  }
}
