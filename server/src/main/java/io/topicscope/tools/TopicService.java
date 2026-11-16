package io.topicscope.tools;

import io.topicscope.config.TopicScopeProperties;
import io.topicscope.config.TopicScopeProperties.Source;
import io.topicscope.config.TopicScopeProperties.SourceType;
import io.topicscope.kafka.KafkaTopicReader;
import io.topicscope.kafka.KafkaTopicReader.PartitionInfo;
import io.topicscope.kafka.KafkaTopicReader.TopicInfo;
import io.topicscope.kafka.KafkaTopicReader.TopicName;
import java.time.Clock;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Locale;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

/** Shapes what the broker knows about its topics into the rows {@code list_topics} and {@code describe_topic} show. */
@Service
public class TopicService {

  /** More rows than this are cut, and the result says so. A catalog can hold thousands of topics. */
  static final int MAX_TOPICS = 200;

  /** Settings shown even when they are the broker's default, because they decide how long messages live. */
  static final List<String> KEY_SETTINGS = List.of(
      "cleanup.policy", "retention.ms", "retention.bytes", "max.message.bytes", "min.insync.replicas", "compression.type");

  /**
   * One row of {@code list_topics}. No field is ever null.
   *
   * @param messages records currently kept: the sum over partitions of end offset minus start offset. An upper
   *     bound for compacted topics and topics written with transactions
   * @param newestMessageTime time of the newest record, ISO-8601 in UTC; empty when the topic holds nothing
   * @param newestMessageAgeSeconds seconds since that record; -1 when the topic holds nothing
   * @param underReplicatedPartitions partitions with fewer in-sync replicas than replicas
   * @param offlinePartitions partitions without a leader; their records are not counted
   */
  public record TopicRow(String name, int partitions, int replicationFactor, boolean internal, long messages,
                         String newestMessageTime, long newestMessageAgeSeconds, int underReplicatedPartitions, int offlinePartitions) {}

  /**
   * The result of {@code list_topics}.
   *
   * @param matching topics that match the filter; {@code topics} holds at most the first 200 of them by name
   * @param note anything the reader must know to read the list correctly; empty when there is nothing
   */
  public record TopicList(String source, String type, List<TopicRow> topics, int matching, int shown, String note) {}

  /** One partition of {@code describe_topic}. Offsets and the leader are -1 when the partition is offline. */
  public record PartitionRow(int partition, int leader, List<Integer> replicas, List<Integer> inSyncReplicas,
                             long startOffset, long endOffset, long messages, String newestMessageTime) {}

  /** One setting of {@code describe_topic}. {@code overridden} is true when it was set on the topic itself. */
  public record SettingRow(String name, String value, boolean overridden) {}

  /** The result of {@code describe_topic}: the topic's row, its partitions, and the settings that matter. */
  public record TopicDetail(String source, TopicRow topic, List<PartitionRow> partitions, List<SettingRow> settings, String note) {}

  private final Sources sources;
  private final KafkaTopicReader kafka;
  private final TopicScopeProperties properties;
  private final Clock clock;

  @Autowired
  public TopicService(Sources sources, KafkaTopicReader kafka, TopicScopeProperties properties) {
    this(sources, kafka, properties, Clock.systemUTC());
  }

  TopicService(Sources sources, KafkaTopicReader kafka, TopicScopeProperties properties, Clock clock) {
    this.sources = sources;
    this.kafka = kafka;
    this.properties = properties;
    this.clock = clock;
  }

  public TopicList listTopics(String sourceName, String nameContains, Boolean includeInternal) {
    Source source = sources.require(sourceName);
    if (source.type() == SourceType.MQTT) {
      return new TopicList(source.name(), source.type().name(), List.of(), 0, 0,
          "Listing topics of an MQTT source is not available yet. Only Kafka sources can be listed for now.");
    }
    String filter = nameContains == null ? "" : nameContains.trim().toLowerCase(Locale.ROOT);
    boolean internalToo = Boolean.TRUE.equals(includeInternal);

    return sources.ask(source, () -> {
      List<String> matching = kafka.listNames(source.address(), properties.requestTimeout()).stream()
          .filter(t -> internalToo || !t.internal())
          .map(TopicName::name)
          .filter(name -> filter.isEmpty() || name.toLowerCase(Locale.ROOT).contains(filter))
          .toList();
      List<String> shown = matching.stream().limit(MAX_TOPICS).toList();
      Instant now = clock.instant();
      List<TopicRow> rows = kafka.describe(source.address(), shown, properties.requestTimeout()).stream().map(t -> row(t, now)).toList();

      String note = "";
      if (matching.size() > shown.size()) {
        note = "Showing the first " + shown.size() + " of " + matching.size() + " matching topics by name. Narrow it down with nameContains.";
      } else if (matching.isEmpty()) {
        note = filter.isEmpty() ? "This cluster has no topics." : "No topic name contains '" + filter + "'.";
      }
      return new TopicList(source.name(), source.type().name(), rows, matching.size(), rows.size(), note);
    });
  }

  public TopicDetail describeTopic(String sourceName, String topic) {
    Source source = sources.require(sourceName);
    if (source.type() == SourceType.MQTT) {
      throw new ToolProblem("describe_topic works on Kafka sources, and '" + source.name() + "' is an MQTT source.",
          "An MQTT broker keeps no partitions, offsets or settings per topic.");
    }
    if (topic == null || topic.isBlank()) throw new ToolProblem("The 'topic' argument is missing.", "Call list_topics to see the names.");
    String name = topic.trim();

    return sources.ask(source, () -> {
      List<TopicInfo> found = kafka.describe(source.address(), List.of(name), properties.requestTimeout());
      if (found.isEmpty()) throw new ToolProblem("There is no topic named '" + name + "' on source '" + source.name() + "'.", otherTopics(source, name));
      TopicInfo info = found.get(0);
      Instant now = clock.instant();

      List<PartitionRow> partitions = info.partitions().stream().map(TopicService::partitionRow).toList();
      List<SettingRow> settings = kafka.settings(source.address(), name, properties.requestTimeout()).orElse(List.of()).stream()
          .filter(s -> s.overridden() || KEY_SETTINGS.contains(s.name()))
          .map(s -> new SettingRow(s.name(), s.value(), s.overridden()))
          .toList();

      List<String> notes = new java.util.ArrayList<>();
      if (info.offlinePartitions() > 0) {
        notes.add(info.offlinePartitions() + " partition(s) have no leader. Their offsets are unknown (-1) and their records are not counted.");
      }
      if (compacted(settings)) {
        notes.add("This topic is compacted (cleanup.policy=" + value(settings, "cleanup.policy") + "): 'messages' counts offsets, so it is an "
            + "upper bound. The broker removes older records that share a key with a newer one, and the offsets stay.");
      }
      return new TopicDetail(source.name(), row(info, now), partitions, settings, String.join(" ", notes));
    });
  }

  /** Names the topics that do exist, closest matches first, so the caller can correct itself in one step. */
  private String otherTopics(Source source, String name) throws Exception {
    String wanted = name.toLowerCase(Locale.ROOT);
    List<String> all = kafka.listNames(source.address(), properties.requestTimeout()).stream().filter(t -> !t.internal()).map(TopicName::name).toList();
    List<String> close = all.stream().filter(n -> n.toLowerCase(Locale.ROOT).contains(wanted) || wanted.contains(n.toLowerCase(Locale.ROOT))).limit(10).toList();
    List<String> offered = close.isEmpty() ? all.stream().limit(20).toList() : close;
    String list = offered.isEmpty() ? "The cluster has no topics." : (close.isEmpty() ? "Topics that exist: " : "Did you mean: ") + String.join(", ", offered) + ".";
    return list
        + (all.size() > offered.size() && close.isEmpty() ? " (" + all.size() + " in total; call list_topics for all.)" : "");
  }

  /** {@code cleanup.policy} is a list: "compact" and "compact,delete" both compact. */
  static boolean compacted(List<SettingRow> settings) {
    return List.of(value(settings, "cleanup.policy").split(",")).stream().anyMatch(p -> p.trim().equals("compact"));
  }

  private static String value(List<SettingRow> settings, String name) {
    return settings.stream().filter(s -> s.name().equals(name)).map(SettingRow::value).findFirst().orElse("");
  }

  static TopicRow row(TopicInfo t, Instant now) {
    long newest = t.newestTimestamp();
    return new TopicRow(t.name(), t.partitions().size(), t.replicationFactor(), t.internal(), t.messages(),
        time(newest), newest < 0 ? -1 : Math.max(0, ChronoUnit.SECONDS.between(Instant.ofEpochMilli(newest), now)),
        t.underReplicatedPartitions(), t.offlinePartitions());
  }

  private static PartitionRow partitionRow(PartitionInfo p) {
    return new PartitionRow(p.partition(), p.leader(), p.replicas(), p.inSyncReplicas(), p.startOffset(), p.endOffset(), p.messages(), time(p.newestTimestamp()));
  }

  /** ISO-8601 in UTC to the second, or empty when there is no time. */
  static String time(long epochMillis) {
    return epochMillis < 0 ? "" : Instant.ofEpochMilli(epochMillis).truncatedTo(ChronoUnit.SECONDS).toString();
  }
}
