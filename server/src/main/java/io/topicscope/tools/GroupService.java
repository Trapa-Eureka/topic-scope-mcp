package io.topicscope.tools;

import io.topicscope.config.TopicScopeProperties;
import io.topicscope.config.TopicScopeProperties.Source;
import io.topicscope.config.TopicScopeProperties.SourceType;
import io.topicscope.kafka.KafkaGroupReader;
import io.topicscope.kafka.KafkaGroupReader.GroupInfo;
import io.topicscope.kafka.KafkaGroupReader.Offsets;
import io.topicscope.kafka.KafkaGroupReader.Partition;
import io.topicscope.kafka.KafkaGroupReader.TwoSamples;
import io.topicscope.kafka.KafkaRecordReader;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

/** Shapes consumer groups and their lag into the rows {@code list_consumer_groups} and {@code consumer_lag} show. */
@Service
public class GroupService {

  static final int MAX_GROUPS = 200;
  static final int DEFAULT_SAMPLE_SECONDS = 5;
  static final int MAX_SAMPLE_SECONDS = 30;

  /** The age of the oldest unread message is looked up for at most this many partitions; it costs one read each. */
  static final int MAX_AGE_LOOKUPS = 100;

  /** One row of {@code list_consumer_groups}. */
  public record GroupRow(String group, String state, int members) {}

  /** The result of {@code list_consumer_groups}. {@code note} is empty when there is nothing to add. */
  public record GroupList(String source, List<GroupRow> groups, int matching, int shown, String note) {}

  /**
   * One partition of {@code consumer_lag}, at the second look.
   *
   * @param committedOffset the next offset the group will read
   * @param endOffset the offset the next record will get; -1 when the partition has no leader
   * @param lag messages waiting: end offset minus committed offset
   * @param lagChange how much the lag changed during the sample; positive means it grew
   * @param oldestUnreadAgeSeconds age of the oldest message the group has not read; -1 when nothing is waiting or it could not be read
   */
  public record PartitionLag(String topic, int partition, long committedOffset, long endOffset, long lag, long lagChange, long oldestUnreadAgeSeconds) {}

  /**
   * One topic of {@code consumer_lag}: its partitions added up, and the verdict.
   *
   * @param consumed how far the group's committed offsets moved during the sample
   * @param arrived how many messages were written during the sample
   * @param oldestUnreadAgeSeconds the largest age among its partitions; -1 when nothing is waiting or it could not be read
   * @param status {@code OK}, {@code LAGGING}, {@code STALLED} or {@code NO_CONSUMER}
   */
  public record TopicLag(String topic, int partitions, long lag, long lagChange, long consumed, long arrived, long oldestUnreadAgeSeconds, String status) {}

  /**
   * The result of {@code consumer_lag}.
   *
   * @param state the cluster's word for the group: {@code STABLE}, {@code EMPTY}, ...
   * @param sampleSeconds how far apart the two looks really were
   * @param status the status that most needs attention among the topics
   * @param summary the status in one plain sentence
   * @param note anything else the reader must know; empty when there is nothing
   */
  public record LagReport(String source, String group, String state, int members, double sampleSeconds, long lag, long lagChange,
                          String status, String summary, List<TopicLag> topics, List<PartitionLag> partitions, String note) {}

  private final Sources sources;
  private final KafkaGroupReader groups;
  private final KafkaRecordReader records;
  private final TopicScopeProperties properties;
  private final Clock clock;

  @Autowired
  public GroupService(Sources sources, KafkaGroupReader groups, KafkaRecordReader records, TopicScopeProperties properties) {
    this(sources, groups, records, properties, Clock.systemUTC());
  }

  GroupService(Sources sources, KafkaGroupReader groups, KafkaRecordReader records, TopicScopeProperties properties, Clock clock) {
    this.sources = sources;
    this.groups = groups;
    this.records = records;
    this.properties = properties;
    this.clock = clock;
  }

  // ---------- list_consumer_groups ----------

  public GroupList listGroups(String sourceName, String nameContains) {
    Source source = requireKafka(sourceName, "list_consumer_groups");
    String filter = nameContains == null ? "" : nameContains.trim().toLowerCase(Locale.ROOT);

    return sources.ask(source, () -> {
      List<GroupRow> matching = groups.list(source, properties.requestTimeout()).stream()
          .filter(g -> filter.isEmpty() || g.group().toLowerCase(Locale.ROOT).contains(filter))
          .map(g -> new GroupRow(g.group(), g.state(), g.members()))
          .toList();
      List<GroupRow> shown = matching.stream().limit(MAX_GROUPS).toList();
      String note = "";
      if (matching.size() > shown.size()) {
        note = "Showing the first " + shown.size() + " of " + matching.size() + " matching groups by name. Narrow it down with nameContains.";
      } else if (matching.isEmpty()) {
        note = filter.isEmpty() ? "This cluster has no consumer groups." : "No consumer group name contains '" + filter + "'.";
      }
      return new GroupList(source.name(), shown, matching.size(), shown.size(), note);
    });
  }

  // ---------- consumer_lag ----------

  public LagReport consumerLag(String sourceName, String group, String topic, Integer sampleSeconds) {
    Source source = requireKafka(sourceName, "consumer_lag");
    if (group == null || group.isBlank()) throw new ToolProblem("The 'group' argument is missing.", "Call list_consumer_groups to see the names.");
    String groupName = group.trim();
    String topicFilter = topic == null || topic.isBlank() ? null : topic.trim();
    if (topicFilter != null) sources.requireAllowed(source, topicFilter);
    int seconds = sampleSeconds == null ? DEFAULT_SAMPLE_SECONDS : sampleSeconds;
    if (seconds < 1 || seconds > MAX_SAMPLE_SECONDS) {
      throw new ToolProblem("'sampleSeconds' must be between 1 and " + MAX_SAMPLE_SECONDS + ", and it was " + seconds + ".",
          "Leave it out for the default of " + DEFAULT_SAMPLE_SECONDS + ". A longer sample tells a slow consumer from a stopped one more reliably.");
    }

    return sources.ask(source, () -> {
      var two = groups.sampleTwice(source, groupName, Duration.ofSeconds(seconds), properties.requestTimeout());
      if (two.isEmpty()) {
        throw new ToolProblem("There is no consumer group named '" + groupName + "' on source '" + source.name() + "'.", otherGroups(source, groupName));
      }
      return report(source, groupName, topicFilter, two.get(), true);
    });
  }

  /**
   * The lag report from two looks the caller already has, for something that looks at regular intervals and
   * compares each look with the one before. The age of the oldest unread message is left out (-1): finding it
   * means reading a record per partition, which is too much to do on every tick.
   */
  public LagReport lagBetween(Source source, String group, KafkaGroupReader.Sample first, KafkaGroupReader.Sample second, Duration between) {
    return report(source, group, null, new TwoSamples(first, second, between), false);
  }

  private LagReport report(Source source, String group, String topicFilter, TwoSamples two, boolean withAges) {
    GroupInfo info = two.second().group();
    var access = sources.access(source);
    Map<Partition, Offsets> first = two.first().offsets();
    Map<Partition, Offsets> second = new HashMap<>(two.second().offsets());
    // A group may read topics this server must not show. Their partitions are taken out before anything is added up or read.
    TreeSet<String> hiddenTopics = new TreeSet<>();
    second.keySet().removeIf(p -> {
      if (access.permits(p.topic())) return false;
      hiddenTopics.add(p.topic());
      return true;
    });
    double sampleSeconds = Math.round(two.between().toMillis() / 100.0) / 10.0;
    List<String> notes = new ArrayList<>();
    String hidden = Sources.hiddenNote(hiddenTopics.size(), "topic(s) this group reads were");
    if (!hidden.isEmpty()) notes.add(hidden);

    TreeSet<String> allTopics = new TreeSet<>();
    second.keySet().forEach(p -> allTopics.add(p.topic()));
    if (topicFilter != null && !allTopics.contains(topicFilter)) {
      throw new ToolProblem("Group '" + group + "' has no committed offset on a topic named '" + topicFilter + "'.",
          allTopics.isEmpty() ? "It has not committed any offset at all." : "Its topics are: " + String.join(", ", allTopics) + ".");
    }

    List<Partition> partitions = second.keySet().stream().filter(p -> topicFilter == null || p.topic().equals(topicFilter)).sorted().toList();
    if (partitions.isEmpty()) {
      return new LagReport(source.name(), group, info.state(), info.members(), sampleSeconds, 0, 0, LagStatus.OK.name(),
          "The group has not committed any offset" + (hiddenTopics.isEmpty() ? "" : " on a topic that is available")
              + ", so there is nothing to measure its lag against.", List.of(), List.of(), String.join(" ", notes));
    }

    Map<Partition, Long> ages = withAges ? oldestUnreadAges(source, partitions, second, notes) : Map.of();

    List<PartitionLag> rows = new ArrayList<>();
    Map<String, long[]> perTopic = new TreeMap<>(); // lag, lagChange, consumed, arrived, partitions, oldest age
    List<Partition> offline = new ArrayList<>();
    for (Partition p : partitions) {
      Offsets now = second.get(p);
      Offsets before = first.getOrDefault(p, now); // committed for the first time during the sample: no movement known
      if (now.end() < 0) offline.add(p);
      long lagChange = now.lag() - before.lag();
      long age = ages.getOrDefault(p, -1L);
      rows.add(new PartitionLag(p.topic(), p.partition(), now.committed(), now.end(), now.lag(), lagChange, age));

      long[] t = perTopic.computeIfAbsent(p.topic(), k -> new long[] {0, 0, 0, 0, 0, -1});
      t[0] += now.lag();
      t[1] += lagChange;
      t[2] += now.committed() - before.committed();
      t[3] += now.end() < 0 || before.end() < 0 ? 0 : now.end() - before.end();
      t[4]++;
      t[5] = Math.max(t[5], age);
    }

    List<TopicLag> topics = new ArrayList<>();
    LagStatus worst = LagStatus.OK;
    long lag = 0, lagChange = 0, consumed = 0, arrived = 0;
    for (Map.Entry<String, long[]> e : perTopic.entrySet()) {
      long[] t = e.getValue();
      LagStatus status = LagStatus.judge(t[0], t[2], t[3], info.members());
      topics.add(new TopicLag(e.getKey(), (int) t[4], t[0], t[1], t[2], t[3], t[5], status.name()));
      worst = worst.worseOf(status);
      lag += t[0];
      lagChange += t[1];
      consumed += t[2];
      arrived += t[3];
    }

    if (!offline.isEmpty()) {
      notes.add(offline.size() + " partition(s) have no leader, so their end offset is unknown (-1) and their lag is counted as 0.");
    }
    if (consumed < 0) {
      notes.add("The committed offsets moved backwards during the sample: someone reset them, or the group is reprocessing.");
    }
    return new LagReport(source.name(), group, info.state(), info.members(), sampleSeconds, lag, lagChange, worst.name(),
        summary(worst, lag, lagChange, consumed, arrived, info.members(), sampleSeconds), topics, rows, String.join(" ", notes));
  }

  /** Reads one record per lagging partition, at the group's committed offset, to learn how long it has been waiting. */
  private Map<Partition, Long> oldestUnreadAges(Source source, List<Partition> partitions, Map<Partition, Offsets> offsets, List<String> notes) {
    Map<Partition, Long> ask = new HashMap<>();
    int lagging = 0;
    for (Partition p : partitions) {
      if (offsets.get(p).lag() <= 0) continue;
      lagging++;
      if (ask.size() < MAX_AGE_LOOKUPS) ask.put(p, offsets.get(p).committed());
    }
    if (lagging > ask.size()) {
      notes.add("The age of the oldest unread message was looked up for " + ask.size() + " of the " + lagging + " lagging partitions.");
    }
    Map<Partition, Long> ages = new HashMap<>();
    long now = clock.millis();
    records.timestampsAt(source, ask, properties.requestTimeout())
        .forEach((p, timestamp) -> ages.put(p, timestamp < 0 ? -1 : Math.max(0, (now - timestamp) / 1000)));
    return ages;
  }

  static String summary(LagStatus status, long lag, long lagChange, long consumed, long arrived, int members, double sampleSeconds) {
    String sample = "during the " + sampleSeconds + " s sample";
    return switch (status) {
      case OK -> lag == 0
          ? "Caught up: nothing is waiting."
          : "Keeping up: " + lag + " waiting, and the group read " + consumed + " " + sample + ".";
      case LAGGING -> "Reading, but behind: " + lag + " waiting. The group read " + consumed + " while " + arrived + " arrived " + sample
          + (lagChange > 0 ? ", so the lag grew by " + lagChange + "." : lagChange < 0 ? ", so the lag shrank by " + (-lagChange) + "." : ", so the lag did not change.");
      case STALLED -> "Not reading: " + lag + " waiting, and the committed offsets did not move " + sample
          + (members == 0 ? "; the group has no members" : "; the group has " + members + " member(s)")
          + (arrived > 0 ? ", while " + arrived + " more arrived." : ".");
      case NO_CONSUMER -> "Nobody is reading: " + lag + " waiting, the group has no members, and nothing new arrived " + sample + ".";
    };
  }

  // ---------- shared ----------

  private Source requireKafka(String sourceName, String tool) {
    Source source = sources.require(sourceName);
    if (source.type() == SourceType.MQTT) {
      throw new ToolProblem(tool + " works on Kafka sources, and '" + source.name() + "' is an MQTT source.",
          "An MQTT broker has no consumer groups and no offsets, so it has no consumer lag.");
    }
    return source;
  }

  private String otherGroups(Source source, String name) throws Exception {
    String wanted = name.toLowerCase(Locale.ROOT);
    List<String> all = groups.list(source, properties.requestTimeout()).stream().map(GroupInfo::group).toList();
    List<String> close = all.stream().filter(n -> n.toLowerCase(Locale.ROOT).contains(wanted) || wanted.contains(n.toLowerCase(Locale.ROOT))).limit(10).toList();
    if (all.isEmpty()) return "The cluster has no consumer groups.";
    if (!close.isEmpty()) return "Did you mean: " + String.join(", ", close) + ".";
    return "Groups that exist: " + String.join(", ", all.stream().limit(20).toList()) + "."
        + (all.size() > 20 ? " (" + all.size() + " in total; call list_consumer_groups for all.)" : "");
  }
}
