package io.topicscope.tools;

import static io.topicscope.tools.TopicServiceTest.problem;
import static org.assertj.core.api.Assertions.assertThat;

import io.topicscope.config.TopicScopeProperties;
import io.topicscope.config.TopicScopeProperties.Source;
import io.topicscope.config.TopicScopeProperties.SourceType;
import io.topicscope.kafka.KafkaGroupReader;
import io.topicscope.kafka.KafkaGroupReader.GroupInfo;
import io.topicscope.kafka.KafkaGroupReader.Offsets;
import io.topicscope.kafka.KafkaGroupReader.Partition;
import io.topicscope.kafka.KafkaGroupReader.Sample;
import io.topicscope.kafka.KafkaGroupReader.TwoSamples;
import io.topicscope.kafka.KafkaRecordReader;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.IntStream;
import org.junit.jupiter.api.Test;

/** The status rule and the arithmetic, with a pretend cluster. What a real cluster answers is checked in KafkaLagIT. */
class GroupServiceTest {

  static final Instant NOW = Instant.parse("2026-11-16T12:00:00Z");

  // ---------- the status rule ----------

  @Test
  void nothingWaitingIsOkWhateverElseIsTrue() {
    assertThat(LagStatus.judge(0, 0, 0, 0)).isEqualTo(LagStatus.OK);
    assertThat(LagStatus.judge(0, 0, 50, 3)).isEqualTo(LagStatus.OK);
    assertThat(LagStatus.judge(0, 40, 40, 1)).isEqualTo(LagStatus.OK);
  }

  @Test
  void readingAndLessThanOneSampleBehindIsOk() {
    assertThat(LagStatus.judge(3, 40, 41, 1)).as("a healthy consumer is always a few behind").isEqualTo(LagStatus.OK);
    assertThat(LagStatus.judge(40, 40, 40, 1)).as("exactly one sample of work").isEqualTo(LagStatus.OK);
  }

  @Test
  void readingButMoreThanOneSampleBehindIsLagging() {
    assertThat(LagStatus.judge(150, 5, 10, 1)).as("falling further behind").isEqualTo(LagStatus.LAGGING);
    assertThat(LagStatus.judge(1000, 800, 10, 1)).as("catching up fast is still behind").isEqualTo(LagStatus.LAGGING);
    assertThat(LagStatus.judge(41, 40, 40, 1)).isEqualTo(LagStatus.LAGGING);
  }

  @Test
  void waitingAndNotMovingIsStalledUnlessItIsAnAbandonedBacklog() {
    assertThat(LagStatus.judge(300, 0, 43, 0)).as("stopped while messages keep arriving").isEqualTo(LagStatus.STALLED);
    assertThat(LagStatus.judge(300, 0, 43, 2)).as("connected but not committing").isEqualTo(LagStatus.STALLED);
    assertThat(LagStatus.judge(300, 0, 0, 2)).as("connected, nothing new, still not reading what waits").isEqualTo(LagStatus.STALLED);
    assertThat(LagStatus.judge(5, 0, 0, 0)).as("no members, nothing arriving").isEqualTo(LagStatus.NO_CONSUMER);
  }

  @Test
  void theGroupTakesTheStatusThatMostNeedsAttention() {
    assertThat(LagStatus.OK.worseOf(LagStatus.LAGGING)).isEqualTo(LagStatus.LAGGING);
    assertThat(LagStatus.LAGGING.worseOf(LagStatus.NO_CONSUMER)).isEqualTo(LagStatus.NO_CONSUMER);
    assertThat(LagStatus.NO_CONSUMER.worseOf(LagStatus.STALLED)).isEqualTo(LagStatus.STALLED);
    assertThat(LagStatus.STALLED.worseOf(LagStatus.OK)).isEqualTo(LagStatus.STALLED);
  }

  // ---------- the report ----------

  /** A cluster with prepared groups and samples. */
  static final class PretendGroups extends KafkaGroupReader {
    final List<GroupInfo> all = new ArrayList<>();
    final Map<String, TwoSamples> samples = new HashMap<>();
    Duration askedPause;

    PretendGroups group(String name, String state, int members, Map<Partition, Offsets> first, Map<Partition, Offsets> second) {
      GroupInfo info = new GroupInfo(name, state, members);
      all.add(info);
      samples.put(name, new TwoSamples(new Sample(info, first), new Sample(info, second), Duration.ofMillis(5040)));
      return this;
    }

    @Override
    public List<GroupInfo> list(String bootstrapServers, Duration timeout) {
      return all.stream().sorted(java.util.Comparator.comparing(GroupInfo::group)).toList();
    }

    @Override
    public Optional<TwoSamples> sampleTwice(String bootstrapServers, String group, Duration pause, Duration timeout) {
      askedPause = pause;
      return Optional.ofNullable(samples.get(group));
    }
  }

  /** Says every asked record is {@code ageSeconds} old, and remembers what was asked. */
  static final class PretendRecords extends KafkaRecordReader {
    long ageSeconds = 57;
    Map<Partition, Long> asked = Map.of();

    @Override
    public Map<Partition, Long> timestampsAt(String bootstrapServers, Map<Partition, Long> offsets, Duration timeout) {
      asked = offsets;
      Map<Partition, Long> out = new HashMap<>();
      offsets.keySet().forEach(p -> out.put(p, NOW.minusSeconds(ageSeconds).toEpochMilli()));
      return out;
    }
  }

  final PretendGroups groups = new PretendGroups();
  final PretendRecords records = new PretendRecords();

  GroupService service() {
    TopicScopeProperties properties = new TopicScopeProperties(List.of(
        new Source("k", SourceType.KAFKA, "kafka:9092"), new Source("m", SourceType.MQTT, "tcp://mqtt:1883")), null, null, null);
    return new GroupService(new Sources(properties), groups, records, properties, Clock.fixed(NOW, ZoneOffset.UTC));
  }

  static Partition p(String topic, int partition) {
    return new Partition(topic, partition);
  }

  static Offsets at(long committed, long end) {
    return new Offsets(committed, end);
  }

  @Test
  void aSlowConsumerIsAddedUpPerTopicAndReportedAsLagging() {
    // The classic picture: one partition is being drained while the others wait their turn.
    groups.group("tracker", "STABLE", 1,
        Map.of(p("flights", 0), at(60, 100), p("flights", 1), at(60, 100), p("flights", 2), at(60, 100)),
        Map.of(p("flights", 0), at(65, 104), p("flights", 1), at(60, 103), p("flights", 2), at(60, 103)));

    GroupService.LagReport report = service().consumerLag("k", " tracker ", null, null);

    assertThat(report.group()).isEqualTo("tracker");
    assertThat(report.state()).isEqualTo("STABLE");
    assertThat(report.members()).isEqualTo(1);
    assertThat(report.sampleSeconds()).isEqualTo(5.0);
    assertThat(report.lag()).isEqualTo(125);
    assertThat(report.lagChange()).isEqualTo(5);
    assertThat(report.status()).isEqualTo("LAGGING");
    assertThat(report.summary()).isEqualTo(
        "Reading, but behind: 125 waiting. The group read 5 while 10 arrived during the 5.0 s sample, so the lag grew by 5.");
    assertThat(report.topics()).containsExactly(new GroupService.TopicLag("flights", 3, 125, 5, 5, 10, 57, "LAGGING"));
    assertThat(report.partitions()).containsExactly(
        new GroupService.PartitionLag("flights", 0, 65, 104, 39, -1, 57),
        new GroupService.PartitionLag("flights", 1, 60, 103, 43, 3, 57),
        new GroupService.PartitionLag("flights", 2, 60, 103, 43, 3, 57));
    assertThat(report.note()).isEmpty();
    assertThat(groups.askedPause).isEqualTo(Duration.ofSeconds(5));
    assertThat(records.asked).as("asked at the committed offset of each lagging partition")
        .isEqualTo(Map.of(p("flights", 0), 65L, p("flights", 1), 60L, p("flights", 2), 60L));
  }

  @Test
  void theFourStatusesEachGetTheirSentence() {
    groups.group("ok", "STABLE", 2, Map.of(p("t", 0), at(100, 100)), Map.of(p("t", 0), at(140, 140)));
    groups.group("keeping-up", "STABLE", 1, Map.of(p("t", 0), at(100, 101)), Map.of(p("t", 0), at(140, 143)));
    groups.group("stopped", "EMPTY", 0, Map.of(p("t", 0), at(20, 100)), Map.of(p("t", 0), at(20, 143)));
    groups.group("stuck", "STABLE", 1, Map.of(p("t", 0), at(20, 100)), Map.of(p("t", 0), at(20, 100)));
    groups.group("abandoned", "EMPTY", 0, Map.of(p("t", 0), at(7, 12)), Map.of(p("t", 0), at(7, 12)));

    assertThat(service().consumerLag("k", "ok", null, null))
        .extracting(GroupService.LagReport::status, GroupService.LagReport::summary)
        .containsExactly("OK", "Caught up: nothing is waiting.");
    assertThat(service().consumerLag("k", "keeping-up", null, null))
        .extracting(GroupService.LagReport::status, GroupService.LagReport::summary)
        .containsExactly("OK", "Keeping up: 3 waiting, and the group read 40 during the 5.0 s sample.");
    assertThat(service().consumerLag("k", "stopped", null, null))
        .extracting(GroupService.LagReport::status, GroupService.LagReport::summary)
        .containsExactly("STALLED", "Not reading: 123 waiting, and the committed offsets did not move during the 5.0 s sample; the group has no members, while 43 more arrived.");
    assertThat(service().consumerLag("k", "stuck", null, null))
        .extracting(GroupService.LagReport::status, GroupService.LagReport::summary)
        .containsExactly("STALLED", "Not reading: 80 waiting, and the committed offsets did not move during the 5.0 s sample; the group has 1 member(s).");
    assertThat(service().consumerLag("k", "abandoned", null, null))
        .extracting(GroupService.LagReport::status, GroupService.LagReport::summary)
        .containsExactly("NO_CONSUMER", "Nobody is reading: 5 waiting, the group has no members, and nothing new arrived during the 5.0 s sample.");
  }

  @Test
  void aGroupOnTwoTopicsIsJudgedPerTopicAndTakesTheWorse() {
    groups.group("mixed", "STABLE", 1,
        Map.of(p("fast", 0), at(100, 100), p("slow", 0), at(10, 500)),
        Map.of(p("fast", 0), at(150, 150), p("slow", 0), at(10, 520)));

    GroupService.LagReport all = service().consumerLag("k", "mixed", null, null);
    assertThat(all.topics()).extracting(GroupService.TopicLag::topic, GroupService.TopicLag::status)
        .containsExactly(org.assertj.core.groups.Tuple.tuple("fast", "OK"), org.assertj.core.groups.Tuple.tuple("slow", "STALLED"));
    assertThat(all.status()).isEqualTo("STALLED");
    assertThat(all.lag()).isEqualTo(510);

    GroupService.LagReport fast = service().consumerLag("k", "mixed", "fast", null);
    assertThat(fast.status()).isEqualTo("OK");
    assertThat(fast.partitions()).extracting(GroupService.PartitionLag::topic).containsExactly("fast");
    assertThat(fast.topics().get(0).oldestUnreadAgeSeconds()).as("nothing is waiting, so nothing has an age").isEqualTo(-1);
    assertThat(records.asked).isEmpty();

    assertThat(problem(() -> service().consumerLag("k", "mixed", "other", null)))
        .isEqualTo("Group 'mixed' has no committed offset on a topic named 'other'. Its topics are: fast, slow.");
  }

  @Test
  void oddSituationsAreSaidInTheNote() {
    groups.group("offline", "STABLE", 1,
        Map.of(p("t", 0), at(5, 10), p("t", 1), at(5, -1)),
        Map.of(p("t", 0), at(8, 10), p("t", 1), at(5, -1)));
    GroupService.LagReport offline = service().consumerLag("k", "offline", null, null);
    assertThat(offline.lag()).isEqualTo(2);
    assertThat(offline.note()).isEqualTo("1 partition(s) have no leader, so their end offset is unknown (-1) and their lag is counted as 0.");
    assertThat(offline.partitions().get(1)).isEqualTo(new GroupService.PartitionLag("t", 1, 5, -1, 0, 0, -1));

    groups.group("rewound", "STABLE", 1, Map.of(p("t", 0), at(90, 100)), Map.of(p("t", 0), at(10, 100)));
    assertThat(service().consumerLag("k", "rewound", null, null).note()).contains("moved backwards");

    groups.group("fresh", "STABLE", 1, Map.of(), Map.of());
    GroupService.LagReport fresh = service().consumerLag("k", "fresh", null, null);
    assertThat(fresh.status()).isEqualTo("OK");
    assertThat(fresh.summary()).startsWith("The group has not committed any offset");
    assertThat(fresh.partitions()).isEmpty();

    // A partition the group committed on for the first time during the sample has no earlier value to compare with.
    groups.group("new-partition", "STABLE", 1, Map.of(p("t", 0), at(5, 10)), Map.of(p("t", 0), at(9, 10), p("t", 1), at(3, 4)));
    GroupService.LagReport grown = service().consumerLag("k", "new-partition", null, null);
    assertThat(grown.topics().get(0)).extracting(GroupService.TopicLag::consumed, GroupService.TopicLag::lag).containsExactly(4L, 2L);
  }

  @Test
  void theSampleLengthHasADefaultAndBounds() {
    groups.group("g", "STABLE", 1, Map.of(p("t", 0), at(1, 1)), Map.of(p("t", 0), at(1, 1)));
    service().consumerLag("k", "g", null, 12);
    assertThat(groups.askedPause).isEqualTo(Duration.ofSeconds(12));
    assertThat(problem(() -> service().consumerLag("k", "g", null, 0))).startsWith("'sampleSeconds' must be between 1 and 30, and it was 0.");
    assertThat(problem(() -> service().consumerLag("k", "g", null, 31))).startsWith("'sampleSeconds' must be between 1 and 30, and it was 31.");
  }

  @Test
  void wrongNamesAreAnsweredWithTheRightOnes() {
    groups.group("demo-tracker", "STABLE", 1, Map.of(), Map.of());
    groups.group("demo-archiver", "STABLE", 1, Map.of(), Map.of());

    assertThat(problem(() -> service().consumerLag("k", "tracker", null, null)))
        .isEqualTo("There is no consumer group named 'tracker' on source 'k'. Did you mean: demo-tracker.");
    assertThat(problem(() -> service().consumerLag("k", "billing", null, null)))
        .isEqualTo("There is no consumer group named 'billing' on source 'k'. Groups that exist: demo-archiver, demo-tracker.");
    assertThat(problem(() -> service().consumerLag("k", " ", null, null))).startsWith("The 'group' argument is missing.");
    assertThat(problem(() -> service().consumerLag("m", "demo-tracker", null, null)))
        .isEqualTo("consumer_lag works on Kafka sources, and 'm' is an MQTT source. An MQTT broker has no consumer groups and no offsets, so it has no consumer lag.");
    assertThat(problem(() -> service().listGroups("m", null))).startsWith("list_consumer_groups works on Kafka sources");
  }

  @Test
  void groupsAreListedFilteredAndCapped() {
    groups.group("demo-tracker", "STABLE", 1, Map.of(), Map.of());
    groups.group("demo-reporter", "EMPTY", 0, Map.of(), Map.of());

    GroupService.GroupList all = service().listGroups("k", null);
    assertThat(all.groups()).containsExactly(new GroupService.GroupRow("demo-reporter", "EMPTY", 0), new GroupService.GroupRow("demo-tracker", "STABLE", 1));
    assertThat(all.note()).isEmpty();
    assertThat(service().listGroups("k", "TRACK").groups()).extracting(GroupService.GroupRow::group).containsExactly("demo-tracker");
    assertThat(service().listGroups("k", "billing").note()).isEqualTo("No consumer group name contains 'billing'.");

    IntStream.range(0, 250).forEach(i -> groups.group(String.format("g%03d", i), "EMPTY", 0, Map.of(), Map.of()));
    GroupService.GroupList many = service().listGroups("k", "g");
    assertThat(many.shown()).isEqualTo(200);
    assertThat(many.matching()).isEqualTo(250);
    assertThat(many.note()).contains("first 200 of 250");
  }
}
