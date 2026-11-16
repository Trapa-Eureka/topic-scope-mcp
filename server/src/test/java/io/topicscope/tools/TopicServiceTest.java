package io.topicscope.tools;

import static org.assertj.core.api.Assertions.assertThat;

import io.topicscope.config.TopicScopeProperties;
import io.topicscope.config.TopicScopeProperties.Source;
import io.topicscope.config.TopicScopeProperties.SourceType;
import io.topicscope.kafka.KafkaTopicReader;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.stream.IntStream;
import org.junit.jupiter.api.Test;

/** The shaping rules, with a pretend cluster. What a real cluster answers is checked in KafkaTopicsIT. */
class TopicServiceTest {

  static final Instant NOW = Instant.parse("2026-11-16T12:00:00Z");
  static final long ONE_MINUTE_AGO = NOW.minusSeconds(60).toEpochMilli();

  static final TopicScopeProperties PROPERTIES = new TopicScopeProperties(List.of(
      new Source("k", SourceType.KAFKA, "kafka:9092"),
      new Source("m", SourceType.MQTT, "tcp://user:secret@mqtt:1883")), null, null, null);

  /** A cluster made of the given topics. */
  static class PretendCluster extends KafkaTopicReader {
    final List<TopicInfo> topics = new ArrayList<>();
    Exception failure;

    PretendCluster topic(String name, boolean internal, PartitionInfo... partitions) {
      topics.add(new TopicInfo(name, internal, List.of(partitions)));
      return this;
    }

    @Override
    public List<TopicName> listNames(String bootstrapServers, Duration timeout) throws Exception {
      if (failure != null) throw failure;
      return topics.stream().map(t -> new TopicName(t.name(), t.internal())).sorted(java.util.Comparator.comparing(TopicName::name)).toList();
    }

    @Override
    public List<TopicInfo> describe(String bootstrapServers, Collection<String> names, Duration timeout) {
      return topics.stream().filter(t -> names.contains(t.name())).sorted(java.util.Comparator.comparing(TopicInfo::name)).toList();
    }

    @Override
    public Optional<List<Setting>> settings(String bootstrapServers, String topic, Duration timeout) {
      return Optional.of(List.of(
          new Setting("cleanup.policy", "delete", false),
          new Setting("flush.ms", "9223372036854775807", false),
          new Setting("retention.ms", "-1", true),
          new Setting("segment.bytes", "1048576", true)));
    }
  }

  static KafkaTopicReader.PartitionInfo healthy(int partition, long start, long end, long newest) {
    return new KafkaTopicReader.PartitionInfo(partition, 1, List.of(1, 2, 3), List.of(1, 2, 3), start, end, newest);
  }

  static TopicService service(PretendCluster cluster) {
    return new TopicService(new Sources(PROPERTIES), cluster, PROPERTIES, Clock.fixed(NOW, ZoneOffset.UTC));
  }

  @Test
  void aTopicRowAddsUpItsPartitions() {
    PretendCluster cluster = new PretendCluster().topic("orders", false,
        healthy(0, 10, 110, ONE_MINUTE_AGO - 5000),
        healthy(1, 0, 50, ONE_MINUTE_AGO),
        new KafkaTopicReader.PartitionInfo(2, 1, List.of(1, 2, 3), List.of(1), 0, 0, -1));

    TopicService.TopicList list = service(cluster).listTopics("k", null, null);

    assertThat(list.source()).isEqualTo("k");
    assertThat(list.type()).isEqualTo("KAFKA");
    assertThat(list.note()).isEmpty();
    assertThat(list.topics()).containsExactly(
        new TopicService.TopicRow("orders", 3, 3, false, 150, "2026-11-16T11:59:00Z", 60, 1, 0));
  }

  @Test
  void anEmptyTopicHasNoNewestMessage() {
    PretendCluster cluster = new PretendCluster().topic("empty", false, healthy(0, 0, 0, -1));
    TopicService.TopicRow row = service(cluster).listTopics("k", null, null).topics().get(0);
    assertThat(row.messages()).isZero();
    assertThat(row.newestMessageTime()).isEmpty();
    assertThat(row.newestMessageAgeSeconds()).isEqualTo(-1);
  }

  @Test
  void anOfflinePartitionIsCountedAndItsRecordsAreNot() {
    PretendCluster cluster = new PretendCluster().topic("broken", false,
        healthy(0, 0, 40, ONE_MINUTE_AGO),
        new KafkaTopicReader.PartitionInfo(1, -1, List.of(1, 2), List.of(), -1, -1, -1));

    TopicService.TopicRow row = service(cluster).listTopics("k", null, null).topics().get(0);
    assertThat(row.messages()).isEqualTo(40);
    assertThat(row.offlinePartitions()).isEqualTo(1);
    assertThat(row.underReplicatedPartitions()).isEqualTo(1);

    TopicService.TopicDetail detail = service(cluster).describeTopic("k", "broken");
    assertThat(detail.note()).contains("1 partition(s) have no leader");
    assertThat(detail.partitions().get(1)).isEqualTo(new TopicService.PartitionRow(1, -1, List.of(1, 2), List.of(), -1, -1, 0, ""));
  }

  @Test
  void internalTopicsAreHiddenUnlessAskedFor() {
    PretendCluster cluster = new PretendCluster()
        .topic("__consumer_offsets", true, healthy(0, 0, 9, ONE_MINUTE_AGO))
        .topic("orders", false, healthy(0, 0, 1, ONE_MINUTE_AGO));

    assertThat(service(cluster).listTopics("k", null, null).topics()).extracting(TopicService.TopicRow::name).containsExactly("orders");
    assertThat(service(cluster).listTopics("k", null, false).topics()).extracting(TopicService.TopicRow::name).containsExactly("orders");
    assertThat(service(cluster).listTopics("k", null, true).topics()).extracting(TopicService.TopicRow::name).containsExactly("__consumer_offsets", "orders");
  }

  @Test
  void theNameFilterIgnoresCaseAndSaysWhenNothingMatches() {
    PretendCluster cluster = new PretendCluster()
        .topic("Flights.Positions", false, healthy(0, 0, 1, ONE_MINUTE_AGO))
        .topic("sensors.telemetry", false, healthy(0, 0, 1, ONE_MINUTE_AGO));

    assertThat(service(cluster).listTopics("k", " flights ", null).topics()).extracting(TopicService.TopicRow::name).containsExactly("Flights.Positions");

    TopicService.TopicList none = service(cluster).listTopics("k", "billing", null);
    assertThat(none.topics()).isEmpty();
    assertThat(none.matching()).isZero();
    assertThat(none.note()).isEqualTo("No topic name contains 'billing'.");
    assertThat(service(new PretendCluster()).listTopics("k", null, null).note()).isEqualTo("This cluster has no topics.");
  }

  @Test
  void aLongCatalogIsCutAndSaysSo() {
    PretendCluster cluster = new PretendCluster();
    IntStream.range(0, 250).forEach(i -> cluster.topic(String.format("t%03d", i), false, healthy(0, 0, 1, ONE_MINUTE_AGO)));

    TopicService.TopicList list = service(cluster).listTopics("k", null, null);
    assertThat(list.matching()).isEqualTo(250);
    assertThat(list.shown()).isEqualTo(200);
    assertThat(list.topics()).hasSize(200);
    assertThat(list.topics().get(199).name()).isEqualTo("t199");
    assertThat(list.note()).contains("first 200 of 250", "nameContains");
  }

  @Test
  void describeShowsOverriddenSettingsAndTheOnesThatDecideRetention() {
    PretendCluster cluster = new PretendCluster().topic("orders", false, healthy(0, 5, 25, ONE_MINUTE_AGO));

    TopicService.TopicDetail detail = service(cluster).describeTopic("k", " orders ");

    assertThat(detail.topic().name()).isEqualTo("orders");
    assertThat(detail.partitions()).containsExactly(
        new TopicService.PartitionRow(0, 1, List.of(1, 2, 3), List.of(1, 2, 3), 5, 25, 20, "2026-11-16T11:59:00Z"));
    assertThat(detail.settings()).containsExactly(
        new TopicService.SettingRow("cleanup.policy", "delete", false),
        new TopicService.SettingRow("retention.ms", "-1", true),
        new TopicService.SettingRow("segment.bytes", "1048576", true));
    assertThat(detail.note()).isEmpty();
  }

  @Test
  void aCompactedTopicSaysThatItsMessageCountIsAnUpperBound() {
    PretendCluster cluster = new PretendCluster() {
      @Override
      public Optional<List<Setting>> settings(String bootstrapServers, String topic, Duration timeout) {
        return Optional.of(List.of(new Setting("cleanup.policy", "compact,delete", true)));
      }
    };
    cluster.topic("state", false, healthy(0, 0, 8, ONE_MINUTE_AGO),
        new KafkaTopicReader.PartitionInfo(1, -1, List.of(1), List.of(), -1, -1, -1));

    String note = service(cluster).describeTopic("k", "state").note();
    assertThat(note).startsWith("1 partition(s) have no leader.").contains("This topic is compacted (cleanup.policy=compact,delete)", "upper bound");

    assertThat(TopicService.compacted(List.of(new TopicService.SettingRow("cleanup.policy", "delete", false)))).isFalse();
    assertThat(TopicService.compacted(List.of(new TopicService.SettingRow("cleanup.policy", "compact", true)))).isTrue();
    assertThat(TopicService.compacted(List.of())).isFalse();
  }

  @Test
  void anUnknownTopicIsAnsweredWithTheNamesThatExist() {
    PretendCluster cluster = new PretendCluster()
        .topic("demo.flights.positions", false, healthy(0, 0, 1, ONE_MINUTE_AGO))
        .topic("demo.alarms", false, healthy(0, 0, 1, ONE_MINUTE_AGO))
        .topic("__consumer_offsets", true, healthy(0, 0, 1, ONE_MINUTE_AGO));

    assertThat(problem(() -> service(cluster).describeTopic("k", "positions")))
        .isEqualTo("There is no topic named 'positions' on source 'k'. Did you mean: demo.flights.positions.");
    assertThat(problem(() -> service(cluster).describeTopic("k", "billing")))
        .isEqualTo("There is no topic named 'billing' on source 'k'. Topics that exist: demo.alarms, demo.flights.positions.");
    assertThat(problem(() -> service(cluster).describeTopic("k", " ")))
        .isEqualTo("The 'topic' argument is missing. Call list_topics to see the names.");
  }

  @Test
  void anUnknownSourceIsAnsweredWithTheNamesThatExist() {
    assertThat(problem(() -> service(new PretendCluster()).listTopics("prod", null, null)))
        .isEqualTo("There is no source named 'prod'. Configured sources: k (kafka), m (mqtt).");
    assertThat(problem(() -> service(new PretendCluster()).listTopics(null, null, null)))
        .isEqualTo("The 'source' argument is missing. Configured sources: k (kafka), m (mqtt).");
  }

  @Test
  void mqttSourcesAreNotListedYetAndCannotBeDescribed() {
    TopicService.TopicList list = service(new PretendCluster()).listTopics("m", null, null);
    assertThat(list.type()).isEqualTo("MQTT");
    assertThat(list.topics()).isEmpty();
    assertThat(list.note()).contains("not available yet");

    assertThat(problem(() -> service(new PretendCluster()).describeTopic("m", "demo/sensors")))
        .isEqualTo("describe_topic works on Kafka sources, and 'm' is an MQTT source. An MQTT broker keeps no partitions, offsets or settings per topic.");
  }

  @Test
  void aBrokerFailureBecomesAMessageWithoutThePassword() {
    PretendCluster cluster = new PretendCluster();
    cluster.failure = new java.util.concurrent.ExecutionException(new java.util.concurrent.TimeoutException());
    assertThat(problem(() -> service(cluster).listTopics("k", null, null)))
        .isEqualTo("Source 'k' (kafka:9092) could not be read: no answer within the timeout. Call list_sources to see whether it answers at all.");

    Sources sources = new Sources(PROPERTIES);
    assertThat(problem(() -> sources.ask(PROPERTIES.source("m").orElseThrow(), () -> { throw new NoClassDefFoundError("x/Y"); })))
        .contains("tcp://user:***@mqtt:1883", "NoClassDefFoundError").doesNotContain("secret");
  }

  @Test
  void aProblemReadsAsOneLineForTheProblemAndOneForTheHint() {
    // Spring AI prints the message, a line break, and the root cause's message. Without a cause the message would appear twice.
    ToolProblem problem = new ToolProblem("Nothing there.", "Look elsewhere.");
    assertThat(problem.getMessage()).isEqualTo("Nothing there.");
    assertThat(problem.getCause().getMessage()).isEqualTo("Look elsewhere.");
    assertThat(problem.getCause().getCause()).isNull();
    assertThat(problem.getCause().getStackTrace()).isEmpty();
  }

  /** The two sentences a caller reads when the call is refused. */
  static String problem(org.assertj.core.api.ThrowableAssert.ThrowingCallable call) {
    Throwable thrown = org.assertj.core.api.Assertions.catchThrowable(call);
    assertThat(thrown).isInstanceOf(ToolProblem.class);
    return ((ToolProblem) thrown).text();
  }
}
