package io.topicscope.tools;

import static io.topicscope.tools.TopicServiceTest.ONE_MINUTE_AGO;
import static io.topicscope.tools.TopicServiceTest.healthy;
import static io.topicscope.tools.TopicServiceTest.problem;
import static org.assertj.core.api.Assertions.assertThat;

import io.topicscope.config.TopicScopeProperties;
import io.topicscope.config.TopicScopeProperties.Limits;
import io.topicscope.config.TopicScopeProperties.Source;
import io.topicscope.config.TopicScopeProperties.SourceType;
import io.topicscope.kafka.KafkaRecordReader;
import io.topicscope.kafka.KafkaRecordReader.RawHeader;
import io.topicscope.kafka.KafkaRecordReader.RawRecord;
import io.topicscope.kafka.KafkaTopicReader;
import io.topicscope.tools.TopicServiceTest.PretendCluster;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

/** The shaping rules, with a pretend topic. What a real broker returns is checked in KafkaMessagesIT. */
class MessageServiceTest {

  /** Hands back prepared records and remembers what it was asked for. */
  static final class PretendReader extends KafkaRecordReader {
    List<RawRecord> records = new ArrayList<>();
    boolean complete = true;
    List<Integer> askedPartitions;
    int askedLimit;

    @Override
    public Tail tail(String bootstrapServers, String topic, List<Integer> partitions, int limit, Duration timeout) {
      askedPartitions = partitions;
      askedLimit = limit;
      return new Tail(records.stream().limit(limit).toList(), complete);
    }
  }

  final PretendCluster cluster = new PretendCluster().topic("orders", false,
      healthy(0, 0, 10, ONE_MINUTE_AGO), healthy(1, 0, 10, ONE_MINUTE_AGO), healthy(2, 0, 10, ONE_MINUTE_AGO));
  final PretendReader reader = new PretendReader();

  MessageService service(Limits limits) {
    TopicScopeProperties properties = new TopicScopeProperties(List.of(
        new Source("k", SourceType.KAFKA, "kafka:9092"), new Source("m", SourceType.MQTT, "tcp://mqtt:1883")), null, null, limits);
    Sources sources = new Sources(properties);
    return new MessageService(sources, new TopicService(sources, cluster, properties, Clock.systemUTC()), reader, properties);
  }

  MessageService service() {
    return service(null);
  }

  static RawRecord record(int partition, long offset, String key, String value, RawHeader... headers) {
    return new RawRecord(partition, offset, 1_794_830_400_123L, key == null ? null : bytes(key), value == null ? null : bytes(value), List.of(headers));
  }

  static byte[] bytes(String s) {
    return s.getBytes(StandardCharsets.UTF_8);
  }

  @Test
  void aRecordBecomesARowWithEveryPartFormatted() {
    reader.records.add(record(1, 42, "d0a001", "{ \"seq\": 7 }", new RawHeader("schema", bytes("alarm.v1")), new RawHeader("empty", null)));

    MessageService.MessageList list = service().recentMessages("k", " orders ", null, null);

    assertThat(list.source()).isEqualTo("k");
    assertThat(list.topic()).isEqualTo("orders");
    assertThat(list.limit()).isEqualTo(10);
    assertThat(list.returned()).isEqualTo(1);
    assertThat(list.note()).isEmpty();
    assertThat(list.messages()).containsExactly(new MessageService.MessageRow(1, 42, "2026-11-16T12:00:00.123Z", "d0a001",
        List.of(new MessageService.HeaderRow("schema", "alarm.v1"), new MessageService.HeaderRow("empty", "")),
        "{\"seq\":7}", "json", 12, false, false));
    assertThat(reader.askedPartitions).containsExactly(0, 1, 2);
    assertThat(reader.askedLimit).isEqualTo(10);
  }

  @Test
  void secretsInValueKeyAndHeadersAreHiddenAndTheRowSaysSo() {
    reader.records.add(record(0, 1, "plain", "ok", new RawHeader("Authorization", bytes("Bearer abcdefgh12345678"))));
    reader.records.add(record(0, 2, "password=hunter2", "ok"));
    reader.records.add(record(0, 3, "plain", "{\"token\":\"abc\"}"));
    reader.records.add(record(0, 4, "plain", "ok", new RawHeader("trace", bytes("id-1"))));

    List<MessageService.MessageRow> rows = service().recentMessages("k", "orders", null, null).messages();

    assertThat(rows.get(0).headers()).containsExactly(new MessageService.HeaderRow("Authorization", "[REDACTED]"));
    assertThat(rows.get(1).key()).isEqualTo("password=[REDACTED]");
    assertThat(rows.get(2).value()).isEqualTo("{\"token\":\"[REDACTED]\"}");
    assertThat(rows).extracting(MessageService.MessageRow::redacted).containsExactly(true, true, true, false);
  }

  @Test
  void aRecordWithoutKeyOrValueIsShownAsSuch() {
    reader.records.add(record(0, 5, null, null));
    MessageService.MessageRow row = service().recentMessages("k", "orders", null, null).messages().get(0);
    assertThat(row.key()).isEmpty();
    assertThat(row.value()).isEmpty();
    assertThat(row.valueFormat()).isEqualTo("none");
    assertThat(row.valueBytes()).isZero();
  }

  @Test
  void theLimitHasADefaultAFloorAndACeiling() {
    assertThat(service().recentMessages("k", "orders", 3, null).limit()).isEqualTo(3);

    MessageService.MessageList capped = service().recentMessages("k", "orders", 5000, null);
    assertThat(capped.limit()).isEqualTo(100);
    assertThat(reader.askedLimit).isEqualTo(100);
    assertThat(capped.note()).startsWith("'limit' was lowered from 5000 to 100");

    assertThat(service(new Limits(5, null, null)).recentMessages("k", "orders", 6, null).limit()).isEqualTo(5);
    assertThat(problem(() -> service().recentMessages("k", "orders", 0, null)))
        .isEqualTo("'limit' must be at least 1, and it was 0. Leave it out for the default of 10.");
  }

  @Test
  void onePartitionCanBeAskedForAndMustExist() {
    service().recentMessages("k", "orders", null, 2);
    assertThat(reader.askedPartitions).containsExactly(2);

    assertThat(problem(() -> service().recentMessages("k", "orders", null, 3)))
        .isEqualTo("Topic 'orders' has no partition 3. Its partitions are 0 to 2.");
    assertThat(problem(() -> service().recentMessages("k", "orders", null, -1))).contains("has no partition -1");

    cluster.topic("single", false, healthy(0, 0, 1, ONE_MINUTE_AGO));
    assertThat(problem(() -> service().recentMessages("k", "single", null, 1))).endsWith("Its only partition is 0.");
  }

  @Test
  void offlinePartitionsAreSkippedWithANoteOrRefusedWhenAskedForByNumber() {
    cluster.topic("broken", false, healthy(0, 0, 5, ONE_MINUTE_AGO),
        new KafkaTopicReader.PartitionInfo(1, -1, List.of(2), List.of(), -1, -1, -1));
    reader.records.add(record(0, 4, "k", "v"));

    MessageService.MessageList list = service().recentMessages("k", "broken", null, null);
    assertThat(reader.askedPartitions).containsExactly(0);
    assertThat(list.note()).isEqualTo("Partition(s) [1] have no leader and were not read; their newest messages are missing from this list.");

    assertThat(problem(() -> service().recentMessages("k", "broken", null, 1)))
        .isEqualTo("Partition 1 of topic 'broken' has no leader, so it cannot be read right now. Call describe_topic to see the state of every partition.");
  }

  @Test
  void anEmptyTopicAndAnUnfinishedReadAreSaid() {
    assertThat(service().recentMessages("k", "orders", null, null).note()).isEqualTo("The topic holds no messages.");
    assertThat(service().recentMessages("k", "orders", null, 1).note()).isEqualTo("Partition 1 holds no messages.");

    reader.complete = false;
    assertThat(service().recentMessages("k", "orders", null, null).note()).startsWith("Reading stopped at the time limit").doesNotContain("holds no messages");
  }

  @Test
  void aValueIsCutAtItsCapAndTheWholeAnswerAtItsBudget() {
    for (int i = 0; i < 6; i++) reader.records.add(record(0, 10 - i, "k", "v".repeat(1000)));

    // 300 characters per value, 700 for the whole answer: the budget is used up after the third row.
    MessageService.MessageList list = service(new Limits(null, 300, 700)).recentMessages("k", "orders", null, null);

    assertThat(list.messages()).allSatisfy(row -> {
      assertThat(row.truncated()).isTrue();
      assertThat(row.valueBytes()).isEqualTo(1000);
    });
    assertThat(list.messages().get(0).value()).startsWith("v".repeat(300) + "… [cut: 300 of 1000");
    assertThat(list.messages().get(2).value()).startsWith("v".repeat(300) + "…");
    assertThat(list.messages().get(3).value()).startsWith("v".repeat(200) + "… [cut: 200 of");
    assertThat(list.messages().get(5).value()).startsWith("v".repeat(200) + "… [cut: 200 of");
    assertThat(list.note()).contains("size budget of 700 characters", "3 oldest message(s)");
  }

  @Test
  void wrongNamesAndMqttSources() {
    assertThat(problem(() -> service().recentMessages("k", "order", null, null)))
        .isEqualTo("There is no topic named 'order' on source 'k'. Did you mean: orders.");
    assertThat(problem(() -> service().recentMessages("k", " ", null, null))).startsWith("The 'topic' argument is missing.");
    assertThat(problem(() -> service().recentMessages("nope", "orders", null, null))).startsWith("There is no source named 'nope'.");

    MessageService.MessageList mqtt = service().recentMessages("m", "demo/sensors/#", null, null);
    assertThat(mqtt.type()).isEqualTo("MQTT");
    assertThat(mqtt.messages()).isEmpty();
    assertThat(mqtt.note()).contains("not available yet");
  }
}
