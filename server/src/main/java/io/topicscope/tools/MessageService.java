package io.topicscope.tools;

import io.topicscope.config.TopicScopeProperties;
import io.topicscope.config.TopicScopeProperties.Source;
import io.topicscope.config.TopicScopeProperties.SourceType;
import io.topicscope.kafka.KafkaRecordReader;
import io.topicscope.kafka.KafkaRecordReader.RawRecord;
import io.topicscope.kafka.KafkaRecordReader.Tail;
import io.topicscope.kafka.KafkaTopicReader.PartitionInfo;
import io.topicscope.kafka.KafkaTopicReader.TopicInfo;
import io.topicscope.mqtt.MqttObserver.Seen;
import io.topicscope.safety.PayloadFormatter;
import io.topicscope.safety.PayloadFormatter.Shown;
import io.topicscope.safety.Redactor;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Function;
import org.springframework.stereotype.Service;

/** Shapes the newest records of a topic into the rows {@code recent_messages} shows. Nothing leaves here unformatted. */
@Service
public class MessageService {

  static final int DEFAULT_LIMIT = 10;

  /** Keys and header values are identifiers, not content: a short cap is enough. */
  static final int SMALL_CHARS = 200;

  /** What is left of a value once the answer as a whole has used up its budget. */
  static final int PREVIEW_CHARS = 200;

  /** One header. The value is formatted and redacted like a message value; a header with a secret-looking name is hidden. */
  public record HeaderRow(String name, String value) {}

  /**
   * One row of {@code recent_messages}. No field is ever null. Fields that only one kind of broker has are
   * -1, empty or false for the other.
   *
   * @param topic the topic the message is on; for MQTT the concrete topic, which a filter with wildcards does not tell
   * @param partition Kafka: the partition. MQTT: -1
   * @param offset Kafka: the offset. MQTT: -1
   * @param timestamp Kafka: the record's timestamp. MQTT: when the message arrived here. ISO-8601 in UTC with milliseconds
   * @param qos MQTT: the quality of service it was published with (0, 1 or 2). Kafka: -1
   * @param retained MQTT: true when the broker had it stored and sent it on subscribing, so its arrival time
   *     says nothing about when it was published. Kafka: false
   * @param key the key as text, or a hex preview when it is binary; empty when the record has no key
   * @param value what is shown of the value; see {@code valueFormat}
   * @param valueFormat {@code json}, {@code text}, {@code binary} (hex preview), or {@code none} (the record has no value: a tombstone)
   * @param valueBytes size of the stored value in bytes
   * @param truncated true when {@code value} does not show the whole value
   * @param redacted true when something that looked like a secret was replaced with {@code [REDACTED]} in the value, key or headers
   */
  public record MessageRow(String topic, int partition, long offset, String timestamp, String key, List<HeaderRow> headers,
                           String value, String valueFormat, int valueBytes, boolean truncated, boolean redacted, int qos, boolean retained) {}

  /**
   * The result of {@code recent_messages}.
   *
   * @param topic Kafka: the topic. MQTT: the topic filter that was listened on
   * @param messages newest first
   * @param limit the limit that was applied, after defaults and caps
   * @param windowSeconds how long an MQTT source was listened to; 0 for a Kafka source
   * @param note anything the reader must know to read the list correctly; empty when there is nothing
   */
  public record MessageList(String source, String type, String topic, List<MessageRow> messages, int returned, int limit,
                            double windowSeconds, String note) {}

  private final Sources sources;
  private final TopicService topics;
  private final KafkaRecordReader reader;
  private final MqttService mqtt;
  private final TopicScopeProperties properties;

  public MessageService(Sources sources, TopicService topics, KafkaRecordReader reader, MqttService mqtt, TopicScopeProperties properties) {
    this.sources = sources;
    this.topics = topics;
    this.reader = reader;
    this.mqtt = mqtt;
    this.properties = properties;
  }

  /**
   * @param partition Kafka only
   * @param windowSeconds MQTT only: how long to listen
   */
  public MessageList recentMessages(String sourceName, String topic, Integer limit, Integer partition, Integer windowSeconds) {
    Source source = sources.require(sourceName);
    if (topic == null || topic.isBlank()) throw new ToolProblem("The 'topic' argument is missing.", "Call list_topics to see the names.");
    String name = topic.trim();
    List<String> notes = new ArrayList<>();
    int max = properties.limits().maxMessages();
    int wanted = limit == null ? DEFAULT_LIMIT : limit;
    if (wanted < 1) throw new ToolProblem("'limit' must be at least 1, and it was " + wanted + ".", "Leave it out for the default of " + DEFAULT_LIMIT + ".");
    if (wanted > max) {
      notes.add("'limit' was lowered from " + wanted + " to " + max + ", the most this server returns in one call.");
      wanted = max;
    }
    int applied = wanted;

    if (source.type() == SourceType.MQTT) return heardMessages(source, name, applied, windowSeconds, notes);

    return sources.ask(source, () -> {
      TopicInfo info = topics.requireTopic(source, name);
      List<Integer> readable = readablePartitions(info, partition, notes);

      Tail tail = reader.tail(source, name, readable, applied, properties.requestTimeout());
      List<MessageRow> rows = rows(tail.records(), RawRecord::value, (record, value) -> row(name, record, value), notes);

      if (!tail.complete()) {
        notes.add("Reading stopped at the time limit before the end of every partition was reached, so some of the newest messages may be missing.");
      }
      if (rows.isEmpty() && tail.complete()) {
        notes.add(partition == null ? "The topic holds no messages." : "Partition " + partition + " holds no messages.");
      }
      return new MessageList(source.name(), source.type().name(), name, rows, rows.size(), applied, 0, String.join(" ", notes));
    });
  }

  /** The MQTT form of recent messages: the retained message of each matching topic, and what is published while listening. */
  private MessageList heardMessages(Source source, String topicFilter, int limit, Integer windowSeconds, List<String> notes) {
    int seconds = MqttService.windowSeconds(windowSeconds, MqttService.DEFAULT_WINDOW_SECONDS, MqttService.MAX_WINDOW_SECONDS);
    MqttService.Heard heard = mqtt.listen(source, topicFilter, seconds, limit, false);

    List<MessageRow> rows = rows(heard.newest(), Seen::payload, (seen, value) -> new MessageRow(seen.topic(), -1, -1,
        Instant.ofEpochMilli(seen.receivedAtMillis()).toString(), "", List.of(), value.text(), value.format(), value.bytes(),
        value.truncated(), value.redacted(), seen.qos(), seen.retained()), notes);

    notes.add(0, "An MQTT broker keeps no history. These are the retained messages on '" + topicFilter + "' and what was published during the "
        + heard.window().seconds() + " s of listening; anything published before that is gone.");
    if (rows.isEmpty()) {
      notes.add("Nothing was heard: no retained message, and nothing was published during the window.");
    } else if (heard.totalMessages() > rows.size()) {
      notes.add(heard.totalMessages() + " messages arrived; the newest " + rows.size() + " are shown.");
    }
    if (!heard.hiddenNote().isEmpty()) notes.add(heard.hiddenNote());
    return new MessageList(source.name(), source.type().name(), topicFilter, rows, rows.size(), limit, heard.window().seconds(), String.join(" ", notes));
  }

  /** The partitions to read: the one asked for, or all that have a leader. */
  private static List<Integer> readablePartitions(TopicInfo info, Integer partition, List<String> notes) {
    int count = info.partitions().size();
    if (partition != null) {
      PartitionInfo wanted = info.partitions().stream().filter(p -> p.partition() == partition).findFirst()
          .orElseThrow(() -> new ToolProblem("Topic '" + info.name() + "' has no partition " + partition + ".",
              count == 1 ? "Its only partition is 0." : "Its partitions are 0 to " + (count - 1) + "."));
      if (wanted.offline()) {
        throw new ToolProblem("Partition " + partition + " of topic '" + info.name() + "' has no leader, so it cannot be read right now.",
            "Call describe_topic to see the state of every partition.");
      }
      return List.of(partition);
    }
    List<Integer> offline = info.partitions().stream().filter(PartitionInfo::offline).map(PartitionInfo::partition).toList();
    if (!offline.isEmpty()) {
      notes.add("Partition(s) " + offline + " have no leader and were not read; their newest messages are missing from this list.");
    }
    return info.partitions().stream().filter(p -> !p.offline()).map(PartitionInfo::partition).toList();
  }

  /** Formats newest first, and keeps the answer as a whole within its budget by cutting the values that come after it is used up. */
  private <T> List<MessageRow> rows(List<T> messages, Function<T, byte[]> payload, BiFunction<T, Shown, MessageRow> toRow, List<String> notes) {
    int valueChars = properties.limits().maxValueChars();
    int budget = properties.limits().maxTotalChars();
    int overBudget = 0;
    List<MessageRow> rows = new ArrayList<>();
    for (T message : messages) {
      Shown value = PayloadFormatter.format(payload.apply(message), valueChars);
      if (budget <= 0 && value.text().length() > PREVIEW_CHARS) {
        value = value.cutTo(PREVIEW_CHARS);
        overBudget++;
      }
      budget -= value.text().length();
      rows.add(toRow.apply(message, value));
    }
    if (overBudget > 0) {
      notes.add("The answer reached its size budget of " + properties.limits().maxTotalChars() + " characters: the values of the "
          + overBudget + " oldest message(s) shown were cut to a preview. Ask for fewer messages, or one partition, to see them whole.");
    }
    return rows;
  }

  private static MessageRow row(String topic, RawRecord record, Shown value) {
    Shown key = PayloadFormatter.format(record.key(), SMALL_CHARS);
    boolean redacted = value.redacted() || key.redacted();
    List<HeaderRow> headers = new ArrayList<>();
    for (KafkaRecordReader.RawHeader h : record.headers()) {
      if (Redactor.secretName(h.name()) && h.value() != null && h.value().length > 0) {
        headers.add(new HeaderRow(h.name(), Redactor.MARK));
        redacted = true;
      } else {
        Shown shown = PayloadFormatter.format(h.value(), SMALL_CHARS);
        headers.add(new HeaderRow(h.name(), shown.text()));
        redacted |= shown.redacted();
      }
    }
    return new MessageRow(topic, record.partition(), record.offset(), Instant.ofEpochMilli(Math.max(0, record.timestamp())).toString(),
        key.text(), headers, value.text(), value.format(), value.bytes(), value.truncated(), redacted, -1, false);
  }
}
