package io.topicscope.kafka;

import io.topicscope.config.TopicScopeProperties.Source;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.GroupListing;
import org.apache.kafka.clients.admin.ListGroupsOptions;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartition;
import org.springframework.stereotype.Component;

/**
 * Reads what a Kafka cluster knows about its consumer groups: which exist, who is in them, how far each has
 * committed, and where each partition ends. Admin API only; the server never becomes a member of a group.
 */
@Component
public class KafkaGroupReader {

  /** One partition of one topic. The server's own type, so Kafka's does not travel into the tool layer. */
  public record Partition(String topic, int partition) implements Comparable<Partition> {
    @Override
    public int compareTo(Partition other) {
      int byTopic = topic.compareTo(other.topic);
      return byTopic != 0 ? byTopic : Integer.compare(partition, other.partition);
    }
  }

  /**
   * A consumer group as the cluster lists it.
   *
   * @param state the cluster's word for it: {@code STABLE}, {@code EMPTY} (no members), {@code PREPARING_REBALANCE}, ...
   * @param members connected consumers right now
   */
  public record GroupInfo(String group, String state, int members) {}

  /**
   * Where a group stands on one partition at one moment.
   *
   * @param committed the next offset the group will read
   * @param end the offset the next record will get, or -1 when the partition has no leader
   */
  public record Offsets(long committed, long end) {
    /** Records written and not yet read by the group. 0 when the end is unknown or the group is ahead of it. */
    public long lag() {
      return end < 0 ? 0 : Math.max(0, end - committed);
    }
  }

  /** A group and all its committed partitions at one moment. */
  public record Sample(GroupInfo group, Map<Partition, Offsets> offsets) {}

  /** The same group looked at twice, {@code between} apart. Movement between the two is what tells a slow consumer from a stopped one. */
  public record TwoSamples(Sample first, Sample second, Duration between) {}

  /** Every consumer group, sorted by name. */
  public List<GroupInfo> list(Source source, Duration timeout) throws Exception {
    Admin admin = KafkaClients.admin(source, timeout);
    try {
      return list(admin, timeout);
    } finally {
      KafkaClients.close(admin);
    }
  }

  /**
   * Looks at one group, waits, and looks again, on one connection.
   *
   * @return empty when the cluster has no group of that name
   */
  public Optional<TwoSamples> sampleTwice(Source source, String group, Duration pause, Duration timeout) throws Exception {
    Admin admin = KafkaClients.admin(source, timeout);
    try {
      if (list(admin, timeout).stream().noneMatch(g -> g.group().equals(group))) return Optional.empty();
      long start = System.nanoTime();
      Sample first = sample(admin, group, timeout);
      Thread.sleep(pause.toMillis());
      long between = System.nanoTime() - start;
      Sample second = sample(admin, group, timeout);
      return Optional.of(new TwoSamples(first, second, Duration.ofNanos(between)));
    } finally {
      KafkaClients.close(admin);
    }
  }

  /** Every group at one moment, on one connection. For callers that look regularly and compare one look with the next. */
  public Map<String, Sample> sampleAll(Source source, Duration timeout) throws Exception {
    Admin admin = KafkaClients.admin(source, timeout);
    try {
      Map<String, Sample> samples = new java.util.TreeMap<>();
      for (GroupInfo group : list(admin, timeout)) samples.put(group.group(), sample(admin, group.group(), timeout));
      return samples;
    } finally {
      KafkaClients.close(admin);
    }
  }

  private static List<GroupInfo> list(Admin admin, Duration timeout) throws Exception {
    Collection<GroupListing> listings = admin.listGroups(ListGroupsOptions.forConsumerGroups()).all().get(timeout.toMillis(), TimeUnit.MILLISECONDS);
    List<String> ids = listings.stream().map(GroupListing::groupId).toList();
    Map<String, KafkaFuture<ConsumerGroupDescription>> described = ids.isEmpty() ? Map.of() : admin.describeConsumerGroups(ids).describedGroups();

    List<GroupInfo> out = new ArrayList<>();
    for (GroupListing listing : listings) {
      String state = listing.groupState().map(Enum::name).orElse("UNKNOWN");
      int members = 0;
      try {
        ConsumerGroupDescription d = described.get(listing.groupId()).get(timeout.toMillis(), TimeUnit.MILLISECONDS);
        state = d.groupState().name();
        members = d.members().size();
      } catch (InterruptedException e) {
        throw e;
      } catch (Exception e) {
        // One group that cannot be described must not hide the others; it keeps the state from the listing.
      }
      out.add(new GroupInfo(listing.groupId(), state, members));
    }
    out.sort(Comparator.comparing(GroupInfo::group));
    return out;
  }

  private static Sample sample(Admin admin, String group, Duration timeout) throws Exception {
    long millis = timeout.toMillis();
    GroupInfo info = list(admin, timeout).stream().filter(g -> g.group().equals(group)).findFirst()
        .orElse(new GroupInfo(group, "DEAD", 0));

    Map<TopicPartition, OffsetAndMetadata> committed = admin.listConsumerGroupOffsets(group).partitionsToOffsetAndMetadata().get(millis, TimeUnit.MILLISECONDS);
    Map<TopicPartition, OffsetSpec> ask = new HashMap<>();
    committed.forEach((tp, offset) -> {
      if (offset != null) ask.put(tp, OffsetSpec.latest());
    });
    ListOffsetsResult ends = ask.isEmpty() ? null : admin.listOffsets(ask);

    Map<Partition, Offsets> offsets = new HashMap<>();
    for (TopicPartition tp : ask.keySet()) {
      long end;
      try {
        end = ends.partitionResult(tp).get(millis, TimeUnit.MILLISECONDS).offset();
      } catch (InterruptedException e) {
        throw e;
      } catch (Exception e) {
        end = -1; // no leader: the lag of this partition is unknown, the others still count
      }
      offsets.put(new Partition(tp.topic(), tp.partition()), new Offsets(committed.get(tp).offset(), end));
    }
    return new Sample(info, Map.copyOf(offsets));
  }
}
