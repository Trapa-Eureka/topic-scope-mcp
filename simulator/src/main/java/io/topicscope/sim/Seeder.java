package io.topicscope.sim;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.acl.AccessControlEntry;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourceType;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;

/**
 * Puts the brokers into the state described by {@link DemoData}: topics, records, committed offsets and
 * retained MQTT messages. Safe to run again; a broker that is already seeded is left alone.
 */
public final class Seeder {

  private Seeder() {}

  /** What a call to {@link #seed} did. */
  public enum Result { SEEDED, ALREADY_SEEDED }

  /** Seeds brokers that ask for no login. */
  public static Result seed(String kafkaBootstrap, String mqttUri) throws Exception {
    return seed(Stack.open(kafkaBootstrap, mqttUri), null);
  }

  /**
   * @param readOnlyKafkaUser when not null, this Kafka user is given the rights a look-only client needs and
   *     no others: describe and read every topic, describe every group, describe the cluster. The simulator's
   *     own account must be allowed to create access rules for this to work
   */
  public static Result seed(Stack stack, String readOnlyKafkaUser) throws Exception {
    Result result = seedKafka(stack, readOnlyKafkaUser);
    seedMqtt(stack);
    return result;
  }

  // ---------- Kafka ----------

  static Result seedKafka(Stack stack, String readOnlyKafkaUser) throws Exception {
    Properties adminProps = stack.kafka();
    adminProps.put(AdminClientConfig.CLIENT_ID_CONFIG, "topic-scope-simulator-seed");

    try (Admin admin = Admin.create(adminProps)) {
      // The committed offsets are written last, so they mark a finished seed.
      GroupSpecMarker marker = new GroupSpecMarker(DemoData.ALARM_NOTIFIER.group(), new TopicPartition(DemoData.ALARMS.name(), 0));
      if (admin.listConsumerGroupOffsets(marker.group()).partitionsToOffsetAndMetadata().get().get(marker.partition()) != null) {
        return Result.ALREADY_SEEDED;
      }
      Set<String> existing = admin.listTopics().names().get();
      List<String> leftovers = DemoData.TOPICS.stream().map(DemoData.TopicSpec::name).filter(existing::contains).toList();
      if (!leftovers.isEmpty()) {
        throw new IllegalStateException("The broker is half seeded (found " + leftovers + " but no committed offsets). "
            + "Start from an empty broker: docker compose down -v, then up again.");
      }

      // retention.ms = -1: seeded records must never expire, or the known counts would drift.
      List<NewTopic> topics = DemoData.TOPICS.stream()
          .map(t -> new NewTopic(t.name(), t.partitions(), (short) 1)
              .configs(t.compacted() ? Map.of("cleanup.policy", "compact") : Map.of("retention.ms", "-1")))
          .toList();
      admin.createTopics(topics).all().get();
      if (readOnlyKafkaUser != null) admin.createAcls(lookOnlyRules(readOnlyKafkaUser)).all().get();

      publishSeedRecords(stack);

      for (DemoData.GroupSpec g : DemoData.GROUPS) {
        Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
        for (int p = 0; p < g.topic().partitions(); p++) {
          offsets.put(new TopicPartition(g.topic().name(), p), new OffsetAndMetadata(g.committedPerPartition()));
        }
        admin.alterConsumerGroupOffsets(g.group(), offsets).all().get();
      }
      return Result.SEEDED;
    }
  }

  private record GroupSpecMarker(String group, TopicPartition partition) {}

  /**
   * What a client needs to list topics, read records by position, list groups and compare their offsets,
   * and nothing that changes anything: no Write, Create, Delete or Alter on any resource.
   */
  static List<AclBinding> lookOnlyRules(String user) {
    String principal = "User:" + user;
    ResourcePattern everyTopic = new ResourcePattern(ResourceType.TOPIC, "*", PatternType.LITERAL);
    ResourcePattern everyGroup = new ResourcePattern(ResourceType.GROUP, "*", PatternType.LITERAL);
    ResourcePattern cluster = new ResourcePattern(ResourceType.CLUSTER, "kafka-cluster", PatternType.LITERAL);
    return List.of(
        allow(principal, everyTopic, AclOperation.DESCRIBE),
        allow(principal, everyTopic, AclOperation.DESCRIBE_CONFIGS),
        allow(principal, everyTopic, AclOperation.READ),
        allow(principal, everyGroup, AclOperation.DESCRIBE),
        allow(principal, cluster, AclOperation.DESCRIBE));
  }

  private static AclBinding allow(String principal, ResourcePattern resource, AclOperation operation) {
    return new AclBinding(resource, new AccessControlEntry(principal, "*", operation, AclPermissionType.ALLOW));
  }

  private static void publishSeedRecords(Stack stack) throws InterruptedException, ExecutionException {
    Properties props = stack.kafka();
    props.put(ProducerConfig.CLIENT_ID_CONFIG, "topic-scope-simulator-seed");
    props.put(ProducerConfig.ACKS_CONFIG, "all");
    props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);

    List<Future<RecordMetadata>> sent = new ArrayList<>();
    try (KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(props)) {
      for (DemoData.SeedRecord r : DemoData.seedRecords()) {
        byte[] key = r.key() == null ? null : r.key().getBytes(StandardCharsets.UTF_8);
        ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(r.topic(), r.partition(), key, r.value());
        r.headers().forEach((k, v) -> record.headers().add(k, v.getBytes(StandardCharsets.UTF_8)));
        sent.add(producer.send(record));
      }
      producer.flush();
      for (Future<RecordMetadata> f : sent) f.get();
    }
  }

  // ---------- MQTT ----------

  /** One retained status message per device. Publishing it again replaces it, so this is repeatable. */
  static void seedMqtt(Stack stack) throws MqttException {
    MqttClient client = new MqttClient(stack.mqttUri(), "topic-scope-simulator-seed", new MemoryPersistence());
    client.connect(stack.mqtt());
    try {
      for (DemoData.Device d : DemoData.DEVICES) {
        client.publish(d.statusTopic(), d.statusJson().getBytes(StandardCharsets.UTF_8), 1, true);
      }
    } finally {
      client.disconnect();
      client.close();
    }
  }
}
