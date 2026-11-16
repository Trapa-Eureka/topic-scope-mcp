package io.topicscope.kafka;

import java.time.Duration;
import java.util.Properties;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;

/** Builds the short-lived Kafka clients the server uses. Every one is bounded by a timeout. */
final class KafkaClients {

  private KafkaClients() {}

  /** An admin client for one tool call. The caller closes it. */
  static Admin admin(String bootstrapServers, Duration timeout) {
    int millis = Math.toIntExact(timeout.toMillis());
    Properties props = new Properties();
    props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    props.put(AdminClientConfig.CLIENT_ID_CONFIG, "topic-scope");
    props.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, millis);
    props.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, millis);
    // Fail fast instead of retrying forever when the address is wrong.
    props.put(AdminClientConfig.SOCKET_CONNECTION_SETUP_TIMEOUT_MS_CONFIG, millis);
    props.put(AdminClientConfig.SOCKET_CONNECTION_SETUP_TIMEOUT_MAX_MS_CONFIG, millis);
    return Admin.create(props);
  }

  /**
   * A consumer that can only look. It belongs to no consumer group, so it cannot join one, and it never
   * commits: positions are set by hand with assign + seek and forgotten when it closes. It also refuses to
   * let a question about a missing topic create that topic. The caller closes it.
   */
  static KafkaConsumer<byte[], byte[]> consumer(String bootstrapServers, Duration timeout) {
    int millis = Math.toIntExact(timeout.toMillis());
    Properties props = new Properties();
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    props.put(ConsumerConfig.CLIENT_ID_CONFIG, "topic-scope");
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
    props.put(ConsumerConfig.ALLOW_AUTO_CREATE_TOPICS_CONFIG, false);
    // Positions are always set explicitly. If one turns out to be invalid, fail instead of jumping somewhere silently.
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "none");
    props.put(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, millis);
    props.put(ConsumerConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, millis);
    props.put(ConsumerConfig.SOCKET_CONNECTION_SETUP_TIMEOUT_MS_CONFIG, millis);
    props.put(ConsumerConfig.SOCKET_CONNECTION_SETUP_TIMEOUT_MAX_MS_CONFIG, millis);
    // One poll never holds more than this, however large the records are.
    props.put(ConsumerConfig.FETCH_MAX_BYTES_CONFIG, 8 * 1024 * 1024);
    props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 500);
    return new KafkaConsumer<>(props);
  }

  /** Closing must not hang a tool call either. */
  static void close(Admin admin) {
    admin.close(Duration.ofSeconds(1));
  }
}
