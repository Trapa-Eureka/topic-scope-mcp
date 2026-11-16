package io.topicscope.kafka;

import io.topicscope.config.TopicScopeProperties.Source;
import java.time.Duration;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;

/** Builds the short-lived Kafka clients the server uses. Every one is bounded by a timeout. */
final class KafkaClients {

  private KafkaClients() {}

  /**
   * The kinds of extra client settings a source may bring: how the connection is encrypted and how the
   * server logs in. An allow list on purpose. Anything else a configuration tries to set is refused at
   * start-up, so no setting can turn the look-only consumer into one that joins a group or commits.
   */
  static final List<String> ALLOWED_SETTING_PREFIXES = List.of("security.protocol", "sasl.", "ssl.", "client.dns.lookup");

  /** An admin client for one tool call. The caller closes it. */
  static Admin admin(Source source, Duration timeout) {
    int millis = Math.toIntExact(timeout.toMillis());
    Properties props = connection(source);
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
  static KafkaConsumer<byte[], byte[]> consumer(Source source, Duration timeout) {
    int millis = Math.toIntExact(timeout.toMillis());
    Properties props = connection(source);
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

  /**
   * Where to connect and how to log in. The source's own settings go in first, so everything the callers
   * above set afterwards cannot be overridden by a configuration file.
   */
  static Properties connection(Source source) {
    Properties props = new Properties();
    for (Map.Entry<String, String> setting : source.properties().entrySet()) {
      requireAllowed(source, setting.getKey());
      props.put(setting.getKey(), setting.getValue());
    }
    props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, source.address());

    if (source.hasLogin()) {
      // Defaults for the common case: a user name and a password, not encrypted. A source that needs TLS or
      // another mechanism says so in its properties (security.protocol: SASL_SSL, sasl.mechanism: SCRAM-SHA-512).
      props.putIfAbsent("security.protocol", "SASL_PLAINTEXT");
      props.putIfAbsent("sasl.mechanism", "PLAIN");
      if (!props.containsKey("sasl.jaas.config")) {
        String mechanism = String.valueOf(props.get("sasl.mechanism")).toUpperCase(Locale.ROOT);
        String module = mechanism.startsWith("SCRAM")
            ? "org.apache.kafka.common.security.scram.ScramLoginModule"
            : "org.apache.kafka.common.security.plain.PlainLoginModule";
        props.put("sasl.jaas.config", module + " required username=\"" + quoted(source.username()) + "\" password=\"" + quoted(source.password()) + "\";");
      }
    }
    return props;
  }

  /** @throws IllegalArgumentException when a source brings a client setting that is not about encryption or login */
  static void requireAllowed(Source source, String setting) {
    for (String prefix : ALLOWED_SETTING_PREFIXES) {
      if (setting.equals(prefix) || (prefix.endsWith(".") && setting.startsWith(prefix))) return;
    }
    throw new IllegalArgumentException("Source '" + source.name() + "' sets the Kafka client property '" + setting + "'. "
        + "Only settings for encryption and login are accepted: " + String.join(", ", ALLOWED_SETTING_PREFIXES) + "*");
  }

  /** Inside the double quotes of a login configuration, a backslash and a quote have to be escaped. */
  private static String quoted(String value) {
    return value.replace("\\", "\\\\").replace("\"", "\\\"");
  }

  /** Closing must not hang a tool call either. */
  static void close(Admin admin) {
    admin.close(Duration.ofSeconds(1));
  }
}
