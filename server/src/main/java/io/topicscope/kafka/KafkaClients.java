package io.topicscope.kafka;

import java.time.Duration;
import java.util.Properties;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;

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

  /** Closing must not hang a tool call either. */
  static void close(Admin admin) {
    admin.close(Duration.ofSeconds(1));
  }
}
