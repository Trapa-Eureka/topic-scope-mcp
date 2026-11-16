package io.topicscope.kafka;

import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.springframework.stereotype.Component;

/** Asks a Kafka cluster who it is. Read-only: one describeCluster call on a short-lived connection. */
@Component
public class KafkaProbe {

  /** What the cluster answered. */
  public record ClusterInfo(String clusterId, int brokers) {}

  /**
   * @throws Exception when the cluster does not answer within the timeout, or refuses
   */
  public ClusterInfo describe(String bootstrapServers, Duration timeout) throws Exception {
    int millis = Math.toIntExact(timeout.toMillis());
    Properties props = new Properties();
    props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    props.put(AdminClientConfig.CLIENT_ID_CONFIG, "topic-scope");
    props.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, millis);
    props.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, millis);
    // Fail fast instead of retrying forever when the address is wrong.
    props.put(AdminClientConfig.SOCKET_CONNECTION_SETUP_TIMEOUT_MS_CONFIG, millis);
    props.put(AdminClientConfig.SOCKET_CONNECTION_SETUP_TIMEOUT_MAX_MS_CONFIG, millis);

    Admin admin = Admin.create(props);
    try {
      DescribeClusterResult cluster = admin.describeCluster();
      String id = cluster.clusterId().get(millis, TimeUnit.MILLISECONDS);
      int brokers = cluster.nodes().get(millis, TimeUnit.MILLISECONDS).size();
      return new ClusterInfo(id, brokers);
    } finally {
      admin.close(Duration.ofSeconds(1));
    }
  }
}
