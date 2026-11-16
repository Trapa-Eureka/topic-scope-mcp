package io.topicscope.kafka;

import io.topicscope.config.TopicScopeProperties.Source;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.Admin;
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
  public ClusterInfo describe(Source source, Duration timeout) throws Exception {
    long millis = timeout.toMillis();
    Admin admin = KafkaClients.admin(source, timeout);
    try {
      DescribeClusterResult cluster = admin.describeCluster();
      String id = cluster.clusterId().get(millis, TimeUnit.MILLISECONDS);
      int brokers = cluster.nodes().get(millis, TimeUnit.MILLISECONDS).size();
      return new ClusterInfo(id, brokers);
    } finally {
      KafkaClients.close(admin);
    }
  }
}
