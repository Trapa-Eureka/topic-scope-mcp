package io.topicscope.tools;

import static org.assertj.core.api.Assertions.assertThat;

import io.topicscope.config.TopicScopeProperties;
import io.topicscope.config.TopicScopeProperties.Source;
import io.topicscope.config.TopicScopeProperties.SourceType;
import io.topicscope.kafka.KafkaProbe;
import io.topicscope.mqtt.MqttProbe;
import java.io.IOException;
import java.net.ServerSocket;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import org.junit.jupiter.api.Test;

/** No broker runs here: these are the cases where nothing answers. The answering cases are in McpServerIT. */
class SourceServiceTest {

  @Test
  void brokersThatDoNotAnswerBecomeRowsNotExceptions() throws Exception {
    int closedPort = closedPort();
    TopicScopeProperties properties = new TopicScopeProperties(List.of(
        Source.open("k", SourceType.KAFKA, "127.0.0.1:" + closedPort),
        Source.open("m", SourceType.MQTT, "tcp://127.0.0.1:" + closedPort)), Duration.ofSeconds(1), null, null, TopicScopeProperties.Audit.off());

    long start = System.nanoTime();
    SourceService.SourceList result = new SourceService(properties, new KafkaProbe(), new MqttProbe()).listSources();
    long tookMillis = (System.nanoTime() - start) / 1_000_000;

    assertThat(result.total()).isEqualTo(2);
    assertThat(result.reachable()).isZero();
    assertThat(result.sources()).extracting(SourceService.SourceStatus::name).containsExactly("k", "m");
    assertThat(result.sources()).allSatisfy(row -> {
      assertThat(row.reachable()).isFalse();
      assertThat(row.detail()).isNotBlank();
    });
    assertThat(tookMillis).as("checked side by side, each within its timeout").isLessThan(4000);
  }

  @Test
  void noSourcesIsAnEmptyList() {
    TopicScopeProperties properties = new TopicScopeProperties(null, null, null, null, TopicScopeProperties.Audit.off());
    assertThat(properties.requestTimeout()).isEqualTo(Duration.ofSeconds(10));
    assertThat(properties.probeTimeout()).isEqualTo(Duration.ofSeconds(5));
    SourceService.SourceList result = new SourceService(properties, new KafkaProbe(), new MqttProbe()).listSources();
    assertThat(result.sources()).isEmpty();
    assertThat(result.total()).isZero();
  }

  @Test
  void passwordsNeverReachTheOutput() {
    assertThat(SourceService.withoutPassword("tcp://operator:s3cret@broker:1883")).isEqualTo("tcp://operator:***@broker:1883");
    assertThat(SourceService.withoutPassword("ssl://u:p@h:8883 refused")).isEqualTo("ssl://u:***@h:8883 refused");
    assertThat(SourceService.withoutPassword("tcp://localhost:1883")).isEqualTo("tcp://localhost:1883");
    assertThat(SourceService.withoutPassword("localhost:9092,other:9092")).isEqualTo("localhost:9092,other:9092");
    assertThat(SourceService.reason(new IllegalStateException("cannot reach tcp://u:hunter2@h:1883"))).doesNotContain("hunter2");
  }

  @Test
  void theReasonIsTheInnermostCauseInPlainWords() {
    Exception wrapped = new ExecutionException(new RuntimeException("outer", new IOException("Connection refused")));
    assertThat(SourceService.reason(wrapped)).isEqualTo("IOException: Connection refused");
    assertThat(SourceService.reason(new ExecutionException(new TimeoutException()))).isEqualTo("no answer within the timeout");
    assertThat(SourceService.reason(new IllegalStateException())).isEqualTo("IllegalStateException");
  }

  @Test
  void sourceNamesMustBeUniqueAndComplete() {
    Source a = Source.open("same", SourceType.KAFKA, "h:1");
    org.assertj.core.api.Assertions.assertThatThrownBy(() -> new TopicScopeProperties(List.of(a, a), null, null, null, TopicScopeProperties.Audit.off()))
        .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("same");
    org.assertj.core.api.Assertions.assertThatThrownBy(() -> Source.open("x", SourceType.MQTT, " "))
        .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("address");
  }

  /** A port nothing listens on: opened to find a free number, then closed again. */
  private static int closedPort() throws IOException {
    try (ServerSocket socket = new ServerSocket(0)) {
      return socket.getLocalPort();
    }
  }
}
