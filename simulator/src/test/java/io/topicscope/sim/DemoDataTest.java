package io.topicscope.sim;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.Test;

/** The constants and the generated records must agree with each other; everything else trusts them. */
class DemoDataTest {

  @Test
  void everyPartitionGetsExactlyTheDeclaredNumberOfRecords() {
    for (DemoData.TopicSpec t : DemoData.TOPICS) {
      for (int p = 0; p < t.partitions(); p++) {
        assertThat(DemoData.seedRecords(t.name(), p)).as("%s-%d", t.name(), p).hasSize(t.seededPerPartition());
      }
    }
    int declared = DemoData.TOPICS.stream().mapToInt(DemoData.TopicSpec::seededTotal).sum();
    assertThat(DemoData.seedRecords()).hasSize(declared).hasSize(358);
  }

  @Test
  void seededLagIsWhatTheReadmeSays() {
    assertThat(DemoData.ARCHIVER.seededLagTotal()).isZero();
    assertThat(DemoData.TRACKER.seededLagTotal()).isEqualTo(120);
    assertThat(DemoData.REPORTER.seededLagTotal()).isEqualTo(240);
    assertThat(DemoData.ALARM_NOTIFIER.seededLagTotal()).isEqualTo(5);
  }

  @Test
  void theDataIsTheSameEveryTime() {
    List<DemoData.SeedRecord> a = DemoData.seedRecords();
    List<DemoData.SeedRecord> b = DemoData.seedRecords();
    for (int i = 0; i < a.size(); i++) {
      assertThat(a.get(i).key()).isEqualTo(b.get(i).key());
      assertThat(Arrays.equals(a.get(i).value(), b.get(i).value())).isTrue();
    }
    assertThat(new String(DemoData.seedRecords(DemoData.POSITIONS.name(), 0).get(0).value(), StandardCharsets.UTF_8))
        .isEqualTo("{\"icao24\":\"d0a001\",\"callsign\":\"DEMO101\",\"seq\":0,\"lat\":10.7130,\"lon\":122.5450,\"altFt\":3000,\"gsKt\":180,\"headingDeg\":45}");
  }

  @Test
  void edgeCasesAreWhatTheirNamesSay() {
    List<DemoData.SeedRecord> edge = DemoData.seedRecords(DemoData.EDGE_CASES.name(), 0);
    assertThat(edge.get(DemoData.EDGE_TEXT_NO_KEY).key()).isNull();
    assertThat(edge.get(DemoData.EDGE_BINARY).value()).hasSize(256);
    assertThat(edge.get(DemoData.EDGE_OVERSIZED).value().length).isGreaterThan(DemoData.OVERSIZED_BLOB_CHARS);
    assertThat(new String(edge.get(DemoData.EDGE_SECRETS).value(), StandardCharsets.UTF_8)).contains("password", "apiKey", "token");
    assertThat(edge.get(DemoData.EDGE_TOMBSTONE).value()).isNull();
    assertThat(edge.get(DemoData.EDGE_JSON).headers()).containsEntry("content-type", "application/json");
  }

  @Test
  void liveRecordsCarryATimestampAndSeededOnesDoNot() {
    assertThat(DemoData.positionJson(0, 50, 1_700_000_000_000L)).endsWith(",\"ts\":1700000000000}");
    assertThat(DemoData.telemetryJson(0, 0, null)).isEqualTo("{\"device\":\"twr-01/wind\",\"seq\":0,\"value\":12.0,\"unit\":\"kt\"}");
    assertThat(DemoData.DEVICES.get(0).telemetryTopic()).isEqualTo("demo/sensors/twr-01/wind/telemetry");
  }
}
