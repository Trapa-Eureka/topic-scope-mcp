package io.topicscope.sim;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * The demo data set: every topic, record, consumer group and MQTT message the seeder creates.
 *
 * <p>Nothing here is random and nothing depends on the clock, so the state of a freshly seeded broker is
 * known in advance. Tests in other modules compare what the server reports against these constants.
 * All names, aircraft and sensors are made up.
 */
public final class DemoData {

  private DemoData() {}

  // ---------- Kafka topics ----------

  /** A Kafka topic and how many records the seeder puts into each of its partitions. */
  public record TopicSpec(String name, int partitions, int seededPerPartition) {
    public int seededTotal() {
      return partitions * seededPerPartition;
    }
  }

  /** Aircraft positions. Keeps receiving records in live mode. */
  public static final TopicSpec POSITIONS = new TopicSpec("demo.flights.positions", 3, 100);
  /** Sensor readings. In live mode it is fed by the MQTT-to-Kafka bridge. */
  public static final TopicSpec TELEMETRY = new TopicSpec("demo.sensors.telemetry", 2, 20);
  /** Alarms. Never changes after seeding. */
  public static final TopicSpec ALARMS = new TopicSpec("demo.alarms", 1, 12);
  /** One record per awkward case: binary, oversized, secret-looking, no key, no value. Never changes after seeding. */
  public static final TopicSpec EDGE_CASES = new TopicSpec("demo.edge-cases", 1, 6);
  /** A topic that exists and holds nothing. */
  public static final TopicSpec EMPTY = new TopicSpec("demo.empty", 1, 0);

  public static final List<TopicSpec> TOPICS = List.of(POSITIONS, TELEMETRY, ALARMS, EDGE_CASES, EMPTY);

  // ---------- Kafka consumer groups ----------

  /** A consumer group and the offset it has committed in every partition of its topic right after seeding. */
  public record GroupSpec(String group, TopicSpec topic, long committedPerPartition) {
    public long seededLagPerPartition() {
      return topic.seededPerPartition() - committedPerPartition;
    }

    public long seededLagTotal() {
      return seededLagPerPartition() * topic.partitions();
    }
  }

  /** Keeps up. In live mode it reads as fast as records arrive. */
  public static final GroupSpec ARCHIVER = new GroupSpec("demo-archiver", POSITIONS, 100);
  /** The slow consumer. In live mode it reads one record per second while two arrive, so it moves and still falls behind. */
  public static final GroupSpec TRACKER = new GroupSpec("demo-tracker", POSITIONS, 60);
  /** Stopped. It never reads again, so in live mode its lag grows while its committed offset stands still. */
  public static final GroupSpec REPORTER = new GroupSpec("demo-reporter", POSITIONS, 20);
  /** Stopped, on a topic that does not change: a constant backlog of 5. */
  public static final GroupSpec ALARM_NOTIFIER = new GroupSpec("demo-alarm-notifier", ALARMS, 7);

  public static final List<GroupSpec> GROUPS = List.of(ARCHIVER, TRACKER, REPORTER, ALARM_NOTIFIER);

  // ---------- Aircraft ----------

  public record Aircraft(String icao24, String callsign, double lat, double lon, int altFt, int gsKt, int headingDeg) {}

  /** Six aircraft. Aircraft i always writes to partition i % 3, so each partition carries two of them. */
  public static final List<Aircraft> AIRCRAFT = List.of(
      new Aircraft("d0a001", "DEMO101", 10.7130, 122.5450, 3000, 180, 45),
      new Aircraft("d0a002", "DEMO202", 10.9000, 122.3000, 12000, 290, 110),
      new Aircraft("d0a003", "DEMO303", 10.4500, 122.7000, 24000, 410, 200),
      new Aircraft("d0a004", "DEMO404", 11.1000, 122.9000, 36000, 455, 270),
      new Aircraft("d0a005", "DEMO505", 10.6000, 122.1000, 8000, 240, 330),
      new Aircraft("d0a006", "DEMO606", 10.8300, 122.4930, 1500, 140, 20));

  /** Seeded position reports per aircraft: sequence numbers 0 to 49. Live mode continues from 50. */
  public static final int SEEDED_STEPS_PER_AIRCRAFT = 50;

  public static int positionPartition(int aircraftIndex) {
    return aircraftIndex % POSITIONS.partitions();
  }

  /** The position report of one aircraft at one step. {@code tsMillis} is left out when null (seeded records have none). */
  public static String positionJson(int aircraftIndex, int seq, Long tsMillis) {
    Aircraft a = AIRCRAFT.get(aircraftIndex);
    double rad = Math.toRadians(a.headingDeg());
    double lat = a.lat() + seq * 0.002 * Math.cos(rad);
    double lon = a.lon() + seq * 0.002 * Math.sin(rad);
    return String.format(Locale.ROOT,
        "{\"icao24\":\"%s\",\"callsign\":\"%s\",\"seq\":%d,\"lat\":%.4f,\"lon\":%.4f,\"altFt\":%d,\"gsKt\":%d,\"headingDeg\":%d%s}",
        a.icao24(), a.callsign(), seq, lat, lon, a.altFt(), a.gsKt(), a.headingDeg(), ts(tsMillis));
  }

  // ---------- Sensors (MQTT devices) ----------

  public record Device(String site, String name, String unit, double base) {
    /** For example {@code twr-01/wind}. Also the Kafka record key. */
    public String id() {
      return site + "/" + name;
    }

    /** Live readings. Not retained: MQTT keeps no history of them. */
    public String telemetryTopic() {
      return "demo/sensors/" + id() + "/telemetry";
    }

    /** One retained message per device. */
    public String statusTopic() {
      return "demo/sensors/" + id() + "/status";
    }

    public String statusJson() {
      return "{\"device\":\"" + id() + "\",\"state\":\"online\"}";
    }
  }

  /** Four sensors. Device j always writes to Kafka partition j % 2. The last one falls silent in live mode. */
  public static final List<Device> DEVICES = List.of(
      new Device("twr-01", "wind", "kt", 12.0),
      new Device("twr-01", "temp", "degC", 29.5),
      new Device("rwy-09", "rvr", "m", 1800.0),
      new Device("rwy-27", "rvr", "m", 1500.0));

  /** The device that stops publishing in live mode. Its retained status keeps saying "online". */
  public static final Device SILENT_DEVICE = DEVICES.get(3);

  public static final int SEEDED_STEPS_PER_DEVICE = 10;

  /** Wildcard filter that matches every telemetry topic. */
  public static final String TELEMETRY_FILTER = "demo/sensors/+/+/telemetry";
  /** Wildcard filter that matches everything the simulator publishes over MQTT. */
  public static final String MQTT_FILTER = "demo/#";

  public static int telemetryPartition(int deviceIndex) {
    return deviceIndex % TELEMETRY.partitions();
  }

  public static int deviceIndex(String deviceId) {
    for (int i = 0; i < DEVICES.size(); i++) {
      if (DEVICES.get(i).id().equals(deviceId)) return i;
    }
    return -1;
  }

  public static String telemetryJson(int deviceIndex, int seq, Long tsMillis) {
    Device d = DEVICES.get(deviceIndex);
    double value = d.base() + (seq % 5) * 0.5;
    return String.format(Locale.ROOT, "{\"device\":\"%s\",\"seq\":%d,\"value\":%.1f,\"unit\":\"%s\"%s}",
        d.id(), seq, value, d.unit(), ts(tsMillis));
  }

  private static String ts(Long tsMillis) {
    return tsMillis == null ? "" : ",\"ts\":" + tsMillis;
  }

  // ---------- Seeded Kafka records ----------

  /** One record exactly as the seeder publishes it. {@code key} and {@code value} may be null. */
  public record SeedRecord(String topic, int partition, String key, byte[] value, Map<String, String> headers) {}

  private static final String[] SEVERITIES = {"MINOR", "MAJOR", "CRITICAL"};

  /** Value of {@code blob} in the oversized edge case: this many 'x' characters. */
  public static final int OVERSIZED_BLOB_CHARS = 20_000;

  /** Index of each record in {@link #EDGE_CASES}, which is also its offset. */
  public static final int EDGE_JSON = 0, EDGE_TEXT_NO_KEY = 1, EDGE_BINARY = 2, EDGE_OVERSIZED = 3, EDGE_SECRETS = 4, EDGE_TOMBSTONE = 5;

  /**
   * Every seeded record, in publishing order. Within one partition the position in this list is the offset order,
   * so the n-th record of a partition has offset n.
   */
  public static List<SeedRecord> seedRecords() {
    List<SeedRecord> out = new ArrayList<>();

    for (int seq = 0; seq < SEEDED_STEPS_PER_AIRCRAFT; seq++) {
      for (int i = 0; i < AIRCRAFT.size(); i++) {
        out.add(new SeedRecord(POSITIONS.name(), positionPartition(i), AIRCRAFT.get(i).icao24(), utf8(positionJson(i, seq, null)), Map.of()));
      }
    }

    for (int seq = 0; seq < SEEDED_STEPS_PER_DEVICE; seq++) {
      for (int j = 0; j < DEVICES.size(); j++) {
        out.add(new SeedRecord(TELEMETRY.name(), telemetryPartition(j), DEVICES.get(j).id(), utf8(telemetryJson(j, seq, null)), Map.of()));
      }
    }

    for (int n = 1; n <= ALARMS.seededPerPartition(); n++) {
      String id = String.format(Locale.ROOT, "ALM-%04d", n);
      Device source = DEVICES.get(n % DEVICES.size());
      String json = String.format(Locale.ROOT, "{\"id\":\"%s\",\"seq\":%d,\"severity\":\"%s\",\"source\":\"%s\",\"text\":\"Reading out of range on %s\"}",
          id, n, SEVERITIES[n % SEVERITIES.length], source.id(), source.id());
      out.add(new SeedRecord(ALARMS.name(), 0, id, utf8(json), headers("schema", "alarm.v1", "origin", "simulator")));
    }

    byte[] allBytes = new byte[256];
    for (int b = 0; b < 256; b++) allBytes[b] = (byte) b;
    String e = EDGE_CASES.name();
    out.add(new SeedRecord(e, 0, "json", utf8("{\"kind\":\"json\",\"nested\":{\"list\":[1,2,3],\"ok\":true}}"), headers("content-type", "application/json")));
    out.add(new SeedRecord(e, 0, null, utf8("Wind shear advisory — 활주로 09"), Map.of()));
    out.add(new SeedRecord(e, 0, "binary", allBytes, headers("content-type", "application/octet-stream")));
    out.add(new SeedRecord(e, 0, "oversized", utf8("{\"kind\":\"oversized\",\"blob\":\"" + "x".repeat(OVERSIZED_BLOB_CHARS) + "\"}"), Map.of()));
    out.add(new SeedRecord(e, 0, "secrets", utf8("{\"user\":\"operator1\",\"password\":\"not-a-real-password\",\"apiKey\":\"not-a-real-key\",\"token\":\"not-a-real-token\"}"), Map.of()));
    out.add(new SeedRecord(e, 0, "tombstone", null, Map.of()));

    return out;
  }

  /** The seeded records of one partition, in offset order. */
  public static List<SeedRecord> seedRecords(String topic, int partition) {
    return seedRecords().stream().filter(r -> r.topic().equals(topic) && r.partition() == partition).toList();
  }

  private static byte[] utf8(String s) {
    return s.getBytes(StandardCharsets.UTF_8);
  }

  private static Map<String, String> headers(String... keyValues) {
    Map<String, String> m = new LinkedHashMap<>();
    for (int i = 0; i < keyValues.length; i += 2) m.put(keyValues[i], keyValues[i + 1]);
    return m;
  }
}
