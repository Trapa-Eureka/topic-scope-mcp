package io.topicscope.sim;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;

/**
 * Entry point of the compose service.
 *
 * <p>Environment:
 * <ul>
 *   <li>{@code KAFKA_BOOTSTRAP} (default {@code localhost:9092})</li>
 *   <li>{@code MQTT_URI} (default {@code tcp://localhost:1883})</li>
 *   <li>{@code SIM_MODE}: {@code all} (default; seed, then live traffic), {@code seed} (seed and exit, the
 *       state stays exactly as {@link DemoData} describes), {@code live} (live traffic on a seeded stack)</li>
 *   <li>{@code SIM_SILENT_AFTER_SECONDS} (default 60): when the silent sensor stops publishing</li>
 * </ul>
 */
public final class Simulator {

  private Simulator() {}

  public static void main(String[] args) throws Exception {
    String kafka = env("KAFKA_BOOTSTRAP", "localhost:9092");
    String mqtt = env("MQTT_URI", "tcp://localhost:1883");
    String mode = env("SIM_MODE", "all");
    if (!mode.equals("all") && !mode.equals("seed") && !mode.equals("live")) {
      throw new IllegalArgumentException("SIM_MODE must be all, seed or live, not: " + mode);
    }
    log("mode=" + mode + " kafka=" + kafka + " mqtt=" + mqtt);

    if (!mode.equals("live")) {
      Seeder.Result result = seedWhenBrokersAnswer(kafka, mqtt);
      log(result == Seeder.Result.SEEDED ? "seeded:" : "already seeded, left as it is. A fresh seed looks like this:");
      for (DemoData.TopicSpec t : DemoData.TOPICS) {
        log("  topic " + t.name() + "  partitions=" + t.partitions() + "  records=" + t.seededTotal());
      }
      for (DemoData.GroupSpec g : DemoData.GROUPS) {
        log("  group " + g.group() + "  topic=" + g.topic().name() + "  lag=" + g.seededLagTotal());
      }
      log("  mqtt  " + DemoData.DEVICES.size() + " retained status messages under demo/sensors/");
    }
    if (mode.equals("seed")) return;

    LiveTraffic.Timing defaults = LiveTraffic.Timing.defaults();
    Duration silentAfter = Duration.ofSeconds(Long.parseLong(env("SIM_SILENT_AFTER_SECONDS", String.valueOf(defaults.silentAfter().toSeconds()))));
    LiveTraffic.Timing timing = new LiveTraffic.Timing(defaults.positionInterval(), defaults.telemetryInterval(), defaults.trackerPause(), silentAfter);

    CountDownLatch stop = new CountDownLatch(1);
    LiveTraffic live = LiveTraffic.start(kafka, mqtt, timing);
    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      live.close();
      stop.countDown();
    }, "sim-shutdown"));
    log("live traffic running. " + DemoData.SILENT_DEVICE.id() + " falls silent after " + silentAfter.toSeconds() + " s");
    stop.await();
  }

  /** The brokers may still be starting, so try for up to a minute. */
  private static Seeder.Result seedWhenBrokersAnswer(String kafka, String mqtt) throws Exception {
    Exception last = null;
    for (int attempt = 1; attempt <= 30; attempt++) {
      try {
        return Seeder.seed(kafka, mqtt);
      } catch (IllegalStateException e) {
        throw e; // half seeded: waiting does not help
      } catch (Exception e) {
        last = e;
        log("brokers not ready (attempt " + attempt + "): " + e);
        Thread.sleep(2000);
      }
    }
    throw last;
  }

  private static String env(String name, String fallback) {
    String v = System.getenv(name);
    return v == null || v.isBlank() ? fallback : v.trim();
  }

  private static void log(String line) {
    System.out.println("[simulator] " + line);
  }
}
