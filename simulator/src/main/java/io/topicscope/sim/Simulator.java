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
 *   <li>{@code KAFKA_USERNAME}, {@code KAFKA_PASSWORD}, {@code MQTT_USERNAME}, {@code MQTT_PASSWORD}: the simulator's
 *       own logins, for brokers that ask for one</li>
 *   <li>{@code READ_ONLY_KAFKA_USER}: a Kafka user that is given look-only rights while seeding (the server's account)</li>
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
    Stack stack = new Stack(kafka, Stack.Login.of(System.getenv("KAFKA_USERNAME"), System.getenv("KAFKA_PASSWORD")),
        mqtt, Stack.Login.of(System.getenv("MQTT_USERNAME"), System.getenv("MQTT_PASSWORD")));
    String readOnlyKafkaUser = env("READ_ONLY_KAFKA_USER", "");
    log("mode=" + mode + " kafka=" + kafka + " mqtt=" + mqtt
        + (stack.kafkaLogin() == null ? "" : " kafka-user=" + stack.kafkaLogin().username())
        + (stack.mqttLogin() == null ? "" : " mqtt-user=" + stack.mqttLogin().username()));

    if (!mode.equals("live")) {
      Seeder.Result result = seedWhenBrokersAnswer(stack, readOnlyKafkaUser.isEmpty() ? null : readOnlyKafkaUser);
      log(result == Seeder.Result.SEEDED ? "seeded:" : "already seeded, left as it is. A fresh seed looks like this:");
      for (DemoData.TopicSpec t : DemoData.TOPICS) {
        log("  topic " + t.name() + "  partitions=" + t.partitions() + "  records=" + t.seededTotal());
      }
      for (DemoData.GroupSpec g : DemoData.GROUPS) {
        log("  group " + g.group() + "  topic=" + g.topic().name() + "  lag=" + g.seededLagTotal());
      }
      log("  mqtt  " + DemoData.DEVICES.size() + " retained status messages under demo/sensors/");
      if (!readOnlyKafkaUser.isEmpty()) log("  kafka user " + readOnlyKafkaUser + " may describe and read, and nothing else");
    }
    if (mode.equals("seed")) return;

    LiveTraffic.Timing defaults = LiveTraffic.Timing.defaults();
    Duration silentAfter = Duration.ofSeconds(Long.parseLong(env("SIM_SILENT_AFTER_SECONDS", String.valueOf(defaults.silentAfter().toSeconds()))));
    LiveTraffic.Timing timing = new LiveTraffic.Timing(defaults.positionInterval(), defaults.telemetryInterval(), defaults.trackerPause(), silentAfter);

    CountDownLatch stop = new CountDownLatch(1);
    LiveTraffic live = LiveTraffic.start(stack, timing);
    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      live.close();
      stop.countDown();
    }, "sim-shutdown"));
    log("live traffic running. " + DemoData.SILENT_DEVICE.id() + " falls silent after " + silentAfter.toSeconds() + " s");
    stop.await();
  }

  /** The brokers may still be starting, so try for up to a minute. */
  private static Seeder.Result seedWhenBrokersAnswer(Stack stack, String readOnlyKafkaUser) throws Exception {
    Exception last = null;
    for (int attempt = 1; attempt <= 30; attempt++) {
      try {
        return Seeder.seed(stack, readOnlyKafkaUser);
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
