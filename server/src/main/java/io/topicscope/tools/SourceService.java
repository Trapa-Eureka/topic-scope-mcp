package io.topicscope.tools;

import io.topicscope.config.TopicScopeProperties;
import io.topicscope.config.TopicScopeProperties.Source;
import io.topicscope.kafka.KafkaProbe;
import io.topicscope.mqtt.MqttProbe;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.springframework.stereotype.Service;

/** Turns the configured sources into the rows {@code list_sources} shows. A source that fails is a row, never an exception. */
@Service
public class SourceService {

  /**
   * One row of {@code list_sources}. No field is ever null.
   *
   * @param name the name to pass to the other tools
   * @param type {@code KAFKA} or {@code MQTT}
   * @param address where it is, with any password removed
   * @param reachable whether the broker answered within the timeout
   * @param detail when reachable: what the broker said about itself. When not: why it could not be reached
   * @param responseMillis how long the check took
   */
  public record SourceStatus(String name, String type, String address, boolean reachable, String detail, long responseMillis) {}

  /** The result of {@code list_sources}. */
  public record SourceList(List<SourceStatus> sources, int reachable, int total) {}

  private final TopicScopeProperties properties;
  private final KafkaProbe kafkaProbe;
  private final MqttProbe mqttProbe;

  public SourceService(TopicScopeProperties properties, KafkaProbe kafkaProbe, MqttProbe mqttProbe) {
    this.properties = properties;
    this.kafkaProbe = kafkaProbe;
    this.mqttProbe = mqttProbe;
  }

  public SourceList listSources() {
    List<SourceStatus> rows = checkAll(properties.sources());
    int reachable = (int) rows.stream().filter(SourceStatus::reachable).count();
    return new SourceList(rows, reachable, rows.size());
  }

  /**
   * All sources at once, so brokers that do not answer cost one timeout together and not one each.
   *
   * <p>Not a parallel stream: its shared threads do not see the classes inside the packaged jar, and the
   * Kafka client then fails to load. These threads take the caller's class loader.
   */
  private List<SourceStatus> checkAll(List<Source> sources) {
    if (sources.isEmpty()) return List.of();
    ClassLoader loader = Thread.currentThread().getContextClassLoader();
    ExecutorService pool = Executors.newFixedThreadPool(Math.min(sources.size(), 8), task -> {
      Thread t = new Thread(task, "topic-scope-probe");
      t.setDaemon(true);
      t.setContextClassLoader(loader);
      return t;
    });
    try {
      List<Callable<SourceStatus>> checks = new ArrayList<>();
      for (Source s : sources) checks.add(() -> check(s));
      List<SourceStatus> rows = new ArrayList<>();
      List<Future<SourceStatus>> done = pool.invokeAll(checks);
      for (int i = 0; i < done.size(); i++) {
        try {
          rows.add(done.get(i).get());
        } catch (Exception e) {
          rows.add(unreachable(sources.get(i), 0, e));
        }
      }
      return rows;
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      return sources.stream().map(s -> unreachable(s, 0, e)).toList();
    } finally {
      pool.shutdownNow();
    }
  }

  private SourceStatus check(Source source) {
    long start = System.nanoTime();
    try {
      String detail = switch (source.type()) {
        case KAFKA -> {
          KafkaProbe.ClusterInfo c = kafkaProbe.describe(source.address(), properties.probeTimeout());
          yield "cluster " + c.clusterId() + ", " + c.brokers() + (c.brokers() == 1 ? " broker" : " brokers");
        }
        case MQTT -> {
          MqttProbe.BrokerInfo b = mqttProbe.connect(source.address(), properties.probeTimeout());
          yield b.version() == null ? "connected (the broker does not announce a version)" : b.version();
        }
      };
      return new SourceStatus(source.name(), source.type().name(), withoutPassword(source.address()), true, detail, millisSince(start));
    } catch (Exception | LinkageError e) {
      // LinkageError too: a client library that cannot load must show up as a row, not kill the tool call.
      return unreachable(source, millisSince(start), e);
    }
  }

  private static SourceStatus unreachable(Source source, long millis, Throwable e) {
    return new SourceStatus(source.name(), source.type().name(), withoutPassword(source.address()), false, reason(e), millis);
  }

  private static long millisSince(long startNanos) {
    return (System.nanoTime() - startNanos) / 1_000_000;
  }

  /** The innermost message is the useful one ("Connection refused"); the outer ones are wrappers. */
  static String reason(Throwable e) {
    Throwable root = e;
    while (root.getCause() != null && root.getCause() != root) root = root.getCause();
    String name = root.getClass().getSimpleName();
    if (name.equals("TimeoutException")) return "no answer within the timeout";
    String message = root.getMessage();
    return withoutPassword(message == null || message.isBlank() ? name : name + ": " + message);
  }

  /** {@code tcp://user:secret@host:1883} becomes {@code tcp://user:***@host:1883}. */
  static String withoutPassword(String text) {
    return text == null ? "" : text.replaceAll("(://[^:/@\\s]+):[^@/\\s]+@", "$1:***@");
  }
}
