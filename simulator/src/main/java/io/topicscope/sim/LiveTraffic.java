package io.topicscope.sim;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;

/**
 * Keeps sample traffic flowing on an already seeded stack:
 *
 * <ul>
 *   <li>one aircraft position every {@code positionInterval} into Kafka,</li>
 *   <li>one reading per sensor every {@code telemetryInterval} over MQTT (one sensor falls silent after {@code silentAfter}),</li>
 *   <li>a bridge that copies every MQTT reading into Kafka,</li>
 *   <li>a consumer that keeps up ({@code demo-archiver}) and one that cannot ({@code demo-tracker}).</li>
 * </ul>
 *
 * Unlike the seed, this is not exact: how many records exist depends on how long it has been running.
 */
public final class LiveTraffic implements AutoCloseable {

  /** How fast things happen. The defaults are the documented behaviour; tests use shorter ones. */
  public record Timing(Duration positionInterval, Duration telemetryInterval, Duration trackerPause, Duration silentAfter) {
    public static Timing defaults() {
      return new Timing(Duration.ofMillis(500), Duration.ofSeconds(2), Duration.ofSeconds(1), Duration.ofSeconds(60));
    }
  }

  private final AtomicBoolean running = new AtomicBoolean(true);
  private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(2, r -> daemon(r, "sim-publisher"));
  private final List<Thread> consumerThreads = new ArrayList<>();
  private final List<KafkaConsumer<byte[], byte[]>> consumers = new ArrayList<>();
  private final KafkaProducer<byte[], byte[]> producer;
  private final MqttClient mqttPublisher;
  private final MqttClient mqttBridge;

  private final AtomicInteger positionTick = new AtomicInteger();
  private final AtomicInteger telemetrySeq = new AtomicInteger(DemoData.SEEDED_STEPS_PER_DEVICE);
  private final long startedAt = System.nanoTime();

  private LiveTraffic(Stack stack, Timing timing) throws MqttException {
    Properties props = stack.kafka();
    props.put(ProducerConfig.CLIENT_ID_CONFIG, "topic-scope-simulator-live");
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
    producer = new KafkaProducer<>(props);

    // The bridge subscribes before anything is published, so no reading is missed.
    mqttBridge = connect(stack, "topic-scope-simulator-bridge");
    mqttBridge.subscribe(DemoData.TELEMETRY_FILTER, 1, (topic, message) -> bridgeToKafka(topic, message.getPayload()));
    mqttPublisher = connect(stack, "topic-scope-simulator-live");

    scheduler.scheduleAtFixedRate(this::publishPosition, 0, timing.positionInterval().toMillis(), TimeUnit.MILLISECONDS);
    scheduler.scheduleAtFixedRate(() -> publishTelemetry(timing.silentAfter()), 0, timing.telemetryInterval().toMillis(), TimeUnit.MILLISECONDS);

    startConsumer(stack, DemoData.ARCHIVER, 500, Duration.ZERO);
    startConsumer(stack, DemoData.TRACKER, 1, timing.trackerPause());
  }

  /** On brokers that ask for no login. */
  public static LiveTraffic start(String kafkaBootstrap, String mqttUri, Timing timing) throws MqttException {
    return new LiveTraffic(Stack.open(kafkaBootstrap, mqttUri), timing);
  }

  public static LiveTraffic start(Stack stack, Timing timing) throws MqttException {
    return new LiveTraffic(stack, timing);
  }

  // ---------- publishers ----------

  /** Aircraft take turns; each one continues its own sequence after the seeded steps. */
  private void publishPosition() {
    try {
      int tick = positionTick.getAndIncrement();
      int aircraft = tick % DemoData.AIRCRAFT.size();
      int seq = DemoData.SEEDED_STEPS_PER_AIRCRAFT + tick / DemoData.AIRCRAFT.size();
      String json = DemoData.positionJson(aircraft, seq, System.currentTimeMillis());
      producer.send(new ProducerRecord<>(DemoData.POSITIONS.name(), DemoData.positionPartition(aircraft),
          utf8(DemoData.AIRCRAFT.get(aircraft).icao24()), utf8(json)));
    } catch (RuntimeException e) {
      warn("position not published", e);
    }
  }

  private void publishTelemetry(Duration silentAfter) {
    int seq = telemetrySeq.getAndIncrement();
    boolean silenced = System.nanoTime() - startedAt > silentAfter.toNanos();
    for (int j = 0; j < DemoData.DEVICES.size(); j++) {
      DemoData.Device d = DemoData.DEVICES.get(j);
      if (silenced && d.equals(DemoData.SILENT_DEVICE)) continue;
      try {
        mqttPublisher.publish(d.telemetryTopic(), utf8(DemoData.telemetryJson(j, seq, System.currentTimeMillis())), 1, false);
      } catch (MqttException | RuntimeException e) {
        warn("reading not published for " + d.id(), e);
      }
    }
  }

  /** {@code demo/sensors/<site>/<name>/telemetry} becomes a Kafka record keyed {@code <site>/<name>}. */
  private void bridgeToKafka(String mqttTopic, byte[] payload) {
    String[] parts = mqttTopic.split("/");
    if (parts.length != 5) return;
    String deviceId = parts[2] + "/" + parts[3];
    int index = DemoData.deviceIndex(deviceId);
    if (index < 0 || !running.get()) return;
    producer.send(new ProducerRecord<>(DemoData.TELEMETRY.name(), DemoData.telemetryPartition(index), utf8(deviceId), payload));
  }

  // ---------- consumers ----------

  /** Reads at most {@code maxRecords} per poll, commits, then waits {@code pause}. A long pause with one record makes a slow consumer. */
  private void startConsumer(Stack stack, DemoData.GroupSpec group, int maxRecords, Duration pause) {
    Properties props = stack.kafka();
    props.put(ConsumerConfig.GROUP_ID_CONFIG, group.group());
    props.put(ConsumerConfig.CLIENT_ID_CONFIG, group.group() + "-1");
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, maxRecords);
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
    KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(props);
    consumers.add(consumer);

    Thread thread = daemon(() -> {
      try {
        consumer.subscribe(List.of(group.topic().name()));
        while (running.get()) {
          ConsumerRecords<byte[], byte[]> records = consumer.poll(Duration.ofMillis(500));
          if (!records.isEmpty()) consumer.commitSync();
          if (!pause.isZero() && !records.isEmpty()) Thread.sleep(pause.toMillis());
        }
      } catch (WakeupException | InterruptedException | org.apache.kafka.common.errors.InterruptException e) {
        // closing
      } catch (RuntimeException e) {
        warn(group.group() + " stopped", e);
      } finally {
        consumer.close();
      }
    }, group.group());
    consumerThreads.add(thread);
    thread.start();
  }

  // ---------- lifecycle ----------

  @Override
  public void close() {
    if (!running.getAndSet(false)) return;
    scheduler.shutdownNow();
    for (KafkaConsumer<byte[], byte[]> c : consumers) c.wakeup();
    // No interrupt: wakeup() ends a poll, and a paused consumer wakes by itself within its pause.
    for (Thread t : consumerThreads) {
      try {
        t.join(10_000);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }
    for (MqttClient c : List.of(mqttPublisher, mqttBridge)) {
      try {
        c.disconnect();
        c.close();
      } catch (MqttException e) {
        warn("MQTT client not closed cleanly", e);
      }
    }
    producer.close(Duration.ofSeconds(5));
  }

  private static MqttClient connect(Stack stack, String clientId) throws MqttException {
    MqttClient client = new MqttClient(stack.mqttUri(), clientId, new MemoryPersistence());
    MqttConnectOptions options = stack.mqtt();
    options.setAutomaticReconnect(true);
    client.connect(options);
    return client;
  }

  private static Thread daemon(Runnable r, String name) {
    Thread t = new Thread(r, name);
    t.setDaemon(true);
    return t;
  }

  private static byte[] utf8(String s) {
    return s.getBytes(StandardCharsets.UTF_8);
  }

  private static void warn(String what, Exception e) {
    System.err.println("[simulator] " + what + ": " + e);
  }
}
