package io.topicscope.tools;

import io.topicscope.config.TopicScopeProperties;
import io.topicscope.config.TopicScopeProperties.Source;
import io.topicscope.mqtt.MqttObserver;
import io.topicscope.mqtt.MqttObserver.Seen;
import io.topicscope.mqtt.MqttObserver.Window;
import io.topicscope.safety.TopicAccess;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import org.springframework.stereotype.Service;

/**
 * Everything the tools learn from an MQTT broker comes from listening for a few seconds. This class does
 * the listening and adds up what was heard; the other services shape it into their own rows.
 */
@Service
public class MqttService {

  static final String ALL_TOPICS = "#";
  static final int DEFAULT_WINDOW_SECONDS = 3;
  static final int MAX_WINDOW_SECONDS = 30;

  /** Counters a broker such as Mosquitto publishes about itself. A broker that does not publish them simply sends nothing. */
  static final List<String> BROKER_COUNTERS = List.of(
      "$SYS/broker/clients/connected",
      "$SYS/broker/subscriptions/count",
      "$SYS/broker/retained messages/count",
      "$SYS/broker/store/messages/count",
      "$SYS/broker/publish/messages/dropped",
      "$SYS/broker/load/messages/received/1min",
      "$SYS/broker/uptime");

  /** A value is kept in memory for display only up to this size; larger ones are remembered by size. */
  private static final int KEEP_PAYLOAD_BYTES = 256 * 1024;

  /**
   * What was heard on one topic during a window.
   *
   * @param messages messages published while listening (retained deliveries are not counted)
   * @param retained whether the broker had a retained message for it
   * @param qos the highest quality of service seen
   * @param lastLiveAtMillis arrival of the newest message published while listening; -1 when there was none
   * @param lastPayloadTimeMillis the time inside the newest payload, live or retained; -1 when it carries none
   * @param lastDelayMillis arrival minus payload time of the newest live message; -1 when unknown
   */
  public record TopicActivity(String topic, int messages, boolean retained, int qos, long lastLiveAtMillis, long lastPayloadTimeMillis, long lastDelayMillis) {}

  /** A counter the broker publishes about itself, as text exactly as published. */
  public record BrokerCounter(String name, String value) {}

  /**
   * The outcome of one listening window. {@code newest} holds the newest messages, newest first, at most as many as were asked for.
   *
   * @param hiddenMessages messages that arrived on topics the server's configuration leaves out; they were dropped without being looked at
   */
  public record Heard(Window window, List<TopicActivity> topics, List<Seen> newest, int totalMessages, List<BrokerCounter> brokerCounters, int hiddenMessages) {

    /** A sentence for a result's note, or nothing when no message was dropped. */
    public String hiddenNote() {
      return Sources.hiddenNote(hiddenMessages, "message(s) on other topics arrived and were dropped unread:");
    }
  }

  private final Sources sources;
  private final MqttObserver observer;
  private final TopicScopeProperties properties;

  public MqttService(Sources sources, MqttObserver observer, TopicScopeProperties properties) {
    this.sources = sources;
    this.observer = observer;
    this.properties = properties;
  }

  /** The window a caller asked for, with the default and the bounds applied. */
  static int windowSeconds(Integer asked, int fallback, int max) {
    int seconds = asked == null ? fallback : asked;
    if (seconds < 1 || seconds > max) {
      throw new ToolProblem("'windowSeconds' must be between 1 and " + max + ", and it was " + seconds + ".",
          "Leave it out for the default of " + fallback + ". An MQTT broker keeps no history, so only what arrives during the window is seen.");
    }
    return seconds;
  }

  /** The filter a caller asked for, or all topics. A filter is trimmed, never otherwise changed. */
  static String filter(String asked) {
    return asked == null || asked.isBlank() ? ALL_TOPICS : asked.trim();
  }

  /**
   * Listens on one filter and adds up what arrives.
   *
   * @param keepNewest how many of the newest messages to keep whole; 0 when only the activity matters
   * @param withCounters whether to also read the broker's own counters
   */
  public Heard listen(Source source, String filter, int windowSeconds, int keepNewest, boolean withCounters) {
    List<String> filters = new ArrayList<>();
    filters.add(filter);
    if (withCounters) filters.addAll(BROKER_COUNTERS);

    TopicAccess access = sources.access(source);
    boolean exact = !filter.contains("+") && !filter.contains("#");
    if (exact) sources.requireAllowed(source, filter);
    int[] hidden = {0};

    Map<String, Tally> tallies = new TreeMap<>();
    Map<String, String> counters = new TreeMap<>();
    Deque<Seen> newest = new ArrayDeque<>();
    int[] total = {0};

    Window window = sources.ask(source, () -> {
      try {
        return observer.observe(source, filters, Duration.ofSeconds(windowSeconds), properties.requestTimeout(), seen -> {
          if (withCounters && BROKER_COUNTERS.contains(seen.topic())) {
            counters.put(seen.topic(), new String(seen.payload(), java.nio.charset.StandardCharsets.UTF_8));
            return;
          }
          if (!access.permits(seen.topic())) {
            hidden[0]++; // dropped here, before its payload is looked at, counted or kept
            return;
          }
          total[0]++;
          tallies.computeIfAbsent(seen.topic(), Tally::new).add(seen);
          if (keepNewest > 0) {
            newest.addFirst(seen.payload().length > KEEP_PAYLOAD_BYTES ? cut(seen) : seen);
            if (newest.size() > keepNewest) newest.removeLast();
          }
        });
      } catch (IllegalArgumentException e) {
        throw new ToolProblem("'" + filter + "' is not a valid MQTT topic filter: " + e.getMessage() + ".",
            "Levels are separated by '/'. '+' stands for exactly one level, '#' for all remaining levels and only at the end. Example: demo/sensors/+/+/telemetry");
      }
    });

    if (window.refusedFilters().contains(filter)) {
      // Not an empty answer: nothing was listened to, and saying "no messages" would be wrong.
      throw new ToolProblem("The broker of source '" + source.name() + "' refused the subscription to '" + filter + "', so nothing was listened to.",
          "The account of this source may not listen to that filter. Some brokers refuse '#' and '$SYS/#' unless their access rules say otherwise "
              + "(EMQX does by default): ask for a narrower filter, such as 'demo/#', or change the broker's access rules.");
    }
    List<TopicActivity> topics = tallies.values().stream().map(Tally::activity).toList();
    List<BrokerCounter> brokerCounters = counters.entrySet().stream().map(e -> new BrokerCounter(e.getKey(), e.getValue())).toList();
    return new Heard(window, topics, List.copyOf(newest), total[0], brokerCounters, hidden[0]);
  }

  /** Keeps the start of an oversized payload: enough to show a preview, and the formatter marks it as cut. */
  private static Seen cut(Seen seen) {
    return new Seen(seen.topic(), java.util.Arrays.copyOf(seen.payload(), KEEP_PAYLOAD_BYTES), seen.qos(), seen.retained(), seen.receivedAtMillis());
  }

  /** Adds up one topic while listening. */
  private static final class Tally {
    private final String topic;
    private int live;
    private boolean retained;
    private int qos;
    private long lastLiveAt = -1;
    private long lastPayloadTime = -1;
    private long lastDelay = -1;

    Tally(String topic) {
      this.topic = topic;
    }

    void add(Seen seen) {
      qos = Math.max(qos, seen.qos());
      long payloadTime = PayloadTime.of(seen.payload());
      if (seen.retained()) {
        retained = true;
        if (lastLiveAt < 0) lastPayloadTime = payloadTime; // a live message, once there is one, is newer than the stored one
        return;
      }
      live++;
      lastLiveAt = seen.receivedAtMillis();
      lastPayloadTime = payloadTime;
      lastDelay = payloadTime < 0 ? -1 : Math.max(0, seen.receivedAtMillis() - payloadTime);
    }

    TopicActivity activity() {
      return new TopicActivity(topic, live, retained, qos, lastLiveAt, lastPayloadTime, lastDelay);
    }
  }
}
