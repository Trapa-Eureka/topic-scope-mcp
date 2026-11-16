package io.topicscope.tools;

import io.topicscope.mqtt.MqttObserver;
import io.topicscope.config.TopicScopeProperties.Source;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

/** A broker that plays back prepared messages. The window starts at {@link #START} and lasts exactly as long as asked. */
final class PretendMqtt extends MqttObserver {

  static final long START = 1_794_830_400_000L; // 2026-11-16T12:00:00Z

  final List<Seen> messages = new ArrayList<>();
  List<String> askedFilters;
  Duration askedWindow;

  /** A message published while listening, {@code atMillis} after the window started. */
  PretendMqtt live(String topic, String payload, long atMillis) {
    messages.add(new Seen(topic, payload.getBytes(StandardCharsets.UTF_8), 1, false, START + atMillis));
    return this;
  }

  /** A message the broker had stored; it arrives right at the start. */
  PretendMqtt retained(String topic, String payload) {
    messages.add(new Seen(topic, payload.getBytes(StandardCharsets.UTF_8), 1, true, START));
    return this;
  }

  @Override
  public Window observe(Source source, List<String> filters, Duration window, Duration timeout, Sink sink) {
    if (filters.get(0).contains("#/")) throw new IllegalArgumentException("Invalid usage of multi-level wildcard in topic string: " + filters.get(0));
    askedFilters = filters;
    askedWindow = window;
    messages.forEach(sink::message);
    return new Window(START, START + window.toMillis());
  }
}
