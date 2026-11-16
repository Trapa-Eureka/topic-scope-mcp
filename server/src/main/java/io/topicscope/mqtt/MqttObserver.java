package io.topicscope.mqtt;

import io.topicscope.config.TopicScopeProperties.Source;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import org.eclipse.paho.client.mqttv3.IMqttMessageListener;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttTopic;
import org.springframework.stereotype.Component;

/**
 * Listens to an MQTT broker for a while. That is the only way to learn anything about one: a broker keeps
 * no list of topics and no history, only the retained message of a topic if it has one.
 *
 * <p>Leaves nothing behind: a random client id, a clean session (the broker forgets the subscription when
 * the connection closes), no last-will message, and it only subscribes.
 */
@Component
public class MqttObserver {

  /**
   * One message as it arrived.
   *
   * @param qos the quality of service it was published with (0, 1 or 2)
   * @param retained true when the broker had it stored and sent it on subscribing; false for a message published while listening
   * @param receivedAtMillis when it arrived here, epoch milliseconds
   */
  public record Seen(String topic, byte[] payload, int qos, boolean retained, long receivedAtMillis) {}

  /** When the listening started and ended, epoch milliseconds. */
  public record Window(long startedAtMillis, long endedAtMillis) {
    public double seconds() {
      return Math.round((endedAtMillis - startedAtMillis) / 100.0) / 10.0;
    }
  }

  /** Receives the messages, one at a time and never two at once. */
  public interface Sink {
    void message(Seen seen);
  }

  /**
   * Subscribes to the filters, hands every message to the sink for the length of the window, disconnects.
   *
   * @param filters MQTT topic filters; {@code +} stands for one level, {@code #} for all remaining levels
   * @throws IllegalArgumentException when a filter is not a valid MQTT topic filter
   * @throws Exception when the broker does not accept the connection or the subscription
   */
  public Window observe(Source source, List<String> filters, Duration window, Duration timeout, Sink sink) throws Exception {
    for (String filter : filters) MqttTopic.validate(filter, true);

    MqttClient client = MqttClients.client(source, timeout);

    AtomicBoolean listening = new AtomicBoolean(true);
    Object oneAtATime = new Object();
    IMqttMessageListener listener = (topic, message) -> {
      long now = System.currentTimeMillis();
      synchronized (oneAtATime) {
        if (listening.get()) sink.message(new Seen(topic, message.getPayload(), message.getQos(), message.isRetained(), now));
      }
    };

    try {
      client.connect(MqttClients.options(source, timeout));
      String[] topics = filters.toArray(String[]::new);
      int[] qos = new int[topics.length];
      IMqttMessageListener[] listeners = new IMqttMessageListener[topics.length];
      for (int i = 0; i < topics.length; i++) {
        // The highest level, so each message arrives with the quality of service it was published with.
        qos[i] = 2;
        listeners[i] = listener;
      }
      long started = System.currentTimeMillis();
      client.subscribe(topics, qos, listeners);
      Thread.sleep(window.toMillis());
      synchronized (oneAtATime) {
        listening.set(false);
      }
      return new Window(started, System.currentTimeMillis());
    } finally {
      listening.set(false);
      MqttClients.close(client);
    }
  }
}
