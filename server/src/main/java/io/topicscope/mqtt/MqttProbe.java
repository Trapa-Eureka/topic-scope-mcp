package io.topicscope.mqtt;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
import org.springframework.stereotype.Component;

/**
 * Checks that an MQTT broker accepts a connection, and reads its version if it announces one.
 *
 * <p>Leaves nothing behind: a random client id, a clean session, no last-will message, and it only subscribes.
 */
@Component
public class MqttProbe {

  /** Brokers such as Mosquitto keep their version as a retained message here. Not every broker does. */
  static final String VERSION_TOPIC = "$SYS/broker/version";

  /** How long to wait for the version after connecting. It is retained, so it arrives at once or not at all. */
  private static final Duration VERSION_WAIT = Duration.ofMillis(300);

  /** @param version the broker's own version string, or null when it does not announce one */
  public record BrokerInfo(String version) {}

  /**
   * @throws MqttException when the broker does not accept the connection within the timeout
   */
  public BrokerInfo connect(String uri, Duration timeout) throws MqttException {
    MqttClient client = new MqttClient(uri, "topic-scope-" + UUID.randomUUID(), new MemoryPersistence());
    MqttConnectOptions options = new MqttConnectOptions();
    options.setCleanSession(true);
    options.setAutomaticReconnect(false);
    options.setConnectionTimeout((int) Math.max(1, timeout.toSeconds()));
    client.setTimeToWait(timeout.toMillis());
    try {
      client.connect(options);
      CompletableFuture<String> version = new CompletableFuture<>();
      client.subscribe(VERSION_TOPIC, 0, (topic, message) -> version.complete(new String(message.getPayload(), StandardCharsets.UTF_8)));
      return new BrokerInfo(await(version));
    } finally {
      try {
        if (client.isConnected()) client.disconnect(1000);
      } finally {
        client.close(true);
      }
    }
  }

  private static String await(CompletableFuture<String> version) {
    try {
      return version.get(VERSION_WAIT.toMillis(), TimeUnit.MILLISECONDS);
    } catch (TimeoutException e) {
      return null;
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      return null;
    } catch (Exception e) {
      return null;
    }
  }
}
