package io.topicscope.mqtt;

import io.topicscope.config.TopicScopeProperties.Source;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttException;
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

  /** @param version the broker's own version string, or null when it does not announce one or does not let this account read it */
  public record BrokerInfo(String version) {}

  /**
   * @throws MqttException when the broker does not accept the connection within the timeout
   */
  public BrokerInfo connect(Source source, Duration timeout) throws MqttException {
    MqttClient client = MqttClients.client(source, timeout);
    try {
      client.connect(MqttClients.options(source, timeout));
      CompletableFuture<String> version = new CompletableFuture<>();
      try {
        client.subscribe(VERSION_TOPIC, 0, (topic, message) -> version.complete(new String(message.getPayload(), StandardCharsets.UTF_8)));
      } catch (MqttException e) {
        // The broker took the connection and refuses this one topic to the account (EMQX does by default). It is reachable.
        if (e.getReasonCode() != MqttException.REASON_CODE_SUBSCRIBE_FAILED) throw e;
        return new BrokerInfo(null);
      }
      return new BrokerInfo(await(version));
    } finally {
      MqttClients.close(client);
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
