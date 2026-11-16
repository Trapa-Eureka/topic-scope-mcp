package io.topicscope.mqtt;

import io.topicscope.config.TopicScopeProperties.Source;
import java.time.Duration;
import java.util.UUID;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;

/**
 * Builds the throwaway MQTT connections the server uses. Each one leaves nothing behind: a random client
 * id, a clean session (the broker forgets it on disconnect), no last-will message, no automatic reconnect.
 */
final class MqttClients {

  private MqttClients() {}

  /** A client that is not connected yet. The caller connects with {@link #options}, and closes it. */
  static MqttClient client(Source source, Duration timeout) throws MqttException {
    MqttClient client = new MqttClient(source.address(), "topic-scope-" + UUID.randomUUID(), new MemoryPersistence());
    client.setTimeToWait(timeout.toMillis());
    return client;
  }

  static MqttConnectOptions options(Source source, Duration timeout) {
    MqttConnectOptions options = new MqttConnectOptions();
    options.setCleanSession(true);
    options.setAutomaticReconnect(false);
    options.setConnectionTimeout((int) Math.max(1, timeout.toSeconds()));
    if (source.hasLogin()) {
      options.setUserName(source.username());
      options.setPassword(source.password().toCharArray());
    }
    return options;
  }

  /** Disconnects if connected and frees the client. Never throws: the answer is already made. */
  static void close(MqttClient client) {
    try {
      if (client.isConnected()) client.disconnect(1000);
    } catch (MqttException e) {
      // the broker is gone or slow; nothing to undo on our side
    } finally {
      try {
        client.close(true);
      } catch (MqttException e) {
        // already closed
      }
    }
  }
}
