package io.topicscope.mqtt;

import io.topicscope.config.TopicScopeProperties.Source;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.TrustManagerFactory;
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

  /** The settings an MQTT source may bring: which certificates to trust for an {@code ssl://} address. Nothing else. */
  static final List<String> ALLOWED_SETTINGS = List.of("ssl.truststore.location", "ssl.truststore.password", "ssl.truststore.type");

  static MqttConnectOptions options(Source source, Duration timeout) {
    MqttConnectOptions options = new MqttConnectOptions();
    options.setCleanSession(true);
    options.setAutomaticReconnect(false);
    options.setConnectionTimeout((int) Math.max(1, timeout.toSeconds()));
    if (source.hasLogin()) {
      options.setUserName(source.username());
      options.setPassword(source.password().toCharArray());
    }
    SSLSocketFactory trust = trust(source);
    if (trust != null) options.setSocketFactory(trust);
    return options;
  }

  /**
   * The certificates to trust for this source, from its {@code ssl.truststore.*} properties, or null when it has
   * none and Java's own trust store is used. Checking the server's name against its certificate stays on.
   *
   * @throws IllegalArgumentException when a property is not one of the allowed ones, or the trust store cannot be read
   */
  static SSLSocketFactory trust(Source source) {
    for (String setting : source.properties().keySet()) {
      if (!ALLOWED_SETTINGS.contains(setting)) {
        throw new IllegalArgumentException("Source '" + source.name() + "' sets the MQTT property '" + setting + "'. "
            + "Only these are accepted: " + String.join(", ", ALLOWED_SETTINGS));
      }
    }
    String location = source.properties().get("ssl.truststore.location");
    if (location == null || location.isBlank()) return null;
    String password = source.properties().getOrDefault("ssl.truststore.password", "");
    String type = source.properties().getOrDefault("ssl.truststore.type", "PKCS12");
    try (InputStream in = Files.newInputStream(Path.of(location.trim()))) {
      KeyStore store = KeyStore.getInstance(type);
      store.load(in, password.toCharArray());
      TrustManagerFactory trusted = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
      trusted.init(store);
      SSLContext context = SSLContext.getInstance("TLS");
      context.init(null, trusted.getTrustManagers(), null);
      return context.getSocketFactory();
    } catch (IOException | GeneralSecurityException e) {
      // The message names the file, never the password.
      throw new IllegalArgumentException("Source '" + source.name() + "': the trust store " + location + " could not be read as " + type
          + " (" + e.getClass().getSimpleName() + "). Check ssl.truststore.location, ssl.truststore.password and ssl.truststore.type.");
    }
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
