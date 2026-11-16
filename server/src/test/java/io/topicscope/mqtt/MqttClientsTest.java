package io.topicscope.mqtt;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.topicscope.config.TopicScopeProperties;
import io.topicscope.config.TopicScopeProperties.Source;
import io.topicscope.config.TopicScopeProperties.SourceType;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** How an MQTT source's login and trust store become connection options, and what a source may not bring. */
class MqttClientsTest {

  @TempDir
  Path folder;

  static Source mqtt(String address, Map<String, String> properties) {
    return new Source("m", SourceType.MQTT, address, "viewer", "viewer-pass", properties, null);
  }

  @Test
  void withoutATrustStoreJavasOwnIsUsed() {
    MqttConnectOptions options = MqttClients.options(mqtt("ssl://broker:8883", null), Duration.ofSeconds(5));
    assertThat(options.getSocketFactory()).isNull();
    assertThat(options.getUserName()).isEqualTo("viewer");
    assertThat(options.isCleanSession()).isTrue();
    assertThat(options.isAutomaticReconnect()).isFalse();
    assertThat(options.getWillMessage()).as("no last-will message").isNull();
  }

  @Test
  void aTrustStoreOfTheSourceIsUsedForItsConnections() throws Exception {
    Path store = folder.resolve("trust.p12");
    KeyStore empty = KeyStore.getInstance("PKCS12");
    empty.load(null, null);
    try (var out = Files.newOutputStream(store)) {
      empty.store(out, "store-pass".toCharArray());
    }
    MqttConnectOptions options = MqttClients.options(
        mqtt("ssl://broker:8883", Map.of("ssl.truststore.location", store.toString(), "ssl.truststore.password", "store-pass")), Duration.ofSeconds(5));
    assertThat(options.getSocketFactory()).isNotNull();
    assertThat(options.isHttpsHostnameVerificationEnabled()).as("the server's name is still checked against its certificate").isTrue();
  }

  @Test
  void aTrustStoreThatCannotBeReadStopsTheServerAndNamesTheFileNotThePassword() throws Exception {
    Path notAStore = Files.writeString(folder.resolve("broken.p12"), "this is not a key store");
    Source broken = mqtt("ssl://broker:8883", Map.of("ssl.truststore.location", notAStore.toString(), "ssl.truststore.password", "secret-store-pass"));
    assertThatThrownBy(() -> new MqttSettingsCheck(new TopicScopeProperties(List.of(broken), null, null, null, null)))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Source 'm': the trust store " + notAStore).hasMessageNotContaining("secret-store-pass");

    Source missing = mqtt("ssl://broker:8883", Map.of("ssl.truststore.location", folder.resolve("nowhere.p12").toString()));
    assertThatThrownBy(() -> MqttClients.trust(missing)).isInstanceOf(IllegalArgumentException.class).hasMessageContaining("could not be read");
  }

  @Test
  void anMqttSourceCannotBringOtherSettings() {
    for (String setting : List.of("ssl.keystore.location", "security.protocol", "sasl.mechanism", "will.topic", "clientId")) {
      assertThatThrownBy(() -> MqttClients.trust(mqtt("tcp://broker:1883", Map.of(setting, "x"))))
          .as(setting).isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("Source 'm' sets the MQTT property '" + setting + "'");
    }
  }
}
