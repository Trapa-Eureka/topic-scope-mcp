package io.topicscope.kafka;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.topicscope.config.TopicScopeProperties;
import io.topicscope.config.TopicScopeProperties.Source;
import io.topicscope.config.TopicScopeProperties.SourceType;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.junit.jupiter.api.Test;

/** How a source's login and extra settings become Kafka client settings, and which settings a source may not bring. */
class KafkaClientsTest {

  static Source kafka(String username, String password, Map<String, String> properties) {
    return new Source("k", SourceType.KAFKA, "broker:9092", username, password, properties, null);
  }

  @Test
  void withoutALoginOnlyTheAddressIsSet() {
    Properties props = KafkaClients.connection(Source.open("k", SourceType.KAFKA, "broker:9092"));
    assertThat(props).containsOnlyKeys("bootstrap.servers").containsEntry("bootstrap.servers", "broker:9092");
  }

  @Test
  void aLoginBecomesSaslPlainByDefault() {
    Properties props = KafkaClients.connection(kafka("viewer", "p\"a\\ss", null));
    assertThat(props).containsEntry("security.protocol", "SASL_PLAINTEXT").containsEntry("sasl.mechanism", "PLAIN");
    assertThat(props.getProperty("sasl.jaas.config"))
        .isEqualTo("org.apache.kafka.common.security.plain.PlainLoginModule required username=\"viewer\" password=\"p\\\"a\\\\ss\";");
  }

  @Test
  void encryptionAndAnotherMechanismComeFromTheSourceProperties() {
    Properties props = KafkaClients.connection(kafka("viewer", "pw", Map.of(
        "security.protocol", "SASL_SSL", "sasl.mechanism", "SCRAM-SHA-512", "ssl.truststore.location", "/etc/trust.jks")));
    assertThat(props).containsEntry("security.protocol", "SASL_SSL").containsEntry("sasl.mechanism", "SCRAM-SHA-512")
        .containsEntry("ssl.truststore.location", "/etc/trust.jks");
    assertThat(props.getProperty("sasl.jaas.config")).startsWith("org.apache.kafka.common.security.scram.ScramLoginModule required username=\"viewer\"");
  }

  @Test
  void aSourceCannotBringSettingsThatAreNotAboutEncryptionOrLogin() {
    // Each of these would change what the look-only consumer does. Named here as plain text on purpose.
    for (String setting : List.of("group" + ".id", "enable.auto.commit", "allow.auto.create.topics", "auto.offset.reset", "bootstrap.servers",
        "client.id", "interceptor.classes", "sasl", "sslx.y", "security.protocols")) {
      assertThatThrownBy(() -> KafkaClients.connection(kafka(null, null, Map.of(setting, "x"))))
          .as(setting).isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("Source 'k' sets the Kafka client property '" + setting + "'");
    }
    assertThatThrownBy(() -> new KafkaSettingsCheck(new TopicScopeProperties(List.of(kafka(null, null, Map.of("enable.auto.commit", "true"))), null, null, null, null)))
        .as("the server does not start").isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void theServersOwnSettingsWinOverTheSource() {
    // The address is put after the source's settings, and so is everything admin() and consumer() set.
    Properties props = KafkaClients.connection(kafka("u", "p", Map.of("sasl.mechanism", "PLAIN")));
    assertThat(props.getProperty("bootstrap.servers")).isEqualTo("broker:9092");
  }

  @Test
  void aSourceNeverPrintsItsPassword() {
    Source source = kafka("viewer", "hunter2-secret", null);
    assertThat(source.toString()).contains("viewer").doesNotContain("hunter2");
    assertThatThrownBy(() -> new Source("k", SourceType.KAFKA, "b:1", "", "orphan-password", null, null))
        .hasMessageContaining("has a password and no username").hasMessageNotContaining("orphan-password");
  }
}
