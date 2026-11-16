package io.topicscope.sim;

import java.util.Properties;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;

/**
 * Where the two brokers are and how the simulator logs in to them.
 *
 * @param kafkaLogin null for a broker that asks for no login
 * @param mqttLogin null for a broker that asks for no login
 */
public record Stack(String kafkaBootstrap, Login kafkaLogin, String mqttUri, Login mqttLogin) {

  /** A user name and its password. */
  public record Login(String username, String password) {
    /** Null when no user name is given: no login. */
    public static Login of(String username, String password) {
      return username == null || username.isBlank() ? null : new Login(username.trim(), password == null ? "" : password);
    }

    /** Never the password, whatever prints this. */
    @Override
    public String toString() {
      return "Login[" + username + "]";
    }
  }

  /** Brokers that ask for no login, as the tests start them. */
  public static Stack open(String kafkaBootstrap, String mqttUri) {
    return new Stack(kafkaBootstrap, null, mqttUri, null);
  }

  /** The settings every Kafka client of the simulator starts from: the address, and the login when there is one (SASL/PLAIN, not encrypted). */
  Properties kafka() {
    Properties props = new Properties();
    props.put("bootstrap.servers", kafkaBootstrap);
    if (kafkaLogin != null) {
      props.put("security.protocol", "SASL_PLAINTEXT");
      props.put("sasl.mechanism", "PLAIN");
      props.put("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username=\""
          + quoted(kafkaLogin.username()) + "\" password=\"" + quoted(kafkaLogin.password()) + "\";");
    }
    return props;
  }

  /** Connection options for an MQTT client, with the login when there is one. */
  MqttConnectOptions mqtt() {
    MqttConnectOptions options = new MqttConnectOptions();
    options.setCleanSession(true);
    options.setConnectionTimeout(10);
    if (mqttLogin != null) {
      options.setUserName(mqttLogin.username());
      options.setPassword(mqttLogin.password().toCharArray());
    }
    return options;
  }

  private static String quoted(String value) {
    return value.replace("\\", "\\\\").replace("\"", "\\\"");
  }
}
