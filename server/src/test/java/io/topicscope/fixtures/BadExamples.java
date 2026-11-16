package io.topicscope.fixtures;

import io.topicscope.sim.DemoData;
import java.util.List;
import java.util.Map;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttMessage;

/**
 * Code the server must never contain. Nothing here is ever run: {@code ReadOnlyArchitectureTest} reads the
 * compiled classes to prove that each of its rules fails on the kind of code it exists to stop.
 */
final class BadExamples {

  private BadExamples() {}

  static final class BadKafkaPublisher {
    void write(KafkaProducer<String, String> producer) {
      producer.send(new ProducerRecord<>("topic", "value"));
    }
  }

  static final class BadGroupConsumer {
    void read(KafkaConsumer<String, String> consumer) {
      consumer.subscribe(List.of("topic"));
      consumer.commitSync();
    }
  }

  static final class BadAdmin {
    void change(Admin admin) {
      admin.deleteTopics(List.of("topic"));
      admin.alterConsumerGroupOffsets("group", Map.of());
    }
  }

  static final class BadMqttPublisher {
    void write(MqttClient client, MqttConnectOptions options) throws Exception {
      options.setWill("topic", new byte[0], 0, false);
      client.publish("topic", new MqttMessage(new byte[0]));
    }
  }

  static final class BadSimulatorUser {
    String topic() {
      return DemoData.POSITIONS.name();
    }
  }
}
