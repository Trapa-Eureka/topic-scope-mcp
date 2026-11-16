package io.topicscope.mqtt;

import io.topicscope.config.TopicScopeProperties;
import io.topicscope.config.TopicScopeProperties.Source;
import io.topicscope.config.TopicScopeProperties.SourceType;
import org.springframework.stereotype.Component;

/**
 * Refuses to start the server when an MQTT source brings a property it may not bring, or names a trust store
 * that cannot be read. Better at start-up, with the file named, than at the first tool call.
 */
@Component
public class MqttSettingsCheck {

  public MqttSettingsCheck(TopicScopeProperties properties) {
    for (Source source : properties.sources()) {
      if (source.type() == SourceType.MQTT) MqttClients.trust(source);
    }
  }
}
