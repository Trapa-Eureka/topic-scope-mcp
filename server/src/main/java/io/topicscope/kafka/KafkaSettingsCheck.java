package io.topicscope.kafka;

import io.topicscope.config.TopicScopeProperties;
import io.topicscope.config.TopicScopeProperties.Source;
import io.topicscope.config.TopicScopeProperties.SourceType;
import org.springframework.stereotype.Component;

/**
 * Refuses to start the server when a Kafka source brings a client setting that is not about encryption or
 * login. Better a server that does not start than one whose consumer was quietly reconfigured.
 */
@Component
public class KafkaSettingsCheck {

  public KafkaSettingsCheck(TopicScopeProperties properties) {
    for (Source source : properties.sources()) {
      if (source.type() == SourceType.KAFKA) KafkaClients.connection(source);
    }
  }
}
