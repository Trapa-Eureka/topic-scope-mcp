package io.topicscope.config;

import java.time.Duration;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * The brokers the server may look at, from {@code topic-scope.*} in the configuration.
 *
 * @param sources the configured brokers; names must be unique
 * @param probeTimeout how long a reachability check may take before the source counts as not answering
 * @param requestTimeout how long any other question to a broker may take before the tool gives up
 */
@ConfigurationProperties("topic-scope")
public record TopicScopeProperties(List<Source> sources, Duration probeTimeout, Duration requestTimeout) {

  public enum SourceType { KAFKA, MQTT }

  /**
   * One broker.
   *
   * @param name the name tools use to refer to it, for example {@code local-kafka}
   * @param type which kind of broker it is
   * @param address Kafka: bootstrap servers ({@code host:port[,host:port]}). MQTT: a URI ({@code tcp://host:port})
   */
  public record Source(String name, SourceType type, String address) {
    public Source {
      if (name == null || name.isBlank()) throw new IllegalArgumentException("A source needs a name");
      if (type == null) throw new IllegalArgumentException("Source '" + name + "' needs a type: kafka or mqtt");
      if (address == null || address.isBlank()) throw new IllegalArgumentException("Source '" + name + "' needs an address");
    }
  }

  public TopicScopeProperties {
    sources = sources == null ? List.of() : List.copyOf(sources);
    probeTimeout = probeTimeout == null ? Duration.ofSeconds(5) : probeTimeout;
    requestTimeout = requestTimeout == null ? Duration.ofSeconds(10) : requestTimeout;
    Set<String> seen = new HashSet<>();
    for (Source s : sources) {
      if (!seen.add(s.name())) throw new IllegalArgumentException("Two sources are named '" + s.name() + "'");
    }
  }

  public Optional<Source> source(String name) {
    return sources.stream().filter(s -> s.name().equals(name)).findFirst();
  }
}
