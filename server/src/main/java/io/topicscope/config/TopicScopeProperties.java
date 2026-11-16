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
 * @param limits how much message content one tool call may return
 */
@ConfigurationProperties("topic-scope")
public record TopicScopeProperties(List<Source> sources, Duration probeTimeout, Duration requestTimeout, Limits limits) {

  /**
   * Hard caps on message content. A caller cannot raise them; only the configuration can.
   *
   * @param maxMessages the most messages one call returns (default 100)
   * @param maxValueChars the most characters shown of one message value (default 2000)
   * @param maxTotalChars the most characters of message values in one answer (default 40000); once it is
   *     used up, the remaining values are cut to a short preview
   */
  public record Limits(Integer maxMessages, Integer maxValueChars, Integer maxTotalChars) {
    public Limits {
      maxMessages = positive(maxMessages, 100, "max-messages");
      maxValueChars = positive(maxValueChars, 2000, "max-value-chars");
      maxTotalChars = positive(maxTotalChars, 40_000, "max-total-chars");
    }

    public static Limits defaults() {
      return new Limits(null, null, null);
    }

    private static int positive(Integer value, int fallback, String name) {
      if (value == null) return fallback;
      if (value < 1) throw new IllegalArgumentException("topic-scope.limits." + name + " must be at least 1");
      return value;
    }
  }

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
    limits = limits == null ? Limits.defaults() : limits;
    Set<String> seen = new HashSet<>();
    for (Source s : sources) {
      if (!seen.add(s.name())) throw new IllegalArgumentException("Two sources are named '" + s.name() + "'");
    }
  }

  public Optional<Source> source(String name) {
    return sources.stream().filter(s -> s.name().equals(name)).findFirst();
  }
}
