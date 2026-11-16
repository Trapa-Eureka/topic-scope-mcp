package io.topicscope.config;

import java.time.Duration;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
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
 * @param audit where tool calls are recorded
 */
@ConfigurationProperties("topic-scope")
public record TopicScopeProperties(List<Source> sources, Duration probeTimeout, Duration requestTimeout, Limits limits, Audit audit) {

  /**
   * The record of tool calls.
   *
   * @param enabled whether calls are recorded (default true). While it is on, a call that cannot be recorded is not answered
   * @param file where the record is appended, one JSON object per line (default {@code ~/.topic-scope/audit.jsonl})
   */
  public record Audit(Boolean enabled, String file) {
    public Audit {
      enabled = enabled == null || enabled;
      file = file == null || file.isBlank() ? System.getProperty("user.home") + "/.topic-scope/audit.jsonl" : file.trim();
    }

    public static Audit off() {
      return new Audit(false, null);
    }
  }

  /**
   * Which topics of a source the tools may show. Patterns are matched against the whole topic name and may use
   * {@code *} for any run of characters, for example {@code demo.*} or {@code plant/*}{@code /telemetry}.
   *
   * @param allow when not empty, only topics that match one of these are available
   * @param deny topics that match one of these are never available; deny wins over allow
   */
  public record TopicRules(List<String> allow, List<String> deny) {
    public TopicRules {
      allow = clean(allow);
      deny = clean(deny);
    }

    public static TopicRules everything() {
      return new TopicRules(null, null);
    }

    private static List<String> clean(List<String> patterns) {
      if (patterns == null) return List.of();
      return patterns.stream().filter(p -> p != null && !p.isBlank()).map(String::trim).toList();
    }
  }

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
   * @param address Kafka: bootstrap servers ({@code host:port[,host:port]}). MQTT: a URI ({@code tcp://host:port},
   *     or {@code ssl://host:port} for TLS)
   * @param username the account to log in with; empty for a broker that asks for none
   * @param password its password. It is used to log in and nowhere else: it is never shown, logged or recorded
   * @param properties further settings for encryption and login. Kafka: client settings such as {@code security.protocol},
   *     {@code sasl.mechanism} or {@code ssl.truststore.location}. MQTT: {@code ssl.truststore.location},
   *     {@code ssl.truststore.password} and {@code ssl.truststore.type}, for an {@code ssl://} address whose
   *     certificate Java does not trust by itself. Only settings of those kinds are accepted
   * @param topics which topics of this source the tools may show; everything when left out
   */
  public record Source(String name, SourceType type, String address, String username, String password, Map<String, String> properties,
                       TopicRules topics) {
    public Source {
      if (name == null || name.isBlank()) throw new IllegalArgumentException("A source needs a name");
      if (type == null) throw new IllegalArgumentException("Source '" + name + "' needs a type: kafka or mqtt");
      if (address == null || address.isBlank()) throw new IllegalArgumentException("Source '" + name + "' needs an address");
      username = username == null ? "" : username.trim();
      password = password == null ? "" : password;
      properties = properties == null ? Map.of() : Map.copyOf(properties);
      topics = topics == null ? TopicRules.everything() : topics;
      if (!password.isEmpty() && username.isEmpty()) throw new IllegalArgumentException("Source '" + name + "' has a password and no username");
    }

    /**
     * A source without a login and without topic rules. A factory and not a second constructor: with two
     * constructors Spring Boot no longer knows which one to bind the configuration to, and the server does not start.
     */
    public static Source open(String name, SourceType type, String address) {
      return new Source(name, type, address, null, null, null, null);
    }

    public boolean hasLogin() {
      return !username.isEmpty();
    }

    /** Never the password, whatever prints this. */
    @Override
    public String toString() {
      return "Source[" + name + ", " + type + ", " + address + (hasLogin() ? ", user " + username : "") + "]";
    }
  }

  public TopicScopeProperties {
    sources = sources == null ? List.of() : List.copyOf(sources);
    probeTimeout = probeTimeout == null ? Duration.ofSeconds(5) : probeTimeout;
    requestTimeout = requestTimeout == null ? Duration.ofSeconds(10) : requestTimeout;
    limits = limits == null ? Limits.defaults() : limits;
    audit = audit == null ? new Audit(null, null) : audit;
    Set<String> seen = new HashSet<>();
    for (Source s : sources) {
      if (!seen.add(s.name())) throw new IllegalArgumentException("Two sources are named '" + s.name() + "'");
    }
  }

  public Optional<Source> source(String name) {
    return sources.stream().filter(s -> s.name().equals(name)).findFirst();
  }
}
