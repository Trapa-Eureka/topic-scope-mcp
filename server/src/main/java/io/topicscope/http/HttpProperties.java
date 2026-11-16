package io.topicscope.http;

import java.util.List;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Settings of HTTP mode, from {@code topic-scope.http.*}. Only used with the {@code http} profile.
 *
 * @param enabled set by the {@code http} profile
 * @param token the shared secret every request must carry as {@code Authorization: Bearer <token>}
 * @param allowedOrigins web page origins that may call the endpoint from a browser; none by default
 */
@ConfigurationProperties("topic-scope.http")
public record HttpProperties(Boolean enabled, String token, List<String> allowedOrigins) {

  /** Shorter than this and a token can be guessed by trying. */
  static final int MIN_TOKEN_CHARS = 24;

  public HttpProperties {
    enabled = Boolean.TRUE.equals(enabled);
    token = token == null ? "" : token.trim();
    allowedOrigins = allowedOrigins == null ? List.of() : allowedOrigins.stream().filter(o -> o != null && !o.isBlank()).map(String::trim).toList();
    if (enabled && token.length() < MIN_TOKEN_CHARS) {
      throw new IllegalArgumentException("HTTP mode needs a token of at least " + MIN_TOKEN_CHARS + " characters in TOPIC_SCOPE_HTTP_TOKEN "
          + "(for example the output of: openssl rand -hex 24). Without one, anybody who can reach the port could read the brokers.");
    }
  }

  /** Never the token, whatever prints this. */
  @Override
  public String toString() {
    return "HttpProperties[enabled=" + enabled + ", allowedOrigins=" + allowedOrigins + "]";
  }
}
