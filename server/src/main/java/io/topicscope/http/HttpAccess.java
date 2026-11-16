package io.topicscope.http;

import jakarta.servlet.Filter;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Who may use the server in HTTP mode: whoever has the token.
 *
 * <ul>
 *   <li>Every request needs {@code Authorization: Bearer <token>}. Without it, or with a wrong one: 401.</li>
 *   <li>A request that says it comes from a web page (it has an {@code Origin} header) is refused with 403
 *       unless that origin is on the allow list, which is empty by default. MCP clients are programs and send
 *       no Origin; a page in somebody's browser does, and it should not be talking to this server.</li>
 * </ul>
 *
 * This is a shared secret, not user accounts: everybody with the token sees the same, and the audit log
 * cannot tell them apart. Encryption is not done here either; put TLS in front when the server leaves the machine.
 */
@Configuration
@ConditionalOnProperty(name = "topic-scope.http.enabled", havingValue = "true")
public class HttpAccess {

  private final byte[] expected;
  private final HttpProperties properties;

  public HttpAccess(HttpProperties properties) {
    this.properties = properties;
    this.expected = ("Bearer " + properties.token()).getBytes(StandardCharsets.UTF_8);
  }

  @Bean
  Filter tokenRequired() {
    return (request, response, chain) -> {
      HttpServletRequest http = (HttpServletRequest) request;
      HttpServletResponse answer = (HttpServletResponse) response;
      answer.setHeader("Cache-Control", "no-store");
      answer.setHeader("X-Content-Type-Options", "nosniff");

      String origin = http.getHeader("Origin");
      if (origin != null && !properties.allowedOrigins().contains(origin)) {
        answer.sendError(HttpServletResponse.SC_FORBIDDEN, "Requests from web pages are not accepted.");
        return;
      }
      String given = http.getHeader("Authorization");
      // Compared in constant time, so the answer's timing tells nothing about how much of a guess was right.
      if (given == null || !MessageDigest.isEqual(expected, given.getBytes(StandardCharsets.UTF_8))) {
        answer.setHeader("WWW-Authenticate", "Bearer");
        answer.sendError(HttpServletResponse.SC_UNAUTHORIZED, "A valid token is required: Authorization: Bearer <token>.");
        return;
      }
      chain.doFilter(request, response);
    };
  }
}
