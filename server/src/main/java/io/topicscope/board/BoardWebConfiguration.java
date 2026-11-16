package io.topicscope.board;

import jakarta.servlet.Filter;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.servlet.config.annotation.ResourceHandlerRegistry;
import org.springframework.web.servlet.config.annotation.ViewControllerRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;
import org.springframework.web.socket.config.annotation.EnableWebSocket;
import org.springframework.web.socket.config.annotation.WebSocketConfigurer;
import org.springframework.web.socket.config.annotation.WebSocketHandlerRegistry;

/**
 * Serves the board: the built page as static files, and the WebSocket at {@code /ws}.
 *
 * <p>The board shows what is on the brokers, so a web page somewhere else must not be able to read it
 * through the user's browser:
 * <ul>
 *   <li>the server listens on 127.0.0.1 only (application-board.yml);</li>
 *   <li>a request whose Host is not a local name is refused, which stops a foreign name that was pointed at
 *       127.0.0.1 (DNS rebinding);</li>
 *   <li>the WebSocket only accepts pages that came from this server;</li>
 *   <li>the page may not be put inside a frame of another page.</li>
 * </ul>
 * It does not protect against other programs or users on the same machine; they can reach the port.
 */
@Configuration
@EnableWebSocket
@ConditionalOnProperty(name = "topic-scope.board.enabled", havingValue = "true")
public class BoardWebConfiguration implements WebSocketConfigurer, WebMvcConfigurer {

  static final Set<String> LOCAL_HOSTS = Set.of("localhost", "127.0.0.1", "[::1]", "::1");

  private final BoardSocket socket;
  private final BoardProperties board;
  private final int port;

  public BoardWebConfiguration(BoardSocket socket, BoardProperties board, @Value("${server.port:8090}") int port) {
    this.socket = socket;
    this.board = board;
    this.port = port;
  }

  @Override
  public void registerWebSocketHandlers(WebSocketHandlerRegistry registry) {
    List<String> origins = new ArrayList<>(List.of("http://127.0.0.1:" + port, "http://localhost:" + port));
    if (!board.devOrigin().isEmpty()) origins.add(board.devOrigin());
    registry.addHandler(socket, "/ws").setAllowedOrigins(origins.toArray(String[]::new));
  }

  @Override
  public void addResourceHandlers(ResourceHandlerRegistry registry) {
    registry.addResourceHandler("/**").addResourceLocations(board.staticFiles());
  }

  @Override
  public void addViewControllers(ViewControllerRegistry registry) {
    registry.addViewController("/").setViewName("forward:/index.html");
  }

  @Bean
  Filter localOnly() {
    return (request, response, chain) -> {
      HttpServletRequest http = (HttpServletRequest) request;
      HttpServletResponse answer = (HttpServletResponse) response;
      if (!LOCAL_HOSTS.contains(http.getServerName())) {
        answer.sendError(HttpServletResponse.SC_FORBIDDEN, "The board answers on this machine only.");
        return;
      }
      answer.setHeader("X-Frame-Options", "DENY");
      answer.setHeader("Content-Security-Policy", "frame-ancestors 'none'");
      answer.setHeader("X-Content-Type-Options", "nosniff");
      answer.setHeader("Cache-Control", "no-store");
      chain.doFilter(request, response);
    };
  }
}
