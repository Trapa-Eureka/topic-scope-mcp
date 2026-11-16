package io.topicscope.board;

import java.time.Duration;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Settings of the web status board, from {@code topic-scope.board.*}. The board only exists in board mode
 * (the {@code board} profile); in MCP mode none of this is used.
 *
 * @param enabled set by the {@code board} profile
 * @param interval how often the brokers are looked at and a new picture is sent to the open pages (default 5 s)
 * @param mqttListenSeconds how long of each interval an MQTT source is listened to (default 2 s)
 * @param expectedIntervalSeconds an MQTT topic that was heard before and has been quiet for more than one and a
 *     half times this long is shown as stale (default 5 s)
 * @param staticFiles where the built page is served from (default {@code file:board/dist/})
 * @param devOrigin an extra page origin that may open the WebSocket, for the front-end development server
 */
@ConfigurationProperties("topic-scope.board")
public record BoardProperties(Boolean enabled, Duration interval, Integer mqttListenSeconds, Integer expectedIntervalSeconds, String staticFiles, String devOrigin) {

  public BoardProperties {
    enabled = Boolean.TRUE.equals(enabled);
    interval = interval == null ? Duration.ofSeconds(5) : interval;
    mqttListenSeconds = mqttListenSeconds == null ? 2 : mqttListenSeconds;
    expectedIntervalSeconds = expectedIntervalSeconds == null ? 5 : expectedIntervalSeconds;
    staticFiles = staticFiles == null || staticFiles.isBlank() ? "file:board/dist/" : staticFiles.trim();
    devOrigin = devOrigin == null ? "" : devOrigin.trim();
    if (interval.toMillis() < 1000) throw new IllegalArgumentException("topic-scope.board.interval must be at least 1 s");
    if (mqttListenSeconds < 1 || mqttListenSeconds * 1000L >= interval.toMillis()) {
      throw new IllegalArgumentException("topic-scope.board.mqtt-listen-seconds must be at least 1 and shorter than the interval");
    }
  }
}
