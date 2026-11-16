package io.topicscope.board;

import io.topicscope.board.BoardSnapshots.Snapshot;
import io.topicscope.board.BoardSnapshots.WatchView;
import io.topicscope.tools.AuditLog;
import io.topicscope.tools.ToolProblem;
import jakarta.annotation.PreDestroy;
import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;
import org.springframework.web.socket.CloseStatus;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketSession;
import org.springframework.web.socket.handler.ConcurrentWebSocketSessionDecorator;
import org.springframework.web.socket.handler.TextWebSocketHandler;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

/**
 * The WebSocket the board's pages hang on. Every interval one picture of the brokers is taken and pushed to
 * every open page; nothing is taken while no page is open. A page can only say one thing: which topic's
 * newest messages it wants to watch.
 *
 * <p>Messages to the page: {@code {"type":"snapshot", ...}}, and with it {@code "watch"} when the page watches a topic.
 * Messages from the page: {@code {"type":"watch","source":"...","topic":"..."}} and {@code {"type":"unwatch"}}.
 */
@Component
@ConditionalOnProperty(name = "topic-scope.board.enabled", havingValue = "true")
public class BoardSocket extends TextWebSocketHandler {

  private static final Logger log = LoggerFactory.getLogger(BoardSocket.class);
  private static final JsonMapper JSON = JsonMapper.builder().build();

  /** A page that does not read for this long, or falls this far behind, is dropped rather than held in memory. */
  private static final int SEND_TIME_LIMIT_MILLIS = 10_000;
  private static final int SEND_BUFFER_BYTES = 2 * 1024 * 1024;

  private record Watch(String source, String topic) {}

  private final BoardSnapshots snapshots;
  private final AuditLog audit;
  private final Map<String, WebSocketSession> pages = new ConcurrentHashMap<>();
  private final Map<String, Watch> watches = new ConcurrentHashMap<>();
  private final ScheduledExecutorService ticker;
  private volatile Snapshot latest;

  public BoardSocket(BoardSnapshots snapshots, BoardProperties board, AuditLog audit) {
    this.snapshots = snapshots;
    this.audit = audit;
    // The thread is made here, on the application's own thread, so it sees the classes inside the packaged jar.
    ClassLoader loader = Thread.currentThread().getContextClassLoader();
    this.ticker = Executors.newSingleThreadScheduledExecutor(task -> {
      Thread t = new Thread(task, "topic-scope-board");
      t.setDaemon(true);
      t.setContextClassLoader(loader);
      return t;
    });
    // At a fixed rate: the interval is the time from one look to the next, not the pause after a look that itself takes seconds.
    ticker.scheduleAtFixedRate(this::tick, 0, board.interval().toMillis(), TimeUnit.MILLISECONDS);
  }

  @PreDestroy
  void stop() {
    ticker.shutdownNow();
  }

  private void tick() {
    if (pages.isEmpty()) return; // nobody is looking: leave the brokers alone
    try {
      Snapshot snapshot = snapshots.take();
      latest = snapshot;
      Map<Watch, WatchView> watched = new HashMap<>(); // two pages watching the same topic cost one read
      for (WebSocketSession page : pages.values()) {
        Watch watch = watches.get(page.getId());
        send(page, snapshot, watch == null ? null : watched.computeIfAbsent(watch, w -> snapshots.watch(w.source(), w.topic())));
      }
    } catch (RuntimeException | LinkageError e) {
      log.error("The board could not take a picture of the brokers", e);
    }
  }

  @Override
  public void afterConnectionEstablished(WebSocketSession session) {
    WebSocketSession page = new ConcurrentWebSocketSessionDecorator(session, SEND_TIME_LIMIT_MILLIS, SEND_BUFFER_BYTES);
    try {
      audit.record("board_open", AuditLog.args("page", session.getId()), () -> "");
    } catch (ToolProblem e) {
      close(session, e.problem());
      return;
    }
    pages.put(session.getId(), page);
    Snapshot known = latest;
    if (known != null) send(page, known, null); // something to show at once; the next tick brings a fresh one
    ticker.execute(this::tick);
  }

  @Override
  protected void handleTextMessage(WebSocketSession session, TextMessage message) {
    WebSocketSession page = pages.get(session.getId());
    if (page == null) return;
    JsonNode request;
    try {
      request = JSON.readTree(message.getPayload());
    } catch (RuntimeException e) {
      return; // not something a board page sends
    }
    String type = request.path("type").asString("");
    if (type.equals("unwatch")) {
      watches.remove(session.getId());
    } else if (type.equals("watch") && request.path("source").isString() && request.path("topic").isString()) {
      Watch watch = new Watch(request.path("source").stringValue(), request.path("topic").stringValue());
      watches.put(session.getId(), watch);
      // Looking at message content is what is worth a line in the audit log; the regular picture holds none.
      WatchView view;
      try {
        view = audit.record("board_watch", AuditLog.args("source", watch.source(), "topic", watch.topic(), "page", session.getId()),
            () -> snapshots.watch(watch.source(), watch.topic()));
      } catch (ToolProblem e) {
        view = new WatchView(watch.source(), watch.topic(), java.util.List.of(), "", e.text());
      }
      Snapshot known = latest;
      if (known != null) send(page, known, view);
    }
  }

  @Override
  public void afterConnectionClosed(WebSocketSession session, CloseStatus status) {
    pages.remove(session.getId());
    watches.remove(session.getId());
  }

  private void send(WebSocketSession page, Snapshot snapshot, WatchView watch) {
    Map<String, Object> message = new LinkedHashMap<>();
    message.put("type", "snapshot");
    message.put("snapshot", snapshot);
    message.put("watch", watch);
    try {
      page.sendMessage(new TextMessage(JSON.writeValueAsString(message)));
    } catch (IOException | RuntimeException e) {
      pages.remove(page.getId());
      watches.remove(page.getId());
      close(page, "The page did not keep up.");
    }
  }

  private static void close(WebSocketSession session, String reason) {
    try {
      session.close(CloseStatus.POLICY_VIOLATION.withReason(reason.length() > 120 ? reason.substring(0, 120) : reason));
    } catch (IOException | RuntimeException e) {
      // already gone
    }
  }
}
