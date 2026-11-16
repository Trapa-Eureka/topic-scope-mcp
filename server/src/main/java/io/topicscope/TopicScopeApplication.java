package io.topicscope;

import java.io.FilterInputStream;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;
import org.springframework.context.ConfigurableApplicationContext;

/**
 * The MCP server. It speaks MCP over stdin/stdout, so nothing else may write to stdout:
 * the banner is off and all logging goes to stderr (see application.yml and logback-spring.xml).
 */
@SpringBootApplication
@ConfigurationPropertiesScan
public class TopicScopeApplication {

  /** After the client has gone: time for an answer that is being written to get out. */
  private static final long EXIT_GRACE_MILLIS = 500;

  public static void main(String[] args) {
    AtomicReference<ConfigurableApplicationContext> context = new AtomicReference<>();
    // In board mode and in HTTP mode nobody owns this process through stdin; it runs until it is stopped.
    if (!standalone(args)) exitWhenStdinCloses(context);
    context.set(SpringApplication.run(TopicScopeApplication.class, args));
  }

  /** Whether one of the profiles is active that serve over the network instead of stdin/stdout. */
  static boolean standalone(String[] args) {
    String profiles = String.join(",", args) + "," + System.getenv().getOrDefault("SPRING_PROFILES_ACTIVE", "")
        + "," + System.getProperty("spring.profiles.active", "");
    return java.util.Arrays.stream(profiles.split("[,=\\s]+")).anyMatch(p -> p.equals("board") || p.equals("http"));
  }

  /**
   * The client owns this process and ends it by closing stdin. The MCP transport stops reading then, but its
   * worker threads would keep the JVM alive, leaving one orphan server per session. So: watch for the end of
   * stdin and shut down.
   */
  private static void exitWhenStdinCloses(AtomicReference<ConfigurableApplicationContext> context) {
    AtomicBoolean closing = new AtomicBoolean();
    Runnable shutdown = () -> {
      if (closing.getAndSet(true)) return;
      Thread t = new Thread(() -> {
        try {
          Thread.sleep(EXIT_GRACE_MILLIS);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
        ConfigurableApplicationContext ctx = context.get();
        System.exit(ctx == null ? 0 : SpringApplication.exit(ctx));
      }, "topic-scope-exit");
      t.setDaemon(true);
      t.start();
    };

    System.setIn(new FilterInputStream(System.in) {
      @Override
      public int read() throws IOException {
        int b = super.read();
        if (b < 0) shutdown.run();
        return b;
      }

      @Override
      public int read(byte[] buffer, int off, int len) throws IOException {
        int n = super.read(buffer, off, len);
        if (n < 0) shutdown.run();
        return n;
      }
    });
  }
}
