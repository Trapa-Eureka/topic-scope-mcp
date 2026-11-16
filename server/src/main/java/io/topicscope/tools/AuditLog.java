package io.topicscope.tools;

import io.topicscope.config.TopicScopeProperties;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.PosixFilePermissions;
import java.time.Clock;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Supplier;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import tools.jackson.databind.json.JsonMapper;

/**
 * The record of what was asked: one line per tool call, appended to a local file.
 *
 * <p>Each line says when, which tool, with which arguments (source, topic, group, limits), how it ended and
 * how long it took. It never holds what a tool returned: no message value, key or header goes in here.
 *
 * <p>While recording is on, a call that cannot be recorded is not answered. In surroundings where it matters
 * who looked at what, an unrecorded look is worse than a refused one.
 */
@Component
public class AuditLog {

  private static final JsonMapper JSON = JsonMapper.builder().build();

  private final boolean enabled;
  private final Path file;
  private final Clock clock;
  private final Object oneWriter = new Object();

  @Autowired
  public AuditLog(TopicScopeProperties properties) {
    this(properties.audit().enabled(), Path.of(properties.audit().file()), Clock.systemUTC());
  }

  AuditLog(boolean enabled, Path file, Clock clock) {
    this.enabled = enabled;
    this.file = file.toAbsolutePath().normalize();
    this.clock = clock;
  }

  /** Builds the argument map of a call from name, value, name, value, ... and leaves out what the caller did not give. */
  public static Map<String, Object> args(Object... namesAndValues) {
    Map<String, Object> args = new LinkedHashMap<>();
    for (int i = 0; i + 1 < namesAndValues.length; i += 2) {
      if (namesAndValues[i + 1] != null) args.put(String.valueOf(namesAndValues[i]), namesAndValues[i + 1]);
    }
    return args;
  }

  /**
   * Runs one tool call and records it.
   *
   * @throws ToolProblem when the call cannot be recorded; the call is then not made, or its answer is withheld
   */
  public <T> T record(String tool, Map<String, Object> arguments, Supplier<T> call) {
    if (!enabled) return call.get();
    try {
      prepare();
    } catch (IOException | RuntimeException e) {
      throw unrecordable(e);
    }

    long start = System.nanoTime();
    T result;
    try {
      result = call.get();
    } catch (ToolProblem e) {
      appendQuietly(tool, arguments, start, "refused", e.text());
      throw e;
    } catch (RuntimeException | Error e) {
      appendQuietly(tool, arguments, start, "error", e.getClass().getSimpleName());
      throw e;
    }
    try {
      append(tool, arguments, start, "ok", null);
    } catch (IOException | RuntimeException e) {
      throw unrecordable(e); // the answer exists, and is withheld
    }
    return result;
  }

  /** For a call that already failed: its own failure is what the caller must see, even if recording that fails too. */
  private void appendQuietly(String tool, Map<String, Object> arguments, long start, String outcome, String problem) {
    try {
      append(tool, arguments, start, outcome, problem);
    } catch (IOException | RuntimeException e) {
      // nothing more to do: the original failure is thrown by the caller
    }
  }

  private void append(String tool, Map<String, Object> arguments, long start, String outcome, String problem) throws IOException {
    Map<String, Object> line = new LinkedHashMap<>();
    line.put("time", clock.instant().toString());
    line.put("tool", tool);
    line.put("arguments", arguments);
    line.put("outcome", outcome);
    line.put("millis", (System.nanoTime() - start) / 1_000_000);
    if (problem != null) line.put("problem", problem);
    byte[] bytes = (JSON.writeValueAsString(line) + "\n").getBytes(StandardCharsets.UTF_8);
    synchronized (oneWriter) {
      Files.write(file, bytes, StandardOpenOption.CREATE, StandardOpenOption.APPEND);
    }
  }

  /** Makes sure the file can be appended to, before anything is asked of a broker. Owner-only where the file system allows. */
  private void prepare() throws IOException {
    synchronized (oneWriter) {
      Path folder = file.getParent();
      if (folder != null && !Files.isDirectory(folder)) {
        Files.createDirectories(folder);
        ownerOnly(folder, "rwx------");
      }
      if (!Files.exists(file)) {
        Files.createFile(file);
        ownerOnly(file, "rw-------");
      }
      if (!Files.isWritable(file)) throw new IOException("the file is not writable");
    }
  }

  private static void ownerOnly(Path path, String permissions) {
    try {
      Files.setPosixFilePermissions(path, PosixFilePermissions.fromString(permissions));
    } catch (IOException | UnsupportedOperationException e) {
      // not a POSIX file system; the default permissions stay
    }
  }

  private ToolProblem unrecordable(Exception e) {
    String reason = e.getMessage() == null || e.getMessage().isBlank() ? e.getClass().getSimpleName() : e.getClass().getSimpleName() + ": " + e.getMessage();
    return new ToolProblem("This call was not answered because it could not be recorded in the audit log (" + file + "): " + reason + ".",
        "Make that file writable, point topic-scope.audit.file somewhere else, or turn recording off with topic-scope.audit.enabled=false.");
  }
}
