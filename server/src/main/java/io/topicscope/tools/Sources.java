package io.topicscope.tools;

import io.topicscope.config.TopicScopeProperties;
import io.topicscope.config.TopicScopeProperties.Source;
import io.topicscope.safety.TopicAccess;
import java.util.List;
import java.util.concurrent.Callable;
import org.springframework.stereotype.Component;

/** Finds the source a tool call names, and turns broker failures into messages a caller can act on. */
@Component
public class Sources {

  private final TopicScopeProperties properties;

  public Sources(TopicScopeProperties properties) {
    this.properties = properties;
  }

  /** @throws ToolProblem when no source has that name; the message lists the names that exist */
  public Source require(String name) {
    if (name == null || name.isBlank()) {
      throw new ToolProblem("The 'source' argument is missing.", "Configured sources: " + names() + ".");
    }
    return properties.source(name.trim())
        .orElseThrow(() -> new ToolProblem("There is no source named '" + name.trim() + "'.", "Configured sources: " + names() + "."));
  }

  /** The topic rules of a source. */
  public TopicAccess access(Source source) {
    return TopicAccess.of(source.topics());
  }

  /**
   * @throws ToolProblem when the server's configuration keeps this topic out of reach. The answer is the same
   *     whether the topic exists or not, so it tells nothing about a topic the caller may not see
   */
  public void requireAllowed(Source source, String topic) {
    if (!access(source).permits(topic)) {
      throw new ToolProblem("Topic '" + topic + "' on source '" + source.name() + "' is not available through this server.",
          "The server's configuration (topics.allow / topics.deny of the source) leaves it out. list_topics shows what is available.");
    }
  }

  /** A sentence for a result's note when topics were left out, or nothing when none were. */
  static String hiddenNote(int hidden, String what) {
    return hidden == 0 ? "" : hidden + " " + what + " left out by this server's configuration (topics.allow / topics.deny).";
  }

  private String names() {
    List<String> names = properties.sources().stream().map(s -> s.name() + " (" + s.type().name().toLowerCase() + ")").toList();
    return names.isEmpty() ? "none" : String.join(", ", names);
  }

  /** Whatever a client library puts into an error message, the password of the source does not leave with it. */
  static String withoutSecret(Source source, String text) {
    return source.password().isEmpty() || text == null ? text : text.replace(source.password(), "***");
  }

  /**
   * Runs one question to a broker. Whatever goes wrong becomes a {@link ToolProblem} that names the source
   * and the reason, so the call always gets an answer.
   */
  public <T> T ask(Source source, Callable<T> question) {
    try {
      return question.call();
    } catch (ToolProblem e) {
      throw e;
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new ToolProblem("Interrupted while asking source '" + source.name() + "'.", "Try again.");
    } catch (Exception | LinkageError e) {
      // LinkageError too: a client library that cannot load must become an answer, not a call that never returns.
      throw new ToolProblem("Source '" + source.name() + "' (" + SourceService.withoutPassword(source.address()) + ") could not be read: "
          + withoutSecret(source, SourceService.reason(e)) + ".",
          "Call list_sources to see whether it answers at all. A broker that asks for a login and gets none, or a wrong one, "
              + "may simply not answer: check the username and password of the source.");
    }
  }
}
