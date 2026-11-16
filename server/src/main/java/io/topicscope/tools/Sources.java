package io.topicscope.tools;

import io.topicscope.config.TopicScopeProperties;
import io.topicscope.config.TopicScopeProperties.Source;
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

  private String names() {
    List<String> names = properties.sources().stream().map(s -> s.name() + " (" + s.type().name().toLowerCase() + ")").toList();
    return names.isEmpty() ? "none" : String.join(", ", names);
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
          + SourceService.reason(e) + ".", "Call list_sources to see whether it answers at all.");
    }
  }
}
