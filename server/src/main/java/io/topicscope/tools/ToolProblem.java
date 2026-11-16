package io.topicscope.tools;

/**
 * A tool call that cannot be answered, in two sentences written for the caller: what is wrong, and what
 * would work instead. The MCP layer turns it into an error result that the assistant reads, so both
 * sentences must stand on their own and must never carry a password or a stack trace.
 *
 * <p>Why the hint travels as the cause: Spring AI builds the error text as the exception's message, a line
 * break, and the root cause's message. An exception without a cause is its own root, and its message would
 * be printed twice. With the hint as the cause, the caller reads the problem on one line and the hint on
 * the next.
 */
public class ToolProblem extends RuntimeException {

  private static final long serialVersionUID = 1L;

  /**
   * @param problem what is wrong, as a full sentence
   * @param hint what the caller can do next, as a full sentence; never null
   */
  public ToolProblem(String problem, String hint) {
    super(problem, new Hint(hint));
  }

  public String problem() {
    return getMessage();
  }

  public String hint() {
    return getCause().getMessage();
  }

  /** Both sentences, the way a caller reads them. */
  public String text() {
    return problem() + " " + hint();
  }

  /** Carries the hint. Not a failure of its own, so it records no stack trace. */
  private static final class Hint extends RuntimeException {
    private static final long serialVersionUID = 1L;

    Hint(String hint) {
      super(hint == null ? "" : hint, null, false, false);
    }
  }
}
