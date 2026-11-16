package io.topicscope.tools;

/**
 * How a consumer group is doing on one topic, judged from two looks a few seconds apart.
 *
 * <p>One look cannot tell a slow consumer from a stopped one: both show a lag. What tells them apart is
 * whether the committed offset moved in between.
 */
public enum LagStatus {

  /** Nothing is waiting, or the group is reading and is less than one sample's worth of work behind. */
  OK,
  /** The group is reading, and more is waiting than it read during the sample. */
  LAGGING,
  /** Messages are waiting and the group's committed offsets did not move during the sample. */
  STALLED,
  /** Messages are waiting, the group has no members, and nothing new arrived during the sample: a backlog nobody is working on. */
  NO_CONSUMER;

  /**
   * The rule, in order:
   *
   * <ol>
   *   <li>nothing waiting: {@code OK}</li>
   *   <li>the committed offsets did not move: {@code NO_CONSUMER} when the group has no members and nothing
   *       arrived either, otherwise {@code STALLED}</li>
   *   <li>they moved: {@code OK} when what is waiting is no more than what was read during the sample,
   *       otherwise {@code LAGGING}</li>
   * </ol>
   *
   * @param lag messages waiting at the second look
   * @param consumed how far the committed offsets moved between the looks
   * @param arrived how far the end offsets moved between the looks
   * @param members consumers connected at the second look
   */
  public static LagStatus judge(long lag, long consumed, long arrived, int members) {
    if (lag <= 0) return OK;
    if (consumed == 0) return members == 0 && arrived == 0 ? NO_CONSUMER : STALLED;
    return lag <= consumed ? OK : LAGGING;
  }

  /** For a group as a whole: the status that most needs attention among its topics. */
  public LagStatus worseOf(LagStatus other) {
    return severity() >= other.severity() ? this : other;
  }

  private int severity() {
    return switch (this) {
      case OK -> 0;
      case LAGGING -> 1;
      case NO_CONSUMER -> 2;
      case STALLED -> 3;
    };
  }
}
