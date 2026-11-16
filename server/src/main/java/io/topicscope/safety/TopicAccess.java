package io.topicscope.safety;

import io.topicscope.config.TopicScopeProperties.TopicRules;
import java.util.List;
import java.util.regex.Pattern;

/**
 * Decides which topics of one source the tools may show, from the source's allow and deny lists.
 *
 * <p>Deny wins. With an allow list, only what it names is available; without one, everything that is not
 * denied. Patterns are matched against the whole topic name, case-sensitively, with {@code *} standing for
 * any run of characters (it also crosses the {@code .} of a Kafka name and the {@code /} of an MQTT topic).
 *
 * <p>This is the server's own second line. The first is the broker: give the server an account that cannot
 * read what it should not see. For MQTT that matters most, because a message on a denied topic that the
 * broker delivers has already arrived before it is dropped here.
 */
public final class TopicAccess {

  private final List<Pattern> allow;
  private final List<Pattern> deny;

  private TopicAccess(List<Pattern> allow, List<Pattern> deny) {
    this.allow = allow;
    this.deny = deny;
  }

  public static TopicAccess of(TopicRules rules) {
    return new TopicAccess(rules.allow().stream().map(TopicAccess::pattern).toList(), rules.deny().stream().map(TopicAccess::pattern).toList());
  }

  /** Whether a topic of this exact name may be shown. */
  public boolean permits(String topic) {
    if (topic == null) return false;
    for (Pattern p : deny) {
      if (p.matcher(topic).matches()) return false;
    }
    if (allow.isEmpty()) return true;
    for (Pattern p : allow) {
      if (p.matcher(topic).matches()) return true;
    }
    return false;
  }

  /** Whether any rule is set at all. Without rules nothing is ever hidden, and nothing needs to be said about it. */
  public boolean restricts() {
    return !allow.isEmpty() || !deny.isEmpty();
  }

  /** Everything is literal except {@code *}. */
  private static Pattern pattern(String glob) {
    StringBuilder regex = new StringBuilder();
    for (String literal : glob.split("\\*", -1)) {
      if (regex.length() > 0) regex.append(".*");
      regex.append(Pattern.quote(literal));
    }
    return Pattern.compile(regex.toString(), Pattern.DOTALL);
  }
}
