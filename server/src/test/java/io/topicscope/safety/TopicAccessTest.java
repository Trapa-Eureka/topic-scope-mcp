package io.topicscope.safety;

import static org.assertj.core.api.Assertions.assertThat;

import io.topicscope.config.TopicScopeProperties.TopicRules;
import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.Test;

class TopicAccessTest {

  static TopicAccess rules(List<String> allow, List<String> deny) {
    return TopicAccess.of(new TopicRules(allow, deny));
  }

  @Test
  void withoutRulesEverythingIsAvailable() {
    TopicAccess access = TopicAccess.of(TopicRules.everything());
    assertThat(access.restricts()).isFalse();
    assertThat(access.permits("anything")).isTrue();
    assertThat(access.permits("a/b/c")).isTrue();
    assertThat(access.permits(null)).isFalse();
  }

  @Test
  void denyHidesWhatItNamesAndNothingElse() {
    TopicAccess access = rules(null, List.of("demo.alarms", "payments.*", "plant/*/secret"));
    assertThat(access.restricts()).isTrue();
    assertThat(access.permits("demo.alarms")).isFalse();
    assertThat(access.permits("demo.alarms.v2")).as("the whole name must match").isTrue();
    assertThat(access.permits("payments.cards")).isFalse();
    assertThat(access.permits("payments.")).isFalse();
    assertThat(access.permits("payments")).isTrue();
    assertThat(access.permits("plant/a/secret")).isFalse();
    assertThat(access.permits("plant/a/b/secret")).as("* crosses the level separator").isFalse();
    assertThat(access.permits("plant/a/telemetry")).isTrue();
  }

  @Test
  void anAllowListLeavesOnlyWhatItNamesAndDenyStillWins() {
    TopicAccess access = rules(List.of("demo.*"), List.of("demo.alarms"));
    assertThat(access.permits("demo.flights.positions")).isTrue();
    assertThat(access.permits("demo.alarms")).isFalse();
    assertThat(access.permits("other.topic")).isFalse();
    assertThat(access.permits("__consumer_offsets")).isFalse();
  }

  @Test
  void patternsAreLiteralExceptForTheStar() {
    TopicAccess access = rules(List.of("a.b", "x+y", "sys/$data", "[raw]"), null);
    assertThat(access.permits("a.b")).isTrue();
    assertThat(access.permits("aXb")).as("a dot is a dot").isFalse();
    assertThat(access.permits("x+y")).isTrue();
    assertThat(access.permits("xxy")).isFalse();
    assertThat(access.permits("sys/$data")).isTrue();
    assertThat(access.permits("[raw]")).isTrue();
    assertThat(access.permits("r")).isFalse();
    assertThat(access.permits("Demo")).as("case matters").isFalse();
  }

  @Test
  void blankPatternsAreIgnored() {
    TopicRules rules = new TopicRules(Arrays.asList(" ", null, " demo.* "), null);
    assertThat(rules.allow()).containsExactly("demo.*");
    assertThat(TopicAccess.of(rules).permits("demo.x")).isTrue();
  }
}
