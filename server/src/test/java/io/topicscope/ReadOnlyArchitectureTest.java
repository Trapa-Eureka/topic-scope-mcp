package io.topicscope;

import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.noClasses;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.tngtech.archunit.base.DescribedPredicate;
import com.tngtech.archunit.core.domain.JavaClasses;
import com.tngtech.archunit.core.domain.JavaConstructorCall;
import com.tngtech.archunit.core.domain.JavaMethodCall;
import com.tngtech.archunit.core.importer.ClassFileImporter;
import com.tngtech.archunit.lang.ArchRule;
import java.nio.file.Path;
import java.util.Set;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.consumer.Consumer;
import org.eclipse.paho.client.mqttv3.IMqttAsyncClient;
import org.eclipse.paho.client.mqttv3.IMqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.junit.jupiter.api.Test;

/**
 * The read-only promise, checked against the compiled server: it fails the build when server code could
 * change a broker.
 *
 * <p>The rules are allow lists. A broker-client method that is not named here is refused, so a new way of
 * writing does not slip in unnoticed; adding a method to a list is a reviewed decision. Each rule is also
 * run against a deliberately bad class in {@code fixtures}, to prove that it can fail.
 */
class ReadOnlyArchitectureTest {

  /** Only the server's own production classes: not tests, not the simulator, not libraries. */
  static final JavaClasses SERVER = new ClassFileImporter().importPath(Path.of("target", "classes"));
  static final JavaClasses BAD_EXAMPLES = new ClassFileImporter().importPackages("io.topicscope.fixtures");

  // ---------- what the server may call ----------

  /** Reading by position. No subscribe (joins a group), no commit (moves a group), no committed (needs a group). */
  static final Set<String> CONSUMER_READS = Set.of(
      "assign", "assignment", "seek", "seekToBeginning", "seekToEnd", "poll", "position",
      "beginningOffsets", "endOffsets", "offsetsForTimes", "partitionsFor", "listTopics",
      "pause", "resume", "paused", "wakeup", "close");

  /** Listing and describing. No create, delete, alter, elect or anything else. */
  static final Set<String> ADMIN_READS = Set.of(
      "create", // Admin.create(...) builds the client; creating topics is createTopics and is not here
      "describeCluster", "listTopics", "describeTopics", "describeConfigs", "listOffsets",
      "listGroups", "listConsumerGroups", "describeConsumerGroups", "listConsumerGroupOffsets",
      "close");

  /** Connecting and subscribing. No publish. */
  static final Set<String> MQTT_READS = Set.of(
      "connect", "subscribe", "unsubscribe", "disconnect", "disconnectForcibly", "close",
      "isConnected", "setCallback", "setTimeToWait", "getClientId", "getServerURI");

  // ---------- the rules ----------

  static final ArchRule NO_KAFKA_PRODUCER = noClasses()
      .should().dependOnClassesThat().resideInAPackage("org.apache.kafka.clients.producer..")
      .because("the server never publishes to Kafka");

  static final ArchRule KAFKA_CONSUMER_ONLY_READS_BY_POSITION = noClasses()
      .should().callMethodWhere(callOutside(Consumer.class, CONSUMER_READS))
      .because("the server reads with assign + seek and never joins or moves a consumer group");

  static final ArchRule KAFKA_ADMIN_ONLY_LISTS_AND_DESCRIBES = noClasses()
      .should().callMethodWhere(callOutside(Admin.class, ADMIN_READS))
      .because("the server never creates, deletes or alters anything on a Kafka cluster");

  static final ArchRule MQTT_CLIENT_ONLY_SUBSCRIBES = noClasses()
      .should().callMethodWhere(callOutside(IMqttClient.class, MQTT_READS))
      .orShould().callMethodWhere(callOutside(IMqttAsyncClient.class, MQTT_READS))
      .because("the server never publishes to an MQTT broker");

  static final ArchRule NO_MQTT_MESSAGE_IS_BUILT = noClasses()
      .should().callConstructorWhere(new DescribedPredicate<JavaConstructorCall>("an MQTT message is created") {
        @Override
        public boolean test(JavaConstructorCall call) {
          return call.getTargetOwner().isAssignableTo(MqttMessage.class);
        }
      })
      .because("the server receives MQTT messages and never makes one");

  static final ArchRule NO_MQTT_LAST_WILL = noClasses()
      .should().callMethodWhere(new DescribedPredicate<JavaMethodCall>("a last-will message is set") {
        @Override
        public boolean test(JavaMethodCall call) {
          return call.getTargetOwner().isAssignableTo(MqttConnectOptions.class) && call.getName().equals("setWill");
        }
      })
      .because("a last will is a message the broker publishes on the server's behalf");

  static final ArchRule TOOLS_HOLD_NO_BROKER_CODE = noClasses().that().resideInAnyPackage("..tools..", "..board..")
      .should().dependOnClassesThat().resideInAnyPackage("org.apache.kafka..", "org.eclipse.paho..")
      .because("tool classes stay thin; broker access lives in the kafka and mqtt packages where these rules watch it");

  static final ArchRule SERVER_DOES_NOT_USE_THE_SIMULATOR = noClasses()
      .should().dependOnClassesThat().resideInAPackage("io.topicscope.sim..")
      .because("the simulator publishes; it is a test dependency only");

  static final ArchRule[] ALL = {
      NO_KAFKA_PRODUCER, KAFKA_CONSUMER_ONLY_READS_BY_POSITION, KAFKA_ADMIN_ONLY_LISTS_AND_DESCRIBES,
      MQTT_CLIENT_ONLY_SUBSCRIBES, NO_MQTT_MESSAGE_IS_BUILT, NO_MQTT_LAST_WILL, TOOLS_HOLD_NO_BROKER_CODE,
      SERVER_DOES_NOT_USE_THE_SIMULATOR};

  // ---------- the checks ----------

  @Test
  void theServerIsImported() {
    assertThat(SERVER.contain("io.topicscope.TopicScopeApplication")).as("run after compile: target/classes must hold the server").isTrue();
    assertThat(SERVER.contain("io.topicscope.kafka.KafkaProbe")).isTrue();
    assertThat(SERVER.stream().noneMatch(c -> c.getPackageName().startsWith("io.topicscope.fixtures"))).isTrue();
  }

  @Test
  void theServerKeepsEveryRule() {
    for (ArchRule rule : ALL) rule.check(SERVER);
  }

  @Test
  void everyRuleCatchesItsBadExample() {
    for (ArchRule rule : ALL) {
      assertThatThrownBy(() -> rule.check(BAD_EXAMPLES))
          .as("the rule must fail on the bad examples: %s", rule.getDescription())
          .isInstanceOf(AssertionError.class);
    }
  }

  @Test
  void eachBadExampleIsCaughtByTheRuleMeantForIt() {
    assertViolation(NO_KAFKA_PRODUCER, "BadKafkaPublisher");
    assertViolation(KAFKA_CONSUMER_ONLY_READS_BY_POSITION, "BadGroupConsumer", "subscribe", "commitSync");
    assertViolation(KAFKA_ADMIN_ONLY_LISTS_AND_DESCRIBES, "BadAdmin", "deleteTopics", "alterConsumerGroupOffsets");
    assertViolation(MQTT_CLIENT_ONLY_SUBSCRIBES, "BadMqttPublisher", "publish");
    assertViolation(NO_MQTT_MESSAGE_IS_BUILT, "BadMqttPublisher");
    assertViolation(NO_MQTT_LAST_WILL, "BadMqttPublisher", "setWill");
    assertViolation(TOOLS_HOLD_NO_BROKER_CODE, "BadTool");
    assertViolation(SERVER_DOES_NOT_USE_THE_SIMULATOR, "BadSimulatorUser");
  }

  private static void assertViolation(ArchRule rule, String... mentions) {
    assertThatThrownBy(() -> rule.check(BAD_EXAMPLES)).isInstanceOf(AssertionError.class).hasMessageContainingAll(mentions);
  }

  /** A call to a method of {@code owner} (or a subtype) whose name is not on the allow list. */
  private static DescribedPredicate<JavaMethodCall> callOutside(Class<?> owner, Set<String> allowed) {
    return new DescribedPredicate<>("a " + owner.getSimpleName() + " method outside the read-only list " + allowed.stream().sorted().toList() + " is called") {
      @Override
      public boolean test(JavaMethodCall call) {
        return call.getTargetOwner().isAssignableTo(owner) && !allowed.contains(call.getName());
      }
    };
  }
}
