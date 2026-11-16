package io.topicscope.tools;

import static io.topicscope.tools.TopicServiceTest.problem;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class AuditLogTest {

  static final Clock CLOCK = Clock.fixed(Instant.parse("2026-11-16T12:00:00Z"), ZoneOffset.UTC);

  @TempDir
  Path folder;

  @Test
  void everyCallIsOneLineWithItsArgumentsAndOutcome() throws Exception {
    Path file = folder.resolve("nested/audit.jsonl");
    AuditLog audit = new AuditLog(true, file, CLOCK);

    String answer = audit.record("recent_messages", AuditLog.args("source", "k", "topic", "orders", "limit", 5, "partition", null), () -> "the answer with message content");
    assertThatThrownBy(() -> audit.record("describe_topic", AuditLog.args("source", "k", "topic", "nope"),
        () -> { throw new ToolProblem("There is no topic named 'nope'.", "Did you mean: orders."); })).isInstanceOf(ToolProblem.class);
    assertThatThrownBy(() -> audit.record("list_topics", AuditLog.args("source", "k"), () -> { throw new IllegalStateException("boom"); }))
        .isInstanceOf(IllegalStateException.class);

    assertThat(answer).isEqualTo("the answer with message content");
    List<String> lines = Files.readAllLines(file);
    assertThat(lines).hasSize(3);
    assertThat(lines.get(0)).startsWith("{\"time\":\"2026-11-16T12:00:00Z\",\"tool\":\"recent_messages\",\"arguments\":{\"source\":\"k\",\"topic\":\"orders\",\"limit\":5},\"outcome\":\"ok\",\"millis\":")
        .as("what the tool returned is never recorded").doesNotContain("message content").doesNotContain("partition");
    assertThat(lines.get(1)).contains("\"tool\":\"describe_topic\"", "\"outcome\":\"refused\"", "\"problem\":\"There is no topic named 'nope'. Did you mean: orders.\"");
    assertThat(lines.get(2)).contains("\"tool\":\"list_topics\"", "\"outcome\":\"error\"", "\"problem\":\"IllegalStateException\"");
  }

  @Test
  void theFileAndItsFolderAreForTheOwnerOnly() throws Exception {
    Path file = folder.resolve("private/audit.jsonl");
    new AuditLog(true, file, CLOCK).record("list_sources", AuditLog.args(), () -> "x");
    assertThat(PosixFilePermissions.toString(Files.getPosixFilePermissions(file))).isEqualTo("rw-------");
    assertThat(PosixFilePermissions.toString(Files.getPosixFilePermissions(file.getParent()))).isEqualTo("rwx------");
  }

  @Test
  void aCallThatCannotBeRecordedIsNotMade() throws Exception {
    Path notAFolder = Files.createFile(folder.resolve("occupied"));
    AuditLog audit = new AuditLog(true, notAFolder.resolve("audit.jsonl"), CLOCK);
    AtomicInteger calls = new AtomicInteger();

    assertThat(problem(() -> audit.record("recent_messages", AuditLog.args("source", "k"), calls::incrementAndGet)))
        .startsWith("This call was not answered because it could not be recorded in the audit log (")
        .contains("topic-scope.audit.enabled=false");
    assertThat(calls).as("the broker was never asked").hasValue(0);
  }

  @Test
  void switchedOffItRecordsNothingAndNeedsNoFile() {
    Path file = folder.resolve("off/audit.jsonl");
    assertThat(new AuditLog(false, file, CLOCK).record("list_sources", AuditLog.args(), () -> "x")).isEqualTo("x");
    assertThat(file).doesNotExist();
  }
}
