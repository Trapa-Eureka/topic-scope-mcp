package io.topicscope.safety;

import static org.assertj.core.api.Assertions.assertThat;

import io.topicscope.safety.PayloadFormatter.Shown;
import io.topicscope.sim.DemoData;
import java.nio.charset.StandardCharsets;
import java.util.List;
import org.junit.jupiter.api.Test;

class PayloadFormatterTest {

  static Shown format(String text) {
    return PayloadFormatter.format(text.getBytes(StandardCharsets.UTF_8), 2000);
  }

  @Test
  void noPayloadAndAnEmptyOneAreDifferentThings() {
    assertThat(PayloadFormatter.format(null, 2000)).isEqualTo(new Shown("", "none", 0, false, false));
    assertThat(PayloadFormatter.format(new byte[0], 2000)).isEqualTo(new Shown("", "text", 0, false, false));
  }

  @Test
  void jsonIsShownCompactAndNumbersStayAsWritten() {
    Shown shown = format("{ \"lat\" : 10.7130,\n  \"big\": 12345678901234567890123, \"n\": 1.0, \"list\": [1, 2.50] }");
    assertThat(shown.text()).isEqualTo("{\"lat\":10.7130,\"big\":12345678901234567890123,\"n\":1.0,\"list\":[1,2.50]}");
    assertThat(shown.format()).isEqualTo("json");
    assertThat(shown.truncated()).isFalse();
    assertThat(shown.redacted()).isFalse();
  }

  @Test
  void secretLookingJsonFieldsAreReplacedAtAnyDepth() {
    Shown shown = format("{\"user\":\"op\",\"password\":\"x\",\"nested\":{\"apiKey\":{\"a\":1},\"list\":[{\"token\":5,\"ok\":true}]},\"note\":\"Bearer abcdefgh12345678\"}");
    assertThat(shown.text()).isEqualTo(
        "{\"user\":\"op\",\"password\":\"[REDACTED]\",\"nested\":{\"apiKey\":\"[REDACTED]\",\"list\":[{\"token\":\"[REDACTED]\",\"ok\":true}]},\"note\":\"Bearer [REDACTED]\"}");
    assertThat(shown.redacted()).isTrue();
  }

  @Test
  void emptyAndNullSecretsAreNotMarkedAsRedacted() {
    Shown shown = format("{\"password\":\"\",\"token\":null}");
    assertThat(shown.text()).isEqualTo("{\"password\":\"\",\"token\":null}");
    assertThat(shown.redacted()).isFalse();
  }

  @Test
  void aLongStringInsideJsonIsShortenedAndTheDocumentStaysJson() {
    Shown shown = format("{\"kind\":\"oversized\",\"blob\":\"" + "x".repeat(20_000) + "\",\"after\":1}");
    assertThat(shown.format()).isEqualTo("json");
    assertThat(shown.truncated()).isTrue();
    assertThat(shown.bytes()).isGreaterThan(20_000);
    assertThat(shown.text()).isEqualTo("{\"kind\":\"oversized\",\"blob\":\"" + "x".repeat(500) + "… (+19500 more characters)\",\"after\":1}");
  }

  @Test
  void jsonWithManyFieldsIsCutAtTheCap() {
    StringBuilder json = new StringBuilder("{");
    for (int i = 0; i < 400; i++) json.append(i == 0 ? "" : ",").append("\"field").append(i).append("\":").append(i);
    json.append("}");
    Shown shown = PayloadFormatter.format(json.toString().getBytes(StandardCharsets.UTF_8), 300);
    assertThat(shown.truncated()).isTrue();
    assertThat(shown.text()).startsWith("{\"field0\":0,").endsWith("… [cut: 300 of " + json.length() + " characters shown]");
  }

  @Test
  void whatOnlyLooksLikeJsonIsText() {
    assertThat(format("{not json} password=abc").text()).isEqualTo("{not json} password=[REDACTED]");
    assertThat(format("{not json}").format()).isEqualTo("text");
    assertThat(format("[1,2] trailing").format()).isEqualTo("text");
    assertThat(format("42").format()).as("a bare number is text, not a document").isEqualTo("text");
    assertThat(format("\"quoted\"").format()).isEqualTo("text");
  }

  @Test
  void textKeepsItsCharactersAndIsCutAtTheCapWithoutSplittingAnEmoji() {
    assertThat(format("Wind shear advisory — 활주로 09").text()).isEqualTo("Wind shear advisory — 활주로 09");
    assertThat(format("line one\n\tline two\r\n").format()).isEqualTo("text");

    Shown cut = PayloadFormatter.format("ab😀cd".getBytes(StandardCharsets.UTF_8), 3);
    assertThat(cut.text()).isEqualTo("ab… [cut: 2 of 6 characters shown]");
    assertThat(cut.truncated()).isTrue();
  }

  @Test
  void binaryIsAHexPreviewAndIsNeverInterpreted() {
    byte[] all = new byte[256];
    for (int i = 0; i < 256; i++) all[i] = (byte) i;
    Shown shown = PayloadFormatter.format(all, 2000);
    assertThat(shown.format()).isEqualTo("binary");
    assertThat(shown.bytes()).isEqualTo(256);
    assertThat(shown.truncated()).isTrue();
    assertThat(shown.text()).startsWith("00 01 02 03").endsWith("3e 3f … (first 64 of 256 bytes)");

    assertThat(PayloadFormatter.format(new byte[] {(byte) 0xCA, (byte) 0xFE}, 2000)).isEqualTo(new Shown("ca fe", "binary", 2, false, false));
    assertThat(PayloadFormatter.format(new byte[] {'o', 'k', 0, 'x'}, 2000).format()).as("a NUL byte makes it binary").isEqualTo("binary");
  }

  @Test
  void cuttingAnAlreadyFormattedValueFurther() {
    Shown shown = format("a".repeat(1000));
    assertThat(shown.cutTo(2000)).isSameAs(shown);
    Shown shorter = shown.cutTo(200);
    assertThat(shorter.text()).isEqualTo("a".repeat(200) + "… [cut: 200 of 1000 characters shown]");
    assertThat(shorter.truncated()).isTrue();
    assertThat(shorter.bytes()).isEqualTo(1000);
  }

  @Test
  void aLargePayloadIsFormattedQuickly() {
    // One megabyte without a single break is the worst case for pattern matching that retries from every character.
    byte[] letters = "x".repeat(1_000_000).getBytes(StandardCharsets.UTF_8);
    byte[] json = ("{\"blob\":\"" + "x".repeat(1_000_000) + "\",\"password\":\"p\"}").getBytes(StandardCharsets.UTF_8);
    byte[] pairs = "a=b ".repeat(250_000).getBytes(StandardCharsets.UTF_8);

    long start = System.nanoTime();
    Shown text = PayloadFormatter.format(letters, 2000);
    Shown document = PayloadFormatter.format(json, 2000);
    Shown many = PayloadFormatter.format(pairs, 2000);
    long millis = (System.nanoTime() - start) / 1_000_000;

    assertThat(millis).as("three one-megabyte payloads").isLessThan(2000);
    assertThat(text.text()).endsWith("… [cut: 2000 of 1000000 characters shown]");
    assertThat(document.text()).contains("(+999500 more characters)", "\"password\":\"[REDACTED]\"");
    assertThat(many.truncated()).isTrue();
  }

  @Test
  void aSecretThatStartsInsideTheShownPartIsReplacedWhole() {
    String secret = "password=" + "s".repeat(300);
    Shown shown = PayloadFormatter.format(("x".repeat(1900) + " " + secret + " tail").getBytes(StandardCharsets.UTF_8), 2000);
    assertThat(shown.text()).contains("password=[REDACTED]").doesNotContain("sss");
    assertThat(shown.redacted()).isTrue();
  }

  /** The awkward records the simulator seeds are exactly the cases this class exists for. */
  @Test
  void theSeededEdgeCasesComeOutAsDocumented() {
    List<DemoData.SeedRecord> edge = DemoData.seedRecords(DemoData.EDGE_CASES.name(), 0);

    assertThat(PayloadFormatter.format(edge.get(DemoData.EDGE_JSON).value(), 2000))
        .isEqualTo(new Shown("{\"kind\":\"json\",\"nested\":{\"list\":[1,2,3],\"ok\":true}}", "json", edge.get(DemoData.EDGE_JSON).value().length, false, false));
    assertThat(PayloadFormatter.format(edge.get(DemoData.EDGE_TEXT_NO_KEY).value(), 2000).format()).isEqualTo("text");
    assertThat(PayloadFormatter.format(edge.get(DemoData.EDGE_BINARY).value(), 2000).format()).isEqualTo("binary");

    Shown oversized = PayloadFormatter.format(edge.get(DemoData.EDGE_OVERSIZED).value(), 2000);
    assertThat(oversized.truncated()).isTrue();
    assertThat(oversized.text().length()).isLessThan(700);

    Shown secrets = PayloadFormatter.format(edge.get(DemoData.EDGE_SECRETS).value(), 2000);
    assertThat(secrets.text()).isEqualTo("{\"user\":\"operator1\",\"password\":\"[REDACTED]\",\"apiKey\":\"[REDACTED]\",\"token\":\"[REDACTED]\"}");
    assertThat(secrets.redacted()).isTrue();

    assertThat(PayloadFormatter.format(edge.get(DemoData.EDGE_TOMBSTONE).value(), 2000).format()).isEqualTo("none");
  }
}
