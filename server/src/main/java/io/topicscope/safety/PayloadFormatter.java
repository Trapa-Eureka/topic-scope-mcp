package io.topicscope.safety;

import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.DeserializationFeature;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

/**
 * Turns the bytes of a message into something that can be shown: readable, redacted and never longer than
 * the cap. Every message value, key and header goes through here before it leaves the server.
 *
 * <ul>
 *   <li><b>json</b>: a JSON object or array. Shown compactly; secret-looking fields replaced; long strings
 *       inside it shortened, so the document usually stays valid JSON.</li>
 *   <li><b>text</b>: any other valid UTF-8 without control characters. Redacted by shape and by
 *       {@code name=value} pairs.</li>
 *   <li><b>binary</b>: everything else. A hex preview of the first bytes; the content is not interpreted.</li>
 *   <li><b>none</b>: there is no value at all (a Kafka tombstone, a record without a key).</li>
 * </ul>
 */
public final class PayloadFormatter {

  /** How many bytes of a binary payload are shown as hex. */
  static final int BINARY_PREVIEW_BYTES = 64;

  /** Strings inside a JSON document are shortened to this, so that the other fields stay visible. */
  static final int JSON_STRING_CHARS = 500;

  /** Numbers are kept as written ("10.7130" stays "10.7130"): a display must not round what was sent. */
  private static final JsonMapper JSON = JsonMapper.builder()
      .enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS)
      .enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS)
      .enable(DeserializationFeature.USE_BIG_INTEGER_FOR_INTS)
      .build();

  private PayloadFormatter() {}

  /**
   * What is shown of one payload.
   *
   * @param text the readable form; empty when there is no payload
   * @param format {@code json}, {@code text}, {@code binary} or {@code none}
   * @param bytes size of the original payload in bytes; 0 when there is none
   * @param truncated true when {@code text} does not show the whole payload
   * @param redacted true when something that looked like a secret was replaced
   */
  public record Shown(String text, String format, int bytes, boolean truncated, boolean redacted) {

    /** The same payload, cut further. Used when an answer as a whole has grown too large. */
    public Shown cutTo(int maxChars) {
      if (text.length() <= maxChars) return this;
      return new Shown(cut(text, maxChars), format, bytes, true, redacted);
    }
  }

  static final Shown NONE = new Shown("", "none", 0, false, false);

  /** Formats one payload, showing at most {@code maxChars} characters of it. */
  public static Shown format(byte[] payload, int maxChars) {
    if (payload == null) return NONE;
    if (payload.length == 0) return new Shown("", "text", 0, false, false);

    String text = decode(payload);
    if (text == null) return binary(payload);

    String trimmed = text.strip();
    if (trimmed.startsWith("{") || trimmed.startsWith("[")) {
      Shown json = json(trimmed, payload.length, maxChars);
      if (json != null) return json;
    }
    Redactor.Result redacted = Redactor.text(Text.window(text, maxChars));
    boolean tooLong = text.length() > maxChars;
    String shown = redacted.text();
    if (tooLong) {
      int keep = Text.safeCut(shown, maxChars);
      shown = shown.substring(0, keep) + "… [cut: " + keep + " of " + text.length() + " characters shown]";
    }
    return new Shown(shown, "text", payload.length, tooLong, redacted.redacted());
  }

  /** Null when the text is not a JSON object or array after all; it is then shown as plain text. */
  private static Shown json(String text, int bytes, int maxChars) {
    JsonNode root;
    try {
      root = JSON.readTree(text);
    } catch (JacksonException e) {
      return null;
    }
    if (root == null || !root.isContainer()) return null;
    Redactor.JsonChanges changes = Redactor.json(root, Math.min(JSON_STRING_CHARS, maxChars));
    String compact = JSON.writeValueAsString(root);
    boolean tooLong = compact.length() > maxChars;
    return new Shown(tooLong ? cut(compact, maxChars) : compact, "json", bytes, tooLong || changes.shortened(), changes.redacted());
  }

  private static Shown binary(byte[] payload) {
    int shown = Math.min(payload.length, BINARY_PREVIEW_BYTES);
    StringBuilder hex = new StringBuilder(shown * 3 + 40);
    for (int i = 0; i < shown; i++) {
      if (i > 0) hex.append(' ');
      hex.append(Character.forDigit((payload[i] >> 4) & 0xF, 16)).append(Character.forDigit(payload[i] & 0xF, 16));
    }
    boolean cut = payload.length > shown;
    if (cut) hex.append(" … (first ").append(shown).append(" of ").append(payload.length).append(" bytes)");
    return new Shown(hex.toString(), "binary", payload.length, cut, false);
  }

  /** The payload as text, or null when it is not text: invalid UTF-8, or control characters other than tab and line breaks. */
  private static String decode(byte[] payload) {
    String text;
    try {
      text = StandardCharsets.UTF_8.newDecoder()
          .onMalformedInput(CodingErrorAction.REPORT)
          .onUnmappableCharacter(CodingErrorAction.REPORT)
          .decode(ByteBuffer.wrap(payload)).toString();
    } catch (CharacterCodingException e) {
      return null;
    }
    for (int i = 0; i < text.length(); i++) {
      char c = text.charAt(i);
      if (Character.isISOControl(c) && c != '\t' && c != '\n' && c != '\r') return null;
    }
    return text;
  }

  static String cut(String text, int maxChars) {
    int keep = Text.safeCut(text, maxChars);
    return text.substring(0, keep) + "… [cut: " + keep + " of " + text.length() + " characters shown]";
  }
}
