package io.topicscope.safety;

import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.node.ArrayNode;
import tools.jackson.databind.node.ObjectNode;
import tools.jackson.databind.node.StringNode;

/**
 * Replaces what looks like a secret with {@code [REDACTED]} before a message is shown.
 *
 * <p>Two ways of looking: by name (a field, header or {@code name=value} pair whose name says it holds a
 * secret) and by shape (strings that look like well-known kinds of keys and tokens, wherever they stand).
 * Both are guesses. A secret under an innocent name and in an unknown shape passes, and a harmless value
 * under a suspicious name is hidden. Treat what the tools return as sensitive anyway.
 */
public final class Redactor {

  public static final String MARK = "[REDACTED]";

  private Redactor() {}

  /** Parts of a name that say "this holds a secret", compared after dropping case and everything but letters and digits. */
  private static final List<String> SECRET_NAME_PARTS = List.of(
      "password", "passwd", "passphrase", "secret", "token", "apikey", "authorization", "credential",
      "privatekey", "accesskey", "cookie", "sessionid", "signature");

  /**
   * Short names that are secrets as a whole word but would hit too much as a part ("auth" in "author").
   * A bare "key" is not here: in messaging it nearly always means an identifier, not a secret.
   */
  private static final List<String> SECRET_NAMES = List.of("pwd", "pass", "auth", "pin", "otp", "jwt", "bearer");

  /** Whether a field, header or parameter name suggests that its value is a secret. */
  public static boolean secretName(String name) {
    if (name == null) return false;
    String plain = name.toLowerCase(Locale.ROOT).replaceAll("[^a-z0-9]", "");
    if (plain.isEmpty()) return false;
    if (SECRET_NAMES.contains(plain)) return true;
    for (String part : SECRET_NAME_PARTS) {
      if (plain.contains(part)) return true;
    }
    return false;
  }

  // ---------- by shape ----------

  private static final List<Pattern> SECRET_SHAPES = List.of(
      // a whole PEM private key, or what is there of it
      Pattern.compile("-----BEGIN [A-Z ]*PRIVATE KEY-----[\\s\\S]*?(?:-----END [A-Z ]*PRIVATE KEY-----|\\z)"),
      // JSON Web Token: three base64url parts, the first two starting like a JSON object
      Pattern.compile("\\beyJ[A-Za-z0-9_-]{8,}\\.eyJ[A-Za-z0-9_-]{8,}\\.[A-Za-z0-9_-]{8,}"),
      Pattern.compile("\\bsk-ant-[A-Za-z0-9_-]{20,}"),
      Pattern.compile("\\bsk-(?:proj-)?[A-Za-z0-9_-]{32,}"),
      Pattern.compile("\\b(?:ghp|gho|ghu|ghs|ghr)_[A-Za-z0-9]{30,}\\b|\\bgithub_pat_[A-Za-z0-9_]{40,}"),
      Pattern.compile("\\b(?:AKIA|ASIA)[0-9A-Z]{16}\\b"),
      Pattern.compile("\\bxox[abprs]-[A-Za-z0-9-]{10,}"),
      Pattern.compile("\\bAIza[0-9A-Za-z_-]{35}\\b"));

  /** {@code Bearer abc...} and {@code Basic abc...}: the scheme stays, the credential goes. */
  private static final Pattern AUTH_SCHEME = Pattern.compile("(?i)\\b(Bearer|Basic)\\s+[A-Za-z0-9._~+/=-]{8,}");

  /** {@code user:password@host} inside a URL. */
  private static final Pattern URL_PASSWORD = Pattern.compile("(://[^:/@\\s]+):[^@/\\s]+@");

  /**
   * {@code name=value}, {@code name: value} and {@code "name":"value"} in plain text, where the name looks secret.
   * A URL scheme is not a name: without the {@code (?!//)}, "https" would take the whole URL as its value and
   * hide a token in the query string from this rule.
   */
  private static final Pattern NAMED_VALUE = Pattern.compile(
      // A name starts where a name can start and is taken whole (*+): otherwise a long run of letters is retried
      // from each of its characters, which is quadratic and stalls on a large payload.
      "(?<![A-Za-z0-9_.-])([\"']?)([A-Za-z][A-Za-z0-9_.-]*+)\\1(\\s*[=:]\\s*)(?!//)([\"']?)([^\\s\"'&,;]+)");

  /** The result of redacting: the text to show, and whether anything was replaced. */
  public record Result(String text, boolean redacted) {}

  /** Redacts plain text, by shape and by {@code name=value} pairs. */
  public static Result text(String text) {
    if (text == null || text.isEmpty()) return new Result("", false);
    String out = text;
    for (Pattern shape : SECRET_SHAPES) out = shape.matcher(out).replaceAll(MARK);
    out = AUTH_SCHEME.matcher(out).replaceAll(m -> Matcher.quoteReplacement(m.group(1) + " " + MARK));
    out = URL_PASSWORD.matcher(out).replaceAll(m -> Matcher.quoteReplacement(m.group(1) + ":" + MARK + "@"));
    out = NAMED_VALUE.matcher(out).replaceAll(m -> {
      String found = m.group(5);
      // "Authorization: Bearer ..." was handled above; the scheme word itself is not the secret.
      boolean scheme = found.equalsIgnoreCase("Bearer") || found.equalsIgnoreCase("Basic");
      boolean secret = secretName(m.group(2)) && !scheme && !found.startsWith(MARK);
      String value = secret ? MARK : m.group(5);
      return Matcher.quoteReplacement(m.group(1) + m.group(2) + m.group(1) + m.group(3) + m.group(4) + value);
    });
    return new Result(out, !out.equals(text));
  }

  // ---------- JSON ----------

  /**
   * Redacts a JSON document in place: the value of every secret-looking field, whatever it is, and secret
   * shapes inside every other string. Strings longer than {@code maxStringChars} are shortened, so one huge
   * field does not push the rest of the document out of view.
   *
   * @return what was done to the document
   */
  public static JsonChanges json(JsonNode root, int maxStringChars) {
    JsonChanges changes = new JsonChanges();
    walk(root, maxStringChars, changes);
    return changes;
  }

  /** Mutable on purpose: one counter set for a whole walk. */
  public static final class JsonChanges {
    private boolean redacted;
    private boolean shortened;

    public boolean redacted() {
      return redacted;
    }

    public boolean shortened() {
      return shortened;
    }
  }

  private static void walk(JsonNode node, int maxStringChars, JsonChanges changes) {
    if (node instanceof ObjectNode object) {
      // Copy the entries first: values are replaced while going through them.
      for (Map.Entry<String, JsonNode> field : List.copyOf(object.properties())) {
        JsonNode value = field.getValue();
        if (secretName(field.getKey()) && !value.isNull() && !(value.isString() && value.stringValue().isEmpty())) {
          object.set(field.getKey(), StringNode.valueOf(MARK));
          changes.redacted = true;
        } else if (value.isString()) {
          object.set(field.getKey(), string(value.stringValue(), maxStringChars, changes));
        } else {
          walk(value, maxStringChars, changes);
        }
      }
    } else if (node instanceof ArrayNode array) {
      for (int i = 0; i < array.size(); i++) {
        JsonNode value = array.get(i);
        if (value.isString()) {
          array.set(i, string(value.stringValue(), maxStringChars, changes));
        } else {
          walk(value, maxStringChars, changes);
        }
      }
    }
  }

  private static StringNode string(String value, int maxStringChars, JsonChanges changes) {
    Result r = text(Text.window(value, maxStringChars));
    if (r.redacted()) changes.redacted = true;
    String out = r.text();
    if (value.length() > maxStringChars) {
      int keep = Text.safeCut(out, maxStringChars);
      out = out.substring(0, keep) + "… (+" + (value.length() - keep) + " more characters)";
      changes.shortened = true;
    }
    return StringNode.valueOf(out);
  }
}
