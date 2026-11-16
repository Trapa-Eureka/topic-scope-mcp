package io.topicscope.safety;

/** Small text helpers shared by the redactor and the formatter. */
final class Text {

  private Text() {}

  /**
   * The start of {@code text} that redaction has to look at when at most {@code shown} characters will be
   * shown: several times that, so a secret that begins inside the shown part is seen whole and replaced,
   * while a megabyte of payload is not scanned for the sake of a short preview.
   */
  static String window(String text, int shown) {
    int size = Math.max(0, shown) * 4 + 1024;
    return text.length() <= size ? text : text.substring(0, safeCut(text, size));
  }

  /** Where to cut {@code text} so that at most {@code max} characters remain and no surrogate pair is split. */
  static int safeCut(String text, int max) {
    if (text.length() <= max) return text.length();
    int cut = Math.max(0, max);
    if (cut > 0 && Character.isHighSurrogate(text.charAt(cut - 1))) cut--;
    return cut;
  }
}
