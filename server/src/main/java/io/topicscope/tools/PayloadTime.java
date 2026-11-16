package io.topicscope.tools;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.List;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

/**
 * Finds the time a message says it was made, when it says so: a JSON object with a field such as
 * {@code ts} or {@code timestamp}. Comparing that with the time of arrival gives the delivery delay.
 */
final class PayloadTime {

  private PayloadTime() {}

  /** Field names that commonly carry the time of the reading, in the order they are tried. */
  static final List<String> FIELDS = List.of("ts", "timestamp", "time", "eventTime", "observedAt", "datetime");

  /** A payload larger than this is not searched; a sensor reading is small. */
  private static final int MAX_BYTES = 64 * 1024;

  private static final JsonMapper JSON = JsonMapper.builder().build();

  /** 2001-09-09 in seconds and in milliseconds: numbers below are not taken for a time. */
  private static final long SECONDS_FLOOR = 1_000_000_000L;
  private static final long MILLIS_FLOOR = 1_000_000_000_000L;

  /** The time in the payload as epoch milliseconds, or -1 when the payload does not carry one. */
  static long of(byte[] payload) {
    if (payload == null || payload.length < 2 || payload.length > MAX_BYTES || payload[0] != '{') return -1;
    JsonNode root;
    try {
      root = JSON.readTree(new String(payload, StandardCharsets.UTF_8));
    } catch (JacksonException e) {
      return -1;
    }
    if (root == null || !root.isObject()) return -1;
    for (String field : FIELDS) {
      JsonNode value = root.get(field);
      if (value == null) continue;
      if (value.isNumber()) {
        long n = value.asLong();
        if (n >= MILLIS_FLOOR) return n;
        if (n >= SECONDS_FLOOR) return n * 1000;
      } else if (value.isString()) {
        try {
          return Instant.parse(value.stringValue()).toEpochMilli();
        } catch (DateTimeParseException e) {
          // not a time after all; try the next field
        }
      }
    }
    return -1;
  }
}
