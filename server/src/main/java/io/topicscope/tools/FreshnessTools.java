package io.topicscope.tools;

import org.springframework.ai.mcp.annotation.McpTool;
import org.springframework.ai.mcp.annotation.McpToolParam;
import org.springframework.stereotype.Component;

/** The MCP face of {@link FreshnessService}. */
@Component
public class FreshnessTools {

  private final FreshnessService freshness;
  private final AuditLog audit;

  public FreshnessTools(FreshnessService freshness, AuditLog audit) {
    this.freshness = freshness;
    this.audit = audit;
  }

  @McpTool(
      name = "topic_freshness",
      title = "Topic freshness",
      description = """
          Tells whether messages are still arriving on topics that should carry one every 'expectedIntervalSeconds', \
          for example a sensor that reports every few seconds. Per topic: status, time and age of the newest message. \
          Status is FRESH (the newest message is no older than one and a half expected intervals), STALE (it is older) \
          or SILENT (no message at all). \
          For a Kafka source the broker knows the time of the newest record, so the answer is immediate; leave 'topic' \
          out to check every topic. For an MQTT source, which has no consumer lag and no history, the tool listens on \
          a topic filter for 'windowSeconds' and also returns how many messages arrived, the delivery delay when the \
          payload carries a timestamp, and the broker's own counters from $SYS. With a wildcard filter a topic that was \
          silent is not listed at all; name a topic exactly to get SILENT for it. The MQTT call takes 'windowSeconds' \
          to answer. Read-only.""",
      generateOutputSchema = true,
      annotations = @McpTool.McpAnnotations(title = "Topic freshness", readOnlyHint = true, destructiveHint = false, idempotentHint = true, openWorldHint = true))
  public FreshnessService.FreshnessReport topicFreshness(
      @McpToolParam(description = "Name of the source, as returned by list_sources. For example: local-mqtt or local-kafka", required = true) String source,
      @McpToolParam(description = "Kafka: exact topic name, or leave out for all topics. MQTT: a topic or topic filter ('+' one level, '#' all remaining levels); default '#'. Example: demo/sensors/+/+/telemetry", required = false) String topic,
      @McpToolParam(description = "The longest gap between two messages that is still normal, in seconds. Default 60.", required = false) Integer expectedIntervalSeconds,
      @McpToolParam(description = "MQTT only. Seconds to listen, 1 to 60. Default: twice the expected interval plus one, at least 5 and at most 60.", required = false) Integer windowSeconds) {
    return audit.record("topic_freshness",
        AuditLog.args("source", source, "topic", topic, "expectedIntervalSeconds", expectedIntervalSeconds, "windowSeconds", windowSeconds),
        () -> freshness.freshness(source, topic, expectedIntervalSeconds, windowSeconds));
  }
}
