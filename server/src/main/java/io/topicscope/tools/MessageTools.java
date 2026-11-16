package io.topicscope.tools;

import org.springframework.ai.mcp.annotation.McpTool;
import org.springframework.ai.mcp.annotation.McpToolParam;
import org.springframework.stereotype.Component;

/** The MCP face of {@link MessageService}. */
@Component
public class MessageTools {

  private final MessageService messages;

  public MessageTools(MessageService messages) {
    this.messages = messages;
  }

  @McpTool(
      name = "recent_messages",
      title = "Recent messages",
      description = """
          Returns the newest messages of one topic, newest first. Use it to see what is actually flowing. \
          For a Kafka source: the newest records stored in the topic, with partition, offset, timestamp, key, headers \
          and value. For an MQTT source 'topic' is a topic filter and there is no history: the tool listens for \
          'windowSeconds' and returns the retained message of each matching topic ('retained' = true) and whatever is \
          published meanwhile, each with its concrete topic, arrival time and QoS. \
          Values are shown as compact JSON, as text, or as a hex preview when they are binary ('valueFormat' says which; \
          'none' means the record has no value, a tombstone). Long values are cut ('truncated'), and anything that looks \
          like a secret is replaced with [REDACTED] ('redacted'); redaction is a best-effort guess, not a guarantee. \
          Kafka is read by position, without a consumer group: no offset is committed and no consumer is disturbed, \
          and records that arrive after the call started are not included. MQTT is only subscribed to, with a session \
          the broker forgets on disconnect. Read-only.""",
      generateOutputSchema = true,
      annotations = @McpTool.McpAnnotations(title = "Recent messages", readOnlyHint = true, destructiveHint = false, idempotentHint = true, openWorldHint = true))
  public MessageService.MessageList recentMessages(
      @McpToolParam(description = "Name of the source, as returned by list_sources. For example: local-kafka or local-mqtt", required = true) String source,
      @McpToolParam(description = "Kafka: exact topic name, as returned by list_topics. MQTT: a topic or topic filter ('+' one level, '#' all remaining levels), for example demo/sensors/+/+/telemetry", required = true) String topic,
      @McpToolParam(description = "How many messages to return, newest first. Default 10. The server caps it (100 unless configured otherwise).", required = false) Integer limit,
      @McpToolParam(description = "Kafka only. Read only this partition number. Leave out to get the newest messages over all partitions.", required = false) Integer partition,
      @McpToolParam(description = "MQTT only. Seconds to listen, 1 to 30. Default 3.", required = false) Integer windowSeconds) {
    return messages.recentMessages(source, topic, limit, partition, windowSeconds);
  }
}
