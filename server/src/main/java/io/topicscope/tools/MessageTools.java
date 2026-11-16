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
          Returns the newest messages stored in one Kafka topic, newest first: partition, offset, timestamp, key, \
          headers and value. Use it to see what is actually flowing through a topic. \
          Values are shown as compact JSON, as text, or as a hex preview when they are binary ('valueFormat' says which; \
          'none' means the record has no value, a tombstone). Long values are cut ('truncated'), and anything that looks \
          like a secret is replaced with [REDACTED] ('redacted'); redaction is a best-effort guess, not a guarantee. \
          Reading is by position, without a consumer group: no offset is committed and no consumer is disturbed. \
          Messages that arrive after the call started are not included. Kafka sources only. Read-only.""",
      generateOutputSchema = true,
      annotations = @McpTool.McpAnnotations(title = "Recent messages", readOnlyHint = true, destructiveHint = false, idempotentHint = true, openWorldHint = true))
  public MessageService.MessageList recentMessages(
      @McpToolParam(description = "Name of a Kafka source, as returned by list_sources. For example: local-kafka", required = true) String source,
      @McpToolParam(description = "Exact topic name, as returned by list_topics.", required = true) String topic,
      @McpToolParam(description = "How many messages to return, newest first. Default 10. The server caps it (100 unless configured otherwise).", required = false) Integer limit,
      @McpToolParam(description = "Read only this partition number. Leave out to get the newest messages over all partitions.", required = false) Integer partition) {
    return messages.recentMessages(source, topic, limit, partition);
  }
}
