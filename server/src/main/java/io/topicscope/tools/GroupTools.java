package io.topicscope.tools;

import org.springframework.ai.mcp.annotation.McpTool;
import org.springframework.ai.mcp.annotation.McpToolParam;
import org.springframework.stereotype.Component;

/** The MCP face of {@link GroupService}. */
@Component
public class GroupTools {

  private final GroupService groups;
  private final AuditLog audit;

  public GroupTools(GroupService groups, AuditLog audit) {
    this.groups = groups;
    this.audit = audit;
  }

  @McpTool(
      name = "list_consumer_groups",
      title = "List consumer groups",
      description = """
          Lists the consumer groups of one Kafka source: name, state as the cluster reports it (STABLE, EMPTY, \
          PREPARING_REBALANCE, ...) and how many consumers are connected right now. EMPTY means the group exists \
          (it has committed offsets) but nobody is connected. Call consumer_lag with a group name to see how far \
          behind it is. At most 200 groups, sorted by name. Kafka sources only. Read-only.""",
      generateOutputSchema = true,
      annotations = @McpTool.McpAnnotations(title = "List consumer groups", readOnlyHint = true, destructiveHint = false, idempotentHint = true, openWorldHint = true))
  public GroupService.GroupList listConsumerGroups(
      @McpToolParam(description = "Name of a Kafka source, as returned by list_sources. For example: local-kafka", required = true) String source,
      @McpToolParam(description = "Only groups whose name contains this text (case-insensitive). Leave out for all groups.", required = false) String nameContains) {
    return audit.record("list_consumer_groups", AuditLog.args("source", source, "nameContains", nameContains), () -> groups.listGroups(source, nameContains));
  }

  @McpTool(
      name = "consumer_lag",
      title = "Consumer lag",
      description = """
          Tells how far one Kafka consumer group is behind, and whether it is moving. It looks at the group twice, \
          'sampleSeconds' apart, because one look cannot tell a slow consumer from a stopped one. \
          Per topic it returns: lag (messages waiting = end offset minus committed offset), how the lag changed, how \
          many messages the group read and how many arrived during the sample, the age of the oldest unread message, \
          and a status: OK (nothing waiting, or reading and less than one sample of work behind), LAGGING (reading, but \
          more is waiting than it read during the sample), STALLED (messages waiting and the committed offsets did not \
          move), NO_CONSUMER (messages waiting, no members, nothing new arriving). Per partition it returns committed \
          offset, end offset and lag. The call takes at least 'sampleSeconds' to answer. It does not join the group \
          and commits nothing. Kafka sources only. Read-only.""",
      generateOutputSchema = true,
      annotations = @McpTool.McpAnnotations(title = "Consumer lag", readOnlyHint = true, destructiveHint = false, idempotentHint = true, openWorldHint = true))
  public GroupService.LagReport consumerLag(
      @McpToolParam(description = "Name of a Kafka source, as returned by list_sources. For example: local-kafka", required = true) String source,
      @McpToolParam(description = "Exact consumer group name, as returned by list_consumer_groups.", required = true) String group,
      @McpToolParam(description = "Only this topic. Leave out for every topic the group has committed offsets on.", required = false) String topic,
      @McpToolParam(description = "Seconds between the two looks, 1 to 30. Default 5. Use a longer sample for consumers that commit rarely.", required = false) Integer sampleSeconds) {
    return audit.record("consumer_lag", AuditLog.args("source", source, "group", group, "topic", topic, "sampleSeconds", sampleSeconds),
        () -> groups.consumerLag(source, group, topic, sampleSeconds));
  }
}
