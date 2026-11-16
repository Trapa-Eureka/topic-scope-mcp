package io.topicscope.tools;

import org.springframework.ai.mcp.annotation.McpTool;
import org.springframework.ai.mcp.annotation.McpToolParam;
import org.springframework.stereotype.Component;

/** The MCP face of {@link TopicService}. */
@Component
public class TopicTools {

  private final TopicService topics;

  public TopicTools(TopicService topics) {
    this.topics = topics;
  }

  @McpTool(
      name = "list_topics",
      title = "List topics",
      description = """
          Lists the topics of one source. For a Kafka source it reads the broker's catalog and returns, in 'topics', \
          per topic: name, number of partitions, replication factor, whether it is an internal topic, how many messages \
          are currently kept (end offset minus start offset over all partitions; an upper bound for compacted topics), \
          the time and age of the newest message, and how many partitions are under-replicated or offline. \
          An MQTT broker keeps no catalog, so for an MQTT source the tool listens on a topic filter for 'windowSeconds' \
          and returns, in 'observed', the topics it heard: topic, messages published during the window, whether the \
          broker holds a retained message for it, the highest QoS seen, and when the newest message arrived. That list \
          is an observation: a topic that was silent during the window and has no retained message is missing. \
          At most 200 topics are returned, sorted by name; 'matching' tells how many there are. Read-only.""",
      generateOutputSchema = true,
      annotations = @McpTool.McpAnnotations(title = "List topics", readOnlyHint = true, destructiveHint = false, idempotentHint = true, openWorldHint = true))
  public TopicService.TopicList listTopics(
      @McpToolParam(description = "Name of the source, as returned by list_sources. For example: local-kafka or local-mqtt", required = true) String source,
      @McpToolParam(description = "Only topics whose name contains this text (case-insensitive). Leave out for all topics.", required = false) String nameContains,
      @McpToolParam(description = "Kafka only. Also list the broker's internal topics such as __consumer_offsets. Default: false.", required = false) Boolean includeInternal,
      @McpToolParam(description = "MQTT only. Topic filter to listen on: '+' is one level, '#' all remaining levels. Default '#', every topic not starting with '$'. Example: demo/sensors/#", required = false) String filter,
      @McpToolParam(description = "MQTT only. Seconds to listen, 1 to 30. Default 3. Topics that publish less often than this may be missed.", required = false) Integer windowSeconds) {
    return topics.listTopics(source, nameContains, includeInternal, filter, windowSeconds);
  }

  @McpTool(
      name = "describe_topic",
      title = "Describe topic",
      description = """
          Describes one Kafka topic in detail: every partition with its leader broker, replicas, in-sync replicas, \
          start offset, end offset, messages kept and time of the newest message; and the topic's settings that decide \
          how long messages live (cleanup.policy, retention.ms, retention.bytes, max.message.bytes, min.insync.replicas, \
          compression.type) plus every setting that was overridden on the topic. Use it when list_topics shows something \
          unexpected, for example an uneven spread over partitions or an offline partition. Kafka sources only. Read-only.""",
      generateOutputSchema = true,
      annotations = @McpTool.McpAnnotations(title = "Describe topic", readOnlyHint = true, destructiveHint = false, idempotentHint = true, openWorldHint = true))
  public TopicService.TopicDetail describeTopic(
      @McpToolParam(description = "Name of a Kafka source, as returned by list_sources. For example: local-kafka", required = true) String source,
      @McpToolParam(description = "Exact topic name, as returned by list_topics.", required = true) String topic) {
    return topics.describeTopic(source, topic);
  }
}
