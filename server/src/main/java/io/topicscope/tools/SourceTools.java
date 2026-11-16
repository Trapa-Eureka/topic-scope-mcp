package io.topicscope.tools;

import org.springframework.ai.mcp.annotation.McpTool;
import org.springframework.stereotype.Component;

/** The MCP face of {@link SourceService}. Tool classes stay thin: names, descriptions, and a call. */
@Component
public class SourceTools {

  private final SourceService sources;
  private final AuditLog audit;

  public SourceTools(SourceService sources, AuditLog audit) {
    this.sources = sources;
    this.audit = audit;
  }

  @McpTool(
      name = "list_sources",
      title = "List sources",
      description = """
          Lists the Kafka and MQTT brokers this server is configured to look at, and checks whether each one \
          answers right now. Call this first: the other tools take a source name from this list. \
          Read-only. A broker that does not answer is reported with reachable=false and the reason; \
          that is a result, not a failure of the tool.""",
      generateOutputSchema = true,
      annotations = @McpTool.McpAnnotations(title = "List sources", readOnlyHint = true, destructiveHint = false, idempotentHint = true, openWorldHint = true))
  public SourceService.SourceList listSources() {
    return audit.record("list_sources", AuditLog.args(), sources::listSources);
  }
}
