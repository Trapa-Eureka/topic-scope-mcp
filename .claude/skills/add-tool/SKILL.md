---
name: add-tool
description: How to add or change an MCP tool in the topic-scope server and prove it end to end - where the code goes, the annotations every tool carries, the read-only allow lists, the tests, and how to talk to the packaged server by hand. Use when implementing a tool from the README's Tools table or debugging why a tool call hangs, fails or returns the wrong shape.
---

# Adding a tool

Stack: Spring Boot 4.1 + Spring AI 2.0 (`spring-ai-starter-mcp-server`), MCP over stdio. Versions are pinned in the root `pom.xml`; Spring AI, Spring Boot and the MCP SDK change together.

## Where code goes

| Package | Holds | May use broker clients |
|---|---|---|
| `io.topicscope.kafka`, `io.topicscope.mqtt` | everything that talks to a broker | yes, read methods only |
| `io.topicscope.tools` | `*Tools` (the `@McpTool` methods) and `*Service` (shapes the result) | no |
| `io.topicscope.config` | `topic-scope.*` properties | no |

## The tool method

```java
@McpTool(
    name = "snake_case_name",
    title = "Human title",
    description = "What it returns, when to call it, what a caller should know. Say it is read-only.",
    generateOutputSchema = true,
    annotations = @McpTool.McpAnnotations(title = "Human title", readOnlyHint = true, destructiveHint = false, idempotentHint = true, openWorldHint = true))
public SomeRecord name(@McpToolParam(description = "...", required = true) String source) { ... }
```

- Imports are `org.springframework.ai.mcp.annotation.*` (not the old `org.springaicommunity` package).
- `McpServerIT` asserts for **every** tool: a description, `readOnlyHint = true`, `destructiveHint = false`, an output schema.
- Return a record. **No field may be null**: the generated schema marks every field required, so use `""`, `0` or an empty list, and say in the Javadoc what empty means.
- A broker problem is a result (a row, a status, a message), not an exception. Since Spring AI 2.0.1 a `RuntimeException` becomes an error result the model sees, but an `Error` kills the call without any answer.
- Tool input that is wrong (unknown source, unknown topic) should answer with what is valid, for example the list of source names.

## Things that only break in the packaged jar

- **No `parallelStream()` and no `ForkJoinPool.commonPool()` around broker clients.** Those threads cannot see classes inside the Spring Boot jar; the Kafka client then dies with a `LinkageError` and the call never answers. Use a thread pool whose threads get the caller's context class loader (see `SourceService.checkAll`).
- **Nothing may print to stdout.** Logging goes to stderr through `logback-spring.xml`.

## Read-only allow lists

`ReadOnlyArchitectureTest` lists the broker-client methods the server may call (`CONSUMER_READS`, `ADMIN_READS`, `MQTT_READS`). If a new tool needs a method that is not listed, the build fails. Add the method only if it cannot change the broker, and say why in the commit. Never add: `subscribe`/`commit*`/`committed` on a Kafka consumer, any `create*`/`delete*`/`alter*` on the admin client, `publish` on MQTT. Kafka records are read with `assign` + `seek`, without a `group.id`, with auto-commit off.

## Tests, in this order

1. Unit test for the pure parts (formatting, status rules) next to the class.
2. A case in `McpServerIT` (or a new `*IT` built the same way): start the jar through the MCP client, seed the brokers with `Seeder.seed(...)` from the simulator, and compare the answer with the constants in `DemoData`. Do not copy numbers into the test.
3. `./mvnw verify`.

## Talking to the packaged server by hand

Brokers from the `local-stack` skill, then:

```bash
./mvnw -q -DskipTests install
( printf '%s\n' \
  '{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-06-18","capabilities":{},"clientInfo":{"name":"hand","version":"0"}}}' \
  '{"jsonrpc":"2.0","method":"notifications/initialized"}' \
  '{"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"list_sources","arguments":{}}}'; sleep 8 ) \
  | java -jar server/target/topic-scope-server-0.1.0-SNAPSHOT.jar
```

One JSON line per answer on stdout. The `sleep` keeps stdin open long enough for the answer; the server exits by itself when stdin closes. If a call gets no answer at all, take a thread dump (`jstack <pid>`) and read stderr: look for a `LinkageError`.
