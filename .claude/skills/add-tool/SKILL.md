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
| `io.topicscope.tools` | `*Tools` (the `@McpTool` methods) and `*Service` (shapes the result) | no, not even their types: readers return their own records |
| `io.topicscope.safety` | `PayloadFormatter` (bytes to a capped, redacted display) and `Redactor` | no |
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
- Resolve the source with `Sources.require(name)` and wrap every broker question in `Sources.ask(source, () -> ...)`. It turns any failure, including a `LinkageError`, into a `ToolProblem`. An `Error` that escapes kills the call without any answer.
- A call that cannot be answered throws `new ToolProblem(problem, hint)`: one sentence for what is wrong, one for what would work (the names that exist, the tool to call instead). Spring AI prints the message, a line break and the root cause's message, so a plain exception shows its message twice; `ToolProblem` carries the hint as its cause for that reason. Never throw anything else on purpose.
- `list_sources` is the exception: there an unreachable broker is a row, because reachability is the answer.
- Times are ISO-8601 in UTC to the second (`TopicService.time`); "no time" is `""`, "no number" is `-1`.
- **Message content never leaves raw.** Every value, key and header goes through `PayloadFormatter.format(bytes, maxChars)`; a header whose name is `Redactor.secretName` is replaced whole. Keep a per-answer budget (see `MessageService.rows`). Readers return bytes; only the service formats.
- Kafka clients come from `KafkaClients` only. Its consumer has no group, auto-commit off and topic auto-creation off; do not build one anywhere else, and do not add `group.id`.
- Reading a live topic must end: take the end offsets once and stop there (`KafkaRecordReader.tail`), judge "finished" by `position`, not by the last record seen, and keep only what is needed in memory.
- A pattern that scans payload text must be linear. A name-like run with `*` that is retried from each character took seconds on 20 kB; `PayloadFormatterTest.aLargePayloadIsFormattedQuickly` guards it.
- Anything judged from movement needs two looks on one connection (`KafkaGroupReader.sampleTwice`) and a rule that is a pure function with its own tests (`LagStatus.judge`). Say in the answer how long the sample really was.
- A test that needs things to move gets brokers of its own with `LiveTraffic` (see `KafkaLagIT`); the shared brokers stay frozen.
- Cap every list (see `TopicService.MAX_TOPICS`) and say in a `note` field when something was cut or is worth knowing. The note is `""` when there is nothing to say.

## Things that only break in the packaged jar

- **No `parallelStream()` and no `ForkJoinPool.commonPool()` around broker clients.** Those threads cannot see classes inside the Spring Boot jar; the Kafka client then dies with a `LinkageError` and the call never answers. Use a thread pool whose threads get the caller's context class loader (see `SourceService.checkAll`).
- **Nothing may print to stdout.** Logging goes to stderr through `logback-spring.xml`.

## Read-only allow lists

`ReadOnlyArchitectureTest` lists the broker-client methods the server may call (`CONSUMER_READS`, `ADMIN_READS`, `MQTT_READS`). If a new tool needs a method that is not listed, the build fails. Add the method only if it cannot change the broker, and say why in the commit. Never add: `subscribe`/`commit*`/`committed` on a Kafka consumer, any `create*`/`delete*`/`alter*` on the admin client, `publish` on MQTT. Kafka records are read with `assign` + `seek`, without a `group.id`, with auto-commit off.

## Tests, in this order

1. Unit test for the pure parts (formatting, status rules) next to the class.
2. A new `*IT` built like `KafkaTopicsIT`: `McpTestSupport.connect()` starts the packaged jar as a child process against brokers that are already seeded (once per run, seed only, no live traffic); `call(...)` returns the structured result, `callExpectingProblem(...)` the error text. Compare with the constants in `DemoData`; do not copy numbers into the test. Add the tool's name to the list in `McpServerIT`. Tests must not change the brokers, they are shared.
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
