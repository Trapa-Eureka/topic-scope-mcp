# topic-scope-mcp

A read-only MCP server (Java 21, Spring Boot, Spring AI MCP) that shows an AI assistant the topics, recent messages and lag of Kafka and MQTT brokers.

`README.md` is the specification: what each view shows, the tools, the safety rules and the phases. Read the section you are working on before changing code.

## Always

- **The server only reads.** No publish, no offset commit, no consumer group, no admin write. Read Kafka records with `assign` + `seek`.
- **Only the local stack.** Never point a tool, a test or a command at a broker that is not from `compose.yaml`.
- **A feature is done when its check passes**, not when the code is written. Mark it Done in the README only then, and say so plainly when something was not tried.
- All files are in English, except `docs/*.ko.md`.

- **Nothing prints to stdout in the server.** It carries the MCP protocol; logs go to stderr.

The first two are enforced by `.claude/hooks/block-dangerous.mjs`. If it blocks you, do not work around it; ask.

## Commands

- `./mvnw verify` — unit tests and integration tests (`*IT` start their own brokers; Docker must run). `-DskipITs` for unit tests only. There is no system Maven; always use the wrapper
- `node --test .claude/hooks/block-dangerous.test.mjs` — after any change to the hook
- `docker compose up -d --build --wait` — the local stack

## Layout

- `simulator/` — seeds and feeds the local brokers. The only module that may publish. `DemoData.java` is the single source of the demo data; tests compare against its constants
- `server/` — the MCP server. Broker access only in `kafka/` and `mqtt/`; `tools/` stays thin. `ReadOnlyArchitectureTest` fails the build on any broker-client call outside its allow lists

## Procedures

Kept as skills in `.claude/skills/`, loaded only when needed:

- `phase` — how a roadmap phase is started, checked and handed over
- `local-stack` — run, reset and cross-check the local brokers; what a seeded stack holds
- `add-tool` — where a tool's code goes, the annotations and tests every tool needs, pitfalls of the packaged jar, talking to the server by hand
