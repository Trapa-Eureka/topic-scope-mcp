# topic-scope-mcp

A read-only MCP server (Java 21, Spring Boot, Spring AI MCP) that shows an AI assistant the topics, recent messages and lag of Kafka and MQTT brokers.

`README.md` is the specification: what each view shows, the tools, the safety rules and the phases. Read the section you are working on before changing code.

## Always

- **The server only reads.** No publish, no offset commit, no consumer group, no admin write. Read Kafka records with `assign` + `seek`.
- **Only the local stack.** Never point a tool, a test or a command at a broker that is not from `compose.yaml`.
- **A feature is done when its check passes**, not when the code is written. Mark it Done in the README only then, and say so plainly when something was not tried.
- All files are in English, except `docs/*.ko.md`.

The first two are enforced by `.claude/hooks/block-dangerous.mjs`. If it blocks you, do not work around it; ask.

## Commands

- `node --test .claude/hooks/block-dangerous.test.mjs` — after any change to the hook
- `./mvnw -v` — the build tool (Maven through the wrapper; there is no system Maven)

## Procedures

Kept as skills in `.claude/skills/`, loaded only when needed:

- `phase` — how a roadmap phase is started, checked and handed over
