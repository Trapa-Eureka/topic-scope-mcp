# Topic Scope MCP

A **read-only** MCP server that lets an AI assistant look inside Kafka and MQTT brokers: which topics exist, what the latest messages are, and whether consumers are keeping up.

> **Status: Phases 0 to 4 of 9 done. Two of the three views work for Kafka: the topic list and recent messages.**
> What exists: an MCP server with four tools (`list_sources`, `list_topics`, `describe_topic`, `recent_messages`), a build that fails if server code could change a broker, and a [local stack](#local-stack-and-demo-data) of Kafka and Mosquitto with demo data known in advance. Lag and MQTT come in Phases 5 and 6. Every feature below is marked **Planned** until it has a test that proves it.

**Jump to:** [What it shows](#what-it-shows) · [Tools](#tools) · [Safety](#safety) · [Roadmap](#roadmap) · [Installation](#installation)

## Overview

In a messaging-based system (sensors publish over MQTT, services exchange events over Kafka) the first questions during development, testing and an incident are always the same:

1. **Is the topic there?** Did the service create it, with the partitions I expect?
2. **What is actually flowing?** Show me the last few messages, not what the documentation says.
3. **Is anyone falling behind?** Which consumer has a backlog, and how old is the newest message?

Answering them today means switching between `kafka-topics.sh`, `kafka-consumer-groups.sh`, `mosquitto_sub` and a broker UI. Topic Scope MCP exposes the same answers as MCP tools, so an assistant such as Claude Code can fetch them while it is helping you debug, write a test or review a change.

It is built for safety-critical surroundings: it **cannot publish, cannot commit offsets and cannot join a consumer group**. Looking must never change what is being looked at.

## What it shows

An MCP server has no screen of its own. "The screen" is the assistant's chat: you ask a question, the assistant calls a tool, and the result is shown as a table. Phase 8 adds an optional web status board that shows the same three views live.

### 1. Topic list

| | Kafka | MQTT |
|---|---|---|
| Where the list comes from | The broker's own catalog (Admin API) | **There is no catalog.** A topic exists only while messages are published to it, so the list is "topics seen while listening to a filter (default `#`) for a few seconds", plus retained messages |
| Columns | name, partitions, replication factor, internal or not, messages kept (end offset − start offset), time and age of newest message | topic, messages seen in the window, has a retained message or not, QoS, time of newest message |
| Says clearly | under-replicated partitions, and partitions without a leader (their messages are not counted) | that the list is an observation, with the window length |

"Messages kept" is an upper bound for compacted topics and topics written with transactions, where offsets have gaps. `describe_topic` says so in its note when the topic is compacted. An empty topic has no newest message: the time is empty and the age is -1.

### 2. Recent messages

| | Kafka | MQTT |
|---|---|---|
| What "recent" means | The newest *N* records stored in the topic (default 10, at most 100), newest first, read by position without a consumer group. Over all partitions, or one partition on request | **The broker keeps no history.** You get the retained message (if any) plus whatever arrives while listening for a few seconds |
| Columns | partition, offset, timestamp, key, headers, value, value format, value size, truncated or not, redacted or not | topic, receive time, QoS, retained flag, payload |
| Value display | see below | same |

How a value is shown (`valueFormat`):

| Format | When | Shown as |
|---|---|---|
| `json` | a JSON object or array | compact JSON. Numbers stay as written (`10.7130` is not rounded to `10.713`). A string longer than 500 characters inside it is shortened, so the rest of the document stays visible |
| `text` | any other valid UTF-8 without control characters | as it is |
| `binary` | everything else | hex of the first 64 bytes and the total size. The content is not interpreted |
| `none` | the record has no value (a tombstone) | empty |

Three things are applied to every value, key and header before it leaves the server:

- **Redaction.** A JSON field, a header or a `name=value` pair whose name says it holds a secret (`password`, `token`, `apiKey`, `authorization`, ...) gets `[REDACTED]` as its value. So do strings shaped like well-known keys and tokens (cloud access keys, JSON Web Tokens, private keys, `Bearer ...`), wherever they stand. This is a guess in both directions: a secret under an innocent name and in an unknown shape passes. **Treat what the tool returns as sensitive.**
- **A cap per value**: 2000 characters. What is cut is marked, and `valueBytes` tells the real size.
- **A budget per answer**: 40 000 characters of values. Once it is used up, the remaining (older) values are cut to a 200-character preview, and the note says so.

Records that arrive after the call started are not included; a busy topic would otherwise never finish.

### 3. Lag

"Lag" means **how far behind the reader is**. The two brokers measure it differently, and the tool says which one you are looking at.

| | Kafka | MQTT |
|---|---|---|
| Definition | **Consumer lag** = newest offset in a partition − offset the consumer group has committed. In messages | No offsets exist, so two substitutes: **freshness** = seconds since the last message on a topic; **delivery delay** = receive time − timestamp inside the payload (only when the payload has one) |
| Columns | group, state, topic, partition, committed offset, end offset, lag, members; total per group | topic, last seen, expected interval, freshness, status |
| Status | `OK` / `LAGGING` / `STALLED` (lag grows and the committed offset does not move between two samples) / `NO_CONSUMER` | `FRESH` / `STALE` (silent for longer than the expected interval) / `SILENT` (nothing in the window) |
| Also shown | estimated time lag: age of the oldest unread message | broker counters from `$SYS` (queued, dropped) when the broker publishes them |

### Example

```
You:    Is the tracker keeping up with the surveillance feed?

Claude: (calls consumer_lag, source "local-kafka", group "tracker")

        group: tracker   state: STABLE   members: 1   total lag: 1 842   status: LAGGING
        topic               partition  committed    end      lag   oldest unread
        surveillance.adsb       0        48 210    48 990    780      41 s
        surveillance.adsb       1        47 655    48 717  1 062      57 s

        The tracker is about one minute behind on partition 1, and the lag grew
        between the two samples. Partition 0 is behind by 41 seconds.
```

The numbers above are an illustration of the layout, not output from a real run.

## Tools

| Tool | What it returns | Status |
|---|---|---|
| `list_sources` | the configured brokers and whether each one answers: name, type, address, reachable, detail (what the broker says about itself, or why it could not be reached), response time | Done |
| `list_topics` | [topic list](#1-topic-list) for one source, with a name filter (`nameContains`) and internal topics on request (`includeInternal`). At most 200 rows, sorted by name | Done for Kafka; MQTT in Phase 6 |
| `describe_topic` | one Kafka topic in detail: per partition the leader, replicas, in-sync replicas, start and end offset, messages and time of the newest message; plus the settings that decide how long messages live, and every setting overridden on the topic | Done |
| `recent_messages` | [recent messages](#2-recent-messages) of one topic, with `limit` and an optional `partition` | Done for Kafka; MQTT in Phase 6 |
| `list_consumer_groups` | Kafka consumer groups with state and member count | Planned |
| `consumer_lag` | [lag](#3-lag) of one Kafka consumer group, sampled twice | Planned |
| `topic_freshness` | [freshness](#3-lag) of Kafka or MQTT topics against an expected interval | Planned |

There is no tool that writes. Every tool is announced to the client as read-only (`readOnlyHint`), and a test checks that for all of them. See [Safety](#safety).

A broker that does not answer is a result, not an error: `list_sources` then shows `reachable: false` and the reason, within the timeout (5 seconds by default).

When a call cannot be answered, the assistant gets two lines: what is wrong, and what would work. A wrong topic name is answered with the names that exist:

```
There is no topic named 'alarm' on source 'local-kafka'.
Did you mean: demo.alarms.
```

## Safety

Rules that matter are enforced by a mechanism and a test, not by a sentence in a document.

| Rule | How it is enforced |
|---|---|
| Never publish | The Kafka producer API and the MQTT publish call are not on the code path; an architecture test fails the build if any class references them |
| Never disturb a consumer group | Records are read with `assign` + `seek`, never `subscribe`; no `group.id` is set and auto-commit is off, so no group is joined and no offset is committed |
| Leave no trace on an MQTT broker | A random client id, a clean session and no last-will message, so nothing stays behind after disconnect |
| Only look where allowed | Topic allow and deny lists per source; a denied topic is refused by name and its payload is never read |
| Do not leak payloads | Size caps on every value, redaction of secret-looking fields, a hard limit on *N* and on listening time |
| Do not overload a broker | One short-lived connection per call, timeouts on every call |
| Leave a trail | Every tool call is appended to a local audit log: time, tool, source, topic. Never the payload |

What is in place today: everything except the allow and deny lists and the audit log (Phase 7).

- [`ReadOnlyArchitectureTest`](server/src/test/java/io/topicscope/ReadOnlyArchitectureTest.java) reads the compiled server and fails the build when it calls a broker-client method outside a short allow list of read methods (list, describe, assign, seek, poll, subscribe on MQTT). Each rule is also run against a deliberately bad class, to prove the rule can fail.
- [`KafkaMessagesIT`](server/src/test/java/io/topicscope/KafkaMessagesIT.java) reads every demo topic through `recent_messages` and then asks the broker itself: the set of consumer groups is the same as before, no committed offset has moved, no end offset has moved. It also asks for a topic that does not exist on a broker that creates topics on first use, and checks that the topic was not created.
- The Kafka consumer is built in one place, with no group, auto-commit off, and topic auto-creation off.

On the broker side, give the server an account that only has read permissions (Kafka ACLs: `Describe`, `DescribeConfigs`, `Read`; MQTT ACL: subscribe only). The server's own rules are the second line of defence, not the first.

## Requirements

- Java 21
- Docker, for the local brokers and for the integration tests (developed on Docker 29)
- An MCP client, for example Claude Code or Claude Desktop
- Node.js 20 or newer, only to run the development hook and its tests

Maven is not needed. The repository ships the Maven wrapper (`./mvnw`), which downloads Maven 3.9.16 on first use and checks it against a pinned SHA-256.

## Installation

There is no release yet. Build from source:

```bash
git clone https://github.com/Trapa-Eureka/topic-scope-mcp.git
cd topic-scope-mcp
./mvnw -DskipTests install            # builds server/target/topic-scope-server-0.1.0-SNAPSHOT.jar
docker compose up -d --build --wait   # Kafka on localhost:9092, Mosquitto on localhost:1883, demo data, sample traffic
```

The first run downloads dependencies and builds the simulator image, which takes a few minutes.

### Connect an MCP client

The server speaks MCP over stdin/stdout: the client starts it as a child process, and it exits when the client closes the pipe.

```
command: java
args:    -jar /absolute/path/to/topic-scope-mcp/server/target/topic-scope-server-0.1.0-SNAPSHOT.jar
```

For Claude Code, the repository ships a project-level [`.mcp.json`](.mcp.json) with exactly this (relative path, so start Claude Code in the repository folder and approve the server when asked). Then ask "which brokers can you see?".

What has been tried: the MCP Java SDK client (the end-to-end test `McpServerIT`), raw JSON-RPC over a pipe, and Claude Code, where the server connects through `.mcp.json` and the question "which brokers can you see?" is answered with both sources of the local stack. Claude Desktop has not been tried.

### Configuration

Out of the box the server looks at the local stack. Two environment variables move it:

| Variable | Default | Meaning |
|---|---|---|
| `TOPIC_SCOPE_KAFKA` | `localhost:9092` | bootstrap servers of the source `local-kafka` |
| `TOPIC_SCOPE_MQTT` | `tcp://localhost:1883` | URI of the source `local-mqtt` |

Two timeouts bound every call: `topic-scope.probe-timeout` (5 s, `list_sources`) and `topic-scope.request-timeout` (10 s, each question the other tools ask a broker). If `recent_messages` runs into it, it returns what it has and says so.

Three limits bound message content. A caller cannot raise them; only the configuration can: `topic-scope.limits.max-messages` (100), `max-value-chars` (2000) and `max-total-chars` (40000).

For other names or more brokers, write your own list and pass it with `--spring.config.additional-location=file:/path/to/topic-scope.yml`:

```yaml
topic-scope:
  sources:
    - name: tower-kafka
      type: kafka
      address: broker-1:9092,broker-2:9092
    - name: field-mqtt
      type: mqtt
      address: tcp://mqtt-host:1883
```

Brokers that need a login or TLS are not supported yet (Phase 7).

### Develop this repository

```bash
./mvnw verify                                        # unit tests + integration tests (these start their own brokers in Docker)
./mvnw verify -DskipITs                              # unit tests only, no Docker
node --test .claude/hooks/block-dangerous.test.mjs   # tests for the development safety hook
```

## Local stack and demo data

`compose.yaml` starts one Kafka broker, one Mosquitto broker and a simulator. The simulator first **seeds** the brokers with a fixed data set, then keeps **live** sample traffic flowing. All names, aircraft and sensors are made up.

```bash
docker compose up -d --build --wait                   # seed, then live traffic
SIM_MODE=seed docker compose up -d --build --wait     # seed only: the state below stays exact
docker compose logs simulator                         # what the simulator did
docker compose down -v                                # stop and forget everything
```

Every port is published on `127.0.0.1` only. Topics are never created automatically, so a typo in a topic name is an error.

### After seeding (exact)

| Kafka topic | Partitions | Records | What it is for |
|---|---|---|---|
| `demo.flights.positions` | 3 | 300 (100 each) | aircraft position reports from six aircraft |
| `demo.sensors.telemetry` | 2 | 40 (20 each) | sensor readings |
| `demo.alarms` | 1 | 12 | never changes; records carry headers |
| `demo.edge-cases` | 1 | 6 | one awkward record each: JSON, text without a key, 256 binary bytes, a 20 kB value, secret-looking fields, a record without a value |
| `demo.empty` | 1 | 0 | a topic that holds nothing |
| `demo.devices.state` | 1 | 8 | a compacted topic: each of the four sensors written twice, keyed by sensor |

| Consumer group | Topic | Committed | Lag |
|---|---|---|---|
| `demo-archiver` | positions | 100 per partition | 0 |
| `demo-tracker` | positions | 60 per partition | 120 |
| `demo-reporter` | positions | 20 per partition | 240 |
| `demo-alarm-notifier` | alarms | 7 | 5 |

MQTT holds four retained messages, one per sensor: `demo/sensors/<site>/<name>/status`.

### With live traffic (behaviour, not exact numbers)

- A position report arrives every 0.5 s. `demo-archiver` keeps up. `demo-tracker` reads one record a second, so it **moves and still falls further behind**. `demo-reporter` never reads again, so its **lag grows while its committed offset stands still**.
- Each sensor publishes a reading over MQTT every 2 s to `demo/sensors/<site>/<name>/telemetry`. A bridge copies every reading into `demo.sensors.telemetry` (the MQTT → Kafka path of a typical IoT pipeline).
- The sensor `rwy-27/rvr` **falls silent after 60 s** (`SIM_SILENT_AFTER_SECONDS`). Its retained status keeps saying "online".
- `demo.alarms`, `demo.edge-cases`, `demo.empty` and `demo.devices.state` are never written again.

The data set is defined in one place, [`DemoData.java`](simulator/src/main/java/io/topicscope/sim/DemoData.java). The integration test [`SeededStackIT`](simulator/src/test/java/io/topicscope/sim/SeededStackIT.java) starts real brokers and checks every number above, reads records back byte for byte, and checks the live behaviour. The tests of later phases compare the server's answers against the same constants.

## Safety hook

This repository ships a Claude Code `PreToolUse` hook (`.claude/hooks/block-dangerous.mjs`, configured in `.claude/settings.json`). While you develop in this repo with Claude Code, it blocks, before the action runs:

- **broker write APIs in the server's source** (`server/src/main/`): Kafka producer classes, offset commits, group subscription, admin create/delete/alter, MQTT publish and last-will. The simulator and the tests may publish; the server may not
- **broker command-line tools pointed at anything but the local stack** (`kafka-*.sh`, `kcat`, `mosquitto_pub`, `mosquitto_sub` with a host other than `localhost` or the compose service names)
- broad recursive deletes (`rm -rf ~`, `rm -rf /`, ...), git force pushes, `reset --hard`, `clean -fd`
- staging or writing secret files (`.env`, keys, keystores), and writing API-key-shaped strings or private keys into any file
- piping a downloaded script into a shell, `mvn deploy`, `docker push`, `docker ... prune`

It is a safety net, not a complete defense. It matches command text and the text being written, so a wrapped or indirect command can slip through, an edit is judged only by its new text, and a harmless line that merely contains a forbidden name can be blocked (a shell command that only *quotes* a forbidden command is refused too). The architecture test in Phase 2 is the check that fails the build.

## Project layout

`board/` and `docs/` do not exist yet. In `server/`, `safety/` holds redaction and the caps today; the allow and deny lists and the audit log come with Phase 7.

```
topic-scope-mcp/
├── server/        # Spring Boot + Spring AI MCP server (Java 21)
│   └── src/main/java/.../
│       ├── kafka/     # Admin API reads, positional record reads, lag
│       ├── mqtt/      # observation window, retained messages, freshness
│       ├── safety/    # allow/deny lists, redaction, caps, audit log
│       └── tools/     # the MCP tools; thin, no broker code
├── simulator/     # seeds the demo data, then publishes sample traffic; one slow consumer that creates lag on purpose. The only module that publishes
├── board/         # optional web status board (React, WebSocket) — Phase 8
├── compose.yaml   # Kafka (KRaft), Mosquitto and the simulator for local use
├── mosquitto/     # Mosquitto configuration shared by compose and the tests
├── docs/          # usage guide (English and Korean), screenshots
├── .claude/
│   ├── settings.json
│   ├── hooks/     # blocks dangerous commands, secrets and broker write APIs before they run
│   └── skills/    # procedures used only sometimes: how one phase is worked, how the local stack is run and checked, how a tool is added
├── .mcp.json      # registers the built server with Claude Code for this folder
├── pom.xml        # Maven parent: versions and test setup for all modules
├── mvnw, .mvn/    # Maven wrapper
└── CLAUDE.md      # a few lines that apply to every task
```

## Roadmap

Each phase ends with something that can be run and checked. A phase is done when its check passes, not when its code is written.

- [x] **Phase 0 — Harness and intent.** Git ignore rules, Maven wrapper, a short `CLAUDE.md`, a blocking hook with tests, a skill for how a phase is worked, this README as the specification. *Check: the hook tests pass.*
- [x] **Phase 1 — Local, deterministic brokers.** `compose.yaml` with Kafka and Mosquitto, the simulator with fixed sample data, a slow consumer that produces a known lag, Testcontainers wiring. *Check: one command brings up brokers whose topics, messages and lag are known in advance.* (Checked by `SeededStackIT`, and by hand against the compose stack with Kafka's own command-line tools.)
- [x] **Phase 2 — MCP server skeleton.** Spring Boot 4.1 + Spring AI 2.0 MCP server over stdio, source configuration, `list_sources`, the architecture test that forbids publishing. *Check: an MCP client lists the tools and `list_sources` reports both brokers.* (Checked by `McpServerIT`: the packaged jar, started as a child process, against real brokers.)
- [x] **Phase 3 — Kafka topic list.** `list_topics`, `describe_topic`. *Check: the result matches the topics the simulator created.* (Checked by `KafkaTopicsIT` against the seeded brokers, by `KafkaOfflinePartitionIT` on a two-broker cluster that loses a broker during the test, and by hand against the compose stack next to Kafka's own `kafka-get-offsets.sh`.)
- [x] **Phase 4 — Kafka recent messages.** `recent_messages` with positional reads, value formatting, caps and redaction. *Check: the last N records match what was published, and no consumer group appears on the broker afterwards.* (Checked by `KafkaMessagesIT` against the seeded brokers, and by hand against the compose stack with live traffic.)
- [ ] **Phase 5 — Kafka lag.** `list_consumer_groups`, `consumer_lag` with two samples and a status. *Check: the slow consumer is reported as `LAGGING`, a stopped one as `STALLED`.*
- [ ] **Phase 6 — MQTT.** Observation-window topic list, retained and live messages, `topic_freshness`. *Check: a sensor that the simulator silences turns `STALE`.*
- [ ] **Phase 7 — Safety hardening.** Allow and deny lists, audit log, timeouts, a read-only broker account in the local stack. *Check: a publish attempt with the server's account is refused by the broker itself.*
- [ ] **Phase 8 — Web status board (optional).** A small React page that shows the three views and updates over a WebSocket. *Check: stopping the slow consumer changes the board without a reload.*
- [ ] **Phase 9 — Release.** Docker image, HTTP transport, usage guide in English and Korean, screenshots, `CHANGELOG.md`, `SECURITY.md`, v0.1.0.

## Contributing

The project is at an early stage. Please open an issue to discuss a change before sending a pull request. All files in this repository are written in English, except the Korean usage guide.

## License

[MIT](LICENSE).
