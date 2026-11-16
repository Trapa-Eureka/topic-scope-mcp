# Topic Scope MCP

A **read-only** MCP server that lets an AI assistant look inside Kafka and MQTT brokers: which topics exist, what the latest messages are, and whether consumers are keeping up.

> **Status: Phases 0 and 1 of 9 done. No tool works yet.**
> What exists: the development harness (safety hook, Maven wrapper, agent guidance) and a [local stack](#local-stack-and-demo-data) of Kafka and Mosquitto filled with demo data whose topics, messages and lag are known in advance. The MCP server itself starts in Phase 2. Every feature below is marked **Planned** until it has a test that proves it.

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
| Columns | name, partitions, replication factor, internal or not, messages kept (end offset − start offset), time of newest message | topic, messages seen in the window, has a retained message or not, QoS, time of newest message |
| Says clearly | under-replicated partitions | that the list is an observation, with the window length |

### 2. Recent messages

| | Kafka | MQTT |
|---|---|---|
| What "recent" means | The last *N* records stored in the topic (default 10), read by position without a consumer group | **The broker keeps no history.** You get the retained message (if any) plus whatever arrives while listening for a few seconds |
| Columns | partition, offset, timestamp, key, headers, value | topic, receive time, QoS, retained flag, payload |
| Value display | JSON pretty-printed, other text as is, binary as a hex preview with its size; long values cut with a marker; secret-looking fields replaced with `[REDACTED]` | same |

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
| `list_sources` | the configured brokers and whether each one answers | Planned |
| `list_topics` | [topic list](#1-topic-list) for one source, with a name filter | Planned |
| `describe_topic` | one Kafka topic in detail: partitions, leaders, replicas, offsets, retention settings | Planned |
| `recent_messages` | [recent messages](#2-recent-messages) of one topic | Planned |
| `list_consumer_groups` | Kafka consumer groups with state and member count | Planned |
| `consumer_lag` | [lag](#3-lag) of one Kafka consumer group, sampled twice | Planned |
| `topic_freshness` | [freshness](#3-lag) of Kafka or MQTT topics against an expected interval | Planned |

There is no tool that writes. See [Safety](#safety).

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

On the broker side, give the server an account that only has read permissions (Kafka ACLs: `Describe`, `DescribeConfigs`, `Read`; MQTT ACL: subscribe only). The server's own rules are the second line of defence, not the first.

## Requirements

- Java 21
- Docker, for the local brokers and for the integration tests (developed on Docker 29)
- An MCP client, for example Claude Code or Claude Desktop
- Node.js 20 or newer, only to run the development hook and its tests

Maven is not needed. The repository ships the Maven wrapper (`./mvnw`), which downloads Maven 3.9.16 on first use and checks it against a pinned SHA-256.

## Installation

The server is not available yet (Phase 2). What works today is the local stack:

```bash
git clone https://github.com/Trapa-Eureka/topic-scope-mcp.git
cd topic-scope-mcp
docker compose up -d --build --wait   # Kafka on localhost:9092, Mosquitto on localhost:1883, demo data, sample traffic
```

The first run builds the simulator image and downloads its dependencies, which takes a few minutes. Registering the server with an MCP client is written here when Phase 2 is done and has been tried.

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
- `demo.alarms`, `demo.edge-cases` and `demo.empty` never change.

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

`server/`, `board/` and `docs/` do not exist yet.

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
│   └── skills/    # procedures used only sometimes: how one phase is worked, how the local stack is run and checked
├── pom.xml        # Maven parent: versions and test setup for all modules
├── mvnw, .mvn/    # Maven wrapper
└── CLAUDE.md      # a few lines that apply to every task
```

## Roadmap

Each phase ends with something that can be run and checked. A phase is done when its check passes, not when its code is written.

- [x] **Phase 0 — Harness and intent.** Git ignore rules, Maven wrapper, a short `CLAUDE.md`, a blocking hook with tests, a skill for how a phase is worked, this README as the specification. *Check: the hook tests pass.*
- [x] **Phase 1 — Local, deterministic brokers.** `compose.yaml` with Kafka and Mosquitto, the simulator with fixed sample data, a slow consumer that produces a known lag, Testcontainers wiring. *Check: one command brings up brokers whose topics, messages and lag are known in advance.* (Checked by `SeededStackIT`, and by hand against the compose stack with Kafka's own command-line tools.)
- [ ] **Phase 2 — MCP server skeleton.** Spring Boot + Spring AI MCP server over stdio, source configuration, `list_sources`, the architecture test that forbids publishing. *Check: an MCP client lists the tools and `list_sources` reports both brokers.*
- [ ] **Phase 3 — Kafka topic list.** `list_topics`, `describe_topic`. *Check: the result matches the topics the simulator created.*
- [ ] **Phase 4 — Kafka recent messages.** `recent_messages` with positional reads, value formatting, caps and redaction. *Check: the last N records match what was published, and no consumer group appears on the broker afterwards.*
- [ ] **Phase 5 — Kafka lag.** `list_consumer_groups`, `consumer_lag` with two samples and a status. *Check: the slow consumer is reported as `LAGGING`, a stopped one as `STALLED`.*
- [ ] **Phase 6 — MQTT.** Observation-window topic list, retained and live messages, `topic_freshness`. *Check: a sensor that the simulator silences turns `STALE`.*
- [ ] **Phase 7 — Safety hardening.** Allow and deny lists, audit log, timeouts, a read-only broker account in the local stack. *Check: a publish attempt with the server's account is refused by the broker itself.*
- [ ] **Phase 8 — Web status board (optional).** A small React page that shows the three views and updates over a WebSocket. *Check: stopping the slow consumer changes the board without a reload.*
- [ ] **Phase 9 — Release.** Docker image, HTTP transport, usage guide in English and Korean, screenshots, `CHANGELOG.md`, `SECURITY.md`, v0.1.0.

## Contributing

The project is at an early stage. Please open an issue to discuss a change before sending a pull request. All files in this repository are written in English, except the Korean usage guide.

## License

[MIT](LICENSE).
