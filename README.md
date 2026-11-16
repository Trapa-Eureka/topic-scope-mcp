# Topic Scope MCP

A **read-only** MCP server that lets an AI assistant look inside Kafka and MQTT brokers: which topics exist, what the latest messages are, and whether consumers are keeping up.

> **Status: Phase 0 of 9 done. No tool works yet.**
> The repository has its development harness (safety hook with tests, Maven wrapper, agent guidance) and this README as the agreed intent: what the server will show, how it stays safe, and the phases it is built in. Every feature below is marked **Planned** until it has a test that proves it.

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
- Docker, for the local brokers and for the integration tests (from Phase 1)
- An MCP client, for example Claude Code or Claude Desktop
- Node.js 20 or newer, only to run the development hook and its tests

Maven is not needed. The repository ships the Maven wrapper (`./mvnw`), which downloads Maven 3.9.16 on first use and checks it against a pinned SHA-256.

## Installation

Not available yet. From Phase 2 on, the planned steps are:

```bash
git clone https://github.com/Trapa-Eureka/topic-scope-mcp.git
cd topic-scope-mcp
./mvnw package
docker compose up -d      # local Kafka + Mosquitto with simulated traffic
```

and then registering the built server with your MCP client. The exact commands are written here when the phase is done and has been tried.

### Develop this repository

```bash
./mvnw -v                                            # Maven through the wrapper (there is nothing to build before Phase 2)
node --test .claude/hooks/block-dangerous.test.mjs   # tests for the development safety hook
```

## Safety hook

This repository ships a Claude Code `PreToolUse` hook (`.claude/hooks/block-dangerous.mjs`, configured in `.claude/settings.json`). While you develop in this repo with Claude Code, it blocks, before the action runs:

- **broker write APIs in the server's source** (`server/src/main/`): Kafka producer classes, offset commits, group subscription, admin create/delete/alter, MQTT publish and last-will. The simulator and the tests may publish; the server may not
- **broker command-line tools pointed at anything but the local stack** (`kafka-*.sh`, `kcat`, `mosquitto_pub`, `mosquitto_sub` with a host other than `localhost` or the compose service names)
- broad recursive deletes (`rm -rf ~`, `rm -rf /`, ...), git force pushes, `reset --hard`, `clean -fd`
- staging or writing secret files (`.env`, keys, keystores), and writing API-key-shaped strings or private keys into any file
- piping a downloaded script into a shell, `mvn deploy`, `docker push`, `docker ... prune`

It is a safety net, not a complete defense. It matches command text and the text being written, so a wrapped or indirect command can slip through, an edit is judged only by its new text, and a harmless line that merely contains a forbidden name can be blocked (a shell command that only *quotes* a forbidden command is refused too). The architecture test in Phase 2 is the check that fails the build.

## Project layout (planned)

```
topic-scope-mcp/
├── server/        # Spring Boot + Spring AI MCP server (Java 21)
│   └── src/main/java/.../
│       ├── kafka/     # Admin API reads, positional record reads, lag
│       ├── mqtt/      # observation window, retained messages, freshness
│       ├── safety/    # allow/deny lists, redaction, caps, audit log
│       └── tools/     # the MCP tools; thin, no broker code
├── simulator/     # publishes sample sensor and aircraft-position messages; one slow consumer that creates lag on purpose
├── board/         # optional web status board (React, WebSocket) — Phase 8
├── compose.yaml   # Kafka (KRaft) and Mosquitto for local use
├── docs/          # usage guide (English and Korean), screenshots
├── .claude/
│   ├── settings.json
│   ├── hooks/     # blocks dangerous commands, secrets and broker write APIs before they run
│   └── skills/    # procedures used only sometimes; today: how one phase is worked and handed over
├── mvnw, .mvn/    # Maven wrapper
└── CLAUDE.md      # a few lines that apply to every task
```

## Roadmap

Each phase ends with something that can be run and checked. A phase is done when its check passes, not when its code is written.

- [x] **Phase 0 — Harness and intent.** Git ignore rules, Maven wrapper, a short `CLAUDE.md`, a blocking hook with tests, a skill for how a phase is worked, this README as the specification. *Check: the hook tests pass.*
- [ ] **Phase 1 — Local, deterministic brokers.** `compose.yaml` with Kafka and Mosquitto, the simulator with fixed sample data, a slow consumer that produces a known lag, Testcontainers wiring. *Check: one command brings up brokers whose topics, messages and lag are known in advance.*
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
