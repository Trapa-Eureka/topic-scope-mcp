# Changelog

All notable changes to this project are written here. The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and versions follow [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

Until 1.0.0 the names of tools, arguments, result fields and settings may still change between minor versions; such changes are listed under **Changed**.

## [Unreleased]

### Changed

- Documentation only: HTTP mode has now been tried with Claude Code 2.1.325 as the client (connected, seven tools, three tool calls answered; refused with a wrong token). The usage guide shows the configuration. The 0.1.0 notes below still say it had not been tried, which was true when 0.1.0 was released.
- Documentation only: the Docker image has now been tried as an MCP server started by Claude Code 2.1.325 over stdin/stdout, with the configuration from the usage guide. The guide also says how to keep such a container's audit log.

## [0.1.0] - 2026-11-16

The first release. A read-only MCP server that lets an AI assistant look inside Kafka and MQTT brokers, with a web status board and a local stack to try it on.

### Added

- **Seven MCP tools**, all announced to the client as read-only:
  - `list_sources`: the configured brokers and whether each one answers.
  - `list_topics`: Kafka topics from the broker's catalog; MQTT topics as heard while listening on a filter.
  - `describe_topic`: one Kafka topic's partitions, offsets, replicas and the settings that decide how long messages live.
  - `recent_messages`: the newest records of a Kafka topic, read by position without a consumer group; for MQTT the retained messages and what is published while listening.
  - `list_consumer_groups`: Kafka consumer groups with state and members.
  - `consumer_lag`: a group's lag, sampled twice, with a status per topic: `OK`, `LAGGING`, `STALLED`, `NO_CONSUMER`.
  - `topic_freshness`: whether messages still arrive at an expected interval, for Kafka and MQTT: `FRESH`, `STALE`, `SILENT`.
- **Three ways to run the same program**: MCP over stdin/stdout (the default), MCP over Streamable HTTP with a required bearer token (`http` profile), and a web status board fed over a WebSocket (`board` profile).
- **Status board**: a React page with consumer lag cards, Kafka and MQTT topic tables and the newest messages of a selected topic. It remembers MQTT topics it has heard, so a sensor that falls silent turns `STALE` instead of disappearing.
- **Message display**: values as compact JSON, text or a hex preview; a cap per value and a budget per answer; redaction of secret-looking fields and of strings shaped like well-known keys and tokens.
- **Broker logins**: SASL for Kafka (PLAIN by default; SCRAM and TLS through `properties`), user name and password for MQTT over `tcp://` or `ssl://`.
- **Topic allow and deny lists** per source.
- **Audit log**: one line per tool call and per topic opened on the board, with the arguments and without any message content. A call that cannot be recorded is not answered.
- **Local stack** (`compose.yaml`): Kafka and Mosquitto with accounts, a simulator that seeds known demo data and keeps sample traffic flowing, a consumer that falls behind and one that has stopped, a sensor that falls silent, and a switch to stop the slow consumer on demand. The server's account there can only read, enforced by the brokers.
- **Docker image** for all three modes, running as a non-root user.
- Usage guide in English and Korean.

### Security

- The server cannot publish, commit offsets, join a consumer group or change a broker. A test reads the compiled classes and fails the build when server code calls a broker-client method outside a short allow list.
- Kafka records are read with `assign` and `seek`, without a group, with auto-commit and topic auto-creation off. A source's configuration can add client settings for encryption and login only.
- An integration test logs in with the server's look-only account and tries to write to Kafka and MQTT; the brokers refuse.
- HTTP mode does not start without a token of at least 24 characters, compares it in constant time, and refuses requests that come from a web page unless their origin is listed.
- The status board listens on 127.0.0.1, refuses foreign host names and foreign WebSocket origins, and cannot be framed.
- Passwords and the HTTP token are not part of any answer, error message, log line or audit line.

### Known limitations

- TLS, SCRAM and `ssl://` are implemented but have only been tried as far as their settings; the brokers used in the tests take SASL/PLAIN and a plain user name and password.
- HTTP mode has one shared token, not user accounts, and does not encrypt the connection.
- The status board has no login and is meant for one machine.
- On MQTT, a message on a denied topic that the broker delivers is dropped on arrival; keeping it from being delivered is the broker's job.
- Redaction is a best-effort guess in both directions.
- `consumer_lag` judges from a sample of a few seconds: a consumer that commits less often than that looks `STALLED`.
- Only Claude Code has been tried as an MCP client, over stdin/stdout.
- No download yet: the jar and the image are built from source.

[0.1.0]: https://github.com/Trapa-Eureka/topic-scope-mcp/releases/tag/v0.1.0
