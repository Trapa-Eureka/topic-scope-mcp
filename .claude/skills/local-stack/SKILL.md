---
name: local-stack
description: Start, check, reset or stop the local Kafka + Mosquitto stack with its demo data, and look up the numbers a seeded stack must show. Use before running or debugging anything that talks to a broker, when a test needs known broker state, or when the user asks to bring the brokers up or down.
---

# The local stack

`compose.yaml` runs Kafka (`localhost:9092`), Mosquitto (`localhost:1883`) and the simulator. Ports are on 127.0.0.1 only. Docker must be running.

Both brokers ask for a login. `simulator` / `simulator-local-only` may do everything; `topicscope` / `topicscope-local-only` may only look and is what the server uses (`.mcp.json` carries it; by hand set `TOPIC_SCOPE_KAFKA_USERNAME`, `TOPIC_SCOPE_KAFKA_PASSWORD`, `TOPIC_SCOPE_MQTT_USERNAME`, `TOPIC_SCOPE_MQTT_PASSWORD`). The passwords are demo values, not secrets. The integration tests mostly use brokers without accounts (`mosquitto/open.conf`, plain `KafkaContainer`); `SecuredStackIT` uses the compose settings.

## Commands

| I want | Command |
|---|---|
| brokers with sample traffic flowing | `docker compose up -d --build --wait` |
| brokers frozen in the exact seeded state | `SIM_MODE=seed docker compose up -d --build --wait` |
| the silent sensor to stop sooner (default 60 s) | add `SIM_SILENT_AFTER_SECONDS=15` in front |
| to see what the simulator did | `docker compose logs simulator` |
| an empty start | `docker compose down -v`, then up again |
| the stack plus the status board in a container (http://127.0.0.1:8090) | `docker compose --profile board up -d --build --wait` |
| the automated check | `./mvnw verify` (starts its own brokers with Testcontainers; the compose stack need not run) |

Switching from seed to live needs no reset: the simulator sees the seed and only starts the traffic. Going back from live to the exact state does need `docker compose down -v`.

## What a seeded stack holds

The single source of truth is `simulator/src/main/java/io/topicscope/sim/DemoData.java`. Compare against its constants in tests; do not copy numbers into test code.

| Kafka topic | Partitions | Records per partition | Live mode |
|---|---|---|---|
| `demo.flights.positions` | 3 | 100 | +2 records a second |
| `demo.sensors.telemetry` | 2 | 20 | fed by the MQTT bridge |
| `demo.alarms` | 1 | 12 | never changes |
| `demo.edge-cases` | 1 | 6 (JSON, text without key, binary, oversized, secret-looking, tombstone) | never changes |
| `demo.empty` | 1 | 0 | never changes |
| `demo.devices.state` | 1 | 8 (compacted: four sensors, each written twice) | never written again |

| Consumer group | Topic | Seeded lag | Live mode | `consumer_lag` status, seeded / live |
|---|---|---|---|---|
| `demo-archiver` | positions | 0 | keeps up | `OK` / `OK` |
| `demo-tracker` | positions | 120 | moves, falls further behind (reads 1 a second) | `NO_CONSUMER` / `LAGGING` |
| `demo-reporter` | positions | 240 | committed offset stands still, lag grows | `NO_CONSUMER` / `STALLED` |
| `demo-alarm-notifier` | alarms | 5 | constant | `NO_CONSUMER` / `NO_CONSUMER` |

On a seeded stack nobody is connected and nothing arrives, so every backlog is `NO_CONSUMER`. The statuses that depend on movement need live traffic.

A switch for demonstrations: `mosquitto_pub ... -t demo/control/tracker -m stop` holds `demo-tracker` (it stays connected and stops reading, so it turns `STALLED`); `-m start` releases it. In tests: `live.holdTracker(true)`.

MQTT: four retained messages `demo/sensors/<site>/<name>/status`. In live mode each sensor publishes `.../telemetry` every 2 s (not retained); `rwy-27/rvr` stops after 60 s.

What the MQTT tools show: on a seeded stack `list_topics` lists the four status topics (retained, 0 messages) and every telemetry topic is `SILENT`. In live mode `topic_freshness` with `expectedIntervalSeconds: 2` reports three telemetry topics `FRESH`; `rwy-27/rvr` is `STALE` only in a window that covers the moment it stops, and after that it is missing behind a wildcard and `SILENT` when named exactly. To watch it turn stale, start with `SIM_SILENT_AFTER_SECONDS=20` and call `topic_freshness` with `windowSeconds: 30` right away.

## Looking with the brokers' own tools

Use these to cross-check what the server reports. They run inside the containers, so they only ever reach the local stack.

```bash
C="--bootstrap-server localhost:9092 --command-config /etc/kafka-client/local-admin.properties"
docker compose exec -T kafka /opt/kafka/bin/kafka-get-offsets.sh $C --topic-partitions 'demo.*'
docker compose exec -T kafka /opt/kafka/bin/kafka-consumer-groups.sh $C --describe --all-groups
docker compose exec -T kafka /opt/kafka/bin/kafka-acls.sh $C --list      # the rights of the look-only account
docker compose exec -T mosquitto mosquitto_sub -h localhost -u simulator -P simulator-local-only -t 'demo/#' -v -W 3
```

(In zsh an unquoted `$C` is not split into words; write the options out or use `${=C}`.)

## When something is off

- **"The broker is half seeded"** in the simulator log: a seed was interrupted. `docker compose down -v` and start again.
- **Every tool says "no answer within the timeout" or "Not authorized"**: the server has no login, or a wrong one. A Kafka listener that asks for a login does not answer a client that sends none.
- **A change to `compose.yaml` or to the account files is not picked up**: `docker compose up` recreates the broker only when its definition changed; a recreated broker is empty and is seeded again.
- **Port 9092 or 1883 already in use**: another broker is running on this machine. Stop it; do not change the ports here without changing the README too.
- **Image build fails on "Failed to validate Maven distribution SHA-256"**: the build image lost `unzip` (see the comment in `simulator/Dockerfile`).
- **A topic from `DemoData` is missing on a stack that was already running**: the stack was seeded before the topic was added, and a seeded broker is left alone. `docker compose down -v`, then up again.
- Changing `DemoData.java` changes what every later test expects. Run `./mvnw verify` and update the tables here and in the README.
