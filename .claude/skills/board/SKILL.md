---
name: board
description: Build, run, develop and demonstrate the web status board of topic-scope (the server's board mode plus the React page in board/). Use when changing the page or the WebSocket snapshot, when asked to show the board or take screenshots of it, or to demonstrate a consumer turning STALLED live.
---

# The status board

Two parts. The **server in board mode** (`--spring.profiles.active=board`, package `io.topicscope.board`) takes a picture of the brokers every interval and pushes it over a WebSocket at `/ws`. The **page** (`board/`, React + TypeScript + Vite) draws what it is given and never asks for a picture. The page can send one thing: which topic's newest messages it wants to watch.

## Run it

```bash
docker compose up -d --build --wait            # the local stack, see the local-stack skill
./mvnw -q -DskipTests install                  # the server jar
(cd board && npm ci && npm run build)          # the page, into board/dist
TOPIC_SCOPE_KAFKA_USERNAME=topicscope TOPIC_SCOPE_KAFKA_PASSWORD=topicscope-local-only \
TOPIC_SCOPE_MQTT_USERNAME=topicscope TOPIC_SCOPE_MQTT_PASSWORD=topicscope-local-only \
  java -jar server/target/topic-scope-server-0.1.0-SNAPSHOT.jar --spring.profiles.active=board
```

Then http://127.0.0.1:8090. Start it from the repository root: the page is served from `board/dist` relative to the working directory (`TOPIC_SCOPE_BOARD_FILES` changes that). `GET /` answering 404 means the page was not built. Other settings: `TOPIC_SCOPE_BOARD_PORT` (8090), `TOPIC_SCOPE_BOARD_INTERVAL` (5s).

## Develop the page

`cd board && npm run dev` serves the page on http://localhost:5173 with hot reload and proxies `/ws` to the server on 8090. The server only accepts WebSocket connections from its own pages, so start it with `TOPIC_SCOPE_BOARD_DEV_ORIGIN=http://localhost:5173`. `npm run typecheck` before building.

The shapes in `board/src/types.ts` mirror the Java records in `BoardSnapshots.java` and the tool results (`TopicRow`, `TopicLag`, `MessageRow`). Change both sides together; nothing generates one from the other.

## Demonstrate a change without a reload

```bash
docker compose exec -T mosquitto mosquitto_pub -h localhost -u simulator -P simulator-local-only -t demo/control/tracker -m stop
```

Within two intervals the `demo-tracker` card turns from LAGGING to STALLED and shows the time of the change. `-m start` lets it read again. The silent sensor (`rwy-27/rvr`) turns STALE about 70 s after the simulator starts; restart the simulator to see it happen (`docker compose restart simulator`).

## Rules that are easy to break

- The board asks through the same services as the tools (`TopicService`, `GroupService`, `MqttService`, `MessageService`). It must not use a broker client or a reader's client types itself; `ReadOnlyArchitectureTest` covers `..board..` too.
- The ticker thread is created in the bean's constructor with the caller's class loader. A thread from a shared pool does not see the classes in the packaged jar.
- Nothing is asked of the brokers while no page is open.
- The regular picture holds no message content. Content only goes out for a watched topic, formatted and redacted by `MessageService`, and each watch is a line in the audit log (`board_watch`).
- Local only: `server.address` 127.0.0.1, Host check, WebSocket origin check, no framing (`BoardWebConfiguration`). Do not add a route around the filter.
- A status must not be told by colour alone: every badge has its own shape and word (`STATUS` in `App.tsx`).

## Checks

`BoardIT` starts the jar in board mode against brokers with live traffic and reads the WebSocket: LAGGING, then STALLED after `live.holdTracker(true)` on the same connection, the silenced sensor STALE, a watched topic's messages, and the refusals for a foreign Host and a foreign Origin. What the page draws is checked by eye; there is no browser test.
