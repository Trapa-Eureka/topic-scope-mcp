# Using Topic Scope

한국어: [usage.ko.md](usage.ko.md)

New here? The [walkthrough](walkthrough.md) goes through everything once, step by step, with real answers and pictures. This page is the reference.

Topic Scope is one program with three ways of running it. All three use the same read-only code, the same configuration and the same audit log.

| Mode | Start it with | Use it when |
|---|---|---|
| [MCP over stdin/stdout](#mcp-over-stdinstdout) | nothing extra (the default) | an assistant such as Claude Code starts the server itself, on your machine |
| [MCP over HTTP](#mcp-over-http) | `--spring.profiles.active=http` | the client cannot start a child process, or several people share one server |
| [Status board](#status-board) | `--spring.profiles.active=board` | you want to watch the brokers on a web page |

**Contents:** [Get it](#get-it) · [Try it on the local stack](#try-it-on-the-local-stack) · [MCP over stdin/stdout](#mcp-over-stdinstdout) · [MCP over HTTP](#mcp-over-http) · [Status board](#status-board) · [The tools](#the-tools) · [Your own brokers](#your-own-brokers) · [The audit log](#the-audit-log) · [When something does not work](#when-something-does-not-work)

## Get it

You need Java 21 to run the jar, or Docker to run the image. There is no download yet; build one of the two:

```bash
git clone https://github.com/Trapa-Eureka/topic-scope-mcp.git
cd topic-scope-mcp

./mvnw -DskipTests install                 # the jar: server/target/topic-scope-server-0.1.1.jar (a few seconds after the first run)
docker build -t topic-scope-mcp:0.1.1 .    # or the image (several minutes the first time)
```

The jar needs nothing else for the two MCP modes. For the status board from the jar, build the page once as well: `(cd board && npm ci && npm run build)` (Node.js 20 or newer). The image already contains it.

## Try it on the local stack

The repository brings brokers to try it on: Kafka and Mosquitto with demo data and sample traffic, on this machine only.

```bash
docker compose up -d --build --wait
```

Both brokers ask for a login. The server's account is `topicscope` with the password `topicscope-local-only`; it may look and nothing else. (These are demo values and not secrets.) Every example below uses the local stack. `docker compose down -v` stops it and forgets everything.

## MCP over stdin/stdout

The client starts the server as a child process and talks to it through its standard input and output. The server exits when the client closes the pipe.

### Claude Code

The repository ships a [`.mcp.json`](../.mcp.json) for the local stack. Start Claude Code in the repository folder and approve the server `topic-scope` when asked (choose "use this MCP server", not "all future servers"). `/mcp` then shows it as connected. This has been tried.

For another project, or other brokers, put the same into that project's `.mcp.json`, with an absolute path:

```json
{
  "mcpServers": {
    "topic-scope": {
      "command": "java",
      "args": ["-jar", "/absolute/path/to/topic-scope-server-0.1.1.jar"],
      "env": {
        "TOPIC_SCOPE_KAFKA": "localhost:9092",
        "TOPIC_SCOPE_KAFKA_USERNAME": "topicscope",
        "TOPIC_SCOPE_KAFKA_PASSWORD": "topicscope-local-only",
        "TOPIC_SCOPE_MQTT": "tcp://localhost:1883",
        "TOPIC_SCOPE_MQTT_USERNAME": "topicscope",
        "TOPIC_SCOPE_MQTT_PASSWORD": "topicscope-local-only"
      }
    }
  }
}
```

### With the Docker image

The same, with Docker as the command. `-i` is required: without it the server has no standard input and exits at once.

```json
{
  "mcpServers": {
    "topic-scope": {
      "command": "docker",
      "args": ["run", "-i", "--rm", "--network", "topic-scope_default",
               "-e", "TOPIC_SCOPE_KAFKA=kafka:19092", "-e", "TOPIC_SCOPE_KAFKA_USERNAME=topicscope", "-e", "TOPIC_SCOPE_KAFKA_PASSWORD=topicscope-local-only",
               "-e", "TOPIC_SCOPE_MQTT=tcp://mosquitto:1883", "-e", "TOPIC_SCOPE_MQTT_USERNAME=topicscope", "-e", "TOPIC_SCOPE_MQTT_PASSWORD=topicscope-local-only",
               "topic-scope-mcp:0.1.1"]
    }
  }
}
```

Inside a container `localhost` is the container itself, so the brokers are addressed by their names on the compose network (`kafka:19092`, `mosquitto:1883`). Tried with Claude Code 2.1.325, with exactly this configuration given through `--mcp-config`: Claude Code started the container, the server showed as connected with its seven tools, `list_sources`, `list_topics` and `recent_messages` were called and answered, and when the session ended the container was gone.

The audit log of such a container is lost with it (`--rm`). To keep it, add `"-v", "topic-scope-audit:/data"` before the image name.

### Other clients

Any MCP client that can start a command works the same way: the command is `java`, the arguments are `-jar` and the path. Claude Desktop takes the JSON above in its `claude_desktop_config.json`. Only Claude Code has been tried.

## MCP over HTTP

The same tools at `http://<host>:8091/mcp` (Streamable HTTP). An HTTP endpoint that shows what is on the brokers must not be open, so this mode **refuses to start without a token**, and every request has to carry it.

```bash
export TOPIC_SCOPE_HTTP_TOKEN=$(openssl rand -hex 24)     # at least 24 characters; keep it like a password
java -jar server/target/topic-scope-server-0.1.1.jar --spring.profiles.active=http
```

A client sends `Authorization: Bearer <token>` with every request. Without it, or with a wrong one, the answer is 401. A request that comes from a web page (it carries an `Origin` header) is refused with 403 unless that origin is listed in `TOPIC_SCOPE_HTTP_ALLOWED_ORIGINS`.

### Claude Code

An HTTP server is registered with its address and the header to send:

```json
{
  "mcpServers": {
    "topic-scope": {
      "type": "http",
      "url": "http://127.0.0.1:8091/mcp",
      "headers": { "Authorization": "Bearer <the token>" }
    }
  }
}
```

This file now holds the token: keep it out of version control. It works as a project's `.mcp.json`, or for one session with `claude --mcp-config /path/to/that-file.json`.

Tried with Claude Code 2.1.325, with the file given through `--mcp-config`: the server shows as connected with its seven tools, and `list_sources`, `consumer_lag` and `topic_freshness` were called and answered, each leaving its line in the server's audit log. With a wrong token in the file the server shows as failed and no tool is offered.

With the image:

```bash
docker run -d --rm --network topic-scope_default -p 127.0.0.1:8091:8091 \
  -e TOPIC_SCOPE_HTTP_TOKEN -e TOPIC_SCOPE_KAFKA=kafka:19092 -e TOPIC_SCOPE_MQTT=tcp://mosquitto:1883 \
  -e TOPIC_SCOPE_KAFKA_USERNAME=topicscope -e TOPIC_SCOPE_KAFKA_PASSWORD=topicscope-local-only \
  -e TOPIC_SCOPE_MQTT_USERNAME=topicscope -e TOPIC_SCOPE_MQTT_PASSWORD=topicscope-local-only \
  topic-scope-mcp:0.1.1 --spring.profiles.active=http
```

| Variable | Default | Meaning |
|---|---|---|
| `TOPIC_SCOPE_HTTP_TOKEN` | none; required | the shared secret |
| `TOPIC_SCOPE_HTTP_PORT` | `8091` | the port |
| `TOPIC_SCOPE_HTTP_ADDRESS` | `127.0.0.1` (`0.0.0.0` in the image) | the address to listen on |
| `TOPIC_SCOPE_HTTP_ALLOWED_ORIGINS` | none | web page origins that may call it, comma-separated |

What this mode is and is not:

- The token is one shared secret, not user accounts. Everybody who has it sees the same, and the audit log cannot tell them apart.
- **The connection is not encrypted.** The token and everything the tools return travel in the clear. On one machine that is fine; anywhere else, put a reverse proxy with TLS in front, or do not use this mode.
- By default it listens on this machine only. The image listens on all of the container's interfaces, so who can reach it is decided where the port is published: `-p 127.0.0.1:8091:8091` keeps it local.
- Tried with Claude Code (see above), with the MCP Java SDK's Streamable HTTP client (the test `HttpIT`) and with `curl`. Not tried with Claude Desktop.

## Status board

A web page that shows topics, consumer lag and MQTT freshness, and keeps them current over a WebSocket.

```bash
docker compose --profile board up -d --build --wait       # the local stack plus the board, in containers
```

or from the jar, after building the page:

```bash
TOPIC_SCOPE_KAFKA_USERNAME=topicscope TOPIC_SCOPE_KAFKA_PASSWORD=topicscope-local-only \
TOPIC_SCOPE_MQTT_USERNAME=topicscope TOPIC_SCOPE_MQTT_PASSWORD=topicscope-local-only \
  java -jar server/target/topic-scope-server-0.1.1.jar --spring.profiles.active=board
```

Then open http://127.0.0.1:8090.

- **Consumer lag**: one card per group. `OK`, `LAGGING`, `STALLED`, `NO CONSUMER`; until the second look a card says `MEASURING`. When a status changes, the card notes the time and what it was before.
- **Kafka topics** and **MQTT topics**: select a topic name to open its newest messages; select it again, or Close, to stop. A Kafka topic is read on the spot. For an MQTT topic the board shows what it has heard on it since it started, because a broker keeps no history.
- The pill at the top right says `LIVE` while the page is connected. If the server goes away it says `RECONNECTING` and the page connects again by itself.

To see a change happen on the local stack, make the slow consumer stop reading and watch its card:

```bash
docker compose exec -T mosquitto mosquitto_pub -h localhost -u simulator -P simulator-local-only -t demo/control/tracker -m stop
docker compose exec -T mosquitto mosquitto_pub -h localhost -u simulator -P simulator-local-only -t demo/control/tracker -m start
```

The board is for one machine and has no login. See [what it does and does not do](../README.md#what-the-board-does-and-does-not-do). Settings: `TOPIC_SCOPE_BOARD_PORT` (8090), `TOPIC_SCOPE_BOARD_INTERVAL` (5s).

## The tools

You do not call tools yourself; you ask, and the assistant picks one. The questions below are examples that lead to each tool. Every tool takes the name of a source, as `list_sources` reports it.

| Ask | Tool | Arguments |
|---|---|---|
| "Which brokers can you see?" | `list_sources` | none |
| "Which topics are on local-kafka?" | `list_topics` | `source`; `nameContains`; Kafka: `includeInternal`; MQTT: `filter`, `windowSeconds` |
| "How is demo.flights.positions spread over its partitions? How long are messages kept?" | `describe_topic` | `source`, `topic` |
| "Show me the last 5 messages on demo.alarms." | `recent_messages` | `source`, `topic`; `limit`; Kafka: `partition`; MQTT: `windowSeconds` |
| "Which consumer groups are there?" | `list_consumer_groups` | `source`; `nameContains` |
| "Is demo-tracker keeping up?" | `consumer_lag` | `source`, `group`; `topic`; `sampleSeconds` |
| "Are the sensors still reporting every 2 seconds?" | `topic_freshness` | `source`; `topic`; `expectedIntervalSeconds`; MQTT: `windowSeconds` |

Things worth knowing when you read an answer:

- **`consumer_lag` takes time.** It looks twice, `sampleSeconds` apart (5 by default), because one look cannot tell a slow consumer from a stopped one. A consumer that commits less often than the sample is long looks `STALLED`; ask for a longer sample.
- **An MQTT answer is an observation.** A broker keeps no list of topics and no history. The tools listen for a few seconds and report what they heard, plus retained messages. A topic that was silent during that time and has no retained message is not there. To ask about one such topic, name it exactly: "is demo/sensors/rwy-27/rvr/telemetry still reporting?"
- **Message values are capped and redacted.** At most 2000 characters of a value are shown, and what looks like a secret is replaced with `[REDACTED]`. Redaction is a guess: treat what you get as sensitive.
- **Every time is UTC.** `list_sources` also returns the current time in UTC, so an assistant in a time zone that is already on the next calendar day does not take a correct timestamp for a clock that is a day off.
- **A wrong name is answered with the right ones.** "There is no topic named 'alarm'. Did you mean: demo.alarms."
- **"Messages" of a Kafka topic** is the distance between its offsets. For a compacted topic that is an upper bound; `describe_topic` says so.

What each column means, and the exact rule behind each status, is in the README under [What it shows](../README.md#what-it-shows).

## Your own brokers

The two built-in sources are moved with environment variables (`TOPIC_SCOPE_KAFKA`, `TOPIC_SCOPE_MQTT` and their `_USERNAME` / `_PASSWORD`). For other names, more brokers, TLS, or topic rules, write a file and pass it:

```bash
java -jar topic-scope-server-0.1.1.jar --spring.config.additional-location=file:/etc/topic-scope/sources.yml
```

```yaml
topic-scope:
  sources:
    - name: tower-kafka
      type: kafka
      address: broker-1:9093,broker-2:9093
      username: viewer
      password: ${TOWER_KAFKA_PASSWORD}      # taken from the environment
      properties:
        security.protocol: SASL_SSL
        sasl.mechanism: SCRAM-SHA-512
      topics:
        allow: ["surveillance.*"]
        deny: ["*.internal"]
    - name: field-mqtt
      type: mqtt
      address: ssl://mqtt-host:8883
      username: viewer
      password: ${FIELD_MQTT_PASSWORD}
```

A file that defines `sources` replaces the built-in two. The full reference is in the README under [Configuration](../README.md#configuration).

**Give the server an account that can only read.** On Kafka: `Describe`, `DescribeConfigs` and `Read` on topics, `Describe` on consumer groups and on the cluster. On MQTT: subscribe only. The server cannot write anyway, but a rule that the broker enforces does not depend on this program being right. The local stack is set up this way, and a test tries to write with the server's account to show that the brokers refuse.

Tried against real brokers: SASL/PLAIN on Kafka and user name with password on Mosquitto. TLS, SCRAM and `ssl://` are implemented as described but have not been tried against a broker.

## The audit log

Every tool call, and every topic opened on the board, is one line in `~/.topic-scope/audit.jsonl` (`/data/audit.jsonl` in the image; mount a volume there to keep it):

```json
{"time":"2026-11-16T14:40:35.389Z","tool":"describe_topic","arguments":{"source":"local-kafka","topic":"demo.alarms"},"outcome":"ok","millis":343}
```

It holds what was asked, never what was answered. While it is on, a call that cannot be recorded is not answered. `TOPIC_SCOPE_AUDIT_FILE` moves it; `TOPIC_SCOPE_AUDIT=false` turns it off. It is not rotated.

## When something does not work

| What you see | Likely cause |
|---|---|
| The assistant does not have the tools | The client was started before the server was registered or built. Restart the client. Check that the jar path exists. |
| `no answer within the timeout` for a Kafka source | The address is wrong, or the broker asks for a login and got none. A Kafka listener that expects a login does not answer a client that sends none. |
| `Authentication failed` / `Not authorized to connect` | Wrong user name or password for that source. |
| `is not available through this server` | The topic is left out by `topics.allow` / `topics.deny` of the source. |
| `could not be recorded in the audit log` | The audit file cannot be written. Fix its path or permissions, or turn recording off. |
| HTTP mode exits at once with "needs a token" | Set `TOPIC_SCOPE_HTTP_TOKEN` to at least 24 characters. |
| The server exits at once in Docker | `-i` is missing in MCP mode: without standard input the server takes it that its client has gone. |
| The board answers 404 | The page was not built (`cd board && npm ci && npm run build`), or the jar was not started from the repository folder. |
| The board stays on "Waiting for the first picture" | The WebSocket is refused: the page must be opened under `127.0.0.1` or `localhost` with the board's own port. |
| A consumer shows `STALLED` although it works | It commits less often than the sample. Use a longer `sampleSeconds`. |
| An MQTT topic is missing | It published nothing while the tool listened and has no retained message. Use a longer `windowSeconds`, or name it exactly. |

Logs go to the server's standard error, never to standard output, which carries the MCP protocol.
