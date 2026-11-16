# Topic Scope 사용 가이드

English: [usage.md](usage.md)

처음이라면 [따라 하기](walkthrough.ko.md)가 실제 응답과 화면 그림으로 전체 과정을 한 번 보여 줍니다. 이 문서는 참고 자료입니다.

Topic Scope는 하나의 프로그램이고, 실행 방식이 세 가지입니다. 셋 다 같은 읽기 전용 코드, 같은 설정, 같은 감사 로그를 씁니다.

| 모드 | 시작 옵션 | 이럴 때 씁니다 |
|---|---|---|
| [stdin/stdout MCP](#stdinstdout-mcp) | 없음 (기본값) | Claude Code 같은 어시스턴트가 내 컴퓨터에서 서버를 직접 띄울 때 |
| [HTTP MCP](#http-mcp) | `--spring.profiles.active=http` | 클라이언트가 자식 프로세스를 띄울 수 없거나, 여러 사람이 서버 하나를 같이 쓸 때 |
| [상태판](#상태판) | `--spring.profiles.active=board` | 브로커 상태를 웹 페이지로 지켜보고 싶을 때 |

**목차:** [받기](#받기) · [로컬 스택으로 써 보기](#로컬-스택으로-써-보기) · [stdin/stdout MCP](#stdinstdout-mcp) · [HTTP MCP](#http-mcp) · [상태판](#상태판) · [도구](#도구) · [내 브로커에 연결하기](#내-브로커에-연결하기) · [감사 로그](#감사-로그) · [동작하지 않을 때](#동작하지-않을-때)

## 받기

jar를 실행하려면 Java 21이, 이미지를 실행하려면 Docker가 필요합니다. 아직 내려받을 수 있는 배포본은 없으니 둘 중 하나를 직접 빌드합니다.

```bash
git clone https://github.com/Trapa-Eureka/topic-scope-mcp.git
cd topic-scope-mcp

./mvnw -DskipTests install                 # jar: server/target/topic-scope-server-0.1.1.jar (첫 실행 이후에는 몇 초)
docker build -t topic-scope-mcp:0.1.1 .    # 또는 이미지 (처음에는 몇 분)
```

두 MCP 모드는 jar만 있으면 됩니다. jar로 상태판을 띄우려면 화면도 한 번 빌드해야 합니다: `(cd board && npm ci && npm run build)` (Node.js 20 이상). 이미지에는 이미 들어 있습니다.

## 로컬 스택으로 써 보기

저장소에 시험용 브로커가 들어 있습니다. 데모 데이터와 샘플 트래픽이 흐르는 Kafka와 Mosquitto이고, 이 컴퓨터에서만 접근할 수 있습니다.

```bash
docker compose up -d --build --wait
```

두 브로커 모두 로그인을 요구합니다. 서버가 쓰는 계정은 `topicscope`, 비밀번호는 `topicscope-local-only`이고, 조회만 할 수 있습니다. (데모용 값이며 비밀이 아닙니다.) 아래 예시는 모두 로컬 스택 기준입니다. `docker compose down -v`로 멈추고 전부 지웁니다.

## stdin/stdout MCP

클라이언트가 서버를 자식 프로세스로 띄우고 표준 입출력으로 대화합니다. 클라이언트가 파이프를 닫으면 서버는 종료합니다.

### Claude Code

저장소에 로컬 스택용 [`.mcp.json`](../.mcp.json)이 들어 있습니다. 저장소 폴더에서 Claude Code를 시작하고, `topic-scope` 서버 승인을 물으면 승인합니다("use this MCP server"를 고르고, "all future servers"는 고르지 않습니다). 그러면 `/mcp`에 연결됨으로 표시됩니다. 이 방법은 실제로 확인했습니다.

다른 프로젝트나 다른 브로커에 쓰려면 그 프로젝트의 `.mcp.json`에 같은 내용을 절대 경로로 넣습니다.

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

### Docker 이미지로

같은 구성에서 명령만 Docker로 바꿉니다. `-i`가 꼭 필요합니다. 없으면 서버에 표준 입력이 없어서 바로 종료합니다.

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

컨테이너 안에서 `localhost`는 컨테이너 자신이므로, 브로커는 compose 네트워크상의 이름(`kafka:19092`, `mosquitto:1883`)으로 지정합니다. Claude Code 2.1.325에서 정확히 이 설정을 `--mcp-config`로 넘겨 확인했습니다. Claude Code가 컨테이너를 띄웠고, 서버가 연결됨으로 표시되며 도구 일곱 개가 보였고, `list_sources`, `list_topics`, `recent_messages`를 호출해 응답을 받았으며, 세션이 끝나자 컨테이너도 사라졌습니다.

이렇게 띄운 컨테이너의 감사 로그는 컨테이너와 함께 사라집니다(`--rm`). 보존하려면 이미지 이름 앞에 `"-v", "topic-scope-audit:/data"`를 추가합니다.

### 다른 클라이언트

명령을 실행할 수 있는 MCP 클라이언트라면 같은 방식으로 동작합니다. 명령은 `java`, 인자는 `-jar`와 경로입니다. Claude Desktop은 위 JSON을 `claude_desktop_config.json`에 넣습니다. 실제로 확인한 클라이언트는 Claude Code뿐입니다.

## HTTP MCP

같은 도구를 `http://<호스트>:8091/mcp`(Streamable HTTP)로 제공합니다. 브로커 내용을 보여 주는 HTTP 엔드포인트를 열어 둘 수는 없으므로, 이 모드는 **토큰 없이는 시작을 거부**하고 모든 요청에 토큰이 있어야 합니다.

```bash
export TOPIC_SCOPE_HTTP_TOKEN=$(openssl rand -hex 24)     # 24자 이상. 비밀번호처럼 다룹니다
java -jar server/target/topic-scope-server-0.1.1.jar --spring.profiles.active=http
```

클라이언트는 모든 요청에 `Authorization: Bearer <토큰>`을 보냅니다. 없거나 틀리면 401로 응답합니다. 웹 페이지에서 온 요청(`Origin` 헤더가 있는 요청)은 그 출처가 `TOPIC_SCOPE_HTTP_ALLOWED_ORIGINS`에 없으면 403으로 거부합니다.

### Claude Code

HTTP 서버는 주소와 보낼 헤더로 등록합니다.

```json
{
  "mcpServers": {
    "topic-scope": {
      "type": "http",
      "url": "http://127.0.0.1:8091/mcp",
      "headers": { "Authorization": "Bearer <토큰>" }
    }
  }
}
```

이 파일에는 토큰이 들어가므로 버전 관리에 넣지 마십시오. 프로젝트의 `.mcp.json`으로 쓰거나, `claude --mcp-config /path/to/that-file.json`으로 한 세션에만 쓸 수 있습니다.

Claude Code 2.1.325에서 이 파일을 `--mcp-config`로 넘겨 확인했습니다. 서버가 연결됨으로 표시되고 도구 일곱 개가 보이며, `list_sources`, `consumer_lag`, `topic_freshness`를 호출해 응답을 받았고, 호출마다 서버의 감사 로그에 한 줄씩 남았습니다. 파일의 토큰이 틀리면 서버가 실패로 표시되고 도구가 제공되지 않습니다.

이미지로 실행:

```bash
docker run -d --rm --network topic-scope_default -p 127.0.0.1:8091:8091 \
  -e TOPIC_SCOPE_HTTP_TOKEN -e TOPIC_SCOPE_KAFKA=kafka:19092 -e TOPIC_SCOPE_MQTT=tcp://mosquitto:1883 \
  -e TOPIC_SCOPE_KAFKA_USERNAME=topicscope -e TOPIC_SCOPE_KAFKA_PASSWORD=topicscope-local-only \
  -e TOPIC_SCOPE_MQTT_USERNAME=topicscope -e TOPIC_SCOPE_MQTT_PASSWORD=topicscope-local-only \
  topic-scope-mcp:0.1.1 --spring.profiles.active=http
```

| 변수 | 기본값 | 의미 |
|---|---|---|
| `TOPIC_SCOPE_HTTP_TOKEN` | 없음. 필수 | 공유 비밀값 |
| `TOPIC_SCOPE_HTTP_PORT` | `8091` | 포트 |
| `TOPIC_SCOPE_HTTP_ADDRESS` | `127.0.0.1` (이미지에서는 `0.0.0.0`) | 수신 주소 |
| `TOPIC_SCOPE_HTTP_ALLOWED_ORIGINS` | 없음 | 호출을 허용할 웹 페이지 출처. 쉼표로 구분 |

이 모드가 하는 것과 하지 않는 것:

- 토큰은 공유 비밀값 하나이고, 사용자 계정이 아닙니다. 토큰을 가진 사람은 모두 같은 것을 보고, 감사 로그로는 누구였는지 구분할 수 없습니다.
- **연결은 암호화되지 않습니다.** 토큰과 도구가 돌려주는 내용이 평문으로 오갑니다. 한 컴퓨터 안에서는 괜찮지만, 그 밖으로 나간다면 TLS를 처리하는 리버스 프록시를 앞에 두거나 이 모드를 쓰지 마십시오.
- 기본으로는 이 컴퓨터에서만 수신합니다. 이미지는 컨테이너의 모든 인터페이스에서 수신하므로, 누가 접근할 수 있는지는 포트를 어디에 공개하느냐로 정해집니다. `-p 127.0.0.1:8091:8091`이면 로컬 전용입니다.
- Claude Code(위 참고), MCP Java SDK의 Streamable HTTP 클라이언트(테스트 `HttpIT`), `curl`로 확인했습니다. Claude Desktop으로는 해 보지 않았습니다.

## 상태판

토픽, 컨슈머 지연량, MQTT 신선도를 보여 주고 WebSocket으로 계속 갱신하는 웹 페이지입니다.

```bash
docker compose --profile board up -d --build --wait       # 로컬 스택과 상태판을 함께 컨테이너로
```

또는 화면을 빌드한 뒤 jar로:

```bash
TOPIC_SCOPE_KAFKA_USERNAME=topicscope TOPIC_SCOPE_KAFKA_PASSWORD=topicscope-local-only \
TOPIC_SCOPE_MQTT_USERNAME=topicscope TOPIC_SCOPE_MQTT_PASSWORD=topicscope-local-only \
  java -jar server/target/topic-scope-server-0.1.1.jar --spring.profiles.active=board
```

그다음 http://127.0.0.1:8090 을 엽니다.

- **Consumer lag**: 그룹마다 카드 하나. 상태는 `OK`, `LAGGING`, `STALLED`, `NO CONSUMER`이고, 두 번째 조회 전까지는 `MEASURING`으로 나옵니다. 상태가 바뀌면 카드에 바뀐 시각과 이전 상태가 표시됩니다.
- **Kafka topics**, **MQTT topics**: 토픽 이름을 누르면 최근 메시지가 열립니다. 다시 누르거나 Close를 누르면 닫힙니다. Kafka 토픽은 그 자리에서 읽습니다. MQTT 토픽은 브로커에 이력이 없으므로, 상태판이 시작한 이후 그 토픽에서 들은 것을 보여 줍니다.
- 오른쪽 위 표시는 페이지가 연결돼 있는 동안 `LIVE`입니다. 서버가 사라지면 `RECONNECTING`으로 바뀌고 페이지가 스스로 다시 연결합니다.

로컬 스택에서 상태가 바뀌는 것을 보려면, 느린 컨슈머가 읽기를 멈추게 하고 카드를 지켜봅니다.

```bash
docker compose exec -T mosquitto mosquitto_pub -h localhost -u simulator -P simulator-local-only -t demo/control/tracker -m stop
docker compose exec -T mosquitto mosquitto_pub -h localhost -u simulator -P simulator-local-only -t demo/control/tracker -m start
```

상태판은 한 대의 컴퓨터용이고 로그인이 없습니다. README의 [What the board does and does not do](../README.md#what-the-board-does-and-does-not-do)를 참고하십시오. 설정: `TOPIC_SCOPE_BOARD_PORT`(8090), `TOPIC_SCOPE_BOARD_INTERVAL`(5s).

## 도구

도구를 직접 호출하지 않습니다. 질문하면 어시스턴트가 도구를 고릅니다. 아래 질문은 각 도구로 이어지는 예시입니다. 모든 도구는 `list_sources`가 알려 주는 소스 이름을 받습니다.

| 질문 | 도구 | 인자 |
|---|---|---|
| "어떤 브로커가 보여?" | `list_sources` | 없음 |
| "local-kafka에 어떤 토픽이 있어?" | `list_topics` | `source`; `nameContains`; Kafka: `includeInternal`; MQTT: `filter`, `windowSeconds` |
| "demo.flights.positions는 파티션에 어떻게 퍼져 있어? 메시지는 얼마나 보관돼?" | `describe_topic` | `source`, `topic` |
| "demo.alarms의 마지막 메시지 5개 보여 줘." | `recent_messages` | `source`, `topic`; `limit`; Kafka: `partition`; MQTT: `windowSeconds` |
| "컨슈머 그룹은 뭐가 있어?" | `list_consumer_groups` | `source`; `nameContains` |
| "demo-tracker가 잘 따라가고 있어?" | `consumer_lag` | `source`, `group`; `topic`; `sampleSeconds` |
| "센서들이 아직 2초마다 보고하고 있어?" | `topic_freshness` | `source`; `topic`; `expectedIntervalSeconds`; MQTT: `windowSeconds` |

답을 읽을 때 알아 둘 점:

- **`consumer_lag`는 시간이 걸립니다.** `sampleSeconds`(기본 5초) 간격으로 두 번 봅니다. 한 번만 봐서는 느린 컨슈머와 멈춘 컨슈머를 구분할 수 없기 때문입니다. 샘플 길이보다 드물게 커밋하는 컨슈머는 `STALLED`로 보이므로, 그럴 때는 샘플을 더 길게 요청합니다.
- **MQTT의 답은 관측 결과입니다.** 브로커에는 토픽 목록도 이력도 없습니다. 도구는 몇 초간 듣고 그동안 들은 것과 retained 메시지를 알려 줍니다. 그동안 조용했고 retained 메시지도 없는 토픽은 나오지 않습니다. 그런 토픽 하나를 물으려면 정확한 이름으로 묻습니다: "demo/sensors/rwy-27/rvr/telemetry가 아직 보고하고 있어?"
- **메시지 값은 잘리고 가려집니다.** 값은 최대 2000자까지 보이고, 비밀값처럼 보이는 것은 `[REDACTED]`로 바뀝니다. 가리기는 추측이므로, 받은 내용은 민감 정보로 다루십시오.
- **모든 시각은 UTC입니다.** `list_sources`는 현재 UTC 시각도 함께 돌려줍니다. 이미 다음 날짜로 넘어간 시간대에 있는 어시스턴트가 올바른 시각을 "하루 어긋난 시계"로 오해하지 않게 하기 위해서입니다.
- **이름이 틀리면 맞는 이름을 알려 줍니다.** "There is no topic named 'alarm'. Did you mean: demo.alarms."
- **Kafka 토픽의 "messages"**는 오프셋 사이의 거리입니다. 압축 토픽에서는 상한값이고, `describe_topic`이 그렇게 알려 줍니다.

각 열의 의미와 상태별 정확한 규칙은 README의 [What it shows](../README.md#what-it-shows)에 있습니다.

## 내 브로커에 연결하기

기본 소스 두 개는 환경 변수(`TOPIC_SCOPE_KAFKA`, `TOPIC_SCOPE_MQTT`와 각각의 `_USERNAME` / `_PASSWORD`)로 바꿉니다. 다른 이름, 더 많은 브로커, TLS, 토픽 규칙이 필요하면 파일을 만들어 넘깁니다.

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
      password: ${TOWER_KAFKA_PASSWORD}      # 환경 변수에서 가져옵니다
      properties:
        security.protocol: SASL_SSL
        sasl.mechanism: SCRAM-SHA-512
        ssl.truststore.location: /etc/topic-scope/kafka-truststore.p12
        ssl.truststore.password: ${TOWER_TRUSTSTORE_PASSWORD}
      topics:
        allow: ["surveillance.*"]
        deny: ["*.internal"]
    - name: field-mqtt
      type: mqtt
      address: ssl://mqtt-host:8883
      username: viewer
      password: ${FIELD_MQTT_PASSWORD}
      properties:                            # 브로커의 인증서를 Java가 기본으로 신뢰하지 않을 때만 필요합니다
        ssl.truststore.location: /etc/topic-scope/mqtt-truststore.p12
        ssl.truststore.password: ${FIELD_TRUSTSTORE_PASSWORD}
```

`sources`를 정의한 파일은 기본 소스 두 개를 통째로 대체합니다. 전체 설정은 README의 [Configuration](../README.md#configuration)에 있습니다.

**서버에는 읽기만 가능한 계정을 주십시오.** Kafka: 토픽에 `Describe`, `DescribeConfigs`, `Read`, 컨슈머 그룹과 클러스터에 `Describe`. MQTT: 구독만. 서버는 어차피 쓰지 못하지만, 브로커가 강제하는 규칙은 이 프로그램이 옳다는 것에 기대지 않습니다. 로컬 스택이 이렇게 구성돼 있고, 서버 계정으로 쓰기를 시도해 브로커가 거부하는 것을 테스트로 확인합니다.

실제 브로커로 확인한 것(테스트 `TlsStackIT`, 테스트용으로 만든 인증서 사용): TLS 위에서 SCRAM-SHA-512로 로그인하고 PKCS12 신뢰 저장소를 쓰는 Kafka, 그리고 사용자명·비밀번호와 PKCS12 신뢰 저장소를 쓰는 `ssl://` Mosquitto. TLS 없는 Kafka의 SASL/PLAIN과 Mosquitto의 `tcp://`도 확인했습니다. 확인하지 않은 것: SCRAM-SHA-256, JKS 신뢰 저장소, 클라이언트 인증서, Kerberos와 OAuth 로그인.

## 감사 로그

도구 호출과 상태판에서 연 토픽은 모두 `~/.topic-scope/audit.jsonl`에 한 줄씩 남습니다(이미지에서는 `/data/audit.jsonl`. 보존하려면 그곳에 볼륨을 마운트합니다).

```json
{"time":"2026-11-16T14:40:35.389Z","tool":"describe_topic","arguments":{"source":"local-kafka","topic":"demo.alarms"},"outcome":"ok","millis":343}
```

무엇을 물었는지만 남기고, 무엇을 답했는지는 남기지 않습니다. 켜져 있는 동안에는 기록할 수 없는 호출에 답하지 않습니다. `TOPIC_SCOPE_AUDIT_FILE`로 위치를 바꾸고, `TOPIC_SCOPE_AUDIT=false`로 끕니다. 로그 파일은 회전하지 않습니다.

## 동작하지 않을 때

| 보이는 현상 | 가능성이 큰 원인 |
|---|---|
| 어시스턴트에 도구가 없다 | 서버를 등록하거나 빌드하기 전에 클라이언트를 시작했습니다. 클라이언트를 다시 시작하고, jar 경로가 실제로 있는지 확인합니다. |
| Kafka 소스에 `no answer within the timeout` | 주소가 틀렸거나, 브로커가 로그인을 요구하는데 로그인 정보가 없습니다. 로그인을 기대하는 Kafka 리스너는 로그인 없는 클라이언트에 응답하지 않습니다. |
| `Authentication failed` / `Not authorized to connect` | 그 소스의 사용자명이나 비밀번호가 틀렸습니다. |
| `is not available through this server` | 소스의 `topics.allow` / `topics.deny`로 제외된 토픽입니다. |
| `could not be recorded in the audit log` | 감사 로그 파일에 쓸 수 없습니다. 경로나 권한을 고치거나 기록을 끕니다. |
| HTTP 모드가 "needs a token"을 출력하고 바로 종료한다 | `TOPIC_SCOPE_HTTP_TOKEN`을 24자 이상으로 설정합니다. |
| Docker에서 서버가 바로 종료한다 | MCP 모드에서 `-i`가 빠졌습니다. 표준 입력이 없으면 서버는 클라이언트가 떠났다고 판단합니다. |
| 상태판이 404로 응답한다 | 화면을 빌드하지 않았거나(`cd board && npm ci && npm run build`), jar를 저장소 폴더에서 시작하지 않았습니다. |
| 상태판이 "Waiting for the first picture"에서 멈춘다 | WebSocket이 거부되고 있습니다. 페이지를 `127.0.0.1`이나 `localhost`와 상태판 자신의 포트로 열어야 합니다. |
| 동작 중인 컨슈머가 `STALLED`로 나온다 | 샘플보다 드물게 커밋합니다. `sampleSeconds`를 늘립니다. |
| MQTT 토픽이 안 보인다 | 도구가 듣는 동안 발행이 없었고 retained 메시지도 없습니다. `windowSeconds`를 늘리거나 정확한 이름으로 묻습니다. |

로그는 서버의 표준 오류로 나갑니다. 표준 출력은 MCP 프로토콜이 쓰므로 로그가 나가지 않습니다.
