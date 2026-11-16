# Security

Topic Scope shows what is on message brokers. In the surroundings it is built for, that can be operational data. This page says how to report a problem, what the project promises, and what it does not.

## Reporting a vulnerability

Please do not open a public issue for a security problem.

Report it privately through GitHub: on the repository page, **Security** → **Report a vulnerability** ([direct link](https://github.com/Trapa-Eureka/topic-scope-mcp/security/advisories/new)). Say what you did, what happened and what you expected, and which mode you used (stdin/stdout, HTTP or board).

This is a one-person project. You can expect a first answer within a week. If a report is confirmed, the fix is released first and the details are published with it, with credit to the reporter unless they prefer otherwise.

## Supported versions

| Version | Supported |
|---|---|
| 0.1.x | yes |

Only the newest release gets fixes.

## What the project promises

These are tested; each line names what checks it.

| Promise | Checked by |
|---|---|
| The server never publishes, commits an offset, joins a consumer group, or creates, deletes or alters anything on a broker | `ReadOnlyArchitectureTest` (fails the build on a broker-client call outside an allow list); `KafkaMessagesIT`, `KafkaLagIT`, `MqttIT` (the brokers are the same after every tool was used) |
| Asking about a topic that does not exist does not create it | `KafkaMessagesIT`, on a broker that creates topics on first use |
| With an account that can only read, every tool works, and the brokers refuse every write from that account | `SecuredStackIT` |
| A topic left out by `topics.allow` / `topics.deny` is not listed, not read, not counted and not suggested | `AccessAndAuditIT` |
| Message content is capped and redacted before it leaves the server, including on the status board | `PayloadFormatterTest`, `RedactorTest`, `KafkaMessagesIT`, `BoardIT` |
| Every tool call is recorded without message content, and a call that cannot be recorded is not answered | `AuditLogTest`, `AccessAndAuditIT` |
| A broker password is not in any answer or error message | `SecuredStackIT` (with a wrong password), `KafkaClientsTest` |
| HTTP mode does not start without a token and answers nothing without it | `HttpIT` |
| The status board refuses foreign host names and foreign WebSocket origins | `BoardIT` |

## What it does not promise

Know these before you rely on it.

- **Redaction is a guess.** It looks for names that suggest a secret and for strings shaped like well-known keys and tokens. A secret under an innocent name and in an unknown shape is shown. Treat everything a tool returns as sensitive, and keep topics that carry secrets out with `topics.deny` or, better, with the broker's own access rules.
- **The assistant sees what the tools return.** Whatever the server sends goes to the MCP client and from there to the AI model the client uses. Deciding whether that is acceptable for your data is yours to do, before you connect the server to a broker.
- **MQTT deny rules act on arrival.** If the broker delivers a message on a denied topic, the server has received it before dropping it. Only the broker's access rules keep it from being sent.
- **HTTP mode has one shared token and no encryption.** It is not user authentication, the audit log cannot tell callers apart, and the token travels in the clear. Use it on one machine, or behind a reverse proxy that does TLS.
- **The status board has no login.** It is bound to 127.0.0.1 and guarded against other web pages, not against other programs or users on the same machine.
- **TLS and SCRAM to brokers are untested against real brokers.** They are passed to the client libraries as configured.
- **The audit log is a plain local file.** Whoever can write to it can change it. It is not tamper-proof and it is not rotated.
- **The development hook** (`.claude/hooks/block-dangerous.mjs`) is a safety net for people who work on this repository with an AI agent. It matches text and can be both evaded and wrong. It is not part of the product's security.
- **The local stack's passwords are public.** `simulator-local-only` and `topicscope-local-only` are in this repository on purpose and protect nothing. Never reuse them.

## Running it carefully

1. Give the server a broker account that can only read. This matters more than anything the server does itself.
2. List the topics it may show with `topics.allow`, rather than only denying some.
3. Keep passwords and the HTTP token in the environment, not in files that get committed.
4. Leave the audit log on, and keep `~/.topic-scope/` for the owner only (it is created that way).
5. Do not expose the status board or HTTP mode beyond the machine without TLS and access control in front.
