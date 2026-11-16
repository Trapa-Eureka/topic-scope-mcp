#!/usr/bin/env node
// PreToolUse hook: blocks hard-to-reverse actions right before they run.
// Input:  Claude Code passes JSON ({ tool_name, tool_input, ... }) on stdin.
// Output: to block, exit code 2 + a reason on stderr (fed back to Claude); to allow, exit 0.
// No dependencies (Node built-ins only). When you change the rules, update block-dangerous.test.mjs too.

import { basename } from "node:path";
import { fileURLToPath } from "node:url";

// ---------- 1) Secret patterns (block if they appear in file content or commands) ----------
export const SECRET_PATTERNS = [
  { name: "Anthropic API key", re: /sk-ant-[A-Za-z0-9_-]{20,}/ },
  { name: "OpenAI API key", re: /\bsk-(?:proj-)?[A-Za-z0-9_-]{32,}/ },
  { name: "GitHub token", re: /\b(?:ghp|gho|ghu|ghs|ghr)_[A-Za-z0-9]{30,}\b|github_pat_[A-Za-z0-9_]{40,}/ },
  { name: "AWS access key", re: /\b(?:AKIA|ASIA)[0-9A-Z]{16}\b/ },
  { name: "Slack token", re: /\bxox[abprs]-[A-Za-z0-9-]{10,}/ },
  { name: "Google API key", re: /\bAIza[0-9A-Za-z_-]{35}\b/ },
  { name: "private key (PEM)", re: /-----BEGIN (?:[A-Z]+ )*PRIVATE KEY-----/ },
];

// ---------- 2) Forbidden write paths (secrets and local data) ----------
const FORBIDDEN_WRITE = [
  { test: (p) => /^\.env(\..+)?$/.test(basename(p)) && !/\.(example|sample)$/.test(basename(p)), why: ".env-style file (secrets)" },
  { test: (p) => /\.(pem|key|p12|pfx|jks|keystore|truststore)$/i.test(p), why: "certificate / private key / keystore file" },
  { test: (p) => /(^|\/)id_(rsa|ed25519|ecdsa)(\.pub)?$/.test(p), why: "SSH key file" },
  { test: (p) => /(^|\/)\.topic-scope\//.test(p), why: "local audit log folder (.topic-scope/)" },
  { test: (p) => /(^|\/)\.git\//.test(p), why: "file inside .git" },
];

// ---------- 3) The server is read-only: broker write APIs must not enter its source ----------
// Applies to files under server/src/main/. The simulator and the tests may publish; the server may not.
// This is the "red underline" at edit time. The architecture test (Phase 2) is the check at build time.
const SERVER_MAIN = /(^|\/)server\/src\/main\//;
export const SERVER_WRITE_APIS = [
  { why: "Kafka producer API (the server never publishes)", re: /\b(?:KafkaProducer|ProducerRecord|KafkaTemplate|ProducerFactory)\b/ },
  { why: "Kafka offset commit (the server never moves a consumer group)", re: /\b(?:commitSync|commitAsync|alterConsumerGroupOffsets|deleteConsumerGroupOffsets)\b/ },
  { why: "Kafka group subscription (read with assign + seek, never subscribe)", re: /\bKafkaConsumer\b[\s\S]*\.subscribe\s*\(|\bConsumerConfig\.GROUP_ID_CONFIG\b|["']group\.id["']/ },
  { why: "Kafka admin write (create/delete/alter)", re: /\b(?:createTopics|deleteTopics|deleteRecords|deleteConsumerGroups|createPartitions|alterConfigs|incrementalAlterConfigs|createAcls|deleteAcls)\b/ },
  { why: "MQTT publish (the server only subscribes)", re: /\.publish(?:With)?\s*\(|\bnew\s+MqttMessage\b|\bMqtt[35]Publish\b|\bwillPublish\b|\bsetWill\b/ },
];

// ---------- 4) Dangerous shell command patterns ----------
const RM_RECURSIVE = /\brm\s+(?:-[a-zA-Z]*\s+)*-[a-zA-Z]*[rR][a-zA-Z]*\b|\brm\s+--recursive\b/;
const DANGEROUS_RM_TARGET = /(?:^|\s)(?:\/|\/\*|~\/?|~\/\*|\$HOME\/?|\$\{HOME\}\/?|\.\.\/?|\.\/?\*?|\*|\/(?:Users|home|etc|usr|var|bin|sbin|System|Library|Volumes)(?:\/\S*)?)(?=\s|$)/;

// Broker command-line tools may only talk to the local stack. A development agent must never reach a real broker.
const LOCAL_BROKER_HOSTS = new Set(["localhost", "127.0.0.1", "::1", "[::1]", "0.0.0.0", "kafka", "mosquitto", "broker", "host.docker.internal"]);
// The tool as a command: its name followed by its arguments. The bare words "kafka" or "mqtt" somewhere in a
// command (a file name, a test name, a string) are not a broker tool.
const KAFKA_CLI = /\b(?:kafka-[a-z-]+(?:\.sh)?|kcat|kafkacat)(?=\s|$)/g;
const MQTT_CLI = /\b(?:mosquitto_(?:pub|sub|rr)|mqttx)(?=\s|$)/g;

function hostOf(address) {
  const a = address.replace(/^["']|["']$/g, "").replace(/^[a-z]+:\/\//i, "");
  if (a.startsWith("[")) return a.slice(0, a.indexOf("]") + 1);
  return a.split(/[:/]/)[0];
}

/** The arguments of each use of a tool: from its name to the end of that command (the next ; & | or line break). */
function argumentsOf(cmd, tool) {
  const found = [];
  for (const m of cmd.matchAll(tool)) {
    const rest = cmd.slice(m.index + m[0].length);
    const end = rest.search(/[;&|\n]/);
    found.push(end < 0 ? rest : rest.slice(0, end));
  }
  return found;
}

/**
 * Only the tool's own arguments are searched for an address. Another command on the same line that happens
 * to take -h or -b (grep, ssh) is not a broker being reached.
 *
 * @returns {string | null} the first non-local broker host that a broker command-line tool is pointed at
 */
export function remoteBrokerHost(cmd) {
  const addresses = [];
  for (const args of argumentsOf(cmd, KAFKA_CLI)) {
    for (const m of args.matchAll(/(?:--bootstrap-server|--broker-list|\s-b)(?:=|\s+)(\S+)/g)) addresses.push(...m[1].split(","));
  }
  for (const args of argumentsOf(cmd, MQTT_CLI)) {
    for (const m of args.matchAll(/(?:\s-h|--host|\s-L|--url)(?:=|\s+)(\S+)/g)) addresses.push(m[1]);
  }
  for (const a of addresses) {
    const host = hostOf(a);
    if (host && !LOCAL_BROKER_HOSTS.has(host)) return host;
  }
  return null;
}

export const DANGEROUS_COMMANDS = [
  {
    why: "recursive delete (rm -rf) on a broad target",
    test: (c) => RM_RECURSIVE.test(c) && DANGEROUS_RM_TARGET.test(c),
  },
  {
    why: "git force push",
    test: (c) => /\bgit\s+push\b[^;&|\n]*(?:--force(?!-with-lease)\b|\s-f\b|--force-with-lease\b|\s\+\S+)/.test(c),
  },
  {
    why: "destroys git work (reset --hard / clean -fd / checkout . / restore .)",
    test: (c) =>
      /\bgit\s+reset\s+--hard\b/.test(c) ||
      /\bgit\s+clean\s+-[a-zA-Z]*f/.test(c) ||
      /\bgit\s+checkout\s+(?:--\s+)?\.(?:\s|$)/.test(c) ||
      /\bgit\s+restore\s+(?:--\S+\s+)*\.(?:\s|$)/.test(c),
  },
  {
    why: "stages secret files in git (git add -f, .env, keys, keystores, audit log)",
    test: (c) =>
      /\bgit\s+add\b[^;&|\n]*(?:\s-f\b|--force\b)/.test(c) ||
      /\bgit\s+add\b[^;&|\n]*(?:\.env(?!\.(?:example|sample)\b)(?:\.[\w-]+)?(?=\s|$)|\.topic-scope\b|\.(?:pem|key|jks|p12|keystore)\b|id_rsa|id_ed25519)/.test(c),
  },
  {
    why: "downloads a remote script and runs it directly (curl | sh)",
    test: (c) => /\b(?:curl|wget)\b[^|;\n]*\|\s*(?:sudo\s+)?(?:ba|z|da)?sh\b/.test(c),
  },
  {
    why: "system-wide permission/ownership change or raw disk write",
    test: (c) =>
      /\bchmod\s+-R\s+[0-7]*7[0-7]*\s+\//.test(c) ||
      /\bchown\s+-R\b[^;&|\n]*\s\/(?:\s|$)/.test(c) ||
      /\bdd\b[^;&|\n]*\bof=\/dev\//.test(c) ||
      /\bmkfs(?:\.\w+)?\b/.test(c) ||
      />\s*\/dev\/(?:sd|disk|nvme)/.test(c),
  },
  {
    why: "publishing (mvn deploy / docker push / npm publish) must be run by a human",
    test: (c) =>
      /(?:\bmvn|mvnw)\b[^;&|\n]*\s(?:deploy|release:perform)\b/.test(c) ||
      /\bdocker\s+(?:image\s+)?push\b/.test(c) ||
      /\b(?:npm|pnpm|yarn)\s+publish\b/.test(c),
  },
  {
    why: "docker prune removes containers, images or volumes of other projects too",
    test: (c) => /\bdocker\s+(?:system|volume|image|container|builder|network)\s+prune\b/.test(c),
  },
];

// ---------- Evaluation ----------
function findSecret(text) {
  if (!text) return null;
  for (const { name, re } of SECRET_PATTERNS) if (re.test(text)) return name;
  return null;
}

function toRelative(p, projectDir) {
  if (!p) return "";
  const norm = String(p).replace(/\\/g, "/");
  if (projectDir && norm.startsWith(projectDir.replace(/\\/g, "/") + "/")) {
    return norm.slice(projectDir.length + 1);
  }
  return norm;
}

/** @returns {{ block: boolean, reason?: string }} */
export function evaluate(input, projectDir = process.env.CLAUDE_PROJECT_DIR ?? "") {
  const tool = input?.tool_name;
  const args = input?.tool_input ?? {};

  if (tool === "Bash") {
    const cmd = String(args.command ?? "");
    for (const rule of DANGEROUS_COMMANDS) {
      if (rule.test(cmd)) return { block: true, reason: `Dangerous command blocked: ${rule.why}` };
    }
    const remote = remoteBrokerHost(cmd);
    if (remote) return { block: true, reason: `Broker command blocked: "${remote}" is not the local stack. Only the brokers from compose.yaml may be reached.` };
    const secret = findSecret(cmd);
    if (secret) return { block: true, reason: `The command contains a ${secret}. Use an environment variable instead.` };
    return { block: false };
  }

  if (["Write", "Edit", "MultiEdit", "NotebookEdit"].includes(tool)) {
    const path = toRelative(args.file_path ?? args.notebook_path ?? "", projectDir);
    for (const rule of FORBIDDEN_WRITE) {
      if (rule.test(path)) return { block: true, reason: `Write blocked: ${rule.why} -> ${path}` };
    }
    const content = [args.content, args.new_string, args.new_source, ...(Array.isArray(args.edits) ? args.edits.map((e) => e?.new_string) : [])]
      .filter((v) => typeof v === "string")
      .join("\n");
    const secret = findSecret(content);
    if (secret) return { block: true, reason: `Attempted to write a ${secret} directly into a file. Reference an environment variable instead (keep .env.example values empty).` };
    if (SERVER_MAIN.test(path)) {
      for (const rule of SERVER_WRITE_APIS) {
        if (rule.re.test(content)) return { block: true, reason: `Read-only rule broken in ${path}: ${rule.why}. The server must not change a broker.` };
      }
    }
    return { block: false };
  }

  return { block: false };
}

// ---------- Entry point ----------
async function main() {
  let raw = "";
  for await (const chunk of process.stdin) raw += chunk;
  let input;
  try {
    input = JSON.parse(raw);
  } catch {
    // If the input cannot be parsed, do not block (a broken hook must not stop development).
    process.exit(0);
  }
  const { block, reason } = evaluate(input);
  if (block) {
    process.stderr.write(`[block-dangerous] ${reason}\nThis action is forbidden from running automatically by policy. Do not work around it; ask the user to confirm.\n`);
    process.exit(2);
  }
  process.exit(0);
}

if (process.argv[1] && fileURLToPath(import.meta.url) === process.argv[1]) {
  main();
}
