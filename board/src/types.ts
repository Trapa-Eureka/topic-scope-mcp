// The shapes the server sends over the WebSocket. They mirror the Java records in
// server/src/main/java/io/topicscope/board/BoardSnapshots.java and the tool results they are built from.

export type LagStatus = "OK" | "LAGGING" | "STALLED" | "NO_CONSUMER" | "MEASURING";
export type Freshness = "FRESH" | "STALE" | "SILENT";

export interface SourceStatus {
  name: string;
  type: "KAFKA" | "MQTT";
  address: string;
  reachable: boolean;
  detail: string;
  responseMillis: number;
}

export interface TopicRow {
  name: string;
  partitions: number;
  replicationFactor: number;
  internal: boolean;
  messages: number;
  newestMessageTime: string;
  newestMessageAgeSeconds: number;
  underReplicatedPartitions: number;
  offlinePartitions: number;
}

export interface TopicLag {
  topic: string;
  partitions: number;
  lag: number;
  lagChange: number;
  consumed: number;
  arrived: number;
  oldestUnreadAgeSeconds: number;
  status: LagStatus;
}

export interface GroupView {
  group: string;
  state: string;
  members: number;
  lag: number;
  lagChange: number;
  status: LagStatus;
  summary: string;
  topics: TopicLag[];
}

export interface KafkaView {
  source: string;
  topics: TopicRow[];
  groups: GroupView[];
  note: string;
  error: string;
}

export interface MqttTopicView {
  topic: string;
  status: Freshness;
  lastSeen: string;
  ageSeconds: number;
  messages: number;
  retained: boolean;
  deliveryDelayMillis: number;
}

export interface BrokerCounter {
  name: string;
  value: string;
}

export interface MqttView {
  source: string;
  topics: MqttTopicView[];
  counters: BrokerCounter[];
  note: string;
  error: string;
}

export interface MessageRow {
  topic: string;
  partition: number;
  offset: number;
  timestamp: string;
  key: string;
  headers: { name: string; value: string }[];
  value: string;
  valueFormat: "json" | "text" | "binary" | "none";
  valueBytes: number;
  truncated: boolean;
  redacted: boolean;
  qos: number;
  retained: boolean;
}

export interface WatchView {
  source: string;
  topic: string;
  messages: MessageRow[];
  note: string;
  error: string;
}

export interface Snapshot {
  time: string;
  intervalSeconds: number;
  sources: SourceStatus[];
  kafka: KafkaView[];
  mqtt: MqttView[];
}

export interface BoardMessage {
  type: "snapshot";
  snapshot: Snapshot;
  watch: WatchView | null;
}

export interface Watching {
  source: string;
  topic: string;
}
