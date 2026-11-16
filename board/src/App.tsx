import { useEffect, useRef, useState } from "react";
import { age, bytes, change, clock, count } from "./format";
import type { Freshness, GroupView, KafkaView, LagStatus, MessageRow, MqttView, SourceStatus, WatchView, Watching } from "./types";
import { type Board, type Connection, useBoard } from "./useBoard";

// ---------- status badges ----------

/** A status is never told by colour alone: each has its own shape and its own word. */
const STATUS: Record<LagStatus | Freshness, { label: string; shape: string; tone: string; meaning: string }> = {
  OK: { label: "OK", shape: "●", tone: "good", meaning: "Nothing is waiting, or the group is reading and keeping pace." },
  LAGGING: { label: "LAGGING", shape: "▲", tone: "warn", meaning: "The group is reading, but more is waiting than it read since the last look." },
  STALLED: { label: "STALLED", shape: "■", tone: "bad", meaning: "Messages are waiting and the committed offsets did not move." },
  NO_CONSUMER: { label: "NO CONSUMER", shape: "◆", tone: "idle", meaning: "Messages are waiting, the group has no members, and nothing new arrives." },
  MEASURING: { label: "MEASURING", shape: "○", tone: "idle", meaning: "First look. Whether the group is reading shows with the next one." },
  FRESH: { label: "FRESH", shape: "●", tone: "good", meaning: "A message arrived recently." },
  STALE: { label: "STALE", shape: "■", tone: "bad", meaning: "The topic was heard before and has been quiet for too long." },
  SILENT: { label: "SILENT", shape: "◆", tone: "idle", meaning: "Nothing has been published on it since the board started." },
};

function Badge({ status }: { status: LagStatus | Freshness }) {
  const s = STATUS[status] ?? { label: status, shape: "○", tone: "idle", meaning: "" };
  return (
    <span className={`badge tone-${s.tone}`} title={s.meaning}>
      <span aria-hidden="true">{s.shape}</span> {s.label}
    </span>
  );
}

// ---------- header ----------

function ConnectionPill({ connection }: { connection: Connection }) {
  const text = connection === "live" ? "LIVE" : connection === "connecting" ? "CONNECTING" : "RECONNECTING";
  return (
    <span className={`pill pill-${connection}`} role="status">
      <span className="dot" aria-hidden="true" /> {text}
    </span>
  );
}

/** Seconds since the latest snapshot arrived, ticking, so a board that stopped updating shows it. */
function useSecondsSince(time: number | null): number | null {
  const [now, setNow] = useState(() => Date.now());
  useEffect(() => {
    const timer = setInterval(() => setNow(Date.now()), 1000);
    return () => clearInterval(timer);
  }, []);
  return time === null ? null : Math.max(0, Math.round((now - time) / 1000));
}

function Header({ board }: { board: Board }) {
  const seconds = useSecondsSince(board.receivedAt);
  const snapshot = board.snapshot;
  const late = snapshot !== null && seconds !== null && seconds > snapshot.intervalSeconds * 3;
  return (
    <header className="top">
      <div className="title">
        <h1>Topic Scope</h1>
        <span className="subtitle">status board · read-only</span>
      </div>
      <div className="top-right">
        {snapshot && (
          <span className={late ? "updated late" : "updated"}>
            picture of {clock(snapshot.time)} UTC · {seconds === null ? "" : `${seconds} s ago`} · every {snapshot.intervalSeconds} s
          </span>
        )}
        <ConnectionPill connection={board.connection} />
      </div>
    </header>
  );
}

function Sources({ sources }: { sources: SourceStatus[] }) {
  return (
    <ul className="sources" aria-label="Sources">
      {sources.map((s) => (
        <li key={s.name} className={s.reachable ? "source up" : "source down"}>
          <span className="source-name">
            <span aria-hidden="true">{s.reachable ? "●" : "✕"}</span> {s.name}
          </span>
          <span className="source-type">{s.type}</span>
          <span className="source-detail">{s.reachable ? s.detail : `not reachable: ${s.detail}`}</span>
        </li>
      ))}
    </ul>
  );
}

// ---------- consumer lag ----------

interface StatusChange {
  from: LagStatus;
  to: LagStatus;
  at: string;
}

/** Remembers, per group, the last time its status changed while this page was open. */
function useStatusChanges(views: KafkaView[], time: string | undefined): Map<string, StatusChange> {
  const last = useRef(new Map<string, LagStatus>());
  const [changes, setChanges] = useState(new Map<string, StatusChange>());
  useEffect(() => {
    if (!time) return;
    let next: Map<string, StatusChange> | null = null;
    for (const view of views) {
      for (const g of view.groups) {
        const key = `${view.source}/${g.group}`;
        const before = last.current.get(key);
        if (before !== undefined && before !== g.status && before !== "MEASURING") {
          next = next ?? new Map(changes);
          next.set(key, { from: before, to: g.status, at: time });
        }
        last.current.set(key, g.status);
      }
    }
    if (next) setChanges(next);
    // Only a new snapshot can change a status.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [time]);
  return changes;
}

function GroupCard({ group, changed }: { group: GroupView; changed: StatusChange | undefined }) {
  const tone = STATUS[group.status]?.tone ?? "idle";
  const read = group.topics.reduce((n, t) => n + t.consumed, 0);
  const arrived = group.topics.reduce((n, t) => n + t.arrived, 0);
  return (
    // The key on status restarts the highlight animation whenever the status changes.
    <article className={`card tone-edge-${tone}`} key={group.status}>
      <div className="card-head">
        <h3>{group.group}</h3>
        <Badge status={group.status} />
      </div>
      <div className="lag">
        <span className="lag-number">{count(group.lag)}</span>
        <span className="lag-label">waiting</span>
        {group.status !== "MEASURING" && (
          <span className={group.lagChange > 0 ? "lag-change up" : group.lagChange < 0 ? "lag-change down" : "lag-change"}>{change(group.lagChange)}</span>
        )}
      </div>
      <dl className="facts">
        <div>
          <dt>read</dt>
          <dd>{group.status === "MEASURING" ? "–" : count(read)}</dd>
        </div>
        <div>
          <dt>arrived</dt>
          <dd>{group.status === "MEASURING" ? "–" : count(arrived)}</dd>
        </div>
        <div>
          <dt>members</dt>
          <dd>{group.members}</dd>
        </div>
        <div>
          <dt>state</dt>
          <dd>{group.state}</dd>
        </div>
      </dl>
      <p className="summary">{group.summary}</p>
      {changed && (
        <p className="changed">
          {clock(changed.at)} UTC: {STATUS[changed.from].label} {"→"} {STATUS[changed.to].label}
        </p>
      )}
    </article>
  );
}

function Lag({ views, time }: { views: KafkaView[]; time: string | undefined }) {
  const changes = useStatusChanges(views, time);
  return (
    <section aria-labelledby="lag-title">
      <h2 id="lag-title">Consumer lag</h2>
      {views.map((view) => (
        <div key={view.source}>
          <p className="section-source">{view.source}</p>
          {view.error ? (
            <p className="error">{view.error}</p>
          ) : view.groups.length === 0 ? (
            <p className="empty">No consumer groups.</p>
          ) : (
            <div className="cards">
              {view.groups.map((g) => (
                <GroupCard key={g.group} group={g} changed={changes.get(`${view.source}/${g.group}`)} />
              ))}
            </div>
          )}
        </div>
      ))}
    </section>
  );
}

// ---------- topics ----------

function isWatched(watching: Watching | null, source: string, topic: string): boolean {
  return watching !== null && watching.source === source && watching.topic === topic;
}

function WatchButton({ board, source, topic }: { board: Board; source: string; topic: string }) {
  const on = isWatched(board.watching, source, topic);
  return (
    <button type="button" className={on ? "link on" : "link"} aria-pressed={on} onClick={() => (on ? board.stopWatching() : board.startWatching({ source, topic }))}>
      {topic}
    </button>
  );
}

function KafkaTopics({ views, board }: { views: KafkaView[]; board: Board }) {
  return (
    <section aria-labelledby="topics-title">
      <h2 id="topics-title">Kafka topics</h2>
      {views.map((view) => (
        <div key={view.source}>
          <p className="section-source">{view.source}</p>
          {view.error ? (
            <p className="error">{view.error}</p>
          ) : (
            <div className="table-wrap">
              <table>
                <thead>
                  <tr>
                    <th scope="col">Topic</th>
                    <th scope="col" className="num">Partitions</th>
                    <th scope="col" className="num">Messages</th>
                    <th scope="col" className="num">Newest</th>
                    <th scope="col">Health</th>
                  </tr>
                </thead>
                <tbody>
                  {view.topics.map((t) => (
                    <tr key={t.name} className={isWatched(board.watching, view.source, t.name) ? "watched" : undefined}>
                      <th scope="row" className="wrap">
                        <WatchButton board={board} source={view.source} topic={t.name} />
                      </th>
                      <td className="num">{t.partitions}</td>
                      <td className="num">{count(t.messages)}</td>
                      <td className="num">{t.newestMessageAgeSeconds < 0 ? "empty" : `${age(t.newestMessageAgeSeconds)} ago`}</td>
                      <td>
                        {t.offlinePartitions > 0 ? (
                          <span className="flag bad">{t.offlinePartitions} offline</span>
                        ) : t.underReplicatedPartitions > 0 ? (
                          <span className="flag warn">{t.underReplicatedPartitions} under-replicated</span>
                        ) : (
                          <span className="flag good">in sync</span>
                        )}
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}
          {view.note && <p className="note">{view.note}</p>}
        </div>
      ))}
    </section>
  );
}

function MqttTopics({ views, board }: { views: MqttView[]; board: Board }) {
  return (
    <section aria-labelledby="mqtt-title">
      <h2 id="mqtt-title">MQTT topics</h2>
      {views.map((view) => (
        <div key={view.source}>
          <p className="section-source">{view.source}</p>
          {view.error ? (
            <p className="error">{view.error}</p>
          ) : (
            <div className="table-wrap">
              <table>
                <thead>
                  <tr>
                    <th scope="col">Topic</th>
                    <th scope="col">Freshness</th>
                    <th scope="col" className="num">Last heard</th>
                    <th scope="col" className="num">Delay</th>
                  </tr>
                </thead>
                <tbody>
                  {view.topics.map((t) => (
                    <tr key={t.topic} className={isWatched(board.watching, view.source, t.topic) ? "watched" : undefined}>
                      <th scope="row" className="wrap">
                        <WatchButton board={board} source={view.source} topic={t.topic} />
                        {t.retained && (
                          <span className="tag" title="The broker holds a retained message for this topic.">
                            retained
                          </span>
                        )}
                      </th>
                      <td>
                        <Badge status={t.status} />
                      </td>
                      <td className="num">{t.ageSeconds < 0 ? "never" : `${age(t.ageSeconds)} ago`}</td>
                      <td className="num">{t.deliveryDelayMillis < 0 ? "–" : `${t.deliveryDelayMillis} ms`}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}
          {view.counters.length > 0 && (
            <dl className="counters">
              {view.counters.map((c) => (
                <div key={c.name}>
                  <dt>{c.name.replace("$SYS/broker/", "")}</dt>
                  <dd>{c.value}</dd>
                </div>
              ))}
            </dl>
          )}
          {view.note && <p className="note">{view.note}</p>}
        </div>
      ))}
    </section>
  );
}

// ---------- recent messages ----------

function Message({ message }: { message: MessageRow }) {
  const where = message.partition >= 0 ? `partition ${message.partition} · offset ${count(message.offset)}` : `${message.topic} · QoS ${message.qos}${message.retained ? " · retained" : ""}`;
  return (
    <li className="message">
      <div className="message-meta">
        <span>{clock(message.timestamp)} UTC</span>
        <span>{where}</span>
        {message.key && <span>key {message.key}</span>}
        <span>
          {message.valueFormat} · {bytes(message.valueBytes)}
        </span>
        {message.truncated && <span className="flag warn">cut</span>}
        {message.redacted && <span className="flag bad">redacted</span>}
      </div>
      {message.headers.length > 0 && <div className="message-headers">{message.headers.map((h) => `${h.name}=${h.value}`).join("  ")}</div>}
      <pre className="message-value">{message.valueFormat === "none" ? "(no value)" : message.value}</pre>
    </li>
  );
}

function Watched({ watching, watch, onClose }: { watching: Watching; watch: WatchView | null; onClose: () => void }) {
  return (
    <section className="watch" aria-labelledby="watch-title">
      <div className="watch-head">
        <h2 id="watch-title">
          Recent messages <span className="watch-topic">{watching.topic}</span> <span className="watch-source">{watching.source}</span>
        </h2>
        <button type="button" className="close" onClick={onClose}>
          Close
        </button>
      </div>
      {watch === null ? (
        <p className="empty">Reading…</p>
      ) : watch.error ? (
        <p className="error">{watch.error}</p>
      ) : (
        <>
          {watch.messages.length === 0 ? <p className="empty">No messages.</p> : <ul className="messages">{watch.messages.map((m, i) => <Message key={`${m.topic}/${m.partition}/${m.offset}/${m.timestamp}/${i}`} message={m} />)}</ul>}
          {watch.note && <p className="note">{watch.note}</p>}
        </>
      )}
    </section>
  );
}

// ---------- page ----------

export function App() {
  const board = useBoard();
  const snapshot = board.snapshot;
  return (
    <div className="page">
      <Header board={board} />
      {snapshot === null ? (
        <p className="waiting">{board.connection === "reconnecting" ? "The server is not answering. Trying again…" : "Waiting for the first picture of the brokers…"}</p>
      ) : (
        <main>
          <Sources sources={snapshot.sources} />
          {snapshot.kafka.length > 0 && <Lag views={snapshot.kafka} time={snapshot.time} />}
          {board.watching && <Watched watching={board.watching} watch={board.watch} onClose={board.stopWatching} />}
          <div className="columns">
            {snapshot.kafka.length > 0 && <KafkaTopics views={snapshot.kafka} board={board} />}
            {snapshot.mqtt.length > 0 && <MqttTopics views={snapshot.mqtt} board={board} />}
          </div>
        </main>
      )}
      <footer>
        This page only looks. It cannot publish, commit offsets or change a broker. Select a topic to see its newest messages; values are capped, and what looks like a
        secret is replaced.
      </footer>
    </div>
  );
}
