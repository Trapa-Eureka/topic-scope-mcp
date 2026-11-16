import { useCallback, useEffect, useRef, useState } from "react";
import type { BoardMessage, Snapshot, WatchView, Watching } from "./types";

export type Connection = "connecting" | "live" | "reconnecting";

export interface Board {
  connection: Connection;
  snapshot: Snapshot | null;
  /** When the latest snapshot arrived here, for "updated n seconds ago". */
  receivedAt: number | null;
  watching: Watching | null;
  watch: WatchView | null;
  startWatching: (target: Watching) => void;
  stopWatching: () => void;
}

const RETRY_MILLIS = [1000, 2000, 5000];

/**
 * Holds the one WebSocket of the page. The server pushes a snapshot every few seconds; the page never asks
 * for one. If the connection drops, it is opened again, and the topic that was being watched is asked for again.
 */
export function useBoard(): Board {
  const [connection, setConnection] = useState<Connection>("connecting");
  const [snapshot, setSnapshot] = useState<Snapshot | null>(null);
  const [receivedAt, setReceivedAt] = useState<number | null>(null);
  const [watching, setWatching] = useState<Watching | null>(null);
  const [watch, setWatch] = useState<WatchView | null>(null);

  const socket = useRef<WebSocket | null>(null);
  const watchingRef = useRef<Watching | null>(null);
  watchingRef.current = watching;

  useEffect(() => {
    let closed = false;
    let attempt = 0;
    let retry: ReturnType<typeof setTimeout> | undefined;

    const open = () => {
      const scheme = window.location.protocol === "https:" ? "wss" : "ws";
      const ws = new WebSocket(`${scheme}://${window.location.host}/ws`);
      socket.current = ws;

      ws.onopen = () => {
        attempt = 0;
        setConnection("live");
        const target = watchingRef.current;
        if (target) ws.send(JSON.stringify({ type: "watch", ...target }));
      };
      ws.onmessage = (event) => {
        let message: BoardMessage;
        try {
          message = JSON.parse(String(event.data)) as BoardMessage;
        } catch {
          return;
        }
        if (message.type !== "snapshot") return;
        setSnapshot(message.snapshot);
        setReceivedAt(Date.now());
        const target = watchingRef.current;
        // A snapshot that was already on its way when the watched topic changed still carries the old one.
        if (message.watch && target && message.watch.source === target.source && message.watch.topic === target.topic) {
          setWatch(message.watch);
        }
      };
      ws.onclose = () => {
        if (closed) return;
        setConnection("reconnecting");
        const wait = RETRY_MILLIS[Math.min(attempt, RETRY_MILLIS.length - 1)];
        attempt += 1;
        retry = setTimeout(open, wait);
      };
      ws.onerror = () => ws.close();
    };

    open();
    return () => {
      closed = true;
      if (retry) clearTimeout(retry);
      socket.current?.close();
    };
  }, []);

  const startWatching = useCallback((target: Watching) => {
    setWatching(target);
    setWatch(null);
    const ws = socket.current;
    if (ws && ws.readyState === WebSocket.OPEN) ws.send(JSON.stringify({ type: "watch", ...target }));
  }, []);

  const stopWatching = useCallback(() => {
    setWatching(null);
    setWatch(null);
    const ws = socket.current;
    if (ws && ws.readyState === WebSocket.OPEN) ws.send(JSON.stringify({ type: "unwatch" }));
  }, []);

  return { connection, snapshot, receivedAt, watching, watch, startWatching, stopWatching };
}
