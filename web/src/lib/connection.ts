// Single auto-reconnecting WebSocket connection shared within a page.
// Receives config / aircraft / status; sends config patches.

import {
  DEFAULT_CONFIG,
  mergeConfig,
  type Aircraft,
  type ClientMessage,
  type Config,
  type GroundAircraft,
  type ServerMessage,
  type SourceStatus,
} from "@shared/index.js";

export interface StreamState {
  connected: boolean;
  config: Config | null;
  now: number;
  aircraft: Aircraft[];
  status: SourceStatus | null;
  /** SFO surface traffic snapshot (TV / stream "who's next" panel). */
  sfoGround: { at: number; aircraft: GroundAircraft[] } | null;
}

type Listener = (state: StreamState) => void;

export class Connection {
  private ws: WebSocket | null = null;
  private listeners = new Set<Listener>();
  private reconnectTimer: ReturnType<typeof setTimeout> | null = null;
  private buildCheckTimer: ReturnType<typeof setInterval> | null = null;
  private lastBuildCheckAt = 0;
  private closed = false;
  private everConnected = false;
  private initialScriptSrc: string | null =
    typeof document !== "undefined"
      ? document.querySelector<HTMLScriptElement>('script[type="module"][src]')?.getAttribute("src") ?? null
      : null;

  state: StreamState = {
    connected: false,
    config: null,
    now: 0,
    aircraft: [],
    status: null,
    sfoGround: null,
  };

  constructor(private role: "display" | "control") {}

  connect(): void {
    this.closed = false;
    this.open();
    if (!this.buildCheckTimer && typeof window !== "undefined") {
      this.buildCheckTimer = setInterval(() => void this.reloadIfBuildChanged(), 10_000);
    }
  }

  private url(): string {
    const proto = location.protocol === "https:" ? "wss" : "ws";
    return `${proto}://${location.host}/ws`;
  }

  private open(): void {
    try {
      this.ws = new WebSocket(this.url());
    } catch {
      this.scheduleReconnect();
      return;
    }
    this.ws.onopen = () => {
      if (this.everConnected) {
        void this.reloadIfBuildChanged();
      }
      this.everConnected = true;
      this.send({ type: "hello", role: this.role });
      this.update({ connected: true });
    };
    this.ws.onclose = () => {
      this.update({ connected: false });
      this.scheduleReconnect();
    };
    this.ws.onerror = () => this.ws?.close();
    this.ws.onmessage = (ev) => this.onMessage(ev.data as string);
  }

  private scheduleReconnect(): void {
    if (this.closed || this.reconnectTimer) return;
    this.reconnectTimer = setTimeout(() => {
      this.reconnectTimer = null;
      if (!this.closed) this.open();
    }, 1500);
  }

  private onMessage(raw: string): void {
    let msg: ServerMessage;
    try {
      msg = JSON.parse(raw) as ServerMessage;
    } catch {
      return;
    }
    switch (msg.type) {
      case "config":
        this.update({ config: mergeConfig(DEFAULT_CONFIG, msg.config) });
        void this.reloadIfBuildChanged();
        break;
      case "aircraft":
        this.update({ now: msg.now, aircraft: msg.aircraft });
        break;
      case "status":
        this.update({ status: msg.status });
        break;
      case "sfoGround":
        this.update({ sfoGround: { at: msg.at, aircraft: msg.aircraft } });
        break;
    }
  }

  send(msg: ClientMessage): void {
    if (this.ws?.readyState === WebSocket.OPEN) {
      this.ws.send(JSON.stringify(msg));
    }
  }

  patchConfig(patch: Partial<Config>): void {
    this.send({ type: "patchConfig", patch });
  }
  resetConfig(): void {
    this.send({ type: "resetConfig" });
  }

  subscribe(fn: Listener): () => void {
    this.listeners.add(fn);
    fn(this.state);
    return () => this.listeners.delete(fn);
  }

  private update(partial: Partial<StreamState>): void {
    this.state = { ...this.state, ...partial };
    for (const fn of this.listeners) fn(this.state);
  }

  private async reloadIfBuildChanged(): Promise<void> {
    if (!this.initialScriptSrc || typeof window === "undefined") return;
    const now = Date.now();
    if (now - this.lastBuildCheckAt < 4000) return;
    this.lastBuildCheckAt = now;
    try {
      const res = await fetch(`${window.location.pathname}?_t=${now}`, { cache: "no-store" });
      if (!res.ok) return;
      const html = await res.text();
      if (html.includes('<script type="module"') && !html.includes(this.initialScriptSrc)) {
        window.location.reload();
      }
    } catch {
      /* ignore */
    }
  }

  close(): void {
    this.closed = true;
    if (this.reconnectTimer) clearTimeout(this.reconnectTimer);
    if (this.buildCheckTimer) {
      clearInterval(this.buildCheckTimer);
      this.buildCheckTimer = null;
    }
    this.ws?.close();
  }
}
