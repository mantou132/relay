/**
 * Reliable WebSocket client for the durable relay server.
 *
 * Implements the client side of the relay's at-least-once delivery contract:
 * a persistent outbox retried until `stored`, a cumulative receive cursor
 * with duplicate suppression, exponential-backoff reconnection, and terminal
 * (retryable) `connection_conflict` handling.
 *
 * Storage is injectable so non-browser hosts can persist the outbox; the
 * default uses localStorage. The browser WebSocket answers the relay's
 * pings automatically, which is what keeps the relay's idle timeout from
 * dropping this connection.
 */

export type RelayConnectionState =
  | 'disconnected'
  | 'connecting'
  | 'connected'
  | 'reconnecting'
  | 'conflict';

export type OutboundMessage = {
  messageId: string;
  payload: unknown;
};

export type ServerFrame =
  | { type: 'ready'; endpoint: '1' | '2' }
  | { type: 'stored'; message_id: string }
  | { type: 'message'; message_id: string; sequence: number; payload: unknown }
  | { type: 'error'; message: string };

export type ClientFrame =
  | { type: 'message'; message_id: string; payload: unknown }
  | { type: 'ack'; sequence: number };

/** Persistence for outbound messages and the receive cursor. */
export type RelayStore = {
  /** Returns all un-`stored` messages. */
  outbox: () => OutboundMessage[] | Promise<OutboundMessage[]>;
  /** Persists a new outbound message with a fresh unique id. */
  enqueue: (message: OutboundMessage) => void | Promise<void>;
  /** Removes a message after `stored`; unknown ids are a no-op. */
  removeFromOutbox: (messageId: string) => void | Promise<void>;
  lastReceived: () => number | undefined | Promise<number | undefined>;
  /** Records a processed sequence after the payload was dispatched. */
  markReceived: (sequence: number) => void | Promise<void>;
  /** Allocates the next unique message id, or undefined to use the default generator. */
  nextMessageId?: () => string | undefined;
};

export type RelayClientOptions = {
  relayId: string;
  endpoint: '1' | '2';
  /** WebSocket endpoint of the relay, e.g. wss://host/ws. */
  relayUrl: string;
  onPayload: (payload: unknown) => void | Promise<void>;
  onStateChange?: (state: RelayConnectionState, error?: string) => void;
  onDisconnect?: (error: Error) => void;
  /** Storage key in localStorage if default store is used. Defaults to 'relay-client.v1'. */
  storageKey?: string;
  /** Defaults to a localStorage-backed store. */
  store?: RelayStore;
  /**
   * What to do on `connection_conflict`. `retry` keeps backing off
   * because the relay drops silent connections, releasing the slot; `terminal` (default)
   * stops after the conflict.
   */
  conflictPolicy?: 'retry' | 'terminal';
};

const MIN_RECONNECT_DELAY = 1_000;
const MAX_RECONNECT_DELAY = 30_000;

export const isRelayId = (value: string) =>
  /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(value);

/**
 * Sequence gate shared with the Rust client: adopt the first observed
 * sequence, drop duplicates, and treat gaps as protocol errors.
 */
export const isNewSequence = (lastReceived: number | undefined, sequence: number): boolean => {
  if (lastReceived === undefined) return true;
  if (sequence <= lastReceived) return false;
  if (sequence !== lastReceived + 1) {
    throw new Error(`Relay 消息序列不连续：预期 ${lastReceived + 1}，收到 ${sequence}`);
  }
  return true;
};

export const DEFAULT_STORAGE_KEY = 'relay-client.v1';

export const localStorageStore = (relayId: string, storageKey = DEFAULT_STORAGE_KEY): RelayStore => {
  const key = storageKey;
  type StoredState = {
    relayId: string;
    prefix: string;
    nextMessage: number;
    lastReceived?: number;
    outbox: OutboundMessage[];
  };

  const load = (): StoredState => {
    try {
      const raw = localStorage.getItem(key);
      const value = raw ? JSON.parse(raw) : null;
      if (
        value &&
        value.relayId === relayId &&
        typeof value.prefix === 'string' &&
        Number.isSafeInteger(value.nextMessage) &&
        Array.isArray(value.outbox)
      ) {
        return {
          relayId,
          prefix: value.prefix,
          nextMessage: value.nextMessage,
          ...(Number.isSafeInteger(value.lastReceived) ? { lastReceived: value.lastReceived } : {}),
          outbox: value.outbox.filter(
            (item: any): item is OutboundMessage =>
              typeof item?.messageId === 'string' && item.messageId.length > 0 && 'payload' in item,
          ),
        };
      }
    } catch {
      // Corrupt or outdated state is replaced with a fresh outbox; the pairing id is
      // untouched so the user can still reconnect from settings.
    }
    return { relayId, prefix: crypto.randomUUID(), nextMessage: 0, outbox: [] };
  };
  const save = (state: StoredState) => localStorage.setItem(key, JSON.stringify(state));
  return {
    outbox: () => load().outbox,
    enqueue: (message) => {
      const state = load();
      save({ ...state, nextMessage: state.nextMessage + 1, outbox: [...state.outbox, message] });
    },
    removeFromOutbox: (messageId) => {
      const state = load();
      save({ ...state, outbox: state.outbox.filter((item) => item.messageId !== messageId) });
    },
    lastReceived: () => load().lastReceived,
    markReceived: (sequence) => {
      const state = load();
      save({ ...state, lastReceived: sequence });
    },
  };
};

export class RelayClient {
  readonly relayId: string;
  readonly #endpoint: '1' | '2';
  readonly #relayUrl: string;
  readonly #store: RelayStore;
  readonly #conflictPolicy: 'retry' | 'terminal';
  #onPayload: RelayClientOptions['onPayload'];
  #onStateChange?: RelayClientOptions['onStateChange'];
  #onDisconnect?: RelayClientOptions['onDisconnect'];
  #socket?: WebSocket;
  #sent = new Set<string>();
  #relayReady = false;
  #manualClose = false;
  #conflictSeen = false;
  #reconnectDelay = MIN_RECONNECT_DELAY;
  #reconnectTimer?: ReturnType<typeof setTimeout>;
  #receiveChain = Promise.resolve();

  constructor({
    relayId,
    endpoint,
    relayUrl,
    onPayload,
    onStateChange,
    onDisconnect,
    storageKey,
    store,
    conflictPolicy = 'terminal',
  }: RelayClientOptions) {
    if (!isRelayId(relayId)) throw new Error('Relay ID 必须是 UUID');
    this.relayId = relayId;
    this.#endpoint = endpoint;
    this.#relayUrl = relayUrl;
    this.#store = store ?? localStorageStore(relayId, storageKey);
    this.#conflictPolicy = conflictPolicy;
    this.#onPayload = onPayload;
    this.#onStateChange = onStateChange;
    this.#onDisconnect = onDisconnect;
  }

  connect = () => {
    if (this.#socket && this.#socket.readyState <= WebSocket.OPEN) return;
    this.#manualClose = false;
    this.#conflictSeen = false;
    clearTimeout(this.#reconnectTimer);
    this.#emitState('connecting');

    const url = new URL(this.#relayUrl);
    url.searchParams.set('id', this.relayId);
    url.searchParams.set('endpoint', this.#endpoint);
    const socket = new WebSocket(url);
    this.#socket = socket;
    socket.addEventListener('open', this.#handleOpen);
    socket.addEventListener('message', this.#handleMessage);
    socket.addEventListener('close', this.#handleClose);
  };

  close = () => {
    this.#manualClose = true;
    clearTimeout(this.#reconnectTimer);
    this.#detachSocket()?.close();
    this.#emitState('disconnected');
    this.#onDisconnect?.(new Error('Relay 连接已关闭'));
  };

  send = async (payload: unknown) => {
    const message: OutboundMessage = { messageId: crypto.randomUUID(), payload };
    await this.#store.enqueue(message);
    this.#flushOutbox();
  };

  #handleOpen = () => {
    this.#relayReady = false;
    this.#sent.clear();
  };

  #handleMessage = (event: MessageEvent<string>) => {
    this.#receiveChain = this.#receiveChain
      .then(async () => {
        const frame = JSON.parse(event.data) as ServerFrame;
        await this.#handleFrame(frame);
      })
      .catch((error) => this.#fail(error instanceof Error ? error : new Error(String(error))));
  };

  #handleClose = () => {
    this.#detachSocket();
    this.#relayReady = false;
    this.#sent.clear();
    this.#onDisconnect?.(new Error('Relay 连接中断'));
    if (this.#manualClose) {
      this.#emitState('disconnected');
      return;
    }
    if (this.#conflictSeen && this.#conflictPolicy === 'terminal') {
      this.#emitState('conflict', '此 Relay ID 已在其他客户端上连接');
      return;
    }
    this.#emitState('reconnecting');
    const delay = this.#reconnectDelay;
    this.#reconnectDelay = Math.min(this.#reconnectDelay * 2, MAX_RECONNECT_DELAY);
    this.#reconnectTimer = setTimeout(this.connect, delay);
  };

  #handleFrame = async (frame: ServerFrame) => {
    switch (frame.type) {
      case 'ready':
        if (frame.endpoint !== this.#endpoint) {
          throw new Error(`Relay 返回了错误的 endpoint: ${frame.endpoint}`);
        }
        this.#relayReady = true;
        this.#reconnectDelay = MIN_RECONNECT_DELAY;
        this.#emitState('connected');
        this.#flushOutbox();
        return;
      case 'stored': {
        this.#sent.delete(frame.message_id);
        await this.#store.removeFromOutbox(frame.message_id);
        return;
      }
      case 'message': {
        const lastReceived = await this.#store.lastReceived();
        if (!isNewSequence(lastReceived, frame.sequence)) {
          this.#sendFrame({ type: 'ack', sequence: frame.sequence });
          return;
        }
        await this.#onPayload(frame.payload);
        await this.#store.markReceived(frame.sequence);
        this.#sendFrame({ type: 'ack', sequence: frame.sequence });
        return;
      }
      case 'error':
        if (frame.message.startsWith('connection_conflict:')) {
          this.#conflictSeen = true;
          this.#socket?.close();
          return;
        }
        throw new Error(`Relay 拒绝消息：${frame.message}`);
    }
  };

  #flushOutbox = async () => {
    if (!this.#relayReady || this.#socket?.readyState !== WebSocket.OPEN) return;
    for (const message of await this.#store.outbox()) {
      if (this.#sent.has(message.messageId)) continue;
      this.#sendFrame({ type: 'message', message_id: message.messageId, payload: message.payload });
      this.#sent.add(message.messageId);
    }
  };

  #sendFrame = (frame: ClientFrame) => {
    if (this.#socket?.readyState !== WebSocket.OPEN) return;
    this.#socket.send(JSON.stringify(frame));
  };

  #fail = (error: Error) => {
    this.#emitState('reconnecting', error.message);
    this.#socket?.close();
  };

  #detachSocket = () => {
    const socket = this.#socket;
    if (!socket) return;
    socket.removeEventListener('open', this.#handleOpen);
    socket.removeEventListener('message', this.#handleMessage);
    socket.removeEventListener('close', this.#handleClose);
    this.#socket = undefined;
    return socket;
  };

  #emitState = (state: RelayConnectionState, error?: string) => this.#onStateChange?.(state, error);
}
