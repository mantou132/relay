/**
 * Reliable WebSocket client for the durable relay server.
 *
 * Implements the client side of the relay's at-least-once delivery contract:
 * a persistent outbox retried until `stored`, a cumulative receive cursor
 * with duplicate suppression, exponential-backoff reconnection, session preemption
 * handling, and multi-device support.
 *
 * Ephemeral messages (`sendEphemeral`, `sendBinary`) bypass the outbox and
 * cursor: they go straight to the live socket, are never stored or
 * acknowledged, and throw while the relay is not connected.
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
  | 'preempted';

export type OutboundMessage = {
  messageId: string;
  payload: unknown;
  targetDeviceId?: string;
};

export type ServerFrame =
  | { type: 'ready'; endpoint: '1' | '2' }
  | { type: 'stored'; message_id: string }
  | { type: 'rejected'; message_id: string; reason: string }
  | { type: 'message'; message_id: string; sequence: number; payload: unknown }
  | { type: 'ephemeral'; message_id: string; payload: unknown }
  | { type: 'undeliverable'; message_id: string; reason: string }
  | { type: 'error'; message: string };

export type ClientFrame =
  | { type: 'message'; message_id: string; payload: unknown; target_device_id?: string; ephemeral?: boolean }
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
  /** Returns the persistent device identifier if stored. */
  deviceId?: () => string | undefined | Promise<string | undefined>;
};

export type RelayClientOptions = {
  relayId: string;
  endpoint: '1' | '2';
  /** Optional stable device ID for multi-device support. Defaults to an auto-persisted UUID. */
  deviceId?: string;
  /** WebSocket endpoint of the relay, e.g. wss://host/ws. */
  relayUrl: string;
  /**
   * If true, the initial connection requests ack_head to align cursors and drop stale server backlog.
   * Subsequent automatic reconnects will not send ack_head, preserving missed message replay.
   */
  ackHead?: boolean;
  /** Receives durable payloads in sequence order, and ephemeral text payloads. */
  onPayload: (payload: unknown) => void | Promise<void>;
  /** Receives ephemeral binary messages. */
  onBinary?: (data: Uint8Array, messageId: string) => void | Promise<void>;
  /** Called when an ephemeral message reached no device of the peer endpoint. */
  onUndeliverable?: (messageId: string, reason: string) => void;
  onStateChange?: (state: RelayConnectionState, error?: string) => void;
  onDisconnect?: (error: Error) => void;
  /** Called before removing a rejected message from the outbox. */
  onMessageRejected?: (messageId: string, reason: string) => void;
  /** Storage key in localStorage if default store is used. Defaults to 'relay-client.v1'. */
  storageKey?: string;
  /** Defaults to a localStorage-backed store. */
  store?: RelayStore;
};

const MIN_RECONNECT_DELAY = 1_000;
const MAX_RECONNECT_DELAY = 30_000;

export const isRelayId = (value: string): boolean =>
  typeof value === 'string' && value.trim().length > 0 && new TextEncoder().encode(value).length <= 256;

/**
 * Sequence gate: adopt the first observed sequence, drop duplicates,
 * and self-heal when a gap is detected (e.g. after multi-device consumption or retention purge).
 */
export const isNewSequence = (lastReceived: number | undefined, sequence: number): boolean => {
  if (lastReceived === undefined) return true;
  if (sequence <= lastReceived) return false;
  if (sequence !== lastReceived + 1) {
    console.warn(
      `[RelayClient] Sequence gap: expected ${lastReceived + 1}, got ${sequence}. Auto-syncing receive cursor.`,
    );
    return true;
  }
  return true;
};

export type BinaryHeader = { message_id: string; target_device_id?: string };

/** Binary frame layout: `[u16 big-endian header length][header JSON][body]`. */
export const encodeBinaryFrame = (header: BinaryHeader, body: Uint8Array): Uint8Array<ArrayBuffer> => {
  const headerBytes = new TextEncoder().encode(JSON.stringify(header));
  const frame = new Uint8Array(2 + headerBytes.length + body.length);
  new DataView(frame.buffer).setUint16(0, headerBytes.length);
  frame.set(headerBytes, 2);
  frame.set(body, 2 + headerBytes.length);
  return frame;
};

export const decodeBinaryFrame = (frame: Uint8Array): { header: BinaryHeader; body: Uint8Array } => {
  if (frame.length < 2) throw new Error('Binary frame is missing its header length');
  const bodyStart = 2 + new DataView(frame.buffer, frame.byteOffset, 2).getUint16(0);
  if (frame.length < bodyStart) throw new Error('Binary frame header is truncated');
  const header = JSON.parse(new TextDecoder().decode(frame.subarray(2, bodyStart))) as BinaryHeader;
  return { header, body: frame.subarray(bodyStart) };
};

export const DEFAULT_STORAGE_KEY = 'relay-client.v1';

export const localStorageStore = (relayId: string, storageKey = DEFAULT_STORAGE_KEY): RelayStore => {
  const key = storageKey;
  type StoredState = {
    relayId: string;
    deviceId: string;
    lastReceived?: number;
    outbox: OutboundMessage[];
  };

  const save = (state: StoredState) => {
    localStorage.setItem(key, JSON.stringify(state));
  };

  const load = (): StoredState => {
    try {
      const raw = localStorage.getItem(key);
      if (!raw) {
        const fresh: StoredState = { relayId, deviceId: crypto.randomUUID(), outbox: [] };
        save(fresh);
        return fresh;
      }
      const parsed = JSON.parse(raw) as Partial<StoredState>;
      const deviceId = parsed.deviceId || crypto.randomUUID();
      const state: StoredState = {
        relayId,
        deviceId,
        lastReceived: parsed.relayId === relayId ? parsed.lastReceived : undefined,
        outbox: parsed.relayId === relayId && Array.isArray(parsed.outbox) ? parsed.outbox : [],
      };
      if (!parsed.deviceId || parsed.relayId !== relayId) {
        save(state);
      }
      return state;
    } catch {
      const fallback: StoredState = { relayId, deviceId: crypto.randomUUID(), outbox: [] };
      save(fallback);
      return fallback;
    }
  };

  return {
    outbox: () => load().outbox,
    enqueue: (message) => {
      const state = load();
      state.outbox.push(message);
      save(state);
    },
    removeFromOutbox: (messageId) => {
      const state = load();
      state.outbox = state.outbox.filter((msg) => msg.messageId !== messageId);
      save(state);
    },
    lastReceived: () => load().lastReceived,
    markReceived: (sequence: number) => {
      const state = load();
      state.lastReceived = sequence;
      save(state);
    },
    deviceId: () => load().deviceId,
  };
};

export class RelayClient {
  readonly relayId: string;
  #endpoint: '1' | '2';
  #deviceId?: string;
  #relayUrl: string;
  #ackHead: boolean;
  #store: RelayStore;
  #onPayload: RelayClientOptions['onPayload'];
  #onBinary?: RelayClientOptions['onBinary'];
  #onUndeliverable?: RelayClientOptions['onUndeliverable'];
  #onStateChange?: RelayClientOptions['onStateChange'];
  #onDisconnect?: RelayClientOptions['onDisconnect'];
  #onMessageRejected?: RelayClientOptions['onMessageRejected'];
  #socket?: WebSocket;
  #sent = new Set<string>();
  #relayReady = false;
  #manualClose = false;
  #preemptedSeen = false;
  #reconnectDelay = MIN_RECONNECT_DELAY;
  #reconnectTimer?: ReturnType<typeof setTimeout>;
  #receiveChain = Promise.resolve();
  #storeChain = Promise.resolve();

  #withStore = <T>(op: (store: RelayStore) => T | Promise<T>): Promise<T> => {
    const task = this.#storeChain.then(() => op(this.#store));
    this.#storeChain = task.then(
      () => {},
      () => {},
    );
    return task;
  };

  constructor({
    relayId,
    endpoint,
    deviceId,
    relayUrl,
    ackHead,
    onPayload,
    onBinary,
    onUndeliverable,
    onStateChange,
    onDisconnect,
    onMessageRejected,
    storageKey,
    store,
  }: RelayClientOptions) {
    if (!isRelayId(relayId)) throw new Error('Relay ID must be a non-empty string of 1-256 bytes');
    if (!store && typeof localStorage === 'undefined') {
      throw new Error('localStorage is not available in current environment; an explicit store implementation must be provided');
    }
    this.relayId = relayId;
    this.#endpoint = endpoint;
    this.#deviceId = deviceId;
    this.#relayUrl = relayUrl;
    this.#ackHead = ackHead ?? false;
    this.#store = store ?? localStorageStore(relayId, storageKey);
    this.#onPayload = onPayload;
    this.#onBinary = onBinary;
    this.#onUndeliverable = onUndeliverable;
    this.#onStateChange = onStateChange;
    this.#onDisconnect = onDisconnect;
    this.#onMessageRejected = onMessageRejected;
  }

  connect = async (options?: { ackHead?: boolean }) => {
    if (options?.ackHead !== undefined) {
      this.#ackHead = options.ackHead;
    }
    if (this.#socket && this.#socket.readyState <= WebSocket.OPEN) return;
    this.#manualClose = false;
    this.#preemptedSeen = false;
    clearTimeout(this.#reconnectTimer);
    this.#emitState('connecting');

    if (!this.#deviceId) {
      if (this.#store.deviceId) {
        this.#deviceId = await this.#withStore((store) => store.deviceId?.());
      }
      if (!this.#deviceId) {
        this.#deviceId = crypto.randomUUID();
      }
    }

    const url = new URL(this.#relayUrl);
    url.searchParams.set('id', this.relayId);
    url.searchParams.set('endpoint', this.#endpoint);
    url.searchParams.set('device_id', this.#deviceId);
    if (this.#ackHead) {
      url.searchParams.set('ack_head', 'true');
    }
    const socket = new WebSocket(url);
    socket.binaryType = 'arraybuffer';
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
    this.#onDisconnect?.(new Error('Relay connection closed'));
  };

  send = async (payload: unknown, targetDeviceId?: string) => {
    const message: OutboundMessage = { messageId: crypto.randomUUID(), payload, targetDeviceId };
    await this.#withStore((store) => store.enqueue(message));
    try {
      await this.#flushOutbox();
    } catch (error) {
      await this.#withStore((store) => store.removeFromOutbox(message.messageId));
      throw error;
    }
  };

  /**
   * Sends over the live connection without storage or acknowledgement; returns the message id.
   * Pass `messageId` to correlate `onUndeliverable` with an id of your own (non-empty, at most 256 bytes).
   */
  sendEphemeral = (payload: unknown, targetDeviceId?: string, messageId: string = crypto.randomUUID()) => {
    this.#liveSocket().send(
      JSON.stringify({
        type: 'message',
        message_id: messageId,
        payload,
        ...(targetDeviceId ? { target_device_id: targetDeviceId } : {}),
        ephemeral: true,
      } satisfies ClientFrame),
    );
    return messageId;
  };

  /** Sends bytes as an ephemeral binary frame; see `sendEphemeral`. */
  sendBinary = (data: Uint8Array, targetDeviceId?: string, messageId: string = crypto.randomUUID()) => {
    const header: BinaryHeader = {
      message_id: messageId,
      ...(targetDeviceId ? { target_device_id: targetDeviceId } : {}),
    };
    this.#liveSocket().send(encodeBinaryFrame(header, data));
    return messageId;
  };

  #liveSocket = () => {
    if (!this.#relayReady || this.#socket?.readyState !== WebSocket.OPEN) {
      throw new Error('Relay is not connected');
    }
    return this.#socket;
  };

  #handleOpen = () => {
    this.#ackHead = false;
    this.#relayReady = false;
    this.#sent.clear();
  };

  #handleMessage = (event: MessageEvent<string | ArrayBuffer>) => {
    this.#receiveChain = this.#receiveChain
      .then(async () => {
        if (typeof event.data === 'string') {
          await this.#handleFrame(JSON.parse(event.data) as ServerFrame);
          return;
        }
        const { header, body } = decodeBinaryFrame(new Uint8Array(event.data));
        await this.#onBinary?.(body, header.message_id);
      })
      .catch((error) => this.#fail(error instanceof Error ? error : new Error(String(error))));
  };

  #handleClose = () => {
    this.#detachSocket();
    this.#relayReady = false;
    this.#sent.clear();
    this.#onDisconnect?.(new Error('Relay connection interrupted'));
    if (this.#manualClose) {
      this.#emitState('disconnected');
      return;
    }
    if (this.#preemptedSeen) {
      this.#emitState('preempted', 'Connection preempted by a new session on the same device');
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
          throw new Error(`Relay returned invalid endpoint: ${frame.endpoint}`);
        }
        this.#relayReady = true;
        this.#reconnectDelay = MIN_RECONNECT_DELAY;
        this.#emitState('connected');
        await this.#flushOutbox();
        return;
      case 'stored': {
        this.#sent.delete(frame.message_id);
        await this.#withStore((store) => store.removeFromOutbox(frame.message_id));
        return;
      }
      case 'rejected': {
        this.#sent.delete(frame.message_id);
        try {
          this.#onMessageRejected?.(frame.message_id, frame.reason);
        } finally {
          await this.#withStore((store) => store.removeFromOutbox(frame.message_id));
        }
        return;
      }
      case 'message': {
        const lastReceived = await this.#withStore((store) => store.lastReceived());
        if (!isNewSequence(lastReceived, frame.sequence)) {
          this.#sendFrame({ type: 'ack', sequence: frame.sequence });
          return;
        }
        await this.#onPayload(frame.payload);
        await this.#withStore((store) => store.markReceived(frame.sequence));
        this.#sendFrame({ type: 'ack', sequence: frame.sequence });
        return;
      }
      case 'ephemeral':
        await this.#onPayload(frame.payload);
        return;
      case 'undeliverable':
        this.#onUndeliverable?.(frame.message_id, frame.reason);
        return;
      case 'error':
        if (frame.message.startsWith('connection_replaced:')) {
          this.#preemptedSeen = true;
          this.#socket?.close();
          return;
        }
        throw new Error(`Relay error: ${frame.message}`);
    }
  };

  #flushOutbox = async () => {
    if (!this.#relayReady || this.#socket?.readyState !== WebSocket.OPEN) return;
    const messages = await this.#withStore((store) => store.outbox());
    for (const message of messages) {
      if (this.#sent.has(message.messageId)) continue;
      this.#sendFrame({
        type: 'message',
        message_id: message.messageId,
        payload: message.payload,
        ...(message.targetDeviceId ? { target_device_id: message.targetDeviceId } : {}),
      });
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
