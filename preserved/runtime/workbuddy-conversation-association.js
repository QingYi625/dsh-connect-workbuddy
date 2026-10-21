import { createHash, randomBytes } from 'node:crypto';
import { readFile } from 'node:fs/promises';

const SESSION_HEADER = 'x-dsh-workbuddy-session';
const TURN_HEADER = 'x-dsh-workbuddy-turn';
const uuid7Pattern = /^[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;
const digest = (value) => createHash('sha256').update(value).digest('hex');

function uuid7() {
  const bytes = randomBytes(16);
  bytes.writeUIntBE(Date.now(), 0, 6);
  bytes[6] = (bytes[6] & 15) | 0x70;
  bytes[8] = (bytes[8] & 63) | 0x80;
  const hex = bytes.toString('hex');
  return `${hex.slice(0,8)}-${hex.slice(8,12)}-${hex.slice(12,16)}-${hex.slice(16,20)}-${hex.slice(20)}`;
}

// Only the actual live Agent turn is accepted. Missing context remains ungrouped.
export function activeTurnPosition(phase) {
  return phase?.kind === 'running' && Number.isSafeInteger(phase.turn) && phase.turn >= 0
    ? { turn: phase.turn } : undefined;
}

export function withWorkBuddyTurnAssociation(api, resolvePosition) {
  if (typeof resolvePosition !== 'function') return api;
  const wrap = (inner) => (model, context, options) => {
    const sessionId = options?.sessionId;
    let position;
    try { if (typeof sessionId === 'string' && sessionId.length > 0) position = resolvePosition(sessionId); } catch {}
    if (!position || !Number.isSafeInteger(position.turn) || position.turn < 0) return inner(model, context, options);
    // These headers travel only to the authenticated loopback shim. Raw session IDs are never sent upstream.
    return inner(model, context, {
      ...options,
      headers: {
        ...options.headers,
        [SESSION_HEADER]: digest(sessionId),
        [TURN_HEADER]: String(position.turn),
      },
    });
  };
  return { ...api, stream: wrap(api.stream), streamSimple: wrap(api.streamSimple) };
}

export function readWorkBuddyTurnPosition(headers) {
  const sessionKey = headers?.[SESSION_HEADER];
  const turn = headers?.[TURN_HEADER];
  if (typeof sessionKey !== 'string' || !/^[0-9a-f]{64}$/.test(sessionKey)) return;
  if (typeof turn !== 'string' || !/^(0|[1-9][0-9]{0,14})$/.test(turn)) return;
  const number = Number(turn);
  if (!Number.isSafeInteger(number)) return;
  return { sessionKey, turn: number };
}

export function createWorkBuddyConversationLinks(filename, { withFileLock, writeFileAtomic }) {
  return {
    async resolve(position, accountId) {
      if (!position || !/^[0-9a-f]{64}$/.test(position.sessionKey) || !Number.isSafeInteger(position.turn) || position.turn < 0) return;
      if (typeof accountId !== 'string' || accountId.length === 0) return;
      return withFileLock(filename, async () => {
        let document;
        try { document = JSON.parse(await readFile(filename, 'utf8')); }
        catch (error) {
          if (error.code !== 'ENOENT') throw error;
          document = { version: 1, links: {} };
        }
        if (document?.version !== 1 || !document.links || typeof document.links !== 'object' || Array.isArray(document.links)) throw new Error('Invalid conversation-link store');
        const key = digest(`${position.sessionKey}\0${accountId}`);
        const turn = String(position.turn);
        let link = document.links[key];
        if (link && (!uuid7Pattern.test(link.conversationId) || !link.turns || typeof link.turns !== 'object' || Array.isArray(link.turns))) throw new Error('Invalid conversation-link entry');
        if (link?.turns[turn]) {
          if (!uuid7Pattern.test(link.turns[turn])) throw new Error('Invalid conversation-turn entry');
          return { conversationId: link.conversationId, conversationRequestId: link.turns[turn] };
        }
        link ??= { conversationId: uuid7(), turns: {} };
        link.turns[turn] = uuid7();
        link.updatedAt = Date.now();
        // Bound metadata growth without storing prompts, credentials or raw account/session IDs.
        const turns = Object.keys(link.turns).sort((a,b) => Number(a)-Number(b));
        while (turns.length > 512) delete link.turns[turns.shift()];
        document.links[key] = link;
        const old = Object.entries(document.links).filter(([k]) => k !== key).sort((a,b) => (a[1].updatedAt ?? 0)-(b[1].updatedAt ?? 0));
        while (Object.keys(document.links).length > 2048) delete document.links[old.shift()[0]];
        await writeFileAtomic(filename, JSON.stringify(document) + '\n', { mode: 0o600, dirMode: 0o700 });
        return { conversationId: link.conversationId, conversationRequestId: link.turns[turn] };
      });
    },
  };
}
