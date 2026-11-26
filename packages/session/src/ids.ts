// Deterministic hashing, canonical JSON serialization for write boundaries, and UUIDv7 identifiers.

import { createHash } from "node:crypto";
import { uuidv7 } from "../../ai/src/index.ts";

/** Compute a lowercase hex SHA-256 digest of a UTF-8 string or byte buffer. */
export function sha256Hex(input: string | Uint8Array): string {
	return createHash("sha256").update(input).digest("hex");
}

/**
 * Produce a deterministic JSON string with lexicographically sorted object keys.
 * Used exclusively once at write/admission boundaries to compute content-addressed hashes
 * (`config_hash`, `payload_hash`, `action_hash`), never for hot-path equality checks.
 */
export function canonicalJson(value: unknown): string {
	return JSON.stringify(sortValueKeys(value));
}

/** Compute the SHA-256 hex digest of `value` serialized as canonical JSON. */
export function canonicalJsonHash(value: unknown): string {
	return sha256Hex(canonicalJson(value));
}

/**
 * Derive a deterministic, header-safe 35-character idempotency key (`op_<32hex>`)
 * from `(tenantId, sessionId, runId, turnIndex, callId)`.
 */
export function deriveOperationId(params: {
	readonly tenantId: string;
	readonly sessionId: string;
	readonly runId: string;
	readonly turnIndex: number;
	readonly callId: string;
}): string {
	const raw = `${params.tenantId}\0${params.sessionId}\0${params.runId}\0${String(params.turnIndex)}\0${params.callId}`;
	return `op_${sha256Hex(raw).slice(0, 32)}`;
}

/** Generate a monotonic UUIDv7 identifier for a new session. */
export function newSessionId(): string {
	return `ses_${uuidv7()}`;
}

/** Generate a provider-facing UUIDv7 session affinity identifier (`prompt_cache_key`). */
export function newProviderSessionId(): string {
	return uuidv7();
}

/** Generate a monotonic UUIDv7 identifier for a run. */
export function newRunId(): string {
	return `run_${uuidv7()}`;
}

/** Generate a monotonic UUIDv7 identifier for a journal event. */
export function newEventId(): string {
	return `evt_${uuidv7()}`;
}

/** Generate a monotonic UUIDv7 identifier for an approval ticket. */
export function newApprovalId(): string {
	return `apr_${uuidv7()}`;
}

/** Generate a monotonic UUIDv7 identifier for a physical model HTTP attempt. */
export function newAttemptId(): string {
	return `att_${uuidv7()}`;
}

/** Generate a monotonic UUIDv7 identifier for a command submission. */
export function newCommandId(): string {
	return `cmd_${uuidv7()}`;
}

/** Generate a monotonic UUIDv7 identifier for an outbox message. */
export function newOutboxMessageId(): string {
	return `msg_${uuidv7()}`;
}

function sortValueKeys(value: unknown): unknown {
	if (value === null || typeof value !== "object") {
		return value;
	}
	if (Array.isArray(value)) {
		return value.map(sortValueKeys);
	}
	const record = value as Record<string, unknown>;
	const sorted: Record<string, unknown> = {};
	for (const key of Object.keys(record).sort()) {
		const item = record[key];
		if (item !== undefined) {
			sorted[key] = sortValueKeys(item);
		}
	}
	return sorted;
}
