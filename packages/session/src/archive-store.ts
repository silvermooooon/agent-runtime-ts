// Pluggable cold-archive object storage contract (`ArchiveObjectStore`) and `.jsonl.gz` codec (zero external deps).

import { gunzipSync, gzipSync } from "node:zlib";
import { sha256Hex } from "./ids.ts";
import type { JournalEntry } from "./types.ts";

export interface ArchiveWriteInput {
	readonly tenantId: string;
	readonly userId: string;
	readonly sessionId: string;
	readonly startSeq: number;
	readonly endSeq: number;
	readonly entries: readonly JournalEntry[];
	readonly signal?: AbortSignal;
}

export interface ArchiveWriteReceipt {
	readonly objectUri: string;
	readonly versionId: string | null;
	readonly sha256: string;
	readonly compressedBytes: number;
	readonly uncompressedBytes: number;
}

/** Storage-agnostic cold archive target for `agent_session_archives` (S3, MinIO, Workspace cold tier, or Memory). */
export interface ArchiveObjectStore {
	writeArchiveChunk(input: ArchiveWriteInput): Promise<ArchiveWriteReceipt>;
	readArchiveChunk(params: {
		readonly objectUri: string;
		readonly expectedSha256: string;
		readonly signal?: AbortSignal;
	}): Promise<readonly JournalEntry[]>;
}

/** Encode `JournalEntry[]` into deterministic `.jsonl.gz` bytes and compute its SHA-256 digest. */
export function encodeArchiveEntries(entries: readonly JournalEntry[]): {
	readonly gzBytes: Uint8Array;
	readonly sha256: string;
	readonly uncompressedBytes: number;
} {
	const jsonl = `${entries.map((entry) => JSON.stringify(entry)).join("\n")}\n`;
	const rawBytes = Buffer.from(jsonl, "utf8");
	const gz = gzipSync(rawBytes);
	const gzBytes = new Uint8Array(gz.buffer, gz.byteOffset, gz.byteLength);
	return {
		gzBytes,
		sha256: sha256Hex(gzBytes),
		uncompressedBytes: rawBytes.byteLength,
	};
}

/** Verify compressed archive SHA-256 and gunzip back into `JournalEntry[]`. */
export function decodeArchiveEntries(gzBytes: Uint8Array, expectedSha256: string): JournalEntry[] {
	const actualSha256 = sha256Hex(gzBytes);
	if (actualSha256 !== expectedSha256) {
		throw new Error(`Cold archive SHA-256 mismatch: expected ${expectedSha256}, got ${actualSha256}`);
	}
	const uncompressed = gunzipSync(gzBytes).toString("utf8");
	const entries: JournalEntry[] = [];
	for (const line of uncompressed.split("\n")) {
		const trimmed = line.trim();
		if (trimmed.length > 0) {
			entries.push(JSON.parse(trimmed) as JournalEntry);
		}
	}
	return entries;
}

/** In-memory `ArchiveObjectStore` for testing cold-archiving workflows without external storage. */
export class MemoryArchiveObjectStore implements ArchiveObjectStore {
	private readonly chunks = new Map<string, Uint8Array>();

	async writeArchiveChunk(input: ArchiveWriteInput): Promise<ArchiveWriteReceipt> {
		const { gzBytes, sha256, uncompressedBytes } = encodeArchiveEntries(input.entries);
		const objectUri = `mem-archive://${encodeURIComponent(input.tenantId)}/${encodeURIComponent(input.sessionId)}/${String(input.startSeq)}-${String(input.endSeq)}.jsonl.gz`;
		const copy = new Uint8Array(gzBytes.byteLength);
		copy.set(gzBytes);
		this.chunks.set(objectUri, copy);
		return {
			objectUri,
			versionId: null,
			sha256,
			compressedBytes: gzBytes.byteLength,
			uncompressedBytes,
		};
	}

	async readArchiveChunk(params: {
		readonly objectUri: string;
		readonly expectedSha256: string;
	}): Promise<readonly JournalEntry[]> {
		const bytes = this.chunks.get(params.objectUri);
		if (!bytes) {
			throw new Error(`Archive chunk not found: ${params.objectUri}`);
		}
		return decodeArchiveEntries(bytes, params.expectedSha256);
	}
}
