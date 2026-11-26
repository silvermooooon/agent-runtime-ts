// Cold journal archiver (`agent_session_archives`): exports `seq < context_start_seq` to cold storage and deletes hot rows.

import type { JsonObject } from "../../ai/src/index.ts";
import {
	type ArchiveObjectStore,
	type JournalEntry,
	type JournalEventPayload,
	type JournalEventType,
	type SessionIdentity,
	SessionNotFoundError,
} from "../../session/src/index.ts";
import { parsePgBigint, parsePgTimestamp, type PgPoolLike, withPgTransaction } from "./client.ts";
import { mapEventRow } from "./session-store.ts";
import type { ArchiveManifestRecord } from "./types.ts";

interface ArchiveRow {
	tenant_id: string;
	session_id: string;
	user_id: string;
	start_seq: string | number;
	end_seq: string | number;
	object_uri: string;
	version_id: string | null;
	sha256: string;
	format_version: number;
	run_ids: string[];
	checkpoint_seqs: (string | number)[];
	metadata: JsonObject;
	created_at: Date | string;
}

interface RawEventRow {
	tenant_id: string;
	session_id: string;
	user_id: string;
	seq: string | number;
	event_id: string;
	run_id: string | null;
	event_type: JournalEventType;
	fence: string | number | null;
	record: JournalEventPayload;
	created_at: Date | string;
}

export class PostgresColdArchiver {
	private readonly pool: PgPoolLike;
	private readonly objectStore: ArchiveObjectStore;

	constructor(pool: PgPoolLike, objectStore: ArchiveObjectStore) {
		this.pool = pool;
		this.objectStore = objectStore;
	}

	/**
	 * Export historical events strictly older than `context_start_seq` (`[archived_up_to_seq + 1, context_start_seq - 1]`)
	 * to `objectStore` (with SHA-256 verification), record `agent_session_archives`, and `DELETE` the hot rows.
	 */
	async archiveHistoricalWindow(
		identity: SessionIdentity,
		options?: { readonly maxBatchEvents?: number; readonly signal?: AbortSignal },
	): Promise<ArchiveManifestRecord | null> {
		const sessionRes = await this.pool.query<{
			user_id: string;
			context_start_seq: string | number;
			archived_up_to_seq: string | number;
		}>(
			`SELECT user_id, context_start_seq, archived_up_to_seq
			 FROM agent_sessions
			 WHERE tenant_id = $1 AND session_id = $2 AND user_id = $3`,
			[identity.tenantId, identity.sessionId, identity.userId],
		);
		if (sessionRes.rows.length === 0) {
			throw new SessionNotFoundError(identity);
		}

		const contextStartSeq = parsePgBigint(sessionRes.rows[0].context_start_seq);
		const archivedUpToSeq = parsePgBigint(sessionRes.rows[0].archived_up_to_seq);
		const startSeq = archivedUpToSeq + 1;
		const maxEndSeq = contextStartSeq - 1;
		if (startSeq > maxEndSeq) {
			return null;
		}

		const batchLimit = options?.maxBatchEvents ?? 2_000;
		const endSeqBound = Math.min(maxEndSeq, startSeq + batchLimit - 1);

		const eventsRes = await this.pool.query<RawEventRow>(
			`SELECT * FROM agent_session_events
			 WHERE tenant_id = $1 AND session_id = $2 AND seq >= $3 AND seq <= $4
			 ORDER BY seq ASC`,
			[identity.tenantId, identity.sessionId, startSeq, endSeqBound],
		);
		if (eventsRes.rows.length === 0) {
			return null;
		}

		const entries = eventsRes.rows.map(mapEventRow);
		const actualStartSeq = entries[0].seq;
		const actualEndSeq = entries[entries.length - 1].seq;

		const receipt = await this.objectStore.writeArchiveChunk({
			tenantId: identity.tenantId,
			userId: identity.userId,
			sessionId: identity.sessionId,
			startSeq: actualStartSeq,
			endSeq: actualEndSeq,
			entries,
			signal: options?.signal,
		});

		const runIdSet = new Set<string>();
		const checkpointSeqs: number[] = [];
		for (const entry of entries) {
			if (entry.runId) runIdSet.add(entry.runId);
			if (entry.eventType === "compaction") checkpointSeqs.push(entry.seq);
		}

		return withPgTransaction(this.pool, async (client) => {
			// Lock session row and verify `archived_up_to_seq` did not race with another archiver worker.
			const locked = await client.query<{ archived_up_to_seq: string | number; context_start_seq: string | number }>(
				`SELECT archived_up_to_seq, context_start_seq
				 FROM agent_sessions
				 WHERE tenant_id = $1 AND session_id = $2 AND user_id = $3
				 FOR UPDATE`,
				[identity.tenantId, identity.sessionId, identity.userId],
			);
			if (locked.rows.length === 0) {
				throw new SessionNotFoundError(identity);
			}
			if (parsePgBigint(locked.rows[0].archived_up_to_seq) !== archivedUpToSeq) {
				return null;
			}

			const archiveRes = await client.query<ArchiveRow>(
				`INSERT INTO agent_session_archives (
					tenant_id, session_id, user_id, start_seq, end_seq, object_uri, version_id,
					sha256, format_version, run_ids, checkpoint_seqs, metadata, created_at
				) VALUES (
					$1, $2, $3, $4, $5, $6, $7,
					$8, 1, $9::text[], $10::bigint[], $11::jsonb, clock_timestamp()
				)
				RETURNING *`,
				[
					identity.tenantId,
					identity.sessionId,
					identity.userId,
					actualStartSeq,
					actualEndSeq,
					receipt.objectUri,
					receipt.versionId,
					receipt.sha256,
					[...runIdSet],
					checkpointSeqs,
					JSON.stringify({
						compressedBytes: receipt.compressedBytes,
						uncompressedBytes: receipt.uncompressedBytes,
					}),
				],
			);

			await client.query(
				`DELETE FROM agent_session_events
				 WHERE tenant_id = $1 AND session_id = $2 AND seq >= $3 AND seq <= $4`,
				[identity.tenantId, identity.sessionId, actualStartSeq, actualEndSeq],
			);

			await client.query(
				`UPDATE agent_sessions
				 SET archived_up_to_seq = $3
				 WHERE tenant_id = $1 AND session_id = $2`,
				[identity.tenantId, identity.sessionId, actualEndSeq],
			);

			return mapArchiveRow(archiveRes.rows[0]);
		});
	}

	/** Seamlessly read `[fromSeq, toSeq]` across cold archived chunks and hot `agent_session_events` rows. */
	async readFullHistoryRange(
		identity: SessionIdentity,
		params: { readonly fromSeq: number; readonly toSeq: number; readonly signal?: AbortSignal },
	): Promise<readonly JournalEntry[]> {
		const [manifestsRes, hotRes] = await Promise.all([
			this.pool.query<ArchiveRow>(
				`SELECT * FROM agent_session_archives
				 WHERE tenant_id = $1 AND session_id = $2 AND user_id = $3
				   AND end_seq >= $4 AND start_seq <= $5
				 ORDER BY start_seq ASC`,
				[identity.tenantId, identity.sessionId, identity.userId, params.fromSeq, params.toSeq],
			),
			this.pool.query<RawEventRow>(
				`SELECT * FROM agent_session_events
				 WHERE tenant_id = $1 AND session_id = $2 AND user_id = $3
				   AND seq >= $4 AND seq <= $5
				 ORDER BY seq ASC`,
				[identity.tenantId, identity.sessionId, identity.userId, params.fromSeq, params.toSeq],
			),
		]);

		const mergedBySeq = new Map<number, JournalEntry>();
		for (const row of manifestsRes.rows) {
			const manifest = mapArchiveRow(row);
			const coldEntries = await this.objectStore.readArchiveChunk({
				objectUri: manifest.objectUri,
				expectedSha256: manifest.sha256,
				signal: params.signal,
			});
			for (const entry of coldEntries) {
				if (entry.seq >= params.fromSeq && entry.seq <= params.toSeq) {
					mergedBySeq.set(entry.seq, entry);
				}
			}
		}

		for (const hotRow of hotRes.rows) {
			const entry = mapEventRow(hotRow);
			mergedBySeq.set(entry.seq, entry);
		}

		return [...mergedBySeq.values()].sort((a, b) => a.seq - b.seq);
	}
}

function mapArchiveRow(row: ArchiveRow): ArchiveManifestRecord {
	return {
		tenantId: row.tenant_id,
		sessionId: row.session_id,
		userId: row.user_id,
		startSeq: parsePgBigint(row.start_seq),
		endSeq: parsePgBigint(row.end_seq),
		objectUri: row.object_uri,
		versionId: row.version_id,
		sha256: row.sha256,
		formatVersion: Number(row.format_version),
		runIds: row.run_ids ?? [],
		checkpointSeqs: (row.checkpoint_seqs ?? []).map(parsePgBigint),
		metadata: row.metadata ?? {},
		createdAt: parsePgTimestamp(row.created_at),
	};
}
