// PostgreSQL implementation of `SessionStorage` (`agent_sessions`, `agent_session_configs`, `agent_session_events`, `agent_model_usage`).

import type { JsonObject } from "../../ai/src/index.ts";
import {
	type ActiveWindowResult,
	type AppendEventsInput,
	type AppendEventsResult,
	canonicalJsonHash,
	type CreateSessionInput,
	FenceConflictError,
	type JournalEntry,
	type JournalEventPayload,
	type JournalEventType,
	type ListSessionsQuery,
	type ModelUsageFinishInput,
	type ModelUsageStartInput,
	newEventId,
	newProviderSessionId,
	type PersistedModelSpec,
	type PersistedRunOptions,
	type PersistedToolSchema,
	type SessionConfigRecord,
	type SessionConfigSpec,
	type SessionIdentity,
	type SessionMetadataRecord,
	SessionNotFoundError,
	type SessionOrigin,
	type SessionStatus,
	type SessionStorage,
} from "../../session/src/index.ts";
import { parsePgBigint, parsePgTimestamp, type PgPoolLike, type PgQueryable, withPgTransaction } from "./client.ts";

interface SessionRow {
	tenant_id: string;
	session_id: string;
	user_id: string;
	root_session_id: string;
	parent_session_id: string | null;
	parent_event_id: string | null;
	origin_kind: "root" | "subagent" | "fork";
	depth: number;
	provider_session_id: string;
	status: SessionStatus;
	active_run_id: string | null;
	active_config_hash: string | null;
	next_seq: string | number;
	context_start_seq: string | number;
	compaction_seq: string | number | null;
	archived_up_to_seq: string | number;
	metadata: JsonObject;
	created_at: Date | string;
	updated_at: Date | string;
}

interface ConfigRow {
	tenant_id: string;
	session_id: string;
	user_id: string;
	config_hash: string;
	model_spec: PersistedModelSpec;
	tools_schema: PersistedToolSchema[];
	options: PersistedRunOptions;
	required_hooks: string[];
	created_at: Date | string;
}

interface EventRow {
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

export class PostgresSessionStorage implements SessionStorage {
	private readonly pool: PgPoolLike;

	constructor(pool: PgPoolLike) {
		this.pool = pool;
	}

	async createSession(input: CreateSessionInput): Promise<SessionMetadataRecord> {
		const { identity } = input;
		const origin: SessionOrigin = input.origin ?? { kind: "root" };
		const providerSessionId = input.providerSessionId ?? newProviderSessionId();
		const rootSessionId = origin.kind === "root" ? identity.sessionId : origin.rootSessionId;
		const parentSessionId = origin.kind === "root" ? null : origin.parentSessionId;
		const parentEventId = origin.kind === "fork" ? origin.parentEventId : null;
		const depth = origin.kind === "root" ? 0 : origin.depth;
		const createdPayload: JournalEventPayload = {
			kind: "session_created",
			providerSessionId,
			systemPrompt: input.systemPrompt,
			origin,
			...(input.metadata ? { metadata: input.metadata } : {}),
		};

		return withPgTransaction(this.pool, async (client) => {
			const sessionRes = await client.query<SessionRow>(
				`INSERT INTO agent_sessions (
					tenant_id, session_id, user_id, root_session_id, parent_session_id, parent_event_id,
					origin_kind, depth, provider_session_id, status, next_seq, context_start_seq,
					compaction_seq, archived_up_to_seq, metadata, created_at, updated_at
				) VALUES (
					$1, $2, $3, $4, $5, $6,
					$7, $8, $9, 'idle', 1, 0,
					NULL, -1, $10::jsonb, clock_timestamp(), clock_timestamp()
				)
				RETURNING *`,
				[
					identity.tenantId,
					identity.sessionId,
					identity.userId,
					rootSessionId,
					parentSessionId,
					parentEventId,
					origin.kind,
					depth,
					providerSessionId,
					JSON.stringify(input.metadata ?? {}),
				],
			);

			await client.query(
				`INSERT INTO agent_session_events (
					tenant_id, session_id, user_id, seq, event_id, run_id, event_type, fence, record, created_at
				) VALUES ($1, $2, $3, 0, $4, NULL, 'session_created', NULL, $5::json, clock_timestamp())`,
				[
					identity.tenantId,
					identity.sessionId,
					identity.userId,
					newEventId(),
					JSON.stringify(createdPayload),
				],
			);

			return mapSessionRow(sessionRes.rows[0]);
		});
	}

	async getSession(identity: SessionIdentity): Promise<SessionMetadataRecord | null> {
		const res = await this.pool.query<SessionRow>(
			`SELECT * FROM agent_sessions WHERE tenant_id = $1 AND session_id = $2 AND user_id = $3`,
			[identity.tenantId, identity.sessionId, identity.userId],
		);
		return res.rows.length > 0 ? mapSessionRow(res.rows[0]) : null;
	}

	async listSessions(query: ListSessionsQuery): Promise<readonly SessionMetadataRecord[]> {
		const limit = query.limit ?? 50;
		if (query.userId !== undefined) {
			const res = await this.pool.query<SessionRow>(
				`SELECT * FROM agent_sessions
				 WHERE tenant_id = $1 AND user_id = $2
				   AND ($3::boolean OR status <> 'archived')
				 ORDER BY created_at DESC, session_id DESC
				 LIMIT $4`,
				[query.tenantId, query.userId, query.includeArchived === true, limit],
			);
			return res.rows.map(mapSessionRow);
		}
		const res = await this.pool.query<SessionRow>(
			`SELECT * FROM agent_sessions
			 WHERE tenant_id = $1
			   AND ($2::boolean OR status <> 'archived')
			 ORDER BY created_at DESC, session_id DESC
			 LIMIT $3`,
			[query.tenantId, query.includeArchived === true, limit],
		);
		return res.rows.map(mapSessionRow);
	}

	async putSessionConfig(identity: SessionIdentity, spec: SessionConfigSpec): Promise<SessionConfigRecord> {
		const configHash = canonicalJsonHash(spec);
		const insertRes = await this.pool.query<ConfigRow>(
			`INSERT INTO agent_session_configs (
				tenant_id, session_id, user_id, config_hash, model_spec, tools_schema, options, required_hooks, created_at
			) VALUES ($1, $2, $3, $4, $5::jsonb, $6::json, $7::jsonb, $8::text[], clock_timestamp())
			ON CONFLICT (tenant_id, session_id, config_hash) DO NOTHING
			RETURNING *`,
			[
				identity.tenantId,
				identity.sessionId,
				identity.userId,
				configHash,
				JSON.stringify(spec.modelSpec),
				JSON.stringify(spec.toolsSchema),
				JSON.stringify(spec.options),
				[...spec.requiredHooks],
			],
		);

		if (insertRes.rows.length > 0) {
			return mapConfigRow(insertRes.rows[0]);
		}

		const existing = await this.getSessionConfig(identity, configHash);
		if (!existing) {
			throw new SessionNotFoundError(identity);
		}
		return existing;
	}

	async getSessionConfig(identity: SessionIdentity, configHash: string): Promise<SessionConfigRecord | null> {
		const res = await this.pool.query<ConfigRow>(
			`SELECT * FROM agent_session_configs
			 WHERE tenant_id = $1 AND session_id = $2 AND user_id = $3 AND config_hash = $4`,
			[identity.tenantId, identity.sessionId, identity.userId, configHash],
		);
		return res.rows.length > 0 ? mapConfigRow(res.rows[0]) : null;
	}

	async appendEvents(input: AppendEventsInput): Promise<AppendEventsResult> {
		return withPgTransaction(this.pool, (client) => this.appendEventsInTx(client, input));
	}

	/** Transaction-participating variant of `appendEvents` following strict lock order: `leases` -> `sessions`. */
	async appendEventsInTx(client: PgQueryable, input: AppendEventsInput): Promise<AppendEventsResult> {
		const { identity, expectedFence, sessionMutation } = input;

		// Iron Law #1 & #2: lock `agent_session_leases` FIRST and validate against PG `clock_timestamp()`.
		if (expectedFence !== undefined) {
			const leaseRes = await client.query<{ fence: string | number; is_valid: boolean }>(
				`SELECT fence, (expires_at > clock_timestamp()) AS is_valid
				 FROM agent_session_leases
				 WHERE tenant_id = $1 AND session_id = $2
				 FOR UPDATE`,
				[identity.tenantId, identity.sessionId],
			);
			if (leaseRes.rows.length === 0) {
				throw new FenceConflictError(identity.tenantId, identity.sessionId, expectedFence, "no lease row exists");
			}
			const currentFence = parsePgBigint(leaseRes.rows[0].fence);
			if (!leaseRes.rows[0].is_valid || currentFence !== expectedFence) {
				throw new FenceConflictError(
					identity.tenantId,
					identity.sessionId,
					expectedFence,
					`active=${String(leaseRes.rows[0].is_valid)}, dbFence=${String(currentFence)}`,
				);
			}
		}

		// Lock `agent_sessions` SECOND.
		const sessionRes = await client.query<SessionRow>(
			`SELECT * FROM agent_sessions
			 WHERE tenant_id = $1 AND session_id = $2 AND user_id = $3
			 FOR UPDATE`,
			[identity.tenantId, identity.sessionId, identity.userId],
		);
		if (sessionRes.rows.length === 0) {
			throw new SessionNotFoundError(identity);
		}
		const currentSession = mapSessionRow(sessionRes.rows[0]);
		let nextSeq = currentSession.nextSeq;
		const insertedEntries: JournalEntry[] = [];

		for (const draft of input.events) {
			const eventId = draft.eventId ?? newEventId();
			const evRes = await client.query<EventRow>(
				`INSERT INTO agent_session_events (
					tenant_id, session_id, user_id, seq, event_id, run_id, event_type, fence, record, created_at
				) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9::json, clock_timestamp())
				RETURNING *`,
				[
					identity.tenantId,
					identity.sessionId,
					identity.userId,
					nextSeq,
					eventId,
					draft.runId ?? null,
					draft.payload.kind,
					expectedFence ?? null,
					JSON.stringify(draft.payload),
				],
			);
			insertedEntries.push(mapEventRow(evRes.rows[0]));
			nextSeq += 1;
		}

		const newStatus = sessionMutation?.status ?? currentSession.status;
		const newActiveRunId =
			sessionMutation?.activeRunId !== undefined ? sessionMutation.activeRunId : currentSession.activeRunId;
		const newActiveConfigHash =
			sessionMutation?.activeConfigHash !== undefined
				? sessionMutation.activeConfigHash
				: currentSession.activeConfigHash;
		const newContextStartSeq = sessionMutation?.contextStartSeq ?? currentSession.contextStartSeq;
		const newCompactionSeq =
			sessionMutation?.compactionSeq !== undefined ? sessionMutation.compactionSeq : currentSession.compactionSeq;

		const updatedRes = await client.query<SessionRow>(
			`UPDATE agent_sessions
			 SET status = $3,
			     active_run_id = $4,
			     active_config_hash = $5,
			     next_seq = $6,
			     context_start_seq = $7,
			     compaction_seq = $8,
			     updated_at = clock_timestamp()
			 WHERE tenant_id = $1 AND session_id = $2
			 RETURNING *`,
			[
				identity.tenantId,
				identity.sessionId,
				newStatus,
				newActiveRunId,
				newActiveConfigHash,
				nextSeq,
				newContextStartSeq,
				newCompactionSeq,
			],
		);

		return {
			entries: insertedEntries,
			session: mapSessionRow(updatedRes.rows[0]),
		};
	}

	async readActiveWindow(identity: SessionIdentity): Promise<ActiveWindowResult> {
		const session = await this.getSession(identity);
		if (!session) {
			throw new SessionNotFoundError(identity);
		}
		const [config, eventsRes] = await Promise.all([
			session.activeConfigHash ? this.getSessionConfig(identity, session.activeConfigHash) : Promise.resolve(null),
			this.pool.query<EventRow>(
				`SELECT * FROM agent_session_events
				 WHERE tenant_id = $1 AND session_id = $2 AND seq >= $3
				 ORDER BY seq ASC`,
				[identity.tenantId, identity.sessionId, session.contextStartSeq],
			),
		]);

		return {
			session,
			config,
			entries: eventsRes.rows.map(mapEventRow),
		};
	}

	async readEvents(
		identity: SessionIdentity,
		options?: { readonly fromSeq?: number; readonly toSeq?: number; readonly limit?: number },
	): Promise<readonly JournalEntry[]> {
		const session = await this.getSession(identity);
		if (!session) {
			throw new SessionNotFoundError(identity);
		}
		const fromSeq = options?.fromSeq ?? 0;
		const toSeq = options?.toSeq ?? Number.MAX_SAFE_INTEGER;
		const limit = options?.limit ?? 1_000;

		const res = await this.pool.query<EventRow>(
			`SELECT * FROM agent_session_events
			 WHERE tenant_id = $1 AND session_id = $2 AND seq >= $3 AND seq <= $4
			 ORDER BY seq ASC
			 LIMIT $5`,
			[identity.tenantId, identity.sessionId, fromSeq, toSeq, limit],
		);
		return res.rows.map(mapEventRow);
	}

	async recordModelUsageStart(input: ModelUsageStartInput): Promise<void> {
		await this.pool.query(
			`INSERT INTO agent_model_usage (
				tenant_id, attempt_id, session_id, run_id, root_run_id, user_id,
				provider, model, api, purpose, status, usage_status,
				started_at, accounting_at, metadata
			) VALUES (
				$1, $2, $3, $4, $5, $6,
				$7, $8, $9, $10, 'started', 'unknown',
				clock_timestamp(), clock_timestamp(), $11::jsonb
			)
			ON CONFLICT (tenant_id, attempt_id) DO NOTHING`,
			[
				input.tenantId,
				input.attemptId,
				input.sessionId,
				input.runId ?? null,
				input.rootRunId ?? null,
				input.userId,
				input.provider,
				input.model,
				input.api,
				input.purpose,
				JSON.stringify(input.metadata ?? {}),
			],
		);
	}

	async recordModelUsageFinish(input: ModelUsageFinishInput): Promise<void> {
		const known = input.usageStatus === "known";
		await this.pool.query(
			`UPDATE agent_model_usage
			 SET status = $3,
			     usage_status = $4,
			     input_tokens = $5,
			     output_tokens = $6,
			     cache_read_tokens = $7,
			     cache_write_tokens = $8,
			     total_tokens = $9,
			     provider_response_id = $10,
			     finished_at = clock_timestamp(),
			     accounting_at = clock_timestamp()
			 WHERE tenant_id = $1 AND attempt_id = $2`,
			[
				input.tenantId,
				input.attemptId,
				input.status,
				input.usageStatus,
				known ? (input.inputTokens ?? null) : null,
				known ? (input.outputTokens ?? null) : null,
				known ? (input.cacheReadTokens ?? null) : null,
				known ? (input.cacheWriteTokens ?? null) : null,
				known ? (input.totalTokens ?? null) : null,
				input.providerResponseId ?? null,
			],
		);
	}
}

export function mapSessionRow(row: SessionRow): SessionMetadataRecord {
	return {
		tenantId: row.tenant_id,
		sessionId: row.session_id,
		userId: row.user_id,
		rootSessionId: row.root_session_id,
		parentSessionId: row.parent_session_id,
		parentEventId: row.parent_event_id,
		originKind: row.origin_kind,
		depth: Number(row.depth),
		providerSessionId: row.provider_session_id,
		status: row.status,
		activeRunId: row.active_run_id,
		activeConfigHash: row.active_config_hash,
		nextSeq: parsePgBigint(row.next_seq),
		contextStartSeq: parsePgBigint(row.context_start_seq),
		compactionSeq: row.compaction_seq !== null ? parsePgBigint(row.compaction_seq) : null,
		archivedUpToSeq: parsePgBigint(row.archived_up_to_seq),
		metadata: row.metadata ?? {},
		createdAt: parsePgTimestamp(row.created_at),
		updatedAt: parsePgTimestamp(row.updated_at),
	};
}

function mapConfigRow(row: ConfigRow): SessionConfigRecord {
	return {
		tenantId: row.tenant_id,
		sessionId: row.session_id,
		userId: row.user_id,
		configHash: row.config_hash,
		modelSpec: row.model_spec,
		toolsSchema: row.tools_schema,
		options: row.options ?? {},
		requiredHooks: row.required_hooks ?? [],
		createdAt: parsePgTimestamp(row.created_at),
	};
}

export function mapEventRow(row: EventRow): JournalEntry {
	return {
		tenantId: row.tenant_id,
		userId: row.user_id,
		sessionId: row.session_id,
		seq: parsePgBigint(row.seq),
		eventId: row.event_id,
		runId: row.run_id,
		eventType: row.event_type,
		fence: row.fence !== null ? parsePgBigint(row.fence) : null,
		payload: row.record,
		createdAt: parsePgTimestamp(row.created_at),
	};
}
