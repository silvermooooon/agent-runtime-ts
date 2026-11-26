// Zero-dependency multi-tenant/multi-user in-memory `SessionStorage` with lease-fence simulation and fork support.

import { canonicalJsonHash, newEventId, newProviderSessionId } from "./ids.ts";
import {
	type ActiveWindowResult,
	type AppendEventsInput,
	type AppendEventsResult,
	type CreateSessionInput,
	FenceConflictError,
	type JournalEntry,
	type ListSessionsQuery,
	type ModelUsageFinishInput,
	type ModelUsageRecord,
	type ModelUsageStartInput,
	type SessionConfigRecord,
	type SessionConfigSpec,
	type SessionIdentity,
	type SessionMetadataRecord,
	SessionNotFoundError,
	type SessionOrigin,
	type SessionStorage,
} from "./types.ts";

function sessionKey(tenantId: string, sessionId: string): string {
	return `${tenantId}\0${sessionId}`;
}

export class MemorySessionStorage implements SessionStorage {
	private readonly sessions = new Map<string, SessionMetadataRecord>();
	private readonly configs = new Map<string, Map<string, SessionConfigRecord>>();
	private readonly events = new Map<string, JournalEntry[]>();
	private readonly fences = new Map<string, number>();
	private readonly modelUsages = new Map<string, ModelUsageRecord>();

	/** Set or advance the active fence for a session in tests to verify `FenceConflictError` handling. */
	setFence(tenantId: string, sessionId: string, fence: number): void {
		this.fences.set(sessionKey(tenantId, sessionId), fence);
	}

	/** Inspect all recorded `ModelUsageRecord` rows across runs. */
	listModelUsages(tenantId?: string, userId?: string): readonly ModelUsageRecord[] {
		const rows = [...this.modelUsages.values()];
		return rows.filter(
			(r) => (tenantId === undefined || r.tenantId === tenantId) && (userId === undefined || r.userId === userId),
		);
	}

	async createSession(input: CreateSessionInput): Promise<SessionMetadataRecord> {
		const { identity } = input;
		const key = sessionKey(identity.tenantId, identity.sessionId);
		if (this.sessions.has(key)) {
			throw new Error(`Session already exists: ${identity.tenantId}/${identity.sessionId}`);
		}

		const origin: SessionOrigin = input.origin ?? { kind: "root" };
		const providerSessionId = input.providerSessionId ?? newProviderSessionId();
		const now = Date.now();

		const initialRecord: SessionMetadataRecord = {
			tenantId: identity.tenantId,
			sessionId: identity.sessionId,
			userId: identity.userId,
			rootSessionId: origin.kind === "root" ? identity.sessionId : origin.rootSessionId,
			parentSessionId: origin.kind === "root" ? null : origin.parentSessionId,
			parentEventId: origin.kind === "fork" ? origin.parentEventId : null,
			originKind: origin.kind,
			depth: origin.kind === "root" ? 0 : origin.depth,
			providerSessionId,
			status: "idle",
			activeRunId: null,
			activeConfigHash: null,
			nextSeq: 0,
			contextStartSeq: 0,
			compactionSeq: null,
			archivedUpToSeq: -1,
			metadata: input.metadata ?? {},
			createdAt: now,
			updatedAt: now,
		};

		this.sessions.set(key, initialRecord);
		this.configs.set(key, new Map());
		this.events.set(key, []);

		const created = await this.appendEvents({
			identity,
			events: [
				{
					eventId: newEventId(),
					runId: null,
					payload: {
						kind: "session_created",
						providerSessionId,
						systemPrompt: input.systemPrompt,
						origin,
						...(input.metadata ? { metadata: input.metadata } : {}),
					},
				},
			],
		});
		return created.session;
	}

	/** Create an isolated branch session forked from `sourceEventId` of `sourceIdentity` with a fresh `providerSessionId`. */
	async forkSession(params: {
		readonly sourceIdentity: SessionIdentity;
		readonly newSessionId: string;
		readonly userId?: string;
		readonly upToEventId?: string;
	}): Promise<SessionMetadataRecord> {
		const sourceSession = await this.getSession(params.sourceIdentity);
		if (!sourceSession) {
			throw new SessionNotFoundError(params.sourceIdentity);
		}
		const sourceKey = sessionKey(params.sourceIdentity.tenantId, params.sourceIdentity.sessionId);
		const allSourceEvents = this.events.get(sourceKey) ?? [];
		let sliceEnd = allSourceEvents.length - 1;
		if (params.upToEventId !== undefined) {
			sliceEnd = allSourceEvents.findIndex((e) => e.eventId === params.upToEventId);
			if (sliceEnd < 0) {
				throw new Error(`Fork target event not found: ${params.upToEventId}`);
			}
		}

		const copiedEvents = allSourceEvents.slice(0, sliceEnd + 1);
		const parentEventId = copiedEvents[copiedEvents.length - 1]?.eventId ?? "genesis";
		const targetUserId = params.userId ?? sourceSession.userId;
		const targetIdentity: SessionIdentity = {
			tenantId: sourceSession.tenantId,
			userId: targetUserId,
			sessionId: params.newSessionId,
		};
		const targetKey = sessionKey(targetIdentity.tenantId, targetIdentity.sessionId);
		const freshProviderId = newProviderSessionId();
		const now = Date.now();

		const clonedEntries: JournalEntry[] = copiedEvents.map((e) => ({
			...e,
			userId: targetUserId,
			sessionId: targetIdentity.sessionId,
			fence: null,
		}));

		let contextStartSeq = 0;
		let compactionSeq: number | null = null;
		for (const e of clonedEntries) {
			if (e.payload.kind === "history_reset") {
				contextStartSeq = e.seq;
				compactionSeq = null;
			} else if (e.payload.kind === "compaction") {
				contextStartSeq = e.payload.firstKeptSeq;
				compactionSeq = e.seq;
			}
		}

		const forkedRecord: SessionMetadataRecord = {
			tenantId: targetIdentity.tenantId,
			sessionId: targetIdentity.sessionId,
			userId: targetUserId,
			rootSessionId: sourceSession.rootSessionId,
			parentSessionId: sourceSession.sessionId,
			parentEventId,
			originKind: "fork",
			depth: sourceSession.depth,
			providerSessionId: freshProviderId,
			status: "idle",
			activeRunId: null,
			activeConfigHash: sourceSession.activeConfigHash,
			nextSeq: clonedEntries.length,
			contextStartSeq,
			compactionSeq,
			archivedUpToSeq: -1,
			metadata: { ...sourceSession.metadata },
			createdAt: now,
			updatedAt: now,
		};

		this.sessions.set(targetKey, forkedRecord);
		const targetConfigs = new Map<string, SessionConfigRecord>();
		const sourceConfigs = this.configs.get(sourceKey);
		if (sourceConfigs) {
			for (const [hash, cfg] of sourceConfigs) {
				targetConfigs.set(hash, { ...cfg, sessionId: targetIdentity.sessionId, userId: targetUserId });
			}
		}
		this.configs.set(targetKey, targetConfigs);
		this.events.set(targetKey, clonedEntries);

		const appended = await this.appendEvents({
			identity: targetIdentity,
			events: [
				{
					eventId: newEventId(),
					runId: null,
					payload: {
						kind: "session_forked",
						sourceSessionId: sourceSession.sessionId,
						sourceEventId: parentEventId,
						providerSessionId: freshProviderId,
					},
				},
			],
		});
		return appended.session;
	}

	async getSession(identity: SessionIdentity): Promise<SessionMetadataRecord | null> {
		const record = this.sessions.get(sessionKey(identity.tenantId, identity.sessionId));
		if (!record || record.userId !== identity.userId) {
			return null;
		}
		return record;
	}

	async listSessions(query: ListSessionsQuery): Promise<readonly SessionMetadataRecord[]> {
		const matches: SessionMetadataRecord[] = [];
		for (const record of this.sessions.values()) {
			if (record.tenantId !== query.tenantId) continue;
			if (query.userId !== undefined && record.userId !== query.userId) continue;
			if (!query.includeArchived && record.status === "archived") continue;
			matches.push(record);
		}
		matches.sort((a, b) => b.createdAt - a.createdAt || b.sessionId.localeCompare(a.sessionId));
		return query.limit !== undefined ? matches.slice(0, query.limit) : matches;
	}

	async putSessionConfig(identity: SessionIdentity, spec: SessionConfigSpec): Promise<SessionConfigRecord> {
		const session = await this.getSession(identity);
		if (!session) {
			throw new SessionNotFoundError(identity);
		}
		const key = sessionKey(identity.tenantId, identity.sessionId);
		let sessionConfigs = this.configs.get(key);
		if (!sessionConfigs) {
			sessionConfigs = new Map();
			this.configs.set(key, sessionConfigs);
		}
		const configHash = canonicalJsonHash(spec);
		const existing = sessionConfigs.get(configHash);
		if (existing) {
			return existing;
		}
		const created: SessionConfigRecord = {
			...spec,
			tenantId: identity.tenantId,
			sessionId: identity.sessionId,
			userId: identity.userId,
			configHash,
			createdAt: Date.now(),
		};
		sessionConfigs.set(configHash, created);
		return created;
	}

	async getSessionConfig(identity: SessionIdentity, configHash: string): Promise<SessionConfigRecord | null> {
		const session = await this.getSession(identity);
		if (!session) {
			return null;
		}
		return this.configs.get(sessionKey(identity.tenantId, identity.sessionId))?.get(configHash) ?? null;
	}

	async appendEvents(input: AppendEventsInput): Promise<AppendEventsResult> {
		const { identity, expectedFence, sessionMutation } = input;
		const key = sessionKey(identity.tenantId, identity.sessionId);
		const current = this.sessions.get(key);
		if (!current || current.userId !== identity.userId) {
			throw new SessionNotFoundError(identity);
		}

		if (expectedFence !== undefined) {
			const activeFence = this.fences.get(key);
			if (activeFence !== undefined && activeFence !== expectedFence) {
				throw new FenceConflictError(
					identity.tenantId,
					identity.sessionId,
					expectedFence,
					`current fence is ${String(activeFence)}`,
				);
			}
		}

		const list = this.events.get(key) ?? [];
		const now = Date.now();
		let seq = current.nextSeq;
		const appended: JournalEntry[] = [];

		for (const draft of input.events) {
			const entry: JournalEntry = {
				tenantId: identity.tenantId,
				userId: identity.userId,
				sessionId: identity.sessionId,
				seq,
				eventId: draft.eventId ?? newEventId(),
				runId: draft.runId ?? null,
				eventType: draft.payload.kind,
				fence: expectedFence ?? null,
				payload: draft.payload,
				createdAt: now,
			};
			list.push(entry);
			appended.push(entry);
			seq += 1;
		}
		this.events.set(key, list);

		const updated: SessionMetadataRecord = {
			...current,
			status: sessionMutation?.status ?? current.status,
			activeRunId: sessionMutation?.activeRunId !== undefined ? sessionMutation.activeRunId : current.activeRunId,
			activeConfigHash:
				sessionMutation?.activeConfigHash !== undefined
					? sessionMutation.activeConfigHash
					: current.activeConfigHash,
			nextSeq: seq,
			contextStartSeq: sessionMutation?.contextStartSeq ?? current.contextStartSeq,
			compactionSeq:
				sessionMutation?.compactionSeq !== undefined ? sessionMutation.compactionSeq : current.compactionSeq,
			updatedAt: input.events.length > 0 || sessionMutation !== undefined ? now : current.updatedAt,
		};
		this.sessions.set(key, updated);

		return { entries: appended, session: updated };
	}

	async readActiveWindow(identity: SessionIdentity): Promise<ActiveWindowResult> {
		const session = await this.getSession(identity);
		if (!session) {
			throw new SessionNotFoundError(identity);
		}
		const key = sessionKey(identity.tenantId, identity.sessionId);
		const all = this.events.get(key) ?? [];
		const entries = all.filter((e) => e.seq >= session.contextStartSeq);
		const config = session.activeConfigHash
			? (this.configs.get(key)?.get(session.activeConfigHash) ?? null)
			: null;
		return { session, config, entries };
	}

	async readEvents(
		identity: SessionIdentity,
		options?: { readonly fromSeq?: number; readonly toSeq?: number; readonly limit?: number },
	): Promise<readonly JournalEntry[]> {
		const session = await this.getSession(identity);
		if (!session) {
			throw new SessionNotFoundError(identity);
		}
		const all = this.events.get(sessionKey(identity.tenantId, identity.sessionId)) ?? [];
		const fromSeq = options?.fromSeq ?? 0;
		const toSeq = options?.toSeq ?? Number.MAX_SAFE_INTEGER;
		const filtered = all.filter((e) => e.seq >= fromSeq && e.seq <= toSeq);
		return options?.limit !== undefined ? filtered.slice(0, options.limit) : filtered;
	}

	async recordModelUsageStart(input: ModelUsageStartInput): Promise<void> {
		const startedAt = input.startedAt ?? Date.now();
		this.modelUsages.set(`${input.tenantId}\0${input.attemptId}`, {
			tenantId: input.tenantId,
			attemptId: input.attemptId,
			sessionId: input.sessionId,
			runId: input.runId ?? null,
			rootRunId: input.rootRunId ?? null,
			userId: input.userId,
			provider: input.provider,
			model: input.model,
			api: input.api,
			purpose: input.purpose,
			status: "started",
			usageStatus: "unknown",
			inputTokens: null,
			outputTokens: null,
			cacheReadTokens: null,
			cacheWriteTokens: null,
			totalTokens: null,
			providerResponseId: null,
			startedAt,
			finishedAt: null,
			accountingAt: startedAt,
			metadata: input.metadata ?? {},
		});
	}

	async recordModelUsageFinish(input: ModelUsageFinishInput): Promise<void> {
		const key = `${input.tenantId}\0${input.attemptId}`;
		const existing = this.modelUsages.get(key);
		if (!existing) {
			return;
		}
		const finishedAt = input.finishedAt ?? Date.now();
		this.modelUsages.set(key, {
			...existing,
			status: input.status,
			usageStatus: input.usageStatus,
			inputTokens: input.usageStatus === "known" ? (input.inputTokens ?? null) : null,
			outputTokens: input.usageStatus === "known" ? (input.outputTokens ?? null) : null,
			cacheReadTokens: input.usageStatus === "known" ? (input.cacheReadTokens ?? null) : null,
			cacheWriteTokens: input.usageStatus === "known" ? (input.cacheWriteTokens ?? null) : null,
			totalTokens: input.usageStatus === "known" ? (input.totalTokens ?? null) : null,
			providerResponseId: input.providerResponseId ?? null,
			finishedAt,
			accountingAt: finishedAt,
		});
	}
}
