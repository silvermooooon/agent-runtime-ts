// Local directory JSONL `SessionStorage` implementation for single-process offline development and inspection.

import { appendFile, mkdir, readdir, readFile, writeFile } from "node:fs/promises";
import { join } from "node:path";
import { canonicalJsonHash, newEventId, newProviderSessionId } from "./ids.ts";
import {
	type ActiveWindowResult,
	type AppendEventsInput,
	type AppendEventsResult,
	type CreateSessionInput,
	type JournalEntry,
	type ListSessionsQuery,
	type ModelUsageFinishInput,
	type ModelUsageStartInput,
	type SessionConfigRecord,
	type SessionConfigSpec,
	type SessionIdentity,
	type SessionMetadataRecord,
	SessionNotFoundError,
	type SessionOrigin,
	type SessionStorage,
} from "./types.ts";

export class JsonlSessionStorage implements SessionStorage {
	private readonly baseDir: string;

	constructor(baseDir: string) {
		this.baseDir = baseDir;
	}

	private sessionDir(tenantId: string, sessionId: string): string {
		return join(this.baseDir, encodeURIComponent(tenantId), encodeURIComponent(sessionId));
	}

	async createSession(input: CreateSessionInput): Promise<SessionMetadataRecord> {
		const { identity } = input;
		const dir = this.sessionDir(identity.tenantId, identity.sessionId);
		await mkdir(join(dir, "configs"), { recursive: true });

		const origin: SessionOrigin = input.origin ?? { kind: "root" };
		const providerSessionId = input.providerSessionId ?? newProviderSessionId();
		const now = Date.now();

		const meta: SessionMetadataRecord = {
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

		await writeFile(join(dir, "session.json"), JSON.stringify(meta, null, 2), "utf8");
		await writeFile(join(dir, "events.jsonl"), "", "utf8");

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

	async getSession(identity: SessionIdentity): Promise<SessionMetadataRecord | null> {
		try {
			const raw = await readFile(join(this.sessionDir(identity.tenantId, identity.sessionId), "session.json"), "utf8");
			const parsed = JSON.parse(raw) as SessionMetadataRecord;
			return parsed.userId === identity.userId ? parsed : null;
		} catch {
			return null;
		}
	}

	async listSessions(query: ListSessionsQuery): Promise<readonly SessionMetadataRecord[]> {
		const tenantDir = join(this.baseDir, encodeURIComponent(query.tenantId));
		let entries: string[];
		try {
			entries = await readdir(tenantDir);
		} catch {
			return [];
		}
		const results: SessionMetadataRecord[] = [];
		for (const sub of entries) {
			try {
				const raw = await readFile(join(tenantDir, sub, "session.json"), "utf8");
				const meta = JSON.parse(raw) as SessionMetadataRecord;
				if (query.userId !== undefined && meta.userId !== query.userId) continue;
				if (!query.includeArchived && meta.status === "archived") continue;
				results.push(meta);
			} catch {
				// Ignore non-session directories.
			}
		}
		results.sort((a, b) => b.createdAt - a.createdAt || b.sessionId.localeCompare(a.sessionId));
		return query.limit !== undefined ? results.slice(0, query.limit) : results;
	}

	async putSessionConfig(identity: SessionIdentity, spec: SessionConfigSpec): Promise<SessionConfigRecord> {
		const session = await this.getSession(identity);
		if (!session) {
			throw new SessionNotFoundError(identity);
		}
		const configHash = canonicalJsonHash(spec);
		const configPath = join(this.sessionDir(identity.tenantId, identity.sessionId), "configs", `${configHash}.json`);
		try {
			const existing = await readFile(configPath, "utf8");
			return JSON.parse(existing) as SessionConfigRecord;
		} catch {
			const record: SessionConfigRecord = {
				...spec,
				tenantId: identity.tenantId,
				sessionId: identity.sessionId,
				userId: identity.userId,
				configHash,
				createdAt: Date.now(),
			};
			await writeFile(configPath, JSON.stringify(record, null, 2), "utf8");
			return record;
		}
	}

	async getSessionConfig(identity: SessionIdentity, configHash: string): Promise<SessionConfigRecord | null> {
		const session = await this.getSession(identity);
		if (!session) return null;
		try {
			const raw = await readFile(
				join(this.sessionDir(identity.tenantId, identity.sessionId), "configs", `${configHash}.json`),
				"utf8",
			);
			return JSON.parse(raw) as SessionConfigRecord;
		} catch {
			return null;
		}
	}

	async appendEvents(input: AppendEventsInput): Promise<AppendEventsResult> {
		const current = await this.getSession(input.identity);
		if (!current) {
			throw new SessionNotFoundError(input.identity);
		}
		const dir = this.sessionDir(input.identity.tenantId, input.identity.sessionId);
		const now = Date.now();
		let seq = current.nextSeq;
		const appended: JournalEntry[] = [];
		const lines: string[] = [];

		for (const draft of input.events) {
			const entry: JournalEntry = {
				tenantId: input.identity.tenantId,
				userId: input.identity.userId,
				sessionId: input.identity.sessionId,
				seq,
				eventId: draft.eventId ?? newEventId(),
				runId: draft.runId ?? null,
				eventType: draft.payload.kind,
				fence: input.expectedFence ?? null,
				payload: draft.payload,
				createdAt: now,
			};
			appended.push(entry);
			lines.push(`${JSON.stringify(entry)}\n`);
			seq += 1;
		}

		if (lines.length > 0) {
			await appendFile(join(dir, "events.jsonl"), lines.join(""), "utf8");
		}

		const mut = input.sessionMutation;
		const updated: SessionMetadataRecord = {
			...current,
			status: mut?.status ?? current.status,
			activeRunId: mut?.activeRunId !== undefined ? mut.activeRunId : current.activeRunId,
			activeConfigHash: mut?.activeConfigHash !== undefined ? mut.activeConfigHash : current.activeConfigHash,
			nextSeq: seq,
			contextStartSeq: mut?.contextStartSeq ?? current.contextStartSeq,
			compactionSeq: mut?.compactionSeq !== undefined ? mut.compactionSeq : current.compactionSeq,
			updatedAt: now,
		};
		await writeFile(join(dir, "session.json"), JSON.stringify(updated, null, 2), "utf8");
		return { entries: appended, session: updated };
	}

	async readActiveWindow(identity: SessionIdentity): Promise<ActiveWindowResult> {
		const session = await this.getSession(identity);
		if (!session) {
			throw new SessionNotFoundError(identity);
		}
		const entries = await this.readEvents(identity, { fromSeq: session.contextStartSeq });
		const config = session.activeConfigHash
			? await this.getSessionConfig(identity, session.activeConfigHash)
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
		const raw = await readFile(join(this.sessionDir(identity.tenantId, identity.sessionId), "events.jsonl"), "utf8");
		const fromSeq = options?.fromSeq ?? 0;
		const toSeq = options?.toSeq ?? Number.MAX_SAFE_INTEGER;
		const entries: JournalEntry[] = [];
		for (const line of raw.split("\n")) {
			const trimmed = line.trim();
			if (!trimmed) continue;
			const parsed = JSON.parse(trimmed) as JournalEntry;
			if (parsed.seq >= fromSeq && parsed.seq <= toSeq) {
				entries.push(parsed);
				if (options?.limit !== undefined && entries.length >= options.limit) {
					break;
				}
			}
		}
		return entries;
	}

	async recordModelUsageStart(input: ModelUsageStartInput): Promise<void> {
		const dir = this.sessionDir(input.tenantId, input.sessionId);
		await appendFile(join(dir, "usage.jsonl"), `${JSON.stringify({ phase: "start", ...input })}\n`, "utf8");
	}

	async recordModelUsageFinish(input: ModelUsageFinishInput): Promise<void> {
		const baseUsagePath = join(this.baseDir, encodeURIComponent(input.tenantId), "usage_finishes.jsonl");
		await mkdir(join(this.baseDir, encodeURIComponent(input.tenantId)), { recursive: true });
		await appendFile(baseUsagePath, `${JSON.stringify({ phase: "finish", ...input })}\n`, "utf8");
	}
}
