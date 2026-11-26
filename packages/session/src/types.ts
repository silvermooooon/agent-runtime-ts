// Core domain types, journal event tagged unions, approval contracts, and abstract `SessionStorage` interface.

import type { AgentTool, ThinkingLevel, ToolExecutionMode } from "../../agent/src/index.ts";
import type { Api, JsonObject, JsonValue, Model, ToolCall } from "../../ai/src/index.ts";
import type { StoredAssistantMessage, StoredMessage, StoredToolResultMessage } from "./blob-store.ts";
import { canonicalJsonHash } from "./ids.ts";

/** Mandatory multi-tenant and multi-user coordinates of every session. */
export type SessionIdentity = {
	readonly tenantId: string;
	readonly userId: string;
	readonly sessionId: string;
};

/** Provenance of a session: root conversation, delegated child subagent, or forked branch. */
export type SessionOrigin =
	| { readonly kind: "root" }
	| {
			readonly kind: "subagent";
			readonly rootSessionId: string;
			readonly parentSessionId: string;
			readonly depth: number;
	  }
	| {
			readonly kind: "fork";
			readonly rootSessionId: string;
			readonly parentSessionId: string;
			readonly parentEventId: string;
			readonly depth: number;
	  };

/** Macro lifecycle state of a session stored in `agent_sessions.status`. */
export type SessionStatus = "idle" | "running" | "waiting_approval" | "archived";

/** Durable directory record mirroring `agent_sessions`. */
export type SessionMetadataRecord = {
	readonly tenantId: string;
	readonly sessionId: string;
	readonly userId: string;
	readonly rootSessionId: string;
	readonly parentSessionId: string | null;
	readonly parentEventId: string | null;
	readonly originKind: "root" | "subagent" | "fork";
	readonly depth: number;
	readonly providerSessionId: string;
	readonly status: SessionStatus;
	readonly activeRunId: string | null;
	readonly activeConfigHash: string | null;
	readonly nextSeq: number;
	readonly contextStartSeq: number;
	readonly compactionSeq: number | null;
	readonly archivedUpToSeq: number;
	readonly metadata: JsonObject;
	readonly createdAt: number;
	readonly updatedAt: number;
};

/** Secret-free model descriptor persisted in `agent_session_configs.model_spec`. */
export type PersistedModelSpec = {
	readonly provider: string;
	readonly id: string;
	readonly api: Api;
	readonly baseUrl?: string;
	readonly reasoning?: boolean;
	readonly contextWindow?: number;
	readonly maxTokens?: number;
};

/** Serializable tool schema declaration persisted in `agent_session_configs.tools_schema`. */
export type PersistedToolSchema = {
	readonly name: string;
	readonly description: string;
	readonly parameters: object;
	readonly replay: "never" | "safe";
	readonly executionMode?: ToolExecutionMode;
};

/** Serializable generation options persisted in `agent_session_configs.options`. */
export type PersistedRunOptions = {
	readonly thinkingLevel?: ThinkingLevel;
	readonly temperature?: number;
	readonly maxTokens?: number;
	readonly toolExecution?: ToolExecutionMode;
};

/** Deduplicated configuration payload hashed into `config_hash`. */
export type SessionConfigSpec = {
	readonly modelSpec: PersistedModelSpec;
	readonly toolsSchema: readonly PersistedToolSchema[];
	readonly options: PersistedRunOptions;
	readonly requiredHooks: readonly string[];
};

export type SessionConfigRecord = SessionConfigSpec & {
	readonly tenantId: string;
	readonly sessionId: string;
	readonly userId: string;
	readonly configHash: string;
	readonly createdAt: number;
};

export type ToolCompletionOutcome = "executed" | "blocked" | "interrupted" | "approved" | "rejected";

/**
 * Canonical journal event payload tagged union (`kind` maps 1:1 to `agent_session_events.event_type`).
 */
export type JournalEventPayload =
	| {
			readonly kind: "session_created";
			readonly providerSessionId: string;
			readonly systemPrompt: string;
			readonly origin: SessionOrigin;
			readonly metadata?: JsonObject;
	  }
	| {
			readonly kind: "session_forked";
			readonly sourceSessionId: string;
			readonly sourceEventId: string;
			readonly providerSessionId: string;
	  }
	| {
			readonly kind: "run_started";
			readonly runId: string;
			readonly commandId?: string;
			readonly rootRunId: string;
			readonly source: "user" | "subagent" | "system";
			readonly configHash: string;
			readonly systemPrompt?: string;
	  }
	| {
			readonly kind: "messages_added";
			readonly runId?: string;
			readonly source: "prompt" | "steer" | "follow_up" | "continuation" | "system";
			readonly messages: readonly StoredMessage[];
	  }
	| {
			readonly kind: "model_completed";
			readonly runId: string;
			readonly turnIndex: number;
			readonly message: StoredAssistantMessage;
	  }
	| {
			readonly kind: "tool_completed";
			readonly runId: string;
			readonly turnIndex: number;
			readonly operationId: string;
			readonly outcome: ToolCompletionOutcome;
			readonly result: StoredToolResultMessage;
	  }
	| {
			readonly kind: "turn_completed";
			readonly runId: string;
			readonly turnIndex: number;
	  }
	| {
			readonly kind: "run_paused";
			readonly runId: string;
			readonly turnIndex: number;
			readonly reason: "approval";
			readonly approvalIds: readonly string[];
	  }
	| {
			readonly kind: "run_resumed";
			readonly runId: string;
			readonly turnIndex: number;
	  }
	| {
			readonly kind: "run_completed";
			readonly runId: string;
	  }
	| {
			readonly kind: "run_interrupted";
			readonly runId: string;
			readonly reason: "cancelled" | "error" | "dead_letter";
			readonly errorMessage?: string;
	  }
	| {
			readonly kind: "compaction";
			readonly summary: string;
			readonly firstKeptSeq: number;
			readonly summarizedUpToSeq: number;
			readonly instructions?: string;
	  }
	| {
			readonly kind: "history_reset";
			readonly handoffText?: string;
	  };

export type JournalEventType = JournalEventPayload["kind"];

/** Persisted journal entry row from `agent_session_events`. */
export type JournalEntry = {
	readonly tenantId: string;
	readonly userId: string;
	readonly sessionId: string;
	readonly seq: number;
	readonly eventId: string;
	readonly runId: string | null;
	readonly eventType: JournalEventType;
	readonly fence: number | null;
	readonly payload: JournalEventPayload;
	readonly createdAt: number;
};

export type AppendEventDraft = {
	readonly eventId?: string;
	readonly runId?: string | null;
	readonly payload: JournalEventPayload;
};

export type SessionStateMutation = {
	readonly status?: SessionStatus;
	readonly activeRunId?: string | null;
	readonly activeConfigHash?: string | null;
	readonly contextStartSeq?: number;
	readonly compactionSeq?: number | null;
};

export type AppendEventsInput = {
	readonly identity: SessionIdentity;
	readonly events: readonly AppendEventDraft[];
	readonly expectedFence?: number;
	readonly sessionMutation?: SessionStateMutation;
};

export type AppendEventsResult = {
	readonly entries: readonly JournalEntry[];
	readonly session: SessionMetadataRecord;
};

export type ActiveWindowResult = {
	readonly session: SessionMetadataRecord;
	readonly config: SessionConfigRecord | null;
	readonly entries: readonly JournalEntry[];
};

export type CreateSessionInput = {
	readonly identity: SessionIdentity;
	readonly systemPrompt: string;
	readonly origin?: SessionOrigin;
	readonly providerSessionId?: string;
	readonly metadata?: JsonObject;
};

export type ListSessionsQuery = {
	readonly tenantId: string;
	readonly userId?: string;
	readonly limit?: number;
	readonly includeArchived?: boolean;
};

export type ModelUsagePurpose = "generation" | "compaction" | "memory_dream" | "classifier";

export type ModelUsageStartInput = {
	readonly tenantId: string;
	readonly userId: string;
	readonly attemptId: string;
	readonly sessionId: string;
	readonly runId?: string | null;
	readonly rootRunId?: string | null;
	readonly provider: string;
	readonly model: string;
	readonly api: string;
	readonly purpose: ModelUsagePurpose;
	readonly startedAt?: number;
	readonly metadata?: JsonObject;
};

export type ModelUsageFinishInput = {
	readonly tenantId: string;
	readonly attemptId: string;
	readonly status: "completed" | "aborted" | "failed";
	readonly usageStatus: "known" | "unknown";
	readonly inputTokens?: number | null;
	readonly outputTokens?: number | null;
	readonly cacheReadTokens?: number | null;
	readonly cacheWriteTokens?: number | null;
	readonly totalTokens?: number | null;
	readonly providerResponseId?: string | null;
	readonly finishedAt?: number;
};

export type ModelUsageRecord = {
	readonly tenantId: string;
	readonly attemptId: string;
	readonly sessionId: string;
	readonly runId: string | null;
	readonly rootRunId: string | null;
	readonly userId: string;
	readonly provider: string;
	readonly model: string;
	readonly api: string;
	readonly purpose: ModelUsagePurpose;
	readonly status: "started" | "completed" | "aborted" | "failed";
	readonly usageStatus: "known" | "unknown";
	readonly inputTokens: number | null;
	readonly outputTokens: number | null;
	readonly cacheReadTokens: number | null;
	readonly cacheWriteTokens: number | null;
	readonly totalTokens: number | null;
	readonly providerResponseId: string | null;
	readonly startedAt: number;
	readonly finishedAt: number | null;
	readonly accountingAt: number;
	readonly metadata: JsonObject;
};

/** Optional host-side security/compliance event emitted without coupling runtime tables (ADR-010). */
export type AuditEvent = {
	readonly tenantId: string;
	readonly userId: string;
	readonly actorId: string;
	readonly actorType: "user" | "agent" | "system" | "admin";
	readonly action: string;
	readonly resourceType: string;
	readonly resourceId: string;
	readonly outcome: "success" | "denied" | "failed";
	readonly sessionId?: string;
	readonly runId?: string;
	readonly operationId?: string;
	readonly metadata?: JsonObject;
};

export type AuditSink = (event: AuditEvent) => void | Promise<void>;

export const NOOP_AUDIT_SINK: AuditSink = () => {};

/** Context exposed to every tool invocation inside `DurableLoop`. */
export type DurableToolExecutionContext = {
	readonly tenantId: string;
	readonly userId: string;
	readonly sessionId: string;
	readonly runId: string;
	readonly rootRunId: string;
	readonly turnIndex: number;
	readonly callId: string;
	readonly toolName: string;
	readonly operationId: string;
	readonly fence: number | null;
};

/** Policy hook deciding whether a validated tool call requires human approval before execution. */
export type ApprovalRequirement =
	| { readonly kind: "allow" }
	| {
			readonly kind: "require_approval";
			readonly approverId: string;
			readonly ttlMs?: number;
			readonly context?: JsonObject;
	  };

export type ApprovalPolicy = (params: {
	readonly identity: SessionIdentity;
	readonly runId: string;
	readonly turnIndex: number;
	readonly operationId: string;
	readonly toolCall: ToolCall;
	readonly validatedArgs: JsonValue;
}) => ApprovalRequirement | Promise<ApprovalRequirement>;

/** Pending approval request produced when `DurableLoop` parks a run on `waiting_approval`. */
export type ApprovalRequestSpec = {
	readonly approvalId: string;
	readonly tenantId: string;
	readonly userId: string;
	readonly sessionId: string;
	readonly runId: string;
	readonly operationId: string;
	readonly callId: string;
	readonly approverId: string;
	readonly toolName: string;
	readonly actionPayload: JsonObject;
	readonly actionHash: string;
	readonly context: JsonObject;
	readonly ttlMs: number;
};

/** Settled approval decision supplied when resuming a parked run. */
export type ApprovalDecision = {
	readonly approvalId: string;
	readonly operationId: string;
	readonly callId: string;
	readonly actionHash: string;
	readonly status: "approved" | "rejected" | "expired";
	readonly decidedBy?: string;
	readonly reason?: string;
};

/** Abstract session persistence interface implemented by Memory, JSONL, and PostgreSQL stores. */
export interface SessionStorage {
	createSession(input: CreateSessionInput): Promise<SessionMetadataRecord>;
	getSession(identity: SessionIdentity): Promise<SessionMetadataRecord | null>;
	listSessions(query: ListSessionsQuery): Promise<readonly SessionMetadataRecord[]>;
	putSessionConfig(identity: SessionIdentity, spec: SessionConfigSpec): Promise<SessionConfigRecord>;
	getSessionConfig(identity: SessionIdentity, configHash: string): Promise<SessionConfigRecord | null>;
	appendEvents(input: AppendEventsInput): Promise<AppendEventsResult>;
	readActiveWindow(identity: SessionIdentity): Promise<ActiveWindowResult>;
	readEvents(
		identity: SessionIdentity,
		options?: { readonly fromSeq?: number; readonly toSeq?: number; readonly limit?: number },
	): Promise<readonly JournalEntry[]>;
	recordModelUsageStart(input: ModelUsageStartInput): Promise<void>;
	recordModelUsageFinish(input: ModelUsageFinishInput): Promise<void>;
}

/** Raised when a write is rejected because the caller's lease `fence` is stale or expired. */
export class FenceConflictError extends Error {
	readonly tenantId: string;
	readonly sessionId: string;
	readonly expectedFence: number;

	constructor(tenantId: string, sessionId: string, expectedFence: number, detail?: string) {
		super(
			`Lease fence conflict for session ${tenantId}/${sessionId} (expected fence=${String(expectedFence)})${detail ? `: ${detail}` : ""}`,
		);
		this.name = "FenceConflictError";
		this.tenantId = tenantId;
		this.sessionId = sessionId;
		this.expectedFence = expectedFence;
	}
}

/** Raised when opening or mutating a session that does not exist or belongs to another user. */
export class SessionNotFoundError extends Error {
	constructor(identity: SessionIdentity) {
		super(`Session not found: ${identity.tenantId}/${identity.userId}/${identity.sessionId}`);
		this.name = "SessionNotFoundError";
	}
}

/** Raised when resuming a session whose persisted `required_hooks` are missing in the current worker. */
export class MissingRequiredHooksError extends Error {
	readonly missingHooks: readonly string[];

	constructor(missingHooks: readonly string[]) {
		super(`Cannot resume session: missing required hooks [${missingHooks.join(", ")}]`);
		this.name = "MissingRequiredHooksError";
		this.missingHooks = missingHooks;
	}
}

/** Raised when an approved ticket's `action_hash` does not match the tool call in the journal. */
export class ApprovalActionTamperedError extends Error {
	readonly operationId: string;

	constructor(operationId: string) {
		super(`Approval action_hash mismatch for operation ${operationId}`);
		this.name = "ApprovalActionTamperedError";
		this.operationId = operationId;
	}
}

/** Build a normalized `SessionConfigSpec` and its SHA-256 `configHash` from runtime `Model` and `AgentTool` list. */
export function buildSessionConfigSpec(params: {
	readonly model: Model<Api>;
	readonly tools: readonly AgentTool[];
	readonly options?: PersistedRunOptions;
	readonly requiredHooks?: readonly string[];
}): { readonly spec: SessionConfigSpec; readonly configHash: string } {
	const spec: SessionConfigSpec = {
		modelSpec: {
			provider: params.model.provider,
			id: params.model.id,
			api: params.model.api,
			...(params.model.baseUrl ? { baseUrl: params.model.baseUrl } : {}),
			...(params.model.reasoning !== undefined ? { reasoning: params.model.reasoning } : {}),
			...(params.model.contextWindow !== undefined ? { contextWindow: params.model.contextWindow } : {}),
			...(params.model.maxTokens !== undefined ? { maxTokens: params.model.maxTokens } : {}),
		},
		toolsSchema: params.tools.map((tool) => ({
			name: tool.name,
			description: tool.description,
			parameters: tool.parameters,
			replay: tool.replay ?? "never",
			...(tool.executionMode ? { executionMode: tool.executionMode } : {}),
		})),
		options: params.options ?? {},
		requiredHooks: [...(params.requiredHooks ?? [])].sort(),
	};
	return { spec, configHash: canonicalJsonHash(spec) };
}
