// Typed records, command/lease/approval/outbox/quota models, and error types for `packages/postgres`.

import type { JsonObject, Message } from "../../ai/src/index.ts";
import type { ApprovalDecision, SessionIdentity } from "../../session/src/index.ts";

export type CommandKind = "prompt" | "steer" | "follow_up" | "continuation";

export type CommandState =
	| "queued"
	| "running"
	| "paused"
	| "completed"
	| "cancelled"
	| "failed"
	| "dead_letter";

export type CommandPayload =
	| {
			readonly kind: "prompt";
			readonly messages: readonly Message[];
			readonly source: "user" | "subagent" | "system";
			readonly parentSessionId?: string;
			readonly parentRunId?: string;
			readonly rootRunId?: string;
			readonly depth?: number;
	  }
	| {
			readonly kind: "steer";
			readonly messages: readonly Message[];
	  }
	| {
			readonly kind: "follow_up";
			readonly messages: readonly Message[];
	  }
	| {
			readonly kind: "continuation";
			readonly targetRunId: string;
			readonly reason: "approval_decided" | "child_result" | "external_signal";
			readonly approvalDecisions?: readonly ApprovalDecision[];
			readonly messages?: readonly Message[];
	  };

export type SessionLeaseRecord = {
	readonly tenantId: string;
	readonly sessionId: string;
	readonly userId: string;
	readonly holder: string;
	readonly fence: number;
	readonly acquiredAt: number;
	readonly renewedAt: number;
	readonly expiresAt: number;
};

export type CommandRecord = {
	readonly tenantId: string;
	readonly commandId: string;
	readonly sessionId: string;
	readonly userId: string;
	readonly commandSeq: number;
	readonly kind: CommandKind;
	readonly payload: CommandPayload;
	readonly payloadHash: string;
	readonly runId: string | null;
	readonly isChild: boolean;
	readonly state: CommandState;
	readonly recoveryAttempts: number;
	readonly maxRecoveryAttempts: number;
	readonly cancelRequestedAt: number | null;
	readonly cancelReason: string | null;
	readonly lastError: string | null;
	readonly acceptedAt: number;
	readonly startedAt: number | null;
	readonly finishedAt: number | null;
	readonly updatedAt: number;
};

export type SubmitCommandInput = {
	readonly identity: SessionIdentity;
	readonly commandId: string;
	readonly payload: CommandPayload;
	readonly runId?: string;
	readonly isChild?: boolean;
	readonly maxRecoveryAttempts?: number;
};

export type SubmitCommandResult = {
	readonly command: CommandRecord;
	readonly deduplicated: boolean;
};

export type ClaimedExecution = {
	readonly command: CommandRecord;
	readonly lease: SessionLeaseRecord;
	readonly runId: string;
	readonly rootRunId: string;
	readonly isRecovery: boolean;
};

export type ApprovalStatus = "pending" | "approved" | "rejected" | "expired" | "consumed";

export type ApprovalRecord = {
	readonly tenantId: string;
	readonly approvalId: string;
	readonly sessionId: string;
	readonly userId: string;
	readonly runId: string;
	readonly operationId: string;
	readonly callId: string;
	readonly approverId: string;
	readonly toolName: string;
	readonly actionPayload: JsonObject;
	readonly actionHash: string;
	readonly context: JsonObject;
	readonly status: ApprovalStatus;
	readonly decidedBy: string | null;
	readonly decisionReason: string | null;
	readonly createdAt: number;
	readonly expiresAt: number;
	readonly decidedAt: number | null;
	readonly consumedAt: number | null;
};

export type OutboxKind = "child_result" | "approval_decided" | "external_signal";

export type OutboxPayload =
	| {
			readonly kind: "approval_decided";
			readonly runId: string;
			readonly approvalIds: readonly string[];
	  }
	| {
			readonly kind: "child_result";
			readonly parentRunId: string;
			readonly childSessionId: string;
			readonly childRunId: string;
			readonly status: "completed" | "failed" | "cancelled";
			readonly summaryText: string;
	  }
	| {
			readonly kind: "external_signal";
			readonly runId: string;
			readonly signalName: string;
			readonly data: JsonObject;
	  };

export type OutboxRecord = {
	readonly messageSeq: number;
	readonly tenantId: string;
	readonly userId: string;
	readonly messageId: string;
	readonly kind: OutboxKind;
	readonly targetSessionId: string;
	readonly sourceSessionId: string | null;
	readonly sourceRunId: string | null;
	readonly payload: OutboxPayload;
	readonly state: "pending" | "delivered" | "dead_letter";
	readonly attempts: number;
	readonly lastError: string | null;
	readonly availableAt: number;
	readonly createdAt: number;
	readonly deliveredAt: number | null;
};

export type DefaultQuotaConfig = {
	readonly tenantMaxActiveRootRuns: number;
	readonly tenantMaxActiveChildRuns: number;
	readonly tenantMaxQueuedCommands: number;
	readonly userMaxActiveRootRuns: number;
	readonly userMaxActiveChildRuns: number;
	readonly userMaxQueuedCommands: number;
};

export const DEFAULT_QUOTA_CONFIG: DefaultQuotaConfig = {
	tenantMaxActiveRootRuns: 20,
	tenantMaxActiveChildRuns: 40,
	tenantMaxQueuedCommands: 500,
	userMaxActiveRootRuns: 5,
	userMaxActiveChildRuns: 10,
	userMaxQueuedCommands: 50,
};

export type EffectiveQuotaLimits = {
	readonly tenantMaxActiveRootRuns: number;
	readonly tenantMaxActiveChildRuns: number;
	readonly tenantMaxQueuedCommands: number;
	readonly userMaxActiveRootRuns: number;
	readonly userMaxActiveChildRuns: number;
	readonly userMaxQueuedCommands: number;
};

export type UpsertTenantQuotaInput = {
	readonly scope: "tenant";
	readonly tenantId: string;
	readonly maxActiveRootRuns: number;
	readonly maxActiveChildRuns: number;
	readonly maxQueuedCommands: number;
	readonly perUserMaxActiveRootRuns?: number | null;
	readonly perUserMaxActiveChildRuns?: number | null;
	readonly perUserMaxQueuedCommands?: number | null;
};

export type UpsertUserQuotaInput = {
	readonly scope: "user";
	readonly tenantId: string;
	readonly userId: string;
	readonly maxActiveRootRuns: number;
	readonly maxActiveChildRuns: number;
	readonly maxQueuedCommands: number;
};

export type UpsertQuotaInput = UpsertTenantQuotaInput | UpsertUserQuotaInput;

export type RunStatus = "running" | "waiting_approval" | "completed" | "cancelled" | "failed";

export type RunRecord = {
	readonly tenantId: string;
	readonly sessionId: string;
	readonly runId: string;
	readonly commandId: string | null;
	readonly userId: string;
	readonly rootRunId: string;
	readonly parentSessionId: string | null;
	readonly parentRunId: string | null;
	readonly depth: number;
	readonly source: "user" | "subagent" | "system";
	readonly status: RunStatus;
	readonly recoveryAttempts: number;
	readonly startedAt: number;
	readonly finishedAt: number | null;
	readonly metadata: JsonObject;
};

export type ArchiveManifestRecord = {
	readonly tenantId: string;
	readonly sessionId: string;
	readonly userId: string;
	readonly startSeq: number;
	readonly endSeq: number;
	readonly objectUri: string;
	readonly versionId: string | null;
	readonly sha256: string;
	readonly formatVersion: number;
	readonly runIds: readonly string[];
	readonly checkpointSeqs: readonly number[];
	readonly metadata: JsonObject;
	readonly createdAt: number;
};

/** Raised with HTTP 409 semantics when a caller reuses `command_id` with a different payload. */
export class CommandPayloadConflictError extends Error {
	readonly tenantId: string;
	readonly commandId: string;

	constructor(tenantId: string, commandId: string) {
		super(`Idempotency conflict: command_id ${commandId} in tenant ${tenantId} already exists with a different payload`);
		this.name = "CommandPayloadConflictError";
		this.tenantId = tenantId;
		this.commandId = commandId;
	}
}

/** Raised with HTTP 429 semantics when tenant or user queued command backpressure limits are exceeded. */
export class QuotaExceededError extends Error {
	readonly scope: "tenant" | "user";
	readonly tenantId: string;
	readonly userId: string;
	readonly limit: number;

	constructor(scope: "tenant" | "user", tenantId: string, userId: string, limit: number) {
		super(
			`Queued command quota exceeded at ${scope} scope (tenant=${tenantId}, user=${userId}, max_queued_commands=${String(limit)})`,
		);
		this.name = "QuotaExceededError";
		this.scope = scope;
		this.tenantId = tenantId;
		this.userId = userId;
		this.limit = limit;
	}
}
