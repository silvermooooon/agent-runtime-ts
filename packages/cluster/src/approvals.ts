// Human-in-the-loop approval service facade with optional `AuditSink` reporting and instant cluster wakeup.

import type { ApprovalRecord, PostgresCoordinationStore } from "../../postgres/src/index.ts";
import { type AuditSink, NOOP_AUDIT_SINK } from "../../session/src/index.ts";

export interface DurableApprovalServiceOptions {
	readonly coordinationStore: PostgresCoordinationStore;
	readonly auditSink?: AuditSink;
	/** Optional callback (e.g. local `WorkerHost.notifyWorkAvailable` or Redis Pub/Sub) to wake workers immediately. */
	readonly onWakeTarget?: (tenantId: string, sessionId: string) => void | Promise<void>;
}

export class DurableApprovalService {
	private readonly coordinationStore: PostgresCoordinationStore;
	private readonly auditSink: AuditSink;
	private readonly onWakeTarget?: (tenantId: string, sessionId: string) => void | Promise<void>;

	constructor(options: DurableApprovalServiceOptions) {
		this.coordinationStore = options.coordinationStore;
		this.auditSink = options.auditSink ?? NOOP_AUDIT_SINK;
		this.onWakeTarget = options.onWakeTarget;
	}

	/** List pending approvals assigned to `approverId` within `tenantId`. */
	listPending(tenantId: string, approverId: string, limit = 50): Promise<readonly ApprovalRecord[]> {
		return this.coordinationStore.listPendingApprovalsForApprover(tenantId, approverId, limit);
	}

	/** Approve a pending tool ticket; automatically enqueues outbox wakeup once all tickets of the run are settled. */
	async approve(params: {
		readonly tenantId: string;
		readonly approvalId: string;
		readonly decidedBy: string;
		readonly reason?: string;
	}): Promise<{ readonly approval: ApprovalRecord; readonly wokeRun: boolean }> {
		return this.decide({ ...params, decision: "approved" });
	}

	/** Reject a pending tool ticket; automatically enqueues outbox wakeup once all tickets of the run are settled. */
	async reject(params: {
		readonly tenantId: string;
		readonly approvalId: string;
		readonly decidedBy: string;
		readonly reason?: string;
	}): Promise<{ readonly approval: ApprovalRecord; readonly wokeRun: boolean }> {
		return this.decide({ ...params, decision: "rejected" });
	}

	/** Sweep overdue `pending` tickets to `expired` and enqueue wakeup notifications. */
	expireOverdue(limit = 50): Promise<number> {
		return this.coordinationStore.expireOverdueApprovals(limit);
	}

	private async decide(params: {
		readonly tenantId: string;
		readonly approvalId: string;
		readonly decision: "approved" | "rejected";
		readonly decidedBy: string;
		readonly reason?: string;
	}): Promise<{ readonly approval: ApprovalRecord; readonly wokeRun: boolean }> {
		const { approval, allSettledForRun } = await this.coordinationStore.decideApproval(params);

		await this.auditSink({
			tenantId: approval.tenantId,
			userId: approval.userId,
			actorId: params.decidedBy,
			actorType: "user",
			action: params.decision === "approved" ? "tool_approved" : "tool_rejected",
			resourceType: "approval",
			resourceId: approval.approvalId,
			outcome: "success",
			sessionId: approval.sessionId,
			runId: approval.runId,
			operationId: approval.operationId,
			...(params.reason ? { metadata: { reason: params.reason } } : {}),
		});

		if (allSettledForRun) {
			await this.coordinationStore.dispatchPendingOutboxBatch(10);
			await this.onWakeTarget?.(approval.tenantId, approval.sessionId);
		}

		return { approval, wokeRun: allSettledForRun };
	}
}
