// Durable command executor bridging `ClaimedExecution` (`packages/postgres`) with `runDurableSession` (`packages/session`).

import type { AgentEvent, AgentTool, StreamFn, ThinkingLevel, ToolExecutionMode } from "../../agent/src/index.ts";
import { type Api, contentText, type Model } from "../../ai/src/index.ts";
import type { ClaimedExecution, PostgresCoordinationStore, PostgresSessionStorage } from "../../postgres/src/index.ts";
import {
	type ApprovalPolicy,
	type AuditSink,
	type BlobStore,
	type CompactionSettings,
	FenceConflictError,
	type NamedRuntimeHooks,
	NOOP_AUDIT_SINK,
	runDurableSession,
	type SessionConfigRecord,
	type SessionIdentity,
	type SessionMetadataRecord,
} from "../../session/src/index.ts";
import type { TelemetryContext } from "../../telemetry/src/index.ts";
import type { LeaseSupervisor } from "./lease-supervisor.ts";

/** Host-provided factory resolving the model, tools, and credentials for a claimed session run. */
export interface ResolvedSessionRuntime {
	readonly model: Model<Api>;
	readonly tools: readonly AgentTool[];
	readonly streamFn: StreamFn;
	readonly systemPrompt?: string;
	readonly thinkingLevel?: ThinkingLevel;
	readonly temperature?: number;
	readonly maxTokens?: number;
	readonly maxTurns?: number;
	readonly toolExecution?: ToolExecutionMode;
	readonly requiredHooks?: readonly string[];
	readonly hooks?: NamedRuntimeHooks;
	readonly approvalPolicy?: ApprovalPolicy;
	readonly compaction?: Partial<CompactionSettings>;
	readonly getApiKey?: (provider: string) => Promise<string | undefined> | string | undefined;
	readonly onAgentEvent?: (identity: SessionIdentity, runId: string, event: AgentEvent) => void | Promise<void>;
}

export type SessionRuntimeResolver = (params: {
	readonly identity: SessionIdentity;
	readonly session: SessionMetadataRecord;
	readonly persistedConfig: SessionConfigRecord | null;
	readonly runId: string;
}) => ResolvedSessionRuntime | Promise<ResolvedSessionRuntime>;

export interface DurableExecutorOptions {
	readonly sessionStorage: PostgresSessionStorage;
	readonly coordinationStore: PostgresCoordinationStore;
	readonly leaseSupervisor: LeaseSupervisor;
	readonly runtimeResolver: SessionRuntimeResolver;
	readonly blobStore?: BlobStore;
	readonly blobThresholdBytes?: number;
	readonly auditSink?: AuditSink;
	readonly telemetry?: TelemetryContext;
}

export class DurableExecutor {
	private readonly sessionStorage: PostgresSessionStorage;
	private readonly coordinationStore: PostgresCoordinationStore;
	private readonly leaseSupervisor: LeaseSupervisor;
	private readonly runtimeResolver: SessionRuntimeResolver;
	private readonly blobStore?: BlobStore;
	private readonly blobThresholdBytes?: number;
	private readonly auditSink: AuditSink;
	private readonly telemetry?: TelemetryContext;

	constructor(options: DurableExecutorOptions) {
		this.sessionStorage = options.sessionStorage;
		this.coordinationStore = options.coordinationStore;
		this.leaseSupervisor = options.leaseSupervisor;
		this.runtimeResolver = options.runtimeResolver;
		this.blobStore = options.blobStore;
		this.blobThresholdBytes = options.blobThresholdBytes;
		this.auditSink = options.auditSink ?? NOOP_AUDIT_SINK;
		this.telemetry = options.telemetry;
	}

	/** Execute one claimed command under lease supervision and settle or park its state cleanly. */
	async executeClaimed(claimed: ClaimedExecution): Promise<void> {
		const identity: SessionIdentity = {
			tenantId: claimed.command.tenantId,
			userId: claimed.command.userId,
			sessionId: claimed.command.sessionId,
		};

		const abortController = this.leaseSupervisor.register(claimed.lease, claimed.command.commandId);

		try {
			const windowSnapshot = await this.sessionStorage.readActiveWindow(identity);
			const runtime = await this.runtimeResolver({
				identity,
				session: windowSnapshot.session,
				persistedConfig: windowSnapshot.config,
				runId: claimed.runId,
			});

			const mode =
				claimed.isRecovery || claimed.command.payload.kind === "continuation"
					? {
							kind: "resume" as const,
							runId: claimed.runId,
							...(claimed.command.payload.kind === "continuation"
								? {
										approvalDecisions: claimed.command.payload.approvalDecisions,
										continuationMessages: claimed.command.payload.messages,
									}
								: {}),
						}
					: {
							kind: "prompt" as const,
							runId: claimed.runId,
							rootRunId: claimed.rootRunId,
							commandId: claimed.command.commandId,
							source:
								claimed.command.payload.kind === "prompt"
									? claimed.command.payload.source
									: ("user" as const),
							prompts:
								claimed.command.payload.kind === "prompt"
									? claimed.command.payload.messages
									: [],
						};

			const outcome = await runDurableSession({
				storage: this.sessionStorage,
				identity,
				mode,
				model: runtime.model,
				tools: runtime.tools,
				streamFn: runtime.streamFn,
				systemPrompt: runtime.systemPrompt,
				thinkingLevel: runtime.thinkingLevel,
				temperature: runtime.temperature,
				maxTokens: runtime.maxTokens,
				maxTurns: runtime.maxTurns,
				toolExecution: runtime.toolExecution,
				requiredHooks: runtime.requiredHooks,
				hooks: runtime.hooks,
				approvalPolicy: runtime.approvalPolicy,
				compaction: runtime.compaction,
				blobStore: this.blobStore,
				blobThresholdBytes: this.blobThresholdBytes,
				expectedFence: claimed.lease.fence,
				ensureLeaseValid: () => this.leaseSupervisor.ensureLeaseSafe(identity),
				drainSteeringMessages: () =>
					this.coordinationStore.drainQueuedSessionMessages({
						identity,
						kind: "steer",
						expectedFence: claimed.lease.fence,
					}),
				drainFollowUpMessages: () =>
					this.coordinationStore.drainQueuedSessionMessages({
						identity,
						kind: "follow_up",
						expectedFence: claimed.lease.fence,
					}),
				getApiKey: runtime.getApiKey,
				onAgentEvent: runtime.onAgentEvent
					? (ev) => runtime.onAgentEvent?.(identity, claimed.runId, ev)
					: undefined,
				telemetry: this.telemetry,
				signal: abortController.signal,
			});

			switch (outcome.status) {
				case "paused_for_approval": {
					await this.coordinationStore.parkExecutionForApprovals({
						identity,
						commandId: claimed.command.commandId,
						runId: claimed.runId,
						holder: this.leaseSupervisor.holder,
						expectedFence: claimed.lease.fence,
						approvals: outcome.approvals,
					});
					await this.auditSink({
						tenantId: identity.tenantId,
						userId: identity.userId,
						actorId: "agent",
						actorType: "agent",
						action: "run_parked_for_approval",
						resourceType: "session",
						resourceId: identity.sessionId,
						outcome: "success",
						sessionId: identity.sessionId,
						runId: claimed.runId,
					});
					break;
				}

				case "completed":
				case "cancelled":
				case "failed": {
					const errMsg =
						outcome.status === "failed"
							? outcome.errorMessage
							: outcome.status === "cancelled"
								? outcome.reason
								: undefined;

					await this.coordinationStore.settleExecution({
						identity,
						commandId: claimed.command.commandId,
						runId: claimed.runId,
						holder: this.leaseSupervisor.holder,
						expectedFence: claimed.lease.fence,
						status: outcome.status,
						errorMessage: errMsg,
					});

					// If this was a delegated subagent session, asynchronously report completion to parent via `agent_outbox`!
					if (windowSnapshot.session.originKind === "subagent" && windowSnapshot.session.parentSessionId) {
						const runRecord = await this.coordinationStore.getRun(
							identity.tenantId,
							identity.sessionId,
							claimed.runId,
						);
						const summaryText =
							outcome.status === "completed" && outcome.lastAssistantMessage
								? contentText(outcome.lastAssistantMessage.content)
								: (errMsg ?? outcome.status);

						await this.coordinationStore.enqueueOutbox({
							tenantId: identity.tenantId,
							userId: identity.userId,
							messageId: `child_done:${identity.sessionId}:${claimed.runId}`,
							kind: "child_result",
							targetSessionId: windowSnapshot.session.parentSessionId,
							sourceSessionId: identity.sessionId,
							sourceRunId: claimed.runId,
							payload: {
								kind: "child_result",
								parentRunId: runRecord?.parentRunId ?? "",
								childSessionId: identity.sessionId,
								childRunId: claimed.runId,
								status: outcome.status,
								summaryText,
							},
						});
					}
					break;
				}
			}
		} catch (err) {
			if (err instanceof FenceConflictError) {
				// Another worker legitimately took over `fence + 1`; exit quietly without touching DB state!
				return;
			}
			// Unexpected unhandled error (e.g., missing required hooks or runtime resolver throw):
			// settle command as `failed` and release lease if we still own the fence.
			try {
				await this.coordinationStore.settleExecution({
					identity,
					commandId: claimed.command.commandId,
					runId: claimed.runId,
					holder: this.leaseSupervisor.holder,
					expectedFence: claimed.lease.fence,
					status: "failed",
					errorMessage: err instanceof Error ? err.message : String(err),
				});
			} catch {
				// If lease was also lost concurrently, next recovery cycle handles it via `recovery_attempts`.
			}
			throw err;
		} finally {
			this.leaseSupervisor.unregister(identity);
		}
	}
}
