// Durable session execution loop: wraps model streaming, tool calls, crash recovery, compaction, and approval parking.

import type {
	AfterToolCallContext,
	AfterToolCallResult,
	AgentContext,
	AgentEvent,
	AgentTool,
	AgentToolResult,
	BeforeToolCallContext,
	BeforeToolCallResult,
	StreamFn,
	ThinkingLevel,
	ToolExecutionMode,
} from "../../agent/src/index.ts";
import {
	type Api,
	type AssistantMessage,
	type Message,
	type Model,
	normalizeContext,
	type SystemMessage,
	type ToolCall,
	type ToolResultMessage,
	toToolDeclaration,
	validateToolArguments,
} from "../../ai/src/index.ts";
import { NOOP_TELEMETRY_CONTEXT, type TelemetryContext } from "../../telemetry/src/index.ts";
import {
	type BlobStore,
	DEFAULT_BLOB_OFFLOAD_THRESHOLD_BYTES,
	offloadMessageBlobs,
	offloadMessagesBlobs,
	type StoredAssistantMessage,
	type StoredToolResultMessage,
} from "./blob-store.ts";
import { type CompactionSettings, compactSessionWindow, DEFAULT_COMPACTION_SETTINGS, estimateStoredMessagesTokens } from "./compaction.ts";
import { canonicalJsonHash, deriveOperationId, newApprovalId, newAttemptId, newEventId } from "./ids.ts";
import {
	createInterruptedToolResultMessage,
	materializeWindowMessages,
	reduceActiveWindow,
	type UnfinishedToolTurn,
} from "./reducer.ts";
import { runWithToolExecutionContext } from "./tool-context.ts";
import {
	ApprovalActionTamperedError,
	type ApprovalDecision,
	type ApprovalPolicy,
	type ApprovalRequestSpec,
	buildSessionConfigSpec,
	MissingRequiredHooksError,
	type SessionIdentity,
	type SessionStorage,
	type ToolCompletionOutcome,
} from "./types.ts";

const DEFAULT_APPROVAL_TTL_MS = 24 * 60 * 60 * 1000;
const DEFAULT_MAX_TURNS = 64;

export type DurableRunStartMode =
	| {
			readonly kind: "prompt";
			readonly runId: string;
			readonly rootRunId: string;
			readonly commandId?: string;
			readonly source: "user" | "subagent" | "system";
			readonly prompts: readonly Message[];
	  }
	| {
			readonly kind: "resume";
			readonly runId: string;
			readonly approvalDecisions?: readonly ApprovalDecision[];
			readonly continuationMessages?: readonly Message[];
	  };

export type DurableRunOutcome =
	| {
			readonly status: "completed";
			readonly runId: string;
			readonly turnCount: number;
			readonly lastAssistantMessage?: AssistantMessage;
	  }
	| {
			readonly status: "paused_for_approval";
			readonly runId: string;
			readonly turnIndex: number;
			readonly approvals: readonly ApprovalRequestSpec[];
	  }
	| {
			readonly status: "cancelled";
			readonly runId: string;
			readonly reason?: string;
	  }
	| {
			readonly status: "failed";
			readonly runId: string;
			readonly errorMessage: string;
	  };

export interface NamedRuntimeHooks {
	readonly names?: readonly string[];
	readonly beforeToolCall?: (
		context: BeforeToolCallContext,
		signal?: AbortSignal,
	) => Promise<BeforeToolCallResult | undefined>;
	readonly afterToolCall?: (
		context: AfterToolCallContext,
		signal?: AbortSignal,
	) => Promise<AfterToolCallResult | undefined>;
}

export interface DurableSessionRunOptions {
	readonly storage: SessionStorage;
	readonly identity: SessionIdentity;
	readonly mode: DurableRunStartMode;
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
	readonly blobStore?: BlobStore;
	readonly blobThresholdBytes?: number;
	readonly expectedFence?: number;
	/** Optional pre-dispatch guard invoked before every external model or tool call to renew lease when near expiry. */
	readonly ensureLeaseValid?: () => Promise<void>;
	/** Drain queued `steer` messages committed while the run is executing. */
	readonly drainSteeringMessages?: () => Promise<readonly Message[]>;
	/** Drain queued `follow_up` messages committed before the run settles. */
	readonly drainFollowUpMessages?: () => Promise<readonly Message[]>;
	readonly getApiKey?: (provider: string) => Promise<string | undefined> | string | undefined;
	readonly onAgentEvent?: (event: AgentEvent) => void | Promise<void>;
	readonly telemetry?: TelemetryContext;
	readonly signal?: AbortSignal;
}

/**
 * Execute or resume a durable multi-turn agent run against `SessionStorage`, enforcing fence tokens,
 * deduplicated tool schemas, `BlobStore` offloading, `pi`-style crash tool recovery, and approval parking.
 */
export async function runDurableSession(options: DurableSessionRunOptions): Promise<DurableRunOutcome> {
	const telemetry = options.telemetry ?? NOOP_TELEMETRY_CONTEXT;
	const { storage, identity, mode } = options;

	return telemetry.startSpan(
		{
			name: "agent.session.run",
			attributes: {
				"tenant.id": identity.tenantId,
				"user.id": identity.userId,
				"session.id": identity.sessionId,
				"run.id": mode.runId,
				"run.mode": mode.kind,
			},
		},
		async () => {
			const compactionSettings: CompactionSettings = {
				...DEFAULT_COMPACTION_SETTINGS,
				...options.compaction,
			};
			const maxTurns = options.maxTurns ?? DEFAULT_MAX_TURNS;

			// 1. Register deduplicated session configuration (`ON CONFLICT DO NOTHING` in PG).
			const { spec: configSpec, configHash } = buildSessionConfigSpec({
				model: options.model,
				tools: options.tools,
				options: {
					...(options.thinkingLevel ? { thinkingLevel: options.thinkingLevel } : {}),
					...(options.temperature !== undefined ? { temperature: options.temperature } : {}),
					...(options.maxTokens !== undefined ? { maxTokens: options.maxTokens } : {}),
					...(options.toolExecution ? { toolExecution: options.toolExecution } : {}),
				},
				requiredHooks: options.requiredHooks,
			});
			await storage.putSessionConfig(identity, configSpec);

			// 2. Load bounded active window (`seq >= context_start_seq`).
			let windowData = await storage.readActiveWindow(identity);
			const persistedRequiredHooks = windowData.config?.requiredHooks ?? configSpec.requiredHooks;
			if (persistedRequiredHooks.length > 0) {
				const providedHooks = new Set(options.hooks?.names ?? []);
				const missing = persistedRequiredHooks.filter((name) => !providedHooks.has(name));
				if (missing.length > 0) {
					throw new MissingRequiredHooksError(missing);
				}
			}

			let rootRunId: string;
			let turnIndex = 0;

			if (mode.kind === "prompt") {
				rootRunId = mode.rootRunId;
				const storedPrompts = await offloadMessagesBlobs(mode.prompts, {
					blobStore: options.blobStore,
					thresholdBytes: options.blobThresholdBytes ?? DEFAULT_BLOB_OFFLOAD_THRESHOLD_BYTES,
					tenantId: identity.tenantId,
					userId: identity.userId,
					sessionId: identity.sessionId,
					runId: mode.runId,
					signal: options.signal,
				});
				await storage.appendEvents({
					identity,
					expectedFence: options.expectedFence,
					events: [
						{
							eventId: newEventId(),
							runId: mode.runId,
							payload: {
								kind: "run_started",
								runId: mode.runId,
								...(mode.commandId ? { commandId: mode.commandId } : {}),
								rootRunId: mode.rootRunId,
								source: mode.source,
								configHash,
								...(options.systemPrompt !== undefined ? { systemPrompt: options.systemPrompt } : {}),
							},
						},
						...(storedPrompts.length > 0
							? [
									{
										eventId: newEventId(),
										runId: mode.runId,
										payload: {
											kind: "messages_added" as const,
											runId: mode.runId,
											source: "prompt" as const,
											messages: storedPrompts,
										},
									},
								]
							: []),
					],
					sessionMutation: {
						status: "running",
						activeRunId: mode.runId,
						activeConfigHash: configHash,
					},
				});
				windowData = await storage.readActiveWindow(identity);
			} else {
				const reducedBeforeResume = reduceActiveWindow(
					windowData.entries,
					options.systemPrompt ?? "",
					windowData.session.providerSessionId,
				);
				rootRunId = reducedBeforeResume.activeRun?.rootRunId ?? `${identity.sessionId}/${mode.runId}`;
				turnIndex = reducedBeforeResume.activeRun?.latestTurnIndex ?? 0;

				// If resuming from paused approval or crash with continuation messages, record `run_resumed`.
				const resumeEvents = [];
				if (reducedBeforeResume.activeRun?.status === "paused") {
					resumeEvents.push({
						eventId: newEventId(),
						runId: mode.runId,
						payload: {
							kind: "run_resumed" as const,
							runId: mode.runId,
							turnIndex,
						},
					});
				}
				if (mode.continuationMessages && mode.continuationMessages.length > 0) {
					const offloadedCont = await offloadMessagesBlobs(mode.continuationMessages, {
						blobStore: options.blobStore,
						thresholdBytes: options.blobThresholdBytes ?? DEFAULT_BLOB_OFFLOAD_THRESHOLD_BYTES,
						tenantId: identity.tenantId,
						userId: identity.userId,
						sessionId: identity.sessionId,
						runId: mode.runId,
						signal: options.signal,
					});
					resumeEvents.push({
						eventId: newEventId(),
						runId: mode.runId,
						payload: {
							kind: "messages_added" as const,
							runId: mode.runId,
							source: "continuation" as const,
							messages: offloadedCont,
						},
					});
				}
				if (resumeEvents.length > 0 || windowData.session.status !== "running") {
					await storage.appendEvents({
						identity,
						expectedFence: options.expectedFence,
						events: resumeEvents,
						sessionMutation: {
							status: "running",
							activeRunId: mode.runId,
							activeConfigHash: configHash,
						},
					});
					windowData = await storage.readActiveWindow(identity);
				}

				// Settle any unfinished tool calls from a mid-turn crash or approval park!
				const reducedForRecovery = reduceActiveWindow(
					windowData.entries,
					options.systemPrompt ?? "",
					windowData.session.providerSessionId,
				);
				if (reducedForRecovery.activeRun?.unfinishedTurn) {
					const recoveryOutcome = await recoverUnfinishedToolTurn({
						options,
						runId: mode.runId,
						rootRunId,
						unfinished: reducedForRecovery.activeRun.unfinishedTurn,
						approvalDecisions: mode.approvalDecisions ?? [],
					});
					if (recoveryOutcome.kind === "paused") {
						return recoveryOutcome.outcome;
					}
					windowData = await storage.readActiveWindow(identity);
				}
			}

			await options.onAgentEvent?.({ type: "agent_start" });

			let lastAssistantMessage: AssistantMessage | undefined;

			// 3. Main Durable Turn Loop
			while (true) {
				if (options.signal?.aborted) {
					await storage.appendEvents({
						identity,
						expectedFence: options.expectedFence,
						events: [
							{
								eventId: newEventId(),
								runId: mode.runId,
								payload: {
									kind: "run_interrupted",
									runId: mode.runId,
									reason: "cancelled",
									errorMessage: "Run cancelled by abort signal",
								},
							},
						],
						sessionMutation: {
							status: "idle",
							activeRunId: null,
						},
					});
					return { status: "cancelled", runId: mode.runId, reason: "Run cancelled by abort signal" };
				}

				if (turnIndex >= maxTurns) {
					const msg = `Exceeded maximum turns per run (${String(maxTurns)})`;
					await storage.appendEvents({
						identity,
						expectedFence: options.expectedFence,
						events: [
							{
								eventId: newEventId(),
								runId: mode.runId,
								payload: {
									kind: "run_interrupted",
									runId: mode.runId,
									reason: "error",
									errorMessage: msg,
								},
							},
						],
						sessionMutation: {
							status: "idle",
							activeRunId: null,
						},
					});
					return { status: "failed", runId: mode.runId, errorMessage: msg };
				}

				// Drain queued steering messages before starting the LLM request.
				const steering = (await options.drainSteeringMessages?.()) ?? [];
				if (steering.length > 0) {
					const storedSteering = await offloadMessagesBlobs(steering, {
						blobStore: options.blobStore,
						thresholdBytes: options.blobThresholdBytes ?? DEFAULT_BLOB_OFFLOAD_THRESHOLD_BYTES,
						tenantId: identity.tenantId,
						userId: identity.userId,
						sessionId: identity.sessionId,
						runId: mode.runId,
						signal: options.signal,
					});
					await storage.appendEvents({
						identity,
						expectedFence: options.expectedFence,
						events: [
							{
								eventId: newEventId(),
								runId: mode.runId,
								payload: {
									kind: "messages_added",
									runId: mode.runId,
									source: "steer",
									messages: storedSteering,
								},
							},
						],
					});
					windowData = await storage.readActiveWindow(identity);
				}

				// Check automatic sliding-window compaction threshold before the model request.
				let reduced = reduceActiveWindow(
					windowData.entries,
					options.systemPrompt ?? "",
					windowData.session.providerSessionId,
				);
				const contextWindowLimit = options.model.contextWindow ?? 128_000;
				if (
					compactionSettings.enabled &&
					estimateStoredMessagesTokens(reduced.storedMessages) >
						contextWindowLimit - compactionSettings.reserveTokens
				) {
					await options.ensureLeaseValid?.();
					const resolvedKey = options.getApiKey ? await options.getApiKey(options.model.provider) : undefined;
					const compacted = await compactSessionWindow({
						storage,
						identity,
						model: options.model,
						streamFn: options.streamFn,
						keepRecentTokens: compactionSettings.keepRecentTokens,
						expectedFence: options.expectedFence,
						runId: mode.runId,
						rootRunId,
						blobStore: options.blobStore,
						apiKey: resolvedKey,
						signal: options.signal,
					});
					if (compacted.compacted) {
						windowData = await storage.readActiveWindow(identity);
						reduced = reduceActiveWindow(
							windowData.entries,
							options.systemPrompt ?? "",
							windowData.session.providerSessionId,
						);
					}
				}

				turnIndex += 1;
				await options.onAgentEvent?.({ type: "turn_start" });

				// Hydrate messages (`BlobRef` -> inline content) and attach active tool declarations to leading SystemMessage.
				const hydratedMessages = await materializeWindowMessages(reduced, {
					blobStore: options.blobStore,
					tenantId: identity.tenantId,
					userId: identity.userId,
					sessionId: identity.sessionId,
					textHydrationMode: "full",
					signal: options.signal,
				});
				const toolDecls = options.tools.map(toToolDeclaration);
				if (hydratedMessages.length > 0 && hydratedMessages[0].role === "system" && toolDecls.length > 0) {
					const sys = hydratedMessages[0] as SystemMessage;
					hydratedMessages[0] = { ...sys, toolsAdded: toolDecls };
				}

				await options.ensureLeaseValid?.();

				const attemptId = newAttemptId();
				const startedAt = Date.now();
				await storage.recordModelUsageStart({
					tenantId: identity.tenantId,
					userId: identity.userId,
					attemptId,
					sessionId: identity.sessionId,
					runId: mode.runId,
					rootRunId,
					provider: options.model.provider,
					model: options.model.id,
					api: options.model.api,
					purpose: "generation",
					startedAt,
				});

				const apiKey = options.getApiKey ? await options.getApiKey(options.model.provider) : undefined;
				const llmContext = normalizeContext({ messages: hydratedMessages });
				const stream = await options.streamFn(options.model, llmContext, {
					apiKey,
					reasoning: options.thinkingLevel === "off" ? undefined : options.thinkingLevel,
					temperature: options.temperature,
					maxTokens: options.maxTokens,
					sessionId: windowData.session.providerSessionId,
					signal: options.signal,
				});

				for await (const streamEvent of stream) {
					if (streamEvent.type === "start") {
						await options.onAgentEvent?.({ type: "message_start", message: { ...streamEvent.partial } });
					} else if (streamEvent.type !== "done" && streamEvent.type !== "error") {
						await options.onAgentEvent?.({
							type: "message_update",
							message: { ...streamEvent.partial },
							assistantMessageEvent: streamEvent,
						});
					}
				}

				const assistantMessage: AssistantMessage = Object.assign(await stream.result(), {
					thinkingLevel: options.thinkingLevel ?? "off",
				});
				lastAssistantMessage = assistantMessage;
				await options.onAgentEvent?.({ type: "message_end", message: assistantMessage });

				const hasKnownUsage =
					assistantMessage.usage.totalTokens > 0 ||
					assistantMessage.stopReason === "stop" ||
					assistantMessage.stopReason === "toolUse" ||
					assistantMessage.stopReason === "length";
				await storage.recordModelUsageFinish({
					tenantId: identity.tenantId,
					attemptId,
					status:
						assistantMessage.stopReason === "error"
							? "failed"
							: assistantMessage.stopReason === "aborted"
								? "aborted"
								: "completed",
					usageStatus: hasKnownUsage ? "known" : "unknown",
					inputTokens: hasKnownUsage ? assistantMessage.usage.input : null,
					outputTokens: hasKnownUsage ? assistantMessage.usage.output : null,
					cacheReadTokens: hasKnownUsage ? assistantMessage.usage.cacheRead : null,
					cacheWriteTokens: hasKnownUsage ? assistantMessage.usage.cacheWrite : null,
					totalTokens: hasKnownUsage ? assistantMessage.usage.totalTokens : null,
					providerResponseId: assistantMessage.responseId ?? null,
					finishedAt: Date.now(),
				});

				const storedAssistant = (await offloadMessageBlobs(assistantMessage, {
					blobStore: options.blobStore,
					thresholdBytes: options.blobThresholdBytes ?? DEFAULT_BLOB_OFFLOAD_THRESHOLD_BYTES,
					tenantId: identity.tenantId,
					userId: identity.userId,
					sessionId: identity.sessionId,
					runId: mode.runId,
					signal: options.signal,
				})) as StoredAssistantMessage;

				// Persist `model_completed` BEFORE running any tool so all toolCall intents are durably recorded.
				await storage.appendEvents({
					identity,
					expectedFence: options.expectedFence,
					events: [
						{
							eventId: newEventId(),
							runId: mode.runId,
							payload: {
								kind: "model_completed",
								runId: mode.runId,
								turnIndex,
								message: storedAssistant,
							},
						},
					],
				});

				if (assistantMessage.stopReason === "error" || assistantMessage.stopReason === "aborted") {
					const reason = assistantMessage.stopReason === "aborted" ? "cancelled" : "error";
					await storage.appendEvents({
						identity,
						expectedFence: options.expectedFence,
						events: [
							{
								eventId: newEventId(),
								runId: mode.runId,
								payload: {
									kind: "run_interrupted",
									runId: mode.runId,
									reason,
									...(assistantMessage.errorMessage ? { errorMessage: assistantMessage.errorMessage } : {}),
								},
							},
						],
						sessionMutation: {
							status: "idle",
							activeRunId: null,
						},
					});
					await options.onAgentEvent?.({ type: "turn_end", message: assistantMessage, toolResults: [] });
					await options.onAgentEvent?.({ type: "agent_end", messages: [assistantMessage] });
					return reason === "cancelled"
						? { status: "cancelled", runId: mode.runId, reason: assistantMessage.errorMessage }
						: {
								status: "failed",
								runId: mode.runId,
								errorMessage: assistantMessage.errorMessage ?? "Model returned error stopReason",
							};
				}

				const toolCalls = assistantMessage.content.filter((part): part is ToolCall => part.type === "toolCall");
				if (toolCalls.length > 0) {
					const batchOutcome = await runToolBatch({
						options,
						runId: mode.runId,
						rootRunId,
						turnIndex,
						assistantMessage,
						currentMessages: [...hydratedMessages, assistantMessage],
						toolCalls,
					});

					if (batchOutcome.kind === "paused") {
						return batchOutcome.outcome;
					}

					await storage.appendEvents({
						identity,
						expectedFence: options.expectedFence,
						events: [
							{
								eventId: newEventId(),
								runId: mode.runId,
								payload: {
									kind: "turn_completed",
									runId: mode.runId,
									turnIndex,
								},
							},
						],
					});

					await options.onAgentEvent?.({
						type: "turn_end",
						message: assistantMessage,
						toolResults: batchOutcome.toolResults,
					});

					windowData = await storage.readActiveWindow(identity);
					if (!batchOutcome.terminate) {
						continue;
					}
				} else {
					await storage.appendEvents({
						identity,
						expectedFence: options.expectedFence,
						events: [
							{
								eventId: newEventId(),
								runId: mode.runId,
								payload: {
									kind: "turn_completed",
									runId: mode.runId,
									turnIndex,
								},
							},
						],
					});
					await options.onAgentEvent?.({ type: "turn_end", message: assistantMessage, toolResults: [] });
				}

				// Check steering or follow-up queues before ending the run.
				const lateSteering = (await options.drainSteeringMessages?.()) ?? [];
				if (lateSteering.length > 0) {
					const storedLate = await offloadMessagesBlobs(lateSteering, {
						blobStore: options.blobStore,
						thresholdBytes: options.blobThresholdBytes ?? DEFAULT_BLOB_OFFLOAD_THRESHOLD_BYTES,
						tenantId: identity.tenantId,
						userId: identity.userId,
						sessionId: identity.sessionId,
						runId: mode.runId,
						signal: options.signal,
					});
					await storage.appendEvents({
						identity,
						expectedFence: options.expectedFence,
						events: [
							{
								eventId: newEventId(),
								runId: mode.runId,
								payload: {
									kind: "messages_added",
									runId: mode.runId,
									source: "steer",
									messages: storedLate,
								},
							},
						],
					});
					windowData = await storage.readActiveWindow(identity);
					continue;
				}

				const followUps = (await options.drainFollowUpMessages?.()) ?? [];
				if (followUps.length > 0) {
					const storedFollowUps = await offloadMessagesBlobs(followUps, {
						blobStore: options.blobStore,
						thresholdBytes: options.blobThresholdBytes ?? DEFAULT_BLOB_OFFLOAD_THRESHOLD_BYTES,
						tenantId: identity.tenantId,
						userId: identity.userId,
						sessionId: identity.sessionId,
						runId: mode.runId,
						signal: options.signal,
					});
					await storage.appendEvents({
						identity,
						expectedFence: options.expectedFence,
						events: [
							{
								eventId: newEventId(),
								runId: mode.runId,
								payload: {
									kind: "messages_added",
									runId: mode.runId,
									source: "follow_up",
									messages: storedFollowUps,
								},
							},
						],
					});
					windowData = await storage.readActiveWindow(identity);
					continue;
				}

				// Settle run cleanly to idle.
				await storage.appendEvents({
					identity,
					expectedFence: options.expectedFence,
					events: [
						{
							eventId: newEventId(),
							runId: mode.runId,
							payload: {
								kind: "run_completed",
								runId: mode.runId,
							},
						},
					],
					sessionMutation: {
						status: "idle",
						activeRunId: null,
					},
				});

				await options.onAgentEvent?.({
					type: "agent_end",
					messages: lastAssistantMessage ? [lastAssistantMessage] : [],
				});
				return {
					status: "completed",
					runId: mode.runId,
					turnCount: turnIndex,
					...(lastAssistantMessage ? { lastAssistantMessage } : {}),
				};
			}
		},
	);
}

type ToolBatchOutcome =
	| {
			readonly kind: "completed";
			readonly toolResults: ToolResultMessage[];
			readonly terminate: boolean;
	  }
	| {
			readonly kind: "paused";
			readonly outcome: Extract<DurableRunOutcome, { status: "paused_for_approval" }>;
	  };

async function runToolBatch(params: {
	readonly options: DurableSessionRunOptions;
	readonly runId: string;
	readonly rootRunId: string;
	readonly turnIndex: number;
	readonly assistantMessage: AssistantMessage;
	readonly currentMessages: Message[];
	readonly toolCalls: readonly ToolCall[];
}): Promise<ToolBatchOutcome> {
	const { options, runId, rootRunId, turnIndex, assistantMessage, currentMessages, toolCalls } = params;
	const { identity, storage } = options;

	// If response hit maxTokens (`stopReason === "length"`), fail all truncated tool calls safely without executing.
	if (assistantMessage.stopReason === "length") {
		const truncatedResults: ToolResultMessage[] = [];
		for (const call of toolCalls) {
			const operationId = deriveOperationId({
				tenantId: identity.tenantId,
				sessionId: identity.sessionId,
				runId,
				turnIndex,
				callId: call.id,
			});
			const errMsg: ToolResultMessage = {
				role: "toolResult",
				toolCallId: call.id,
				toolName: call.name,
				isError: true,
				content: [
					{
						type: "text",
						text: `Tool call "${call.name}" was not executed because the model response hit maxTokens and arguments may be truncated.`,
					},
				],
				timestamp: Date.now(),
			};
			await persistSingleToolCompletion(options, runId, turnIndex, operationId, "blocked", errMsg);
			truncatedResults.push(errMsg);
		}
		return { kind: "completed", toolResults: truncatedResults, terminate: false };
	}

	const agentContext: AgentContext = {
		messages: currentMessages,
		tools: [...options.tools],
	};

	type PreparedCall =
		| {
				readonly kind: "settled";
				readonly call: ToolCall;
				readonly operationId: string;
				readonly message: ToolResultMessage;
				readonly outcome: ToolCompletionOutcome;
				readonly terminate: boolean;
		  }
		| {
				readonly kind: "execute";
				readonly call: ToolCall;
				readonly operationId: string;
				readonly tool: AgentTool;
				readonly validatedArgs: unknown;
		  };

	const prepared: PreparedCall[] = [];
	const approvalSpecs: ApprovalRequestSpec[] = [];

	for (const call of toolCalls) {
		const operationId = deriveOperationId({
			tenantId: identity.tenantId,
			sessionId: identity.sessionId,
			runId,
			turnIndex,
			callId: call.id,
		});
		const tool = options.tools.find((t) => t.name === call.name);
		if (!tool) {
			prepared.push({
				kind: "settled",
				call,
				operationId,
				outcome: "blocked",
				terminate: false,
				message: createTextErrorResult(call, `Tool "${call.name}" is not available.`),
			});
			continue;
		}

		let validatedArgs: unknown;
		try {
			const preparedCall = tool.prepareArguments
				? { ...call, arguments: tool.prepareArguments(call.arguments) as ToolCall["arguments"] }
				: call;
			validatedArgs = validateToolArguments(tool, preparedCall);
		} catch (err) {
			prepared.push({
				kind: "settled",
				call,
				operationId,
				outcome: "blocked",
				terminate: false,
				message: createTextErrorResult(call, err instanceof Error ? err.message : String(err)),
			});
			continue;
		}

		if (options.hooks?.beforeToolCall) {
			const hookRes = await options.hooks.beforeToolCall(
				{
					assistantMessage,
					toolCall: call,
					args: validatedArgs,
					context: agentContext,
				},
				options.signal,
			);
			if (hookRes?.block) {
				prepared.push({
					kind: "settled",
					call,
					operationId,
					outcome: "blocked",
					terminate: hookRes.terminate === true,
					message: createTextErrorResult(call, hookRes.reason ?? `Tool "${call.name}" blocked by policy.`),
				});
				continue;
			}
		}

		if (options.approvalPolicy) {
			const decision = await options.approvalPolicy({
				identity,
				runId,
				turnIndex,
				operationId,
				toolCall: call,
				validatedArgs: validatedArgs as never,
			});
			if (decision.kind === "require_approval") {
				const actionPayload = { toolName: call.name, arguments: call.arguments };
				approvalSpecs.push({
					approvalId: newApprovalId(),
					tenantId: identity.tenantId,
					userId: identity.userId,
					sessionId: identity.sessionId,
					runId,
					operationId,
					callId: call.id,
					approverId: decision.approverId,
					toolName: call.name,
					actionPayload,
					actionHash: canonicalJsonHash(actionPayload),
					context: decision.context ?? {},
					ttlMs: decision.ttlMs ?? DEFAULT_APPROVAL_TTL_MS,
				});
				continue;
			}
		}

		prepared.push({
			kind: "execute",
			call,
			operationId,
			tool,
			validatedArgs,
		});
	}

	// Persist any immediately settled (blocked/invalid) calls right away so recovery never re-evaluates them.
	const settledByCallId = new Map<string, { message: ToolResultMessage; terminate: boolean }>();
	for (const item of prepared) {
		if (item.kind === "settled") {
			await persistSingleToolCompletion(options, runId, turnIndex, item.operationId, item.outcome, item.message);
			settledByCallId.set(item.call.id, { message: item.message, terminate: item.terminate });
		}
	}

	// If any tool call requires human approval, park the run (`waiting_approval`) so the caller releases the lease!
	if (approvalSpecs.length > 0) {
		await storage.appendEvents({
			identity,
			expectedFence: options.expectedFence,
			events: [
				{
					eventId: newEventId(),
					runId,
					payload: {
						kind: "run_paused",
						runId,
						turnIndex,
						reason: "approval",
						approvalIds: approvalSpecs.map((a) => a.approvalId),
					},
				},
			],
			sessionMutation: {
				status: "waiting_approval",
				activeRunId: runId,
			},
		});
		return {
			kind: "paused",
			outcome: {
				status: "paused_for_approval",
				runId,
				turnIndex,
				approvals: approvalSpecs,
			},
		};
	}

	const executable = prepared.filter((item): item is Extract<PreparedCall, { kind: "execute" }> => item.kind === "execute");
	const forceSequential =
		options.toolExecution === "sequential" ||
		executable.some((item) => item.tool.executionMode === "sequential");

	const runOne = async (item: Extract<PreparedCall, { kind: "execute" }>): Promise<void> => {
		await options.ensureLeaseValid?.();
		const executed = await invokeToolWithHooks({
			options,
			runId,
			rootRunId,
			turnIndex,
			assistantMessage,
			agentContext,
			call: item.call,
			operationId: item.operationId,
			tool: item.tool,
			validatedArgs: item.validatedArgs,
		});
		await persistSingleToolCompletion(options, runId, turnIndex, item.operationId, "executed", executed.message);
		settledByCallId.set(item.call.id, executed);
	};

	if (forceSequential) {
		for (const item of executable) {
			await runOne(item);
		}
	} else {
		await Promise.all(executable.map((item) => runOne(item)));
	}

	const orderedMessages: ToolResultMessage[] = [];
	let allTerminate = toolCalls.length > 0;
	for (const call of toolCalls) {
		const entry = settledByCallId.get(call.id);
		if (entry) {
			orderedMessages.push(entry.message);
			if (!entry.terminate) {
				allTerminate = false;
			}
		}
	}

	return {
		kind: "completed",
		toolResults: orderedMessages,
		terminate: allTerminate,
	};
}

async function recoverUnfinishedToolTurn(params: {
	readonly options: DurableSessionRunOptions;
	readonly runId: string;
	readonly rootRunId: string;
	readonly unfinished: UnfinishedToolTurn;
	readonly approvalDecisions: readonly ApprovalDecision[];
}): Promise<{ readonly kind: "completed" } | { readonly kind: "paused"; readonly outcome: Extract<DurableRunOutcome, { status: "paused_for_approval" }> }> {
	const { options, runId, rootRunId, unfinished, approvalDecisions } = params;
	const { identity, storage } = options;
	const decisionByOpId = new Map<string, ApprovalDecision>();
	for (const d of approvalDecisions) {
		decisionByOpId.set(d.operationId, d);
	}

	const agentContext: AgentContext = {
		messages: [unfinished.assistantMessage],
		tools: [...options.tools],
	};

	for (const call of unfinished.pendingToolCalls) {
		const operationId = deriveOperationId({
			tenantId: identity.tenantId,
			sessionId: identity.sessionId,
			runId,
			turnIndex: unfinished.turnIndex,
			callId: call.id,
		});
		const approval = decisionByOpId.get(operationId);

		if (approval) {
			const expectedActionHash = canonicalJsonHash({ toolName: call.name, arguments: call.arguments });
			if (approval.actionHash !== expectedActionHash) {
				throw new ApprovalActionTamperedError(operationId);
			}
			if (approval.status !== "approved") {
				const rejectedMsg = createTextErrorResult(
					call,
					`Tool "${call.name}" was ${approval.status}${approval.reason ? `: ${approval.reason}` : "."}`,
				);
				await persistSingleToolCompletion(options, runId, unfinished.turnIndex, operationId, "rejected", rejectedMsg);
				continue;
			}
			const tool = options.tools.find((t) => t.name === call.name);
			if (!tool) {
				const missingMsg = createTextErrorResult(call, `Approved tool "${call.name}" is no longer registered.`);
				await persistSingleToolCompletion(options, runId, unfinished.turnIndex, operationId, "blocked", missingMsg);
				continue;
			}
			const preparedCall = tool.prepareArguments
				? { ...call, arguments: tool.prepareArguments(call.arguments) as ToolCall["arguments"] }
				: call;
			const validatedArgs = validateToolArguments(tool, preparedCall);
			await options.ensureLeaseValid?.();
			const executed = await invokeToolWithHooks({
				options,
				runId,
				rootRunId,
				turnIndex: unfinished.turnIndex,
				assistantMessage: unfinished.assistantMessage,
				agentContext,
				call,
				operationId,
				tool,
				validatedArgs,
			});
			await persistSingleToolCompletion(options, runId, unfinished.turnIndex, operationId, "approved", executed.message);
			continue;
		}

		// Mid-flight crash recovery (ADR-005 `pi` compromise):
		// - `replay: "safe"` tools rerun automatically.
		// - All other tools receive a structured `interrupted` error result so the LLM verifies state first.
		const tool = options.tools.find((t) => t.name === call.name);
		if (tool?.replay === "safe") {
			const preparedCall = tool.prepareArguments
				? { ...call, arguments: tool.prepareArguments(call.arguments) as ToolCall["arguments"] }
				: call;
			const validatedArgs = validateToolArguments(tool, preparedCall);
			await options.ensureLeaseValid?.();
			const executed = await invokeToolWithHooks({
				options,
				runId,
				rootRunId,
				turnIndex: unfinished.turnIndex,
				assistantMessage: unfinished.assistantMessage,
				agentContext,
				call,
				operationId,
				tool,
				validatedArgs,
			});
			await persistSingleToolCompletion(options, runId, unfinished.turnIndex, operationId, "executed", executed.message);
		} else {
			const interruptedMsg = createInterruptedToolResultMessage(call, operationId);
			await persistSingleToolCompletion(
				options,
				runId,
				unfinished.turnIndex,
				operationId,
				"interrupted",
				interruptedMsg,
			);
		}
	}

	await storage.appendEvents({
		identity,
		expectedFence: options.expectedFence,
		events: [
			{
				eventId: newEventId(),
				runId,
				payload: {
					kind: "turn_completed",
					runId,
					turnIndex: unfinished.turnIndex,
				},
			},
		],
	});

	return { kind: "completed" };
}

async function invokeToolWithHooks(params: {
	readonly options: DurableSessionRunOptions;
	readonly runId: string;
	readonly rootRunId: string;
	readonly turnIndex: number;
	readonly assistantMessage: AssistantMessage;
	readonly agentContext: AgentContext;
	readonly call: ToolCall;
	readonly operationId: string;
	readonly tool: AgentTool;
	readonly validatedArgs: unknown;
}): Promise<{ readonly message: ToolResultMessage; readonly terminate: boolean }> {
	const { options, runId, rootRunId, turnIndex, assistantMessage, agentContext, call, operationId, tool, validatedArgs } =
		params;

	await options.onAgentEvent?.({
		type: "tool_execution_start",
		toolCallId: call.id,
		toolName: call.name,
		args: validatedArgs,
	});

	const startedNs = process.hrtime.bigint();
	let rawResult: AgentToolResult;
	let isError = false;

	try {
		rawResult = await runWithToolExecutionContext(
			{
				tenantId: options.identity.tenantId,
				userId: options.identity.userId,
				sessionId: options.identity.sessionId,
				runId,
				rootRunId,
				turnIndex,
				callId: call.id,
				toolName: call.name,
				operationId,
				fence: options.expectedFence ?? null,
			},
			() =>
				tool.execute(call.id, validatedArgs as never, options.signal, (partial) => {
					void options.onAgentEvent?.({
						type: "tool_execution_update",
						toolCallId: call.id,
						toolName: call.name,
						args: validatedArgs,
						partialResult: partial,
					});
				}),
		);
		isError = rawResult.isError === true;
	} catch (err) {
		rawResult = {
			content: [{ type: "text", text: err instanceof Error ? err.message : String(err) }],
			details: undefined,
			isError: true,
		};
		isError = true;
	}

	const durationMs = Math.max(0, Number(process.hrtime.bigint() - startedNs) / 1_000_000);

	if (options.hooks?.afterToolCall) {
		const override = await options.hooks.afterToolCall(
			{
				assistantMessage,
				toolCall: call,
				args: validatedArgs,
				result: rawResult,
				isError,
				context: agentContext,
			},
			options.signal,
		);
		if (override) {
			rawResult = {
				...rawResult,
				...(override.content !== undefined ? { content: override.content } : {}),
				...(override.details !== undefined ? { details: override.details as never } : {}),
				...(override.isError !== undefined ? { isError: override.isError } : {}),
				...(override.usage !== undefined ? { usage: override.usage } : {}),
				...(override.terminate !== undefined ? { terminate: override.terminate } : {}),
			};
			if (override.isError !== undefined) {
				isError = override.isError;
			}
		}
	}

	await options.onAgentEvent?.({
		type: "tool_execution_end",
		toolCallId: call.id,
		toolName: call.name,
		result: rawResult,
		isError,
		durationMs,
	});

	const message: ToolResultMessage = {
		role: "toolResult",
		toolCallId: call.id,
		toolName: call.name,
		content: rawResult.content,
		...(rawResult.details !== undefined ? { details: rawResult.details } : {}),
		...(rawResult.usage !== undefined ? { usage: rawResult.usage } : {}),
		isError,
		timestamp: Date.now(),
		durationMs,
	};

	await options.onAgentEvent?.({ type: "message_start", message });
	await options.onAgentEvent?.({ type: "message_end", message });

	return { message, terminate: rawResult.terminate === true };
}

async function persistSingleToolCompletion(
	options: DurableSessionRunOptions,
	runId: string,
	turnIndex: number,
	operationId: string,
	outcome: ToolCompletionOutcome,
	message: ToolResultMessage,
): Promise<void> {
	const offloaded = (await offloadMessageBlobs(message, {
		blobStore: options.blobStore,
		thresholdBytes: options.blobThresholdBytes ?? DEFAULT_BLOB_OFFLOAD_THRESHOLD_BYTES,
		tenantId: options.identity.tenantId,
		userId: options.identity.userId,
		sessionId: options.identity.sessionId,
		runId,
		signal: options.signal,
	})) as StoredToolResultMessage;

	await options.storage.appendEvents({
		identity: options.identity,
		expectedFence: options.expectedFence,
		events: [
			{
				eventId: newEventId(),
				runId,
				payload: {
					kind: "tool_completed",
					runId,
					turnIndex,
					operationId,
					outcome,
					result: offloaded,
				},
			},
		],
	});
}

function createTextErrorResult(call: ToolCall, errorText: string): ToolResultMessage {
	return {
		role: "toolResult",
		toolCallId: call.id,
		toolName: call.name,
		content: [{ type: "text", text: errorText }],
		isError: true,
		timestamp: Date.now(),
	};
}
