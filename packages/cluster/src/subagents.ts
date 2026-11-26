// Durable subagent spawning coordinator enforcing max depth / fan-out bounds and lock-free outbox result delivery.

import type { AgentTool } from "../../agent/src/index.ts";
import type { JsonObject, Message } from "../../ai/src/index.ts";
import type { PostgresCoordinationStore, PostgresSessionStorage } from "../../postgres/src/index.ts";
import { obj, opt, str } from "../../schema/src/index.ts";
import {
	defineDurableTool,
	newCommandId,
	newRunId,
	newSessionId,
	type SessionIdentity,
} from "../../session/src/index.ts";

export interface SubagentCoordinatorOptions {
	readonly sessionStorage: PostgresSessionStorage;
	readonly coordinationStore: PostgresCoordinationStore;
	/** Maximum allowed subagent recursion depth (`root` is 0). Default: 3. */
	readonly maxDepth?: number;
	/** Default system prompt when spawning a subagent without explicit instructions. */
	readonly defaultSystemPrompt?: string;
	readonly onWakeCluster?: () => void | Promise<void>;
}

export interface SpawnSubagentInput {
	readonly parentIdentity: SessionIdentity;
	readonly parentRunId: string;
	readonly rootRunId: string;
	readonly taskPrompt: string;
	readonly systemPrompt?: string;
	readonly metadata?: JsonObject;
	readonly commandId?: string;
}

export interface SpawnSubagentResult {
	readonly childIdentity: SessionIdentity;
	readonly childRunId: string;
	readonly commandId: string;
	readonly depth: number;
}

export class DurableSubagentCoordinator {
	private readonly sessionStorage: PostgresSessionStorage;
	private readonly coordinationStore: PostgresCoordinationStore;
	private readonly maxDepth: number;
	private readonly defaultSystemPrompt: string;
	private readonly onWakeCluster?: () => void | Promise<void>;

	constructor(options: SubagentCoordinatorOptions) {
		this.sessionStorage = options.sessionStorage;
		this.coordinationStore = options.coordinationStore;
		this.maxDepth = options.maxDepth ?? 3;
		this.defaultSystemPrompt =
			options.defaultSystemPrompt ?? "You are a focused subagent. Complete the assigned task concisely and accurately.";
		this.onWakeCluster = options.onWakeCluster;
	}

	/**
	 * Create an isolated child session under the same `(tenantId, userId)` and enqueue its initial `prompt`
	 * onto the dedicated `is_child = true` worker pool so parent slots never deadlock waiting on children.
	 */
	async spawnSubagent(input: SpawnSubagentInput): Promise<SpawnSubagentResult> {
		const parentSession = await this.sessionStorage.getSession(input.parentIdentity);
		if (!parentSession) {
			throw new Error(
				`Parent session not found: ${input.parentIdentity.tenantId}/${input.parentIdentity.sessionId}`,
			);
		}
		const childDepth = parentSession.depth + 1;
		if (childDepth > this.maxDepth) {
			throw new Error(
				`Subagent depth limit exceeded: depth ${String(childDepth)} > maxDepth ${String(this.maxDepth)}`,
			);
		}

		const childIdentity: SessionIdentity = {
			tenantId: input.parentIdentity.tenantId,
			userId: input.parentIdentity.userId,
			sessionId: newSessionId(),
		};

		await this.sessionStorage.createSession({
			identity: childIdentity,
			systemPrompt: input.systemPrompt ?? this.defaultSystemPrompt,
			origin: {
				kind: "subagent",
				rootSessionId: parentSession.rootSessionId,
				parentSessionId: parentSession.sessionId,
				depth: childDepth,
			},
			metadata: {
				...(input.metadata ?? {}),
				parentRunId: input.parentRunId,
			},
		});

		const childRunId = newRunId();
		const commandId = input.commandId ?? newCommandId();
		const promptMsg: Message = {
			role: "user",
			content: input.taskPrompt,
			timestamp: Date.now(),
		};

		await this.coordinationStore.submitCommand({
			identity: childIdentity,
			commandId,
			runId: childRunId,
			isChild: true,
			payload: {
				kind: "prompt",
				source: "subagent",
				messages: [promptMsg],
				parentSessionId: parentSession.sessionId,
				parentRunId: input.parentRunId,
				rootRunId: input.rootRunId,
				depth: childDepth,
			},
		});

		await this.onWakeCluster?.();

		return {
			childIdentity,
			childRunId,
			commandId,
			depth: childDepth,
		};
	}

	/** Build a `delegate_subagent` `AgentTool` that spawns an asynchronous durable subagent and reports its IDs. */
	createDelegateTool(): AgentTool {
		const SubagentParams = obj({
			task: str({ description: "Clear, self-contained task description for the subagent" }),
			systemPrompt: opt(str({ description: "Optional role-specific system instructions for the subagent" })),
		});

		return defineDurableTool({
			name: "delegate_subagent",
			label: "Delegate Subagent",
			description:
				"Spawn an isolated asynchronous subagent session to work on an independent subtask. " +
				"Its result will be delivered back to this session automatically when finished.",
			parameters: SubagentParams,
			replay: "never",
			execute: async (params, ctx) => {
				if (!ctx) {
					throw new Error("delegate_subagent requires an active DurableSession execution context");
				}
				const spawned = await this.spawnSubagent({
					parentIdentity: {
						tenantId: ctx.tenantId,
						userId: ctx.userId,
						sessionId: ctx.sessionId,
					},
					parentRunId: ctx.runId,
					rootRunId: ctx.rootRunId,
					taskPrompt: params.task,
					systemPrompt: params.systemPrompt,
					// Use deterministic `operationId` as `commandId` so duplicate invocations stay idempotent!
					commandId: `sub_${ctx.operationId}`,
				});
				return {
					content: [
						{
							type: "text",
							text:
								`Spawned durable subagent session ${spawned.childIdentity.sessionId} ` +
								`(run_id=${spawned.childRunId}, depth=${String(spawned.depth)}). ` +
								"It is executing asynchronously and will post its completion back to this conversation.",
						},
					],
					details: {
						childSessionId: spawned.childIdentity.sessionId,
						childRunId: spawned.childRunId,
						depth: spawned.depth,
					},
				};
			},
		});
	}
}
