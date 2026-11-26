// AsyncLocalStorage propagation of `DurableToolExecutionContext` and typed `defineDurableTool` helper.

import { AsyncLocalStorage } from "node:async_hooks";
import type {
	AgentTool,
	AgentToolParams,
	AgentToolResult,
	AgentToolUpdateCallback,
	ToolExecutionMode,
} from "../../agent/src/index.ts";
import type { DurableToolExecutionContext } from "./types.ts";

const TOOL_EXECUTION_STORAGE = new AsyncLocalStorage<DurableToolExecutionContext>();

/** Run a tool callback with `DurableToolExecutionContext` bound in async local storage. */
export function runWithToolExecutionContext<T>(
	context: DurableToolExecutionContext,
	callback: () => Promise<T>,
): Promise<T> {
	return TOOL_EXECUTION_STORAGE.run(context, callback);
}

/** Return the active `DurableToolExecutionContext` when invoked inside `DurableLoop`, or `undefined` in plain `Agent`. */
export function getCurrentToolExecutionContext(): DurableToolExecutionContext | undefined {
	return TOOL_EXECUTION_STORAGE.getStore();
}

export interface DurableToolDefinition<TParameters extends object = object, TDetails = unknown> {
	readonly name: string;
	readonly label: string;
	readonly description: string;
	readonly parameters: TParameters;
	readonly outputSchema?: object;
	readonly replay?: "never" | "safe";
	readonly executionMode?: ToolExecutionMode;
	readonly prepareArguments?: (args: unknown) => AgentToolParams<TParameters>;
	readonly execute: (
		params: AgentToolParams<TParameters>,
		context: DurableToolExecutionContext | undefined,
		signal?: AbortSignal,
		onUpdate?: AgentToolUpdateCallback<TDetails>,
	) => Promise<AgentToolResult<TDetails>>;
}

/** Define an `AgentTool` that receives `DurableToolExecutionContext` (including deterministic `operationId` and `tenantId`/`userId`). */
export function defineDurableTool<TParameters extends object, TDetails = unknown>(
	definition: DurableToolDefinition<TParameters, TDetails>,
): AgentTool<TParameters, TDetails> {
	return {
		name: definition.name,
		label: definition.label,
		description: definition.description,
		parameters: definition.parameters,
		...(definition.outputSchema ? { outputSchema: definition.outputSchema } : {}),
		...(definition.replay ? { replay: definition.replay } : {}),
		...(definition.executionMode ? { executionMode: definition.executionMode } : {}),
		...(definition.prepareArguments ? { prepareArguments: definition.prepareArguments } : {}),
		execute(toolCallId, params, signal, onUpdate) {
			const ctx = getCurrentToolExecutionContext();
			const effectiveCtx: DurableToolExecutionContext | undefined =
				ctx && ctx.callId === toolCallId ? ctx : ctx;
			return definition.execute(params, effectiveCtx, signal, onUpdate);
		},
	};
}
