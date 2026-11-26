// Pure sliding-window Reducer folding `[contextStartSeq .. nextSeq - 1]` into active messages and crash-recovery state.

import type { AssistantMessage, Message, SystemMessage, ToolCall, ToolResultMessage } from "../../ai/src/index.ts";
import {
	type BlobHydrateOptions,
	hydrateMessagesBlobs,
	type StoredMessage,
	type StoredToolResultMessage,
} from "./blob-store.ts";
import type { JournalEntry } from "./types.ts";

const COMPACTION_SECTION_NAME = "compaction_summary";

/** Unfinished assistant tool-calling turn detected during crash recovery or approval wakeup. */
export type UnfinishedToolTurn = {
	readonly turnIndex: number;
	readonly assistantMessage: AssistantMessage;
	readonly allToolCalls: readonly ToolCall[];
	readonly completedByCallId: ReadonlyMap<string, StoredToolResultMessage>;
	readonly pendingToolCalls: readonly ToolCall[];
	readonly pendingApprovalIds: readonly string[];
};

export type ActiveRunState = {
	readonly runId: string;
	readonly rootRunId: string;
	readonly commandId?: string;
	readonly source: "user" | "subagent" | "system";
	readonly configHash: string;
	readonly status: "running" | "paused";
	readonly latestTurnIndex: number;
	readonly unfinishedTurn: UnfinishedToolTurn | null;
};

export type ReducedWindowState = {
	readonly providerSessionId: string;
	readonly systemPrompt: string;
	readonly compactionSummary: string | null;
	readonly storedMessages: readonly StoredMessage[];
	readonly activeRun: ActiveRunState | null;
};

/**
 * Deterministically fold active-window journal entries (`seq >= contextStartSeq`) into
 * stored messages and exact mid-turn recovery state, without touching any external I/O.
 */
export function reduceActiveWindow(
	entries: readonly JournalEntry[],
	fallbackSystemPrompt = "",
	fallbackProviderSessionId = "",
): ReducedWindowState {
	// First locate the newest `history_reset` or `compaction` in the window so kept verbatim
	// entries (`firstKeptSeq <= seq < compactionSeq`) and post-compaction entries assemble cleanly.
	let resetIndex = -1;
	let latestCompactionIndex = -1;
	let providerSessionId = fallbackProviderSessionId;
	let systemPrompt = fallbackSystemPrompt;

	for (let i = 0; i < entries.length; i++) {
		const { payload } = entries[i];
		switch (payload.kind) {
			case "session_created":
				providerSessionId = payload.providerSessionId;
				systemPrompt = payload.systemPrompt;
				break;
			case "session_forked":
				providerSessionId = payload.providerSessionId;
				break;
			case "run_started":
				if (payload.systemPrompt !== undefined) {
					systemPrompt = payload.systemPrompt;
				}
				break;
			case "history_reset":
				resetIndex = i;
				latestCompactionIndex = -1;
				break;
			case "compaction":
				latestCompactionIndex = i;
				break;
			default:
				break;
		}
	}

	let compactionSummary: string | null = null;
	let effectiveStartSeq = entries.length > 0 ? entries[0].seq : 0;
	if (resetIndex >= 0) {
		effectiveStartSeq = entries[resetIndex].seq;
	}
	if (latestCompactionIndex > resetIndex) {
		const compactionEntry = entries[latestCompactionIndex];
		if (compactionEntry.payload.kind === "compaction") {
			compactionSummary = compactionEntry.payload.summary;
			effectiveStartSeq = Math.max(effectiveStartSeq, compactionEntry.payload.firstKeptSeq);
		}
	}

	const leadingSystemMessage: SystemMessage = {
		role: "system",
		content: systemPrompt,
		...(compactionSummary !== null
			? {
					sections: {
						[COMPACTION_SECTION_NAME]: `<conversation_summary>\n${compactionSummary}\n</conversation_summary>`,
					},
				}
			: {}),
		timestamp: entries[0]?.createdAt ?? Date.now(),
	};

	const conversationMessages: StoredMessage[] = [leadingSystemMessage];

	let activeRunId: string | null = null;
	let activeRootRunId = "";
	let activeCommandId: string | undefined;
	let activeSource: "user" | "subagent" | "system" = "user";
	let activeConfigHash = "";
	let activeRunStatus: "running" | "paused" = "running";
	let latestTurnIndex = 0;

	let openTurnIndex: number | null = null;
	let openAssistantMessage: AssistantMessage | null = null;
	let openToolCalls: ToolCall[] = [];
	let openCompletedResults = new Map<string, StoredToolResultMessage>();
	let openApprovalIds: readonly string[] = [];

	function flushOpenTurnCompletedResults(): void {
		if (!openAssistantMessage) {
			return;
		}
		for (const call of openToolCalls) {
			const completed = openCompletedResults.get(call.id);
			if (completed) {
				conversationMessages.push(completed);
			}
		}
	}

	for (let i = 0; i < entries.length; i++) {
		const entry = entries[i];
		if (entry.seq < effectiveStartSeq) {
			continue;
		}
		const { payload } = entry;

		switch (payload.kind) {
			case "history_reset": {
				if (payload.handoffText && payload.handoffText.trim().length > 0) {
					conversationMessages.push({
						role: "user",
						content: payload.handoffText,
						timestamp: entry.createdAt,
					});
				}
				break;
			}

			case "run_started": {
				activeRunId = payload.runId;
				activeRootRunId = payload.rootRunId;
				activeCommandId = payload.commandId;
				activeSource = payload.source;
				activeConfigHash = payload.configHash;
				activeRunStatus = "running";
				latestTurnIndex = 0;
				openTurnIndex = null;
				openAssistantMessage = null;
				openToolCalls = [];
				openCompletedResults = new Map();
				openApprovalIds = [];
				break;
			}

			case "messages_added": {
				for (const msg of payload.messages) {
					if (msg.role === "system" && msg.content === systemPrompt && !msg.toolsAdded && !msg.toolsRemoved) {
						continue;
					}
					conversationMessages.push(msg);
				}
				break;
			}

			case "model_completed": {
				flushOpenTurnCompletedResults();
				conversationMessages.push(payload.message);
				latestTurnIndex = payload.turnIndex;

				const toolCalls = payload.message.content.filter((part): part is ToolCall => part.type === "toolCall");
				if (
					toolCalls.length > 0 &&
					payload.message.stopReason !== "error" &&
					payload.message.stopReason !== "aborted"
				) {
					// Hydrate stored assistant text blocks lazily if needed; toolCalls themselves are never offloaded.
					openTurnIndex = payload.turnIndex;
					openAssistantMessage = {
						...payload.message,
						content: payload.message.content.map((part) =>
							part.type === "blob_text"
								? {
										type: "text",
										text: part.ref.preview ?? `[External text ${part.ref.uri}]`,
										...(part.textSignature !== undefined ? { textSignature: part.textSignature } : {}),
									}
								: part,
						),
					};
					openToolCalls = toolCalls;
					openCompletedResults = new Map();
					openApprovalIds = [];
				} else {
					openTurnIndex = null;
					openAssistantMessage = null;
					openToolCalls = [];
					openCompletedResults = new Map();
					openApprovalIds = [];
				}
				break;
			}

			case "tool_completed": {
				if (openAssistantMessage && openTurnIndex === payload.turnIndex) {
					// Keep in map so toolResult messages are emitted in deterministic assistant toolCall order!
					openCompletedResults.set(payload.result.toolCallId, payload.result);
				} else {
					conversationMessages.push(payload.result);
				}
				break;
			}

			case "turn_completed": {
				flushOpenTurnCompletedResults();
				openTurnIndex = null;
				openAssistantMessage = null;
				openToolCalls = [];
				openCompletedResults = new Map();
				openApprovalIds = [];
				break;
			}

			case "run_paused": {
				activeRunStatus = "paused";
				openApprovalIds = payload.approvalIds;
				break;
			}

			case "run_resumed": {
				activeRunStatus = "running";
				openApprovalIds = [];
				break;
			}

			case "run_completed":
			case "run_interrupted": {
				flushOpenTurnCompletedResults();
				openTurnIndex = null;
				openAssistantMessage = null;
				openToolCalls = [];
				openCompletedResults = new Map();
				openApprovalIds = [];
				activeRunId = null;
				break;
			}

			case "session_created":
			case "session_forked":
			case "compaction":
				break;
		}
	}

	let unfinishedTurn: UnfinishedToolTurn | null = null;
	if (activeRunId !== null && openAssistantMessage !== null && openTurnIndex !== null) {
		const pendingToolCalls = openToolCalls.filter((call) => !openCompletedResults.has(call.id));
		if (pendingToolCalls.length > 0) {
			unfinishedTurn = {
				turnIndex: openTurnIndex,
				assistantMessage: openAssistantMessage,
				allToolCalls: openToolCalls,
				completedByCallId: openCompletedResults,
				pendingToolCalls,
				pendingApprovalIds: openApprovalIds,
			};
		} else {
			flushOpenTurnCompletedResults();
		}
	}

	const activeRun: ActiveRunState | null =
		activeRunId !== null
			? {
					runId: activeRunId,
					rootRunId: activeRootRunId,
					...(activeCommandId !== undefined ? { commandId: activeCommandId } : {}),
					source: activeSource,
					configHash: activeConfigHash,
					status: activeRunStatus,
					latestTurnIndex,
					unfinishedTurn,
				}
			: null;

	return {
		providerSessionId,
		systemPrompt,
		compactionSummary,
		storedMessages: conversationMessages,
		activeRun,
	};
}

/** Hydrate a `ReducedWindowState`'s `storedMessages` into ready-to-send `Message[]`. */
export async function materializeWindowMessages(
	reduced: ReducedWindowState,
	options: BlobHydrateOptions,
): Promise<Message[]> {
	return hydrateMessagesBlobs(reduced.storedMessages, options);
}

/**
 * Construct the `pi`-style interrupted error `ToolResultMessage` (ADR-005) when a non-replay-safe
 * tool call was cut off mid-flight by a worker crash or lease takeover.
 */
export function createInterruptedToolResultMessage(
	toolCall: ToolCall,
	operationId: string,
	timestamp = Date.now(),
): ToolResultMessage {
	return {
		role: "toolResult",
		toolCallId: toolCall.id,
		toolName: toolCall.name,
		isError: true,
		content: [
			{
				type: "text",
				text:
					`[INTERRUPTED] Tool "${toolCall.name}" (call_id=${toolCall.id}, operation_id=${operationId}) ` +
					"was interrupted by a worker restart before its outcome could be recorded. " +
					"External side effects may or may not have taken effect. " +
					"Do NOT blindly repeat mutating actions; use read/query tools to verify current state first.",
			},
		],
		details: {
			diagnostic: "interrupted",
			operationId,
		},
		timestamp,
	};
}
