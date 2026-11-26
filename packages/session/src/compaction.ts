// Sliding-window context compaction: safe turn-boundary cut selection and durable summary commit.

import type { StreamFn } from "../../agent/src/index.ts";
import {
	type Api,
	contentText,
	type Message,
	type Model,
	normalizeContext,
	type SystemMessage,
} from "../../ai/src/index.ts";
import type { BlobStore, StoredMessage } from "./blob-store.ts";
import { newAttemptId, newEventId } from "./ids.ts";
import { materializeWindowMessages, reduceActiveWindow } from "./reducer.ts";
import type { JournalEntry, SessionIdentity, SessionMetadataRecord, SessionStorage } from "./types.ts";

export interface CompactionSettings {
	readonly enabled: boolean;
	/** Trigger compaction when estimated context tokens exceed `contextWindow - reserveTokens`. Default: 16384. */
	readonly reserveTokens: number;
	/** Approximate recent verbatim tokens to preserve after compaction (`seq >= firstKeptSeq`). Default: 20000. */
	readonly keepRecentTokens: number;
}

export const DEFAULT_COMPACTION_SETTINGS: CompactionSettings = {
	enabled: true,
	reserveTokens: 16_384,
	keepRecentTokens: 20_000,
};

const CHARS_PER_TOKEN_ESTIMATE = 4;

/** Fast conservative token estimate (~4 UTF-8 chars per token + image fixed budget) for threshold checks. */
export function estimateStoredMessagesTokens(messages: readonly StoredMessage[]): number {
	let chars = 0;
	for (const msg of messages) {
		switch (msg.role) {
			case "system": {
				if (typeof msg.content === "string") {
					chars += msg.content.length;
				} else {
					for (const part of msg.content) chars += part.text.length;
				}
				if (msg.sections) {
					for (const val of Object.values(msg.sections)) {
						if (val) chars += val.length;
					}
				}
				break;
			}
			case "user": {
				if (typeof msg.content === "string") {
					chars += msg.content.length;
				} else {
					for (const part of msg.content) {
						if (part.type === "text") chars += part.text.length;
						else if (part.type === "blob_text") chars += part.ref.preview?.length ?? 256;
						else chars += 4_000;
					}
				}
				break;
			}
			case "assistant": {
				for (const part of msg.content) {
					if (part.type === "text") chars += part.text.length;
					else if (part.type === "thinking") chars += part.thinking.length;
					else if (part.type === "blob_text") chars += part.ref.preview?.length ?? 256;
					else chars += part.name.length + JSON.stringify(part.arguments).length;
				}
				break;
			}
			case "toolResult": {
				for (const part of msg.content) {
					if (part.type === "text") chars += part.text.length;
					else if (part.type === "blob_text") chars += part.ref.preview?.length ?? 256;
					else chars += 4_000;
				}
				break;
			}
		}
	}
	return Math.ceil(chars / CHARS_PER_TOKEN_ESTIMATE);
}

/**
 * Select a clean `firstKeptSeq` boundary that preserves roughly `keepRecentTokens` of the newest entries
 * WITHOUT ever splitting an `AssistantMessage (toolCalls)` from its subsequent `tool_completed` / `turn_completed`
 * entries (which would violate OpenAI/Azure call_id pairing invariants).
 */
export function selectCompactionCut(
	entries: readonly JournalEntry[],
	keepRecentTokens: number,
): { readonly firstKeptSeq: number; readonly summarizedUpToSeq: number } | null {
	if (entries.length < 4) {
		return null;
	}

	// Candidate safe cut indices in `entries`: `messages_added` (user prompt/steer/follow_up) or `run_started`.
	const safeCutIndices: number[] = [];
	for (let i = 1; i < entries.length; i++) {
		const kind = entries[i].payload.kind;
		if (kind === "messages_added" || kind === "run_started") {
			safeCutIndices.push(i);
		}
	}
	if (safeCutIndices.length === 0) {
		return null;
	}

	// Walk backward from newest safe cut until accumulated recent tokens reach `keepRecentTokens`.
	let chosenCutIndex = safeCutIndices[safeCutIndices.length - 1];
	for (let c = safeCutIndices.length - 1; c >= 0; c--) {
		const candidateIdx = safeCutIndices[c];
		const tailSlice = entries.slice(candidateIdx);
		const reducedTail = reduceActiveWindow(tailSlice);
		const estimatedTailTokens = estimateStoredMessagesTokens(reducedTail.storedMessages);
		chosenCutIndex = candidateIdx;
		if (estimatedTailTokens >= keepRecentTokens) {
			break;
		}
	}

	const firstKeptSeq = entries[chosenCutIndex].seq;
	const windowStartSeq = entries[0].seq;
	if (firstKeptSeq <= windowStartSeq) {
		return null;
	}
	return {
		firstKeptSeq,
		summarizedUpToSeq: firstKeptSeq - 1,
	};
}

export interface CompactSessionParams {
	readonly storage: SessionStorage;
	readonly identity: SessionIdentity;
	readonly model: Model<Api>;
	readonly streamFn: StreamFn;
	readonly keepRecentTokens?: number;
	readonly instructions?: string;
	readonly expectedFence?: number;
	readonly runId?: string | null;
	readonly rootRunId?: string | null;
	readonly blobStore?: BlobStore;
	readonly apiKey?: string;
	readonly signal?: AbortSignal;
}

/**
 * Summarize historical entries prior to `firstKeptSeq`, append a `compaction` event, and advance
 * `agent_sessions.context_start_seq = firstKeptSeq` in a single atomic storage write (ADR-004).
 */
export async function compactSessionWindow(
	params: CompactSessionParams,
): Promise<{ readonly compacted: boolean; readonly session: SessionMetadataRecord; readonly summary?: string }> {
	const active = await params.storage.readActiveWindow(params.identity);
	const keepRecentTokens = params.keepRecentTokens ?? DEFAULT_COMPACTION_SETTINGS.keepRecentTokens;
	const cut = selectCompactionCut(active.entries, keepRecentTokens);
	if (!cut) {
		return { compacted: false, session: active.session };
	}

	const prefixEntries = active.entries.filter((e) => e.seq <= cut.summarizedUpToSeq);
	const reducedPrefix = reduceActiveWindow(prefixEntries, "", active.session.providerSessionId);
	const hydratedPrefix = await materializeWindowMessages(reducedPrefix, {
		blobStore: params.blobStore,
		tenantId: params.identity.tenantId,
		userId: params.identity.userId,
		sessionId: params.identity.sessionId,
		textHydrationMode: "preview",
		signal: params.signal,
	});

	const serializedTranscript = formatMessagesForSummary(hydratedPrefix, reducedPrefix.compactionSummary);
	const summarySystem: SystemMessage = {
		role: "system",
		content:
			"Summarize the conversation transcript concisely and accurately so a future assistant turn can continue seamlessly. " +
			"Preserve key user goals, decisions, identifiers, file paths, tool outcomes, and unresolved tasks.",
		timestamp: Date.now(),
	};
	const summaryPrompt: Message = {
		role: "user",
		content: params.instructions
			? `${serializedTranscript}\n\nAdditional focus instructions: ${params.instructions}`
			: serializedTranscript,
		timestamp: Date.now(),
	};

	const attemptId = newAttemptId();
	const startedAt = Date.now();
	await params.storage.recordModelUsageStart({
		tenantId: params.identity.tenantId,
		userId: params.identity.userId,
		attemptId,
		sessionId: params.identity.sessionId,
		runId: params.runId ?? null,
		rootRunId: params.rootRunId ?? null,
		provider: params.model.provider,
		model: params.model.id,
		api: params.model.api,
		purpose: "compaction",
		startedAt,
	});

	const llmContext = normalizeContext({ messages: [summarySystem, summaryPrompt] });
	const stream = await params.streamFn(params.model, llmContext, {
		apiKey: params.apiKey,
		sessionId: active.session.providerSessionId,
		signal: params.signal,
	});
	const assistantMsg = await stream.result();

	const hasKnownUsage = assistantMsg.usage.totalTokens > 0 || assistantMsg.stopReason === "stop";
	await params.storage.recordModelUsageFinish({
		tenantId: params.identity.tenantId,
		attemptId,
		status:
			assistantMsg.stopReason === "error"
				? "failed"
				: assistantMsg.stopReason === "aborted"
					? "aborted"
					: "completed",
		usageStatus: hasKnownUsage ? "known" : "unknown",
		inputTokens: hasKnownUsage ? assistantMsg.usage.input : null,
		outputTokens: hasKnownUsage ? assistantMsg.usage.output : null,
		cacheReadTokens: hasKnownUsage ? assistantMsg.usage.cacheRead : null,
		cacheWriteTokens: hasKnownUsage ? assistantMsg.usage.cacheWrite : null,
		totalTokens: hasKnownUsage ? assistantMsg.usage.totalTokens : null,
		providerResponseId: assistantMsg.responseId ?? null,
		finishedAt: Date.now(),
	});

	if (assistantMsg.stopReason === "error" || assistantMsg.stopReason === "aborted") {
		throw new Error(`Compaction summarization failed: ${assistantMsg.errorMessage ?? assistantMsg.stopReason}`);
	}

	const summaryText = contentText(assistantMsg.content).trim();
	if (summaryText.length === 0) {
		return { compacted: false, session: active.session };
	}

	const compactionSeq = active.session.nextSeq;
	const appended = await params.storage.appendEvents({
		identity: params.identity,
		expectedFence: params.expectedFence,
		events: [
			{
				eventId: newEventId(),
				runId: params.runId ?? null,
				payload: {
					kind: "compaction",
					summary: summaryText,
					firstKeptSeq: cut.firstKeptSeq,
					summarizedUpToSeq: cut.summarizedUpToSeq,
					...(params.instructions ? { instructions: params.instructions } : {}),
				},
			},
		],
		sessionMutation: {
			contextStartSeq: cut.firstKeptSeq,
			compactionSeq,
		},
	});

	return { compacted: true, session: appended.session, summary: summaryText };
}

function formatMessagesForSummary(messages: readonly Message[], previousSummary: string | null): string {
	const lines: string[] = [];
	if (previousSummary) {
		lines.push(`[Previous Summary]\n${previousSummary}\n`);
	}
	for (const msg of messages) {
		if (msg.role === "system") continue;
		if (msg.role === "user") {
			const text = typeof msg.content === "string" ? msg.content : contentText(msg.content);
			lines.push(`[User]: ${text}`);
		} else if (msg.role === "assistant") {
			const text = contentText(msg.content);
			const calls = msg.content.filter((c) => c.type === "toolCall");
			const callDesc = calls.map((c) => `${c.name}(${JSON.stringify(c.arguments)})`).join(", ");
			lines.push(`[Assistant]: ${text}${callDesc ? ` [ToolCalls: ${callDesc}]` : ""}`);
		} else if (msg.role === "toolResult") {
			lines.push(`[ToolResult ${msg.toolName} (${msg.isError ? "error" : "ok"})]: ${contentText(msg.content)}`);
		}
	}
	return lines.join("\n\n");
}
