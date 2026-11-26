// Pluggable external payload storage contract (`BlobStore`), scheme router, and message payload offloading.

import type {
	AssistantMessage,
	ImageContent,
	Message,
	SystemMessage,
	TextContent,
	ThinkingContent,
	ToolCall,
	ToolResultMessage,
	UserMessage,
} from "../../ai/src/index.ts";
import { sha256Hex } from "./ids.ts";

/** Default byte threshold (64 KiB) above which images or text blocks are offloaded via `BlobStore`. */
export const DEFAULT_BLOB_OFFLOAD_THRESHOLD_BYTES = 64 * 1024;

const DEFAULT_TEXT_PREVIEW_CHARS = 512;

/** Lightweight URI-addressed reference persisted inside journal records in place of large inline payloads. */
export type BlobRef = {
	readonly type: "blob_ref";
	readonly uri: string;
	readonly sha256: string;
	readonly mimeType: string;
	readonly byteLength: number;
	readonly preview?: string;
};

/** Tenant/user/session context passed when storing a new external blob. */
export type BlobPutContext = {
	readonly tenantId: string;
	readonly userId: string;
	readonly sessionId: string;
	readonly runId?: string;
	readonly sha256: string;
	readonly mimeType: string;
	readonly byteLength: number;
	readonly signal?: AbortSignal;
};

/** Tenant/user/session context passed when resolving an existing `BlobRef`. */
export type BlobGetContext = {
	readonly tenantId: string;
	readonly userId: string;
	readonly sessionId: string;
	readonly signal?: AbortSignal;
};

/** Storage-agnostic contract for offloading large images and tool outputs (S3, Workspace URL, PostgreSQL, etc.). */
export interface BlobStore {
	readonly scheme: string;
	canResolve?(uri: string): boolean;
	put(data: Uint8Array, context: BlobPutContext): Promise<{ readonly uri: string }>;
	get(ref: BlobRef, context: BlobGetContext): Promise<Uint8Array>;
	delete?(ref: BlobRef, context: BlobGetContext): Promise<void>;
}

/** Stored user/tool content item where large text or base64 images may be replaced by a `BlobRef`. */
export type StoredRichContentPart =
	| TextContent
	| ImageContent
	| { readonly type: "blob_text"; readonly ref: BlobRef; readonly textSignature?: string }
	| { readonly type: "blob_image"; readonly ref: BlobRef };

export type StoredUserMessage = Omit<UserMessage, "content"> & {
	readonly content: string | readonly StoredRichContentPart[];
};

export type StoredAssistantContentPart =
	| TextContent
	| ThinkingContent
	| ToolCall
	| { readonly type: "blob_text"; readonly ref: BlobRef; readonly textSignature?: string };

export type StoredAssistantMessage = Omit<AssistantMessage, "content"> & {
	readonly content: readonly StoredAssistantContentPart[];
};

export type StoredToolResultMessage = Omit<ToolResultMessage, "content"> & {
	readonly content: readonly StoredRichContentPart[];
};

/** Serializable message representation persisted in `agent_session_events.record`. */
export type StoredMessage = SystemMessage | StoredUserMessage | StoredAssistantMessage | StoredToolResultMessage;

export interface BlobOffloadOptions {
	readonly blobStore?: BlobStore;
	readonly thresholdBytes?: number;
	readonly tenantId: string;
	readonly userId: string;
	readonly sessionId: string;
	readonly runId?: string;
	readonly signal?: AbortSignal;
}

export interface BlobHydrateOptions {
	readonly blobStore?: BlobStore;
	readonly tenantId: string;
	readonly userId: string;
	readonly sessionId: string;
	readonly textHydrationMode?: "full" | "preview";
	readonly signal?: AbortSignal;
}

/**
 * Route writes to `primaryWriter` while dispatching reads across `readers` by URI scheme or `canResolve()`,
 * enabling zero-migration transitions (for example, switching from `s3://` to `workspace://` URLs).
 */
export function createBlobRouter(options: {
	readonly primaryWriter: BlobStore;
	readonly readers: readonly BlobStore[];
}): BlobStore {
	const { primaryWriter, readers } = options;
	const allStores = readers.includes(primaryWriter) ? readers : [primaryWriter, ...readers];

	function resolveStore(uri: string): BlobStore {
		for (const store of allStores) {
			if (store.canResolve ? store.canResolve(uri) : uri.startsWith(`${store.scheme}://`)) {
				return store;
			}
		}
		throw new Error(`No registered BlobStore can resolve URI: ${uri}`);
	}

	return {
		scheme: primaryWriter.scheme,
		canResolve(uri: string): boolean {
			return allStores.some((store) =>
				store.canResolve ? store.canResolve(uri) : uri.startsWith(`${store.scheme}://`),
			);
		},
		put(data: Uint8Array, context: BlobPutContext): Promise<{ readonly uri: string }> {
			return primaryWriter.put(data, context);
		},
		get(ref: BlobRef, context: BlobGetContext): Promise<Uint8Array> {
			return resolveStore(ref.uri).get(ref, context);
		},
		async delete(ref: BlobRef, context: BlobGetContext): Promise<void> {
			const target = resolveStore(ref.uri);
			if (target.delete) {
				await target.delete(ref, context);
			}
		},
	};
}

/** Zero-dependency in-memory `BlobStore` (`mem-blob://`) for unit and integration tests. */
export class MemoryBlobStore implements BlobStore {
	readonly scheme = "mem-blob";
	private readonly blobs = new Map<string, { readonly tenantId: string; readonly data: Uint8Array; readonly sha256: string }>();

	async put(data: Uint8Array, context: BlobPutContext): Promise<{ readonly uri: string }> {
		const uri = `mem-blob://${encodeURIComponent(context.tenantId)}/${encodeURIComponent(context.sessionId)}/${context.sha256}`;
		const copy = new Uint8Array(data.byteLength);
		copy.set(data);
		this.blobs.set(uri, { tenantId: context.tenantId, data: copy, sha256: context.sha256 });
		return { uri };
	}

	async get(ref: BlobRef, context: BlobGetContext): Promise<Uint8Array> {
		const entry = this.blobs.get(ref.uri);
		if (!entry || entry.tenantId !== context.tenantId) {
			throw new Error(`Memory blob not found for tenant ${context.tenantId}: ${ref.uri}`);
		}
		const actualHash = sha256Hex(entry.data);
		if (actualHash !== ref.sha256) {
			throw new Error(`Blob SHA-256 mismatch for ${ref.uri}: expected ${ref.sha256}, got ${actualHash}`);
		}
		const copy = new Uint8Array(entry.data.byteLength);
		copy.set(entry.data);
		return copy;
	}

	async delete(ref: BlobRef, context: BlobGetContext): Promise<void> {
		const entry = this.blobs.get(ref.uri);
		if (entry && entry.tenantId === context.tenantId) {
			this.blobs.delete(ref.uri);
		}
	}

	get size(): number {
		return this.blobs.size;
	}
}

/** Offload oversized image or text blocks from a single `Message` when `blobStore` is configured. */
export async function offloadMessageBlobs(message: Message, options: BlobOffloadOptions): Promise<StoredMessage> {
	const { blobStore } = options;
	if (!blobStore) {
		return message;
	}
	const threshold = options.thresholdBytes ?? DEFAULT_BLOB_OFFLOAD_THRESHOLD_BYTES;

	switch (message.role) {
		case "system":
			return message;

		case "user": {
			if (typeof message.content === "string") {
				const bytes = Buffer.from(message.content, "utf8");
				if (bytes.byteLength < threshold) {
					return message;
				}
				const ref = await storeBytes(blobStore, bytes, "text/plain;charset=utf-8", buildPreview(message.content), options);
				return {
					...message,
					content: [{ type: "blob_text", ref }],
				};
			}
			const parts = await Promise.all(message.content.map((part) => offloadRichPart(part, blobStore, threshold, options)));
			return { ...message, content: parts };
		}

		case "assistant": {
			const parts = await Promise.all(
				message.content.map(async (part): Promise<StoredAssistantContentPart> => {
					if (part.type !== "text") {
						return part;
					}
					const bytes = Buffer.from(part.text, "utf8");
					if (bytes.byteLength < threshold) {
						return part;
					}
					const ref = await storeBytes(blobStore, bytes, "text/plain;charset=utf-8", buildPreview(part.text), options);
					return part.textSignature !== undefined
						? { type: "blob_text", ref, textSignature: part.textSignature }
						: { type: "blob_text", ref };
				}),
			);
			return { ...message, content: parts };
		}

		case "toolResult": {
			const parts = await Promise.all(message.content.map((part) => offloadRichPart(part, blobStore, threshold, options)));
			return { ...message, content: parts };
		}
	}
}

/** Offload a batch of `Message` items in order. */
export async function offloadMessagesBlobs(
	messages: readonly Message[],
	options: BlobOffloadOptions,
): Promise<StoredMessage[]> {
	return Promise.all(messages.map((message) => offloadMessageBlobs(message, options)));
}

/** Rehydrate a `StoredMessage` back into an in-memory `Message` for model requests or callers. */
export async function hydrateMessageBlobs(stored: StoredMessage, options: BlobHydrateOptions): Promise<Message> {
	switch (stored.role) {
		case "system":
			return stored;

		case "user": {
			if (typeof stored.content === "string") {
				return stored as UserMessage;
			}
			const parts = await Promise.all(stored.content.map((part) => hydrateRichPart(part, options)));
			return { ...stored, content: parts };
		}

		case "assistant": {
			const parts = await Promise.all(
				stored.content.map(async (part): Promise<TextContent | ThinkingContent | ToolCall> => {
					if (part.type !== "blob_text") {
						return part;
					}
					const text = await hydrateTextRef(part.ref, options);
					return part.textSignature !== undefined
						? { type: "text", text, textSignature: part.textSignature }
						: { type: "text", text };
				}),
			);
			return { ...stored, content: parts };
		}

		case "toolResult": {
			const parts = await Promise.all(stored.content.map((part) => hydrateRichPart(part, options)));
			return { ...stored, content: parts };
		}
	}
}

/** Rehydrate a list of `StoredMessage` items in order. */
export async function hydrateMessagesBlobs(
	messages: readonly StoredMessage[],
	options: BlobHydrateOptions,
): Promise<Message[]> {
	return Promise.all(messages.map((message) => hydrateMessageBlobs(message, options)));
}

async function offloadRichPart(
	part: TextContent | ImageContent,
	blobStore: BlobStore,
	threshold: number,
	options: BlobOffloadOptions,
): Promise<StoredRichContentPart> {
	if (part.type === "text") {
		const bytes = Buffer.from(part.text, "utf8");
		if (bytes.byteLength < threshold) {
			return part;
		}
		const ref = await storeBytes(blobStore, bytes, "text/plain;charset=utf-8", buildPreview(part.text), options);
		return part.textSignature !== undefined
			? { type: "blob_text", ref, textSignature: part.textSignature }
			: { type: "blob_text", ref };
	}

	const bytes = Buffer.from(part.data, "base64");
	if (bytes.byteLength < threshold) {
		return part;
	}
	const ref = await storeBytes(blobStore, bytes, part.mimeType, undefined, options);
	return { type: "blob_image", ref };
}

async function hydrateRichPart(
	part: StoredRichContentPart,
	options: BlobHydrateOptions,
): Promise<TextContent | ImageContent> {
	switch (part.type) {
		case "text":
		case "image":
			return part;

		case "blob_text": {
			const text = await hydrateTextRef(part.ref, options);
			return part.textSignature !== undefined
				? { type: "text", text, textSignature: part.textSignature }
				: { type: "text", text };
		}

		case "blob_image": {
			if (!options.blobStore) {
				throw new Error(`BlobStore is required to hydrate image blob ${part.ref.uri}`);
			}
			const bytes = await options.blobStore.get(part.ref, {
				tenantId: options.tenantId,
				userId: options.userId,
				sessionId: options.sessionId,
				signal: options.signal,
			});
			return {
				type: "image",
				data: Buffer.from(bytes).toString("base64"),
				mimeType: part.ref.mimeType,
			};
		}
	}
}

async function hydrateTextRef(ref: BlobRef, options: BlobHydrateOptions): Promise<string> {
	if (options.textHydrationMode === "preview" || !options.blobStore) {
		const preview = ref.preview ?? "";
		return `${preview}\n[External payload stored at ${ref.uri} (${String(ref.byteLength)} bytes)]`;
	}
	const bytes = await options.blobStore.get(ref, {
		tenantId: options.tenantId,
		userId: options.userId,
		sessionId: options.sessionId,
		signal: options.signal,
	});
	return Buffer.from(bytes).toString("utf8");
}

async function storeBytes(
	blobStore: BlobStore,
	bytes: Uint8Array,
	mimeType: string,
	preview: string | undefined,
	options: BlobOffloadOptions,
): Promise<BlobRef> {
	const sha256 = sha256Hex(bytes);
	const { uri } = await blobStore.put(bytes, {
		tenantId: options.tenantId,
		userId: options.userId,
		sessionId: options.sessionId,
		runId: options.runId,
		sha256,
		mimeType,
		byteLength: bytes.byteLength,
		signal: options.signal,
	});
	return preview !== undefined
		? { type: "blob_ref", uri, sha256, mimeType, byteLength: bytes.byteLength, preview }
		: { type: "blob_ref", uri, sha256, mimeType, byteLength: bytes.byteLength };
}

function buildPreview(text: string): string {
	if (text.length <= DEFAULT_TEXT_PREVIEW_CHARS) {
		return text;
	}
	const half = Math.floor(DEFAULT_TEXT_PREVIEW_CHARS / 2);
	return `${text.slice(0, half)}\n... [${String(text.length - DEFAULT_TEXT_PREVIEW_CHARS)} chars omitted] ...\n${text.slice(-half)}`;
}
