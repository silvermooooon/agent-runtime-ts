// Comprehensive tests for L2 `packages/session` & L3 `packages/blob-s3`:
// - Multi-tenant/multi-user isolation & config_hash deduplication
// - ADR-005 `pi`-style crash tool recovery (`replay: "safe"` vs `interrupted`)
// - ADR-008 Human-in-the-loop approval park/resume & `actionHash` tamper guard
// - ADR-006 `S3BlobStore` (SigV4) + custom `WorkspaceBlobStore` hybrid `BlobRouter`
// - ADR-004 Sliding-window `compactSessionWindow` & `contextStartSeq` slicing + `.jsonl.gz` cold archive codec

import assert from "node:assert/strict";
import { describe, it } from "node:test";
import { createModels, fauxAssistantMessage, fauxProvider, fauxToolCall } from "../dist/ai/src/index.js";
import { S3ArchiveObjectStore, S3BlobStore } from "../dist/blob-s3/src/index.js";
import { obj, str } from "../dist/schema/src/index.js";

function makeFauxHarness() {
	const faux = fauxProvider();
	const models = createModels();
	models.setProvider(faux.provider);
	return {
		faux,
		model: faux.getModel(),
		streamFn: models.streamSimple.bind(models),
	};
}
import {
	ApprovalActionTamperedError,
	compactSessionWindow,
	createBlobRouter,
	defineDurableTool,
	FenceConflictError,
	MemorySessionStorage,
	reduceActiveWindow,
	runDurableSession,
} from "../dist/session/src/index.js";

describe("Durable Session Core, Crash Recovery, Approvals, BlobRouter & Compaction", () => {
	it("enforces tenantId + userId isolation, deduplicates config_hash, and tracks ModelUsage", async () => {
		const storage = new MemorySessionStorage();
		const idA1 = { tenantId: "tenant_acme", userId: "user_alice", sessionId: "s_1" };
		const idA2 = { tenantId: "tenant_acme", userId: "user_bob", sessionId: "s_2" };

		await storage.createSession({ identity: idA1, systemPrompt: "You help Alice." });
		await storage.createSession({ identity: idA2, systemPrompt: "You help Bob." });

		// Cross-user read within the same tenant must return null.
		const crossRead = await storage.getSession({
			tenantId: "tenant_acme",
			userId: "user_bob",
			sessionId: "s_1",
		});
		assert.equal(crossRead, null);

		const { faux, model, streamFn } = makeFauxHarness();
		faux.setResponses([
			fauxAssistantMessage("Hello Alice turn 1!"),
			fauxAssistantMessage("Hello Alice turn 2!"),
		]);

		const echoTool = defineDurableTool({
			name: "ping",
			label: "Ping",
			description: "Read-only ping",
			parameters: obj({ msg: str() }),
			replay: "safe",
			execute: async ({ msg }) => ({ content: [{ type: "text", text: msg }], details: null }),
		});

		const out1 = await runDurableSession({
			storage,
			identity: idA1,
			mode: {
				kind: "prompt",
				runId: "run_1",
				rootRunId: "s_1/run_1",
				source: "user",
				prompts: [{ role: "user", content: "Hi 1", timestamp: Date.now() }],
			},
			model,
			tools: [echoTool],
			streamFn,
		});
		assert.equal(out1.status, "completed");

		const win1 = await storage.readActiveWindow(idA1);
		const firstConfigHash = win1.session.activeConfigHash;
		assert.ok(firstConfigHash);

		const out2 = await runDurableSession({
			storage,
			identity: idA1,
			mode: {
				kind: "prompt",
				runId: "run_2",
				rootRunId: "s_1/run_2",
				source: "user",
				prompts: [{ role: "user", content: "Hi 2", timestamp: Date.now() }],
			},
			model,
			tools: [echoTool],
			streamFn,
		});
		assert.equal(out2.status, "completed");

		const win2 = await storage.readActiveWindow(idA1);
		assert.equal(win2.session.activeConfigHash, firstConfigHash);
		assert.equal(win2.session.status, "idle");

		const usages = storage.listModelUsages("tenant_acme", "user_alice");
		assert.equal(usages.length, 2);
		assert.equal(usages[0].status, "completed");
		assert.equal(usages[0].usageStatus, "known");
	});

	it("recovers mid-turn tool crashes using pi compromise: reruns replay:'safe' and marks mutating tool 'interrupted'", async () => {
		const storage = new MemorySessionStorage();
		const identity = { tenantId: "t_1", userId: "u_1", sessionId: "s_crash" };
		await storage.createSession({ identity, systemPrompt: "Verify state if interrupted." });

		let readExecutions = 0;
		let createOrderExecutions = 0;
		const seenOperationIds = [];

		const readOrderTool = defineDurableTool({
			name: "read_order_status",
			label: "Read Order",
			description: "Safe read-only lookup",
			parameters: obj({ orderKey: str() }),
			replay: "safe",
			execute: async ({ orderKey }, ctx) => {
				readExecutions += 1;
				seenOperationIds.push(ctx?.operationId);
				return {
					content: [{ type: "text", text: `Order ${orderKey} exists: status=PAID` }],
					details: null,
				};
			},
		});

		const createOrderTool = defineDurableTool({
			name: "create_order",
			label: "Create Order",
			description: "Mutating external order creation",
			parameters: obj({ orderKey: str() }),
			// Default `replay: "never"`
			execute: async () => {
				createOrderExecutions += 1;
				return { content: [{ type: "text", text: "Created" }], details: null };
			},
		});

		const { faux, model, streamFn } = makeFauxHarness();
		const assistantWithTwoTools = fauxAssistantMessage([
			fauxToolCall("read_order_status", { orderKey: "ORD-99" }, { id: "call_safe_1" }),
			fauxToolCall("create_order", { orderKey: "ORD-99" }, { id: "call_mut_2" }),
		]);

		// Simulate Worker 1 crashing right after `model_completed` was committed (before either tool wrote `tool_completed`)!
		await storage.appendEvents({
			identity,
			events: [
				{
					runId: "run_crash_1",
					payload: {
						kind: "run_started",
						runId: "run_crash_1",
						rootRunId: "s_crash/run_crash_1",
						source: "user",
						configHash: "cfg_1",
					},
				},
				{
					runId: "run_crash_1",
					payload: {
						kind: "messages_added",
						runId: "run_crash_1",
						source: "prompt",
						messages: [{ role: "user", content: "Create order ORD-99", timestamp: Date.now() }],
					},
				},
				{
					runId: "run_crash_1",
					payload: {
						kind: "model_completed",
						runId: "run_crash_1",
						turnIndex: 1,
						message: assistantWithTwoTools,
					},
				},
			],
			sessionMutation: {
				status: "running",
				activeRunId: "run_crash_1",
			},
		});

		// Verify pure Reducer detects both unfinished tool calls cleanly.
		const preRecoveryWindow = await storage.readActiveWindow(identity);
		const preReduced = reduceActiveWindow(preRecoveryWindow.entries);
		assert.equal(preReduced.activeRun?.unfinishedTurn?.pendingToolCalls.length, 2);

		// Worker 2 takes over with fence = 2 and resumes `run_crash_1`.
		storage.setFence("t_1", "s_crash", 2);

		let capturedSecondTurnMessages = [];
		faux.setResponses([
			(context) => {
				capturedSecondTurnMessages = context.messages;
				return fauxAssistantMessage("Verified ORD-99 was already created; skipping duplicate creation.");
			},
		]);

		const resumed = await runDurableSession({
			storage,
			identity,
			expectedFence: 2,
			mode: {
				kind: "resume",
				runId: "run_crash_1",
			},
			model,
			tools: [readOrderTool, createOrderTool],
			streamFn,
		});

		assert.equal(resumed.status, "completed");
		assert.equal(readExecutions, 1, "replay:'safe' tool must rerun automatically");
		assert.equal(createOrderExecutions, 0, "mutating tool must NOT rerun blindly on crash recovery");
		assert.ok(seenOperationIds[0]?.startsWith("op_"), "deterministic operationId must be injected");

		const toolResultsSeenByModel = capturedSecondTurnMessages.filter((m) => m.role === "toolResult");
		assert.equal(toolResultsSeenByModel.length, 2);
		assert.equal(toolResultsSeenByModel[0].toolCallId, "call_safe_1");
		assert.equal(toolResultsSeenByModel[0].isError, false);
		assert.equal(toolResultsSeenByModel[1].toolCallId, "call_mut_2");
		assert.equal(toolResultsSeenByModel[1].isError, true);
		assert.match(toolResultsSeenByModel[1].content[0].text, /\[INTERRUPTED\]/);

		// Stale Worker 1 (fence = 1) attempting a late write must be rejected with FenceConflictError!
		await assert.rejects(
			() =>
				storage.appendEvents({
					identity,
					expectedFence: 1,
					events: [{ runId: "run_crash_1", payload: { kind: "run_completed", runId: "run_crash_1" } }],
				}),
			FenceConflictError,
		);
	});

	it("parks on human approval, guards against action_hash tampering, and resumes cleanly when approved", async () => {
		const storage = new MemorySessionStorage();
		const identity = { tenantId: "t_bank", userId: "u_ops", sessionId: "s_apr" };
		await storage.createSession({ identity, systemPrompt: "Execute transfers safely." });

		let executedAmount = null;
		const transferTool = defineDurableTool({
			name: "wire_transfer",
			label: "Wire Transfer",
			description: "Transfer funds",
			parameters: obj({ to: str(), amount: str() }),
			execute: async ({ amount }) => {
				executedAmount = amount;
				return { content: [{ type: "text", text: `Transferred ${amount}` }], details: null };
			},
		});

		const { faux, model, streamFn } = makeFauxHarness();
		faux.setResponses([
			fauxAssistantMessage([fauxToolCall("wire_transfer", { to: "acct_1", amount: "50000" }, { id: "call_wire_1" })]),
			fauxAssistantMessage("Transfer completed after approval."),
		]);

		const firstPass = await runDurableSession({
			storage,
			identity,
			mode: {
				kind: "prompt",
				runId: "run_apr_1",
				rootRunId: "s_apr/run_apr_1",
				source: "user",
				prompts: [{ role: "user", content: "Send 50000 to acct_1", timestamp: Date.now() }],
			},
			model,
			tools: [transferTool],
			streamFn,
			approvalPolicy: ({ toolCall }) =>
				toolCall.name === "wire_transfer"
					? { kind: "require_approval", approverId: "mgr_1" }
					: { kind: "allow" },
		});

		assert.equal(firstPass.status, "paused_for_approval");
		assert.equal(executedAmount, null, "tool must not execute before approval");
		assert.equal(firstPass.approvals.length, 1);
		const ticket = firstPass.approvals[0];

		const parkedSession = await storage.getSession(identity);
		assert.equal(parkedSession?.status, "waiting_approval");

		// Tampered actionHash must be rejected!
		await assert.rejects(
			() =>
				runDurableSession({
					storage,
					identity,
					mode: {
						kind: "resume",
						runId: "run_apr_1",
						approvalDecisions: [
							{
								approvalId: ticket.approvalId,
								operationId: ticket.operationId,
								callId: ticket.callId,
								actionHash: "deadbeef_tampered_hash",
								status: "approved",
							},
						],
					},
					model,
					tools: [transferTool],
					streamFn,
				}),
			ApprovalActionTamperedError,
		);

		// Valid approval decision executes the tool and completes the run!
		const secondPass = await runDurableSession({
			storage,
			identity,
			mode: {
				kind: "resume",
				runId: "run_apr_1",
				approvalDecisions: [
					{
						approvalId: ticket.approvalId,
						operationId: ticket.operationId,
						callId: ticket.callId,
						actionHash: ticket.actionHash,
						status: "approved",
						decidedBy: "mgr_1",
					},
				],
			},
			model,
			tools: [transferTool],
			streamFn,
		});

		assert.equal(secondPass.status, "completed");
		assert.equal(executedAmount, "50000");
		const settledSession = await storage.getSession(identity);
		assert.equal(settledSession?.status, "idle");
	});

	it("offloads oversized outputs via S3BlobStore (SigV4) and routes seamlessly to custom WorkspaceBlobStore", async () => {
		const s3Objects = new Map();
		const seenAuthHeaders = [];

		const mockS3Fetch = async (url, init) => {
			seenAuthHeaders.push(init.headers.authorization);
			const key = new URL(url).pathname;
			if (init.method === "PUT") {
				s3Objects.set(key, new Uint8Array(init.body));
				return new Response("", { status: 200 });
			}
			if (init.method === "GET") {
				const found = s3Objects.get(key);
				return found ? new Response(found, { status: 200 }) : new Response("Not Found", { status: 404 });
			}
			return new Response("", { status: 204 });
		};

		const s3Store = new S3BlobStore({
			endpoint: "https://minio.corp.internal:9000",
			region: "us-east-1",
			bucket: "agent-blobs",
			accessKeyId: "test-s3-access-key",
			secretAccessKey: "test-s3-secret-key",
			fetch: mockS3Fetch,
		});

		const workspaceObjects = new Map();
		const workspaceStore = {
			scheme: "workspace",
			canResolve: (uri) => uri.startsWith("https://workspace.corp.internal/blobs/"),
			async put(data, ctx) {
				const url = `https://workspace.corp.internal/blobs/${ctx.tenantId}/${ctx.userId}/${ctx.sha256}`;
				workspaceObjects.set(url, new Uint8Array(data));
				return { uri: url };
			},
			async get(ref) {
				const bytes = workspaceObjects.get(ref.uri);
				if (!bytes) throw new Error("Missing workspace blob");
				return bytes;
			},
		};

		const storage = new MemorySessionStorage();
		const identity = { tenantId: "t_ws", userId: "u_dev", sessionId: "s_blob" };
		await storage.createSession({ identity, systemPrompt: "Handle large logs." });

		const bigLogText = "LINE_DATA_0123456789_".repeat(200); // ~4,200 bytes (threshold set to 1,024 bytes)
		const logTool = defineDurableTool({
			name: "fetch_logs",
			label: "Fetch Logs",
			description: "Fetch large logs",
			parameters: obj({ service: str() }),
			replay: "safe",
			execute: async ({ service }) => ({
				content: [{ type: "text", text: `[${service}] ${bigLogText}` }],
				details: null,
			}),
		});

		const { faux, model, streamFn } = makeFauxHarness();
		faux.setResponses([
			fauxAssistantMessage([fauxToolCall("fetch_logs", { service: "auth" }, { id: "c_log_1" })]),
			fauxAssistantMessage("Analyzed S3 log."),
			fauxAssistantMessage([fauxToolCall("fetch_logs", { service: "billing" }, { id: "c_log_2" })]),
			fauxAssistantMessage("Analyzed Workspace + S3 logs together."),
		]);

		// Run 1 writes large tool output to S3 (`s3://agent-blobs/...`).
		await runDurableSession({
			storage,
			identity,
			blobStore: s3Store,
			blobThresholdBytes: 1_024,
			mode: {
				kind: "prompt",
				runId: "r_s3",
				rootRunId: "s_blob/r_s3",
				source: "user",
				prompts: [{ role: "user", content: "Check auth logs", timestamp: Date.now() }],
			},
			model,
			tools: [logTool],
			streamFn,
		});

		assert.ok(s3Objects.size === 1);
		assert.match(seenAuthHeaders[0], /^AWS4-HMAC-SHA256 Credential=/);

		// Switch primary writer to `workspaceStore` via `createBlobRouter` while keeping `s3Store` as reader!
		const hybridRouter = createBlobRouter({
			primaryWriter: workspaceStore,
			readers: [workspaceStore, s3Store],
		});

		const outHybrid = await runDurableSession({
			storage,
			identity,
			blobStore: hybridRouter,
			blobThresholdBytes: 1_024,
			mode: {
				kind: "prompt",
				runId: "r_ws",
				rootRunId: "s_blob/r_ws",
				source: "user",
				prompts: [{ role: "user", content: "Now check billing logs too", timestamp: Date.now() }],
			},
			model,
			tools: [logTool],
			streamFn,
		});

		assert.equal(outHybrid.status, "completed");
		assert.equal(workspaceObjects.size, 1, "new blobs must go to Workspace URL store");
		const allEvents = await storage.readEvents(identity);
		const toolCompletedEvents = allEvents.filter((e) => e.eventType === "tool_completed");
		assert.equal(toolCompletedEvents[0].payload.result.content[0].type, "blob_text");
		assert.match(toolCompletedEvents[0].payload.result.content[0].ref.uri, /^s3:\/\/agent-blobs\//);
		assert.match(
			toolCompletedEvents[1].payload.result.content[0].ref.uri,
			/^https:\/\/workspace\.corp\.internal\/blobs\//,
		);
	});

	it("slides contextStartSeq during compaction and archives historical events via S3ArchiveObjectStore", async () => {
		const storage = new MemorySessionStorage();
		const identity = { tenantId: "t_cmp", userId: "u_1", sessionId: "s_cmp" };
		await storage.createSession({ identity, systemPrompt: "Long running assistant." });

		const { faux, model, streamFn } = makeFauxHarness();
		for (let i = 1; i <= 5; i++) {
			faux.setResponses([fauxAssistantMessage(`Detailed response for turn ${String(i)}: ${"x".repeat(400)}`)]);
			await runDurableSession({
				storage,
				identity,
				mode: {
					kind: "prompt",
					runId: `run_${String(i)}`,
					rootRunId: `s_cmp/run_${String(i)}`,
					source: "user",
					prompts: [{ role: "user", content: `Question ${String(i)}: ${"y".repeat(400)}`, timestamp: Date.now() }],
				},
				model,
				tools: [],
				streamFn,
			});
		}

		const beforeWin = await storage.readActiveWindow(identity);
		assert.equal(beforeWin.session.contextStartSeq, 0);
		const totalBefore = beforeWin.entries.length;
		assert.ok(totalBefore > 15);

		faux.setResponses([fauxAssistantMessage("Summary: User asked Questions 1..4 and received detailed answers.")]);
		const compacted = await compactSessionWindow({
			storage,
			identity,
			model,
			streamFn,
			keepRecentTokens: 250,
		});

		assert.equal(compacted.compacted, true);
		const afterWin = await storage.readActiveWindow(identity);
		assert.ok(afterWin.session.contextStartSeq > 0, "contextStartSeq must advance past summarized prefix");
		assert.ok(afterWin.entries.length < totalBefore, "active window read must scan fewer rows than full history");

		// Verify S3ArchiveObjectStore encodes `.jsonl.gz` and verifies SHA-256 on readback!
		const archiveBuckets = new Map();
		const archiveStore = new S3ArchiveObjectStore({
			endpoint: "http://minio.local:9000",
			region: "us-east-1",
			bucket: "cold-archives",
			accessKeyId: "minio",
			secretAccessKey: "minio123",
			fetch: async (url, init) => {
				const k = new URL(url).pathname;
				if (init.method === "PUT") {
					archiveBuckets.set(k, new Uint8Array(init.body));
					return new Response("", { status: 200, headers: { "x-amz-version-id": "v1" } });
				}
				return new Response(archiveBuckets.get(k), { status: 200 });
			},
		});

		const coldCandidates = (await storage.readEvents(identity, {
			fromSeq: 0,
			toSeq: afterWin.session.contextStartSeq - 1,
		}));
		const receipt = await archiveStore.writeArchiveChunk({
			tenantId: identity.tenantId,
			userId: identity.userId,
			sessionId: identity.sessionId,
			startSeq: coldCandidates[0].seq,
			endSeq: coldCandidates[coldCandidates.length - 1].seq,
			entries: coldCandidates,
		});
		assert.equal(receipt.versionId, "v1");
		const restored = await archiveStore.readArchiveChunk({
			objectUri: receipt.objectUri,
			expectedSha256: receipt.sha256,
		});
		assert.equal(restored.length, coldCandidates.length);
	});
});
