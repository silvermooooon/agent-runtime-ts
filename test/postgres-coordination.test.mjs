// Tests for L3/L4 cluster coordination guarantees:
// - `LeaseSupervisor` transient network blip resilience vs explicit `lost` fence targeted eviction (ADR-007)
// - `PostgresSessionStorage` lock-ordering (`agent_session_leases` -> `agent_sessions`) & `FenceConflictError`

import assert from "node:assert/strict";
import { describe, it } from "node:test";
import { DurableSubagentCoordinator, LeaseSupervisor } from "../dist/cluster/src/index.js";
import {
	CommandPayloadConflictError,
	PostgresCoordinationStore,
	PostgresSessionStorage,
	QuotaExceededError,
} from "../dist/postgres/src/index.js";
import { FenceConflictError, runWithToolExecutionContext } from "../dist/session/src/index.js";

describe("Cluster LeaseSupervisor & PostgreSQL Lock Ordering", () => {
	it("survives transient DB errors during renewal without mass-aborting healthy sessions, and evicts only lost fences", async () => {
		let shouldThrowNetworkError = false;
		let lostSessionIds = new Set();

		const fakeCoordinationStore = {
			async renewLeasesBatch(_holder, leases) {
				if (shouldThrowNetworkError) {
					throw new Error("ECONNRESET transient pg pool blip");
				}
				const renewed = [];
				const lost = [];
				const now = Date.now();
				for (const l of leases) {
					if (lostSessionIds.has(l.sessionId)) {
						lost.push(l);
					} else {
						renewed.push({
							tenantId: l.tenantId,
							sessionId: l.sessionId,
							userId: "u_1",
							holder: "worker_1",
							fence: l.fence,
							acquiredAt: now,
							renewedAt: now,
							expiresAt: now + 30_000,
						});
					}
				}
				return { renewed, lost };
			},
			async checkCancelRequested() {
				return { requested: false, reason: null };
			},
		};

		const supervisor = new LeaseSupervisor({
			holder: "worker_1",
			coordinationStore: fakeCoordinationStore,
			leaseTtlMs: 30_000,
			safetyMarginMs: 5_000,
		});

		const ctrl1 = supervisor.register(
			{
				tenantId: "t_1",
				sessionId: "s_keep",
				userId: "u_1",
				holder: "worker_1",
				fence: 1,
				acquiredAt: Date.now(),
				renewedAt: Date.now(),
				expiresAt: Date.now() + 30_000,
			},
			"cmd_1",
		);
		const ctrl2 = supervisor.register(
			{
				tenantId: "t_1",
				sessionId: "s_stolen",
				userId: "u_1",
				holder: "worker_1",
				fence: 1,
				acquiredAt: Date.now(),
				renewedAt: Date.now(),
				expiresAt: Date.now() + 30_000,
			},
			"cmd_2",
		);

		// 1. Transient DB network blip while 29s+ lease budget remains -> NEITHER session is aborted!
		shouldThrowNetworkError = true;
		await supervisor.tick();
		assert.equal(ctrl1.signal.aborted, false, "s_keep must survive transient network blip");
		assert.equal(ctrl2.signal.aborted, false, "s_stolen must survive transient network blip");
		assert.equal(supervisor.activeCount, 2);

		// 2. Network recovers, DB reports `s_stolen` lost its fence to another worker -> ONLY `s_stolen` is aborted!
		shouldThrowNetworkError = false;
		lostSessionIds = new Set(["s_stolen"]);
		await supervisor.tick();
		assert.equal(ctrl1.signal.aborted, false, "healthy session s_keep must continue unaffected");
		assert.equal(ctrl2.signal.aborted, true, "stolen session s_stolen must be aborted immediately");
		assert.equal(supervisor.activeCount, 1);
	});

	it("locks agent_session_leases BEFORE agent_sessions in PostgresSessionStorage.appendEvents and rejects stale fence", async () => {
		const executedSql = [];
		let simulatedDbFence = 3;

		const mockPool = {
			async connect() {
				return {
					async query(sql) {
						const normalized = sql.replace(/\s+/g, " ").trim();
						executedSql.push(normalized);
						if (normalized === "BEGIN" || normalized === "COMMIT" || normalized === "ROLLBACK") {
							return { rows: [], rowCount: 0 };
						}
						if (normalized.includes("FROM agent_session_leases")) {
							return {
								rows: [{ fence: String(simulatedDbFence), is_valid: true }],
								rowCount: 1,
							};
						}
						if (normalized.includes("FROM agent_sessions") && normalized.includes("FOR UPDATE")) {
							return {
								rows: [
									{
										tenant_id: "t_1",
										session_id: "s_1",
										user_id: "u_1",
										root_session_id: "s_1",
										parent_session_id: null,
										parent_event_id: null,
										origin_kind: "root",
										depth: 0,
										provider_session_id: "prov_1",
										status: "running",
										active_run_id: "r_1",
										active_config_hash: null,
										next_seq: "1",
										context_start_seq: "0",
										compaction_seq: null,
										archived_up_to_seq: "-1",
										metadata: {},
										created_at: new Date(),
										updated_at: new Date(),
									},
								],
								rowCount: 1,
							};
						}
						if (normalized.startsWith("INSERT INTO agent_session_events")) {
							return {
								rows: [
									{
										tenant_id: "t_1",
										session_id: "s_1",
										user_id: "u_1",
										seq: "1",
										event_id: "evt_1",
										run_id: "r_1",
										event_type: "run_completed",
										fence: "3",
										record: { kind: "run_completed", runId: "r_1" },
										created_at: new Date(),
									},
								],
								rowCount: 1,
							};
						}
						if (normalized.startsWith("UPDATE agent_sessions")) {
							return {
								rows: [
									{
										tenant_id: "t_1",
										session_id: "s_1",
										user_id: "u_1",
										root_session_id: "s_1",
										parent_session_id: null,
										parent_event_id: null,
										origin_kind: "root",
										depth: 0,
										provider_session_id: "prov_1",
										status: "idle",
										active_run_id: null,
										active_config_hash: null,
										next_seq: "2",
										context_start_seq: "0",
										compaction_seq: null,
										archived_up_to_seq: "-1",
										metadata: {},
										created_at: new Date(),
										updated_at: new Date(),
									},
								],
								rowCount: 1,
							};
						}
						throw new Error(`Unexpected SQL in mock: ${normalized}`);
					},
					release() {},
				};
			},
			async query() {
				return { rows: [], rowCount: 0 };
			},
		};

		const pgStorage = new PostgresSessionStorage(mockPool);
		const identity = { tenantId: "t_1", userId: "u_1", sessionId: "s_1" };

		// Valid fence = 3 succeeds and locks `agent_session_leases` strictly before `agent_sessions`!
		await pgStorage.appendEvents({
			identity,
			expectedFence: 3,
			events: [{ runId: "r_1", payload: { kind: "run_completed", runId: "r_1" } }],
			sessionMutation: { status: "idle", activeRunId: null },
		});

		const leaseLockIdx = executedSql.findIndex((s) => s.includes("FROM agent_session_leases") && s.includes("FOR UPDATE"));
		const sessionLockIdx = executedSql.findIndex((s) => s.includes("FROM agent_sessions") && s.includes("FOR UPDATE"));
		assert.ok(leaseLockIdx > 0 && sessionLockIdx > leaseLockIdx, "must lock leases strictly before sessions");

		// Stale fence = 2 rolls back immediately before touching `agent_sessions`!
		executedSql.length = 0;
		simulatedDbFence = 4;
		await assert.rejects(
			() =>
				pgStorage.appendEvents({
					identity,
					expectedFence: 3,
					events: [{ runId: "r_1", payload: { kind: "run_completed", runId: "r_1" } }],
				}),
			FenceConflictError,
		);
		assert.ok(executedSql.includes("ROLLBACK"));
		assert.equal(
			executedSql.some((s) => s.includes("FROM agent_sessions")),
			false,
			"must abort transaction before locking agent_sessions when fence is stale",
		);
	});

	it("enforces command_id idempotency, 409 payload conflict, and intra-tenant per-user queue quota (ADR-009)", async () => {
		const storedCommands = new Map();
		let userQueuedCount = 0;

		const mockPool = {
			async query(sql, values = []) {
				const norm = sql.replace(/\s+/g, " ").trim();
				if (norm.startsWith("SELECT * FROM agent_commands WHERE tenant_id = $1 AND command_id = $2")) {
					const found = storedCommands.get(`${values[0]}:${values[1]}`);
					return { rows: found ? [found] : [], rowCount: found ? 1 : 0 };
				}
				return { rows: [], rowCount: 0 };
			},
			async connect() {
				return {
					async query(sql, values = []) {
						const norm = sql.replace(/\s+/g, " ").trim();
						if (norm === "BEGIN" || norm === "COMMIT" || norm === "ROLLBACK") {
							return { rows: [], rowCount: 0 };
						}
						if (norm.includes("SELECT origin_kind FROM agent_sessions")) {
							return { rows: [{ origin_kind: "root" }], rowCount: 1 };
						}
						if (norm.startsWith("SELECT * FROM agent_commands WHERE tenant_id = $1 AND command_id = $2")) {
							const found = storedCommands.get(`${values[0]}:${values[1]}`);
							return { rows: found ? [found] : [], rowCount: found ? 1 : 0 };
						}
						if (norm.includes("FROM agent_quotas")) {
							// Tenant allows 100 queued commands, but per-user cap inside tenant is 2!
							return {
								rows: [
									{
										scope_type: "tenant",
										max_active_root_runs: 20,
										max_active_child_runs: 40,
										max_queued_commands: 100,
										per_user_max_active_root_runs: 3,
										per_user_max_active_child_runs: 5,
										per_user_max_queued_commands: 2,
									},
								],
								rowCount: 1,
							};
						}
						if (norm.includes("AS tenant_queued")) {
							return {
								rows: [{ tenant_queued: String(userQueuedCount), user_queued: String(userQueuedCount) }],
								rowCount: 1,
							};
						}
						if (norm.includes("AS next_cmd_seq")) {
							return { rows: [{ next_cmd_seq: String(storedCommands.size + 1) }], rowCount: 1 };
						}
						if (norm.startsWith("INSERT INTO agent_commands")) {
							userQueuedCount += 1;
							const row = {
								tenant_id: values[0],
								command_id: values[1],
								session_id: values[2],
								user_id: values[3],
								command_seq: String(values[4]),
								kind: values[5],
								payload: JSON.parse(values[6]),
								payload_hash: values[7],
								run_id: values[8],
								is_child: values[9],
								state: "queued",
								recovery_attempts: 0,
								max_recovery_attempts: values[10],
								cancel_requested_at: null,
								cancel_reason: null,
								last_error: null,
								accepted_at: new Date(),
								started_at: null,
								finished_at: null,
								updated_at: new Date(),
							};
							storedCommands.set(`${values[0]}:${values[1]}`, row);
							return { rows: [row], rowCount: 1 };
						}
						throw new Error(`Unexpected SQL: ${norm}`);
					},
					release() {},
				};
			},
		};

		const pgSessions = new PostgresSessionStorage(mockPool);
		const coord = new PostgresCoordinationStore(mockPool, pgSessions);
		const identity = { tenantId: "t_ent", userId: "u_noisy", sessionId: "s_1" };

		const first = await coord.submitCommand({
			identity,
			commandId: "cmd_idem_1",
			payload: { kind: "prompt", source: "user", messages: [{ role: "user", content: "Hello", timestamp: 1 }] },
		});
		assert.equal(first.deduplicated, false);

		// Exact retry with same command_id + payload returns deduplicated: true!
		const retry = await coord.submitCommand({
			identity,
			commandId: "cmd_idem_1",
			payload: { kind: "prompt", source: "user", messages: [{ role: "user", content: "Hello", timestamp: 1 }] },
		});
		assert.equal(retry.deduplicated, true);
		assert.equal(retry.command.runId, first.command.runId);

		// Reusing command_id with mutated payload throws 409 CommandPayloadConflictError!
		await assert.rejects(
			() =>
				coord.submitCommand({
					identity,
					commandId: "cmd_idem_1",
					payload: { kind: "prompt", source: "user", messages: [{ role: "user", content: "Tampered", timestamp: 1 }] },
				}),
			CommandPayloadConflictError,
		);

		// 2nd distinct command succeeds (reaching per-user cap = 2).
		await coord.submitCommand({
			identity,
			commandId: "cmd_idem_2",
			payload: { kind: "prompt", source: "user", messages: [{ role: "user", content: "Second", timestamp: 2 }] },
		});

		// 3rd command for `u_noisy` hits user-scope 429 QuotaExceededError even though tenant cap is 100!
		await assert.rejects(
			() =>
				coord.submitCommand({
					identity,
					commandId: "cmd_idem_3",
					payload: { kind: "prompt", source: "user", messages: [{ role: "user", content: "Third", timestamp: 3 }] },
				}),
			(err) => err instanceof QuotaExceededError && err.scope === "user" && err.limit === 2,
		);
	});

	it("enforces maxDepth in DurableSubagentCoordinator and binds deterministic sub_<operationId> commandId", async () => {
		let parentDepth = 0;
		const submittedChildCommands = [];

		const fakeSessionStorage = {
			async getSession(identity) {
				return {
					tenantId: identity.tenantId,
					sessionId: identity.sessionId,
					userId: identity.userId,
					rootSessionId: "s_root",
					parentSessionId: null,
					parentEventId: null,
					originKind: parentDepth === 0 ? "root" : "subagent",
					depth: parentDepth,
					providerSessionId: "prov_root",
					status: "running",
					activeRunId: "r_root",
					activeConfigHash: null,
					nextSeq: 2,
					contextStartSeq: 0,
					compactionSeq: null,
					archivedUpToSeq: -1,
					metadata: {},
					createdAt: Date.now(),
					updatedAt: Date.now(),
				};
			},
			async createSession(input) {
				return input;
			},
		};

		const fakeCoordStore = {
			async submitCommand(input) {
				submittedChildCommands.push(input);
				return { command: input, deduplicated: false };
			},
		};

		const coordinator = new DurableSubagentCoordinator({
			sessionStorage: fakeSessionStorage,
			coordinationStore: fakeCoordStore,
			maxDepth: 2,
		});
		const delegateTool = coordinator.createDelegateTool();

		const res = await runWithToolExecutionContext(
			{
				tenantId: "t_1",
				userId: "u_1",
				sessionId: "s_root",
				runId: "r_root",
				rootRunId: "s_root/r_root",
				turnIndex: 1,
				callId: "call_sub_1",
				toolName: "delegate_subagent",
				operationId: "op_1234567890abcdef1234567890abcdef",
				fence: 1,
			},
			() => delegateTool.execute("call_sub_1", { task: "Audit billing module" }),
		);

		assert.equal(submittedChildCommands.length, 1);
		assert.equal(submittedChildCommands[0].isChild, true);
		assert.equal(submittedChildCommands[0].commandId, "sub_op_1234567890abcdef1234567890abcdef");
		assert.equal(res.details.depth, 1);

		// Depth 2 trying to spawn Depth 3 (> maxDepth 2) must reject!
		parentDepth = 2;
		await assert.rejects(() =>
			coordinator.spawnSubagent({
				parentIdentity: { tenantId: "t_1", userId: "u_1", sessionId: "s_child_2" },
				parentRunId: "r_child_2",
				rootRunId: "s_root/r_root",
				taskPrompt: "Too deep",
			}),
		);
	});
});
