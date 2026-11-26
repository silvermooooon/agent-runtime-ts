// PostgreSQL cluster coordination store: leases/fencing, command queue, quotas, approvals, runs, and transactional outbox.

import type { JsonObject, Message } from "../../ai/src/index.ts";
import {
	type ApprovalDecision,
	type ApprovalRequestSpec,
	canonicalJsonHash,
	FenceConflictError,
	newCommandId,
	newEventId,
	newOutboxMessageId,
	newRunId,
	type SessionIdentity,
	SessionNotFoundError,
} from "../../session/src/index.ts";
import { parsePgBigint, parsePgTimestamp, type PgPoolLike, type PgQueryable, withPgTransaction } from "./client.ts";
import type { PostgresSessionStorage } from "./session-store.ts";
import {
	type ApprovalRecord,
	type ApprovalStatus,
	type ClaimedExecution,
	type CommandKind,
	CommandPayloadConflictError,
	type CommandPayload,
	type CommandRecord,
	type CommandState,
	DEFAULT_QUOTA_CONFIG,
	type DefaultQuotaConfig,
	type EffectiveQuotaLimits,
	type OutboxKind,
	type OutboxPayload,
	type OutboxRecord,
	QuotaExceededError,
	type RunRecord,
	type RunStatus,
	type SessionLeaseRecord,
	type SubmitCommandInput,
	type SubmitCommandResult,
	type UpsertQuotaInput,
} from "./types.ts";

interface LeaseRow {
	tenant_id: string;
	session_id: string;
	user_id: string;
	holder: string;
	fence: string | number;
	acquired_at: Date | string;
	renewed_at: Date | string;
	expires_at: Date | string;
}

interface CommandRow {
	tenant_id: string;
	command_id: string;
	session_id: string;
	user_id: string;
	command_seq: string | number;
	kind: CommandKind;
	payload: CommandPayload;
	payload_hash: string;
	run_id: string | null;
	is_child: boolean;
	state: CommandState;
	recovery_attempts: number;
	max_recovery_attempts: number;
	cancel_requested_at: Date | string | null;
	cancel_reason: string | null;
	last_error: string | null;
	accepted_at: Date | string;
	started_at: Date | string | null;
	finished_at: Date | string | null;
	updated_at: Date | string;
}

interface ApprovalRow {
	tenant_id: string;
	approval_id: string;
	session_id: string;
	user_id: string;
	run_id: string;
	operation_id: string;
	call_id: string;
	approver_id: string;
	tool_name: string;
	action_payload: JsonObject;
	action_hash: string;
	context: JsonObject;
	status: ApprovalStatus;
	decided_by: string | null;
	decision_reason: string | null;
	created_at: Date | string;
	expires_at: Date | string;
	decided_at: Date | string | null;
	consumed_at: Date | string | null;
}

interface OutboxRow {
	message_seq: string | number;
	tenant_id: string;
	user_id: string;
	message_id: string;
	kind: OutboxKind;
	target_session_id: string;
	source_session_id: string | null;
	source_run_id: string | null;
	payload: OutboxPayload;
	state: "pending" | "delivered" | "dead_letter";
	attempts: number;
	last_error: string | null;
	available_at: Date | string;
	created_at: Date | string;
	delivered_at: Date | string | null;
}

interface QuotaRow {
	scope_type: "tenant" | "user";
	max_active_root_runs: number;
	max_active_child_runs: number;
	max_queued_commands: number;
	per_user_max_active_root_runs: number | null;
	per_user_max_active_child_runs: number | null;
	per_user_max_queued_commands: number | null;
}

interface RunRow {
	tenant_id: string;
	session_id: string;
	run_id: string;
	command_id: string | null;
	user_id: string;
	root_run_id: string;
	parent_session_id: string | null;
	parent_run_id: string | null;
	depth: number;
	source: "user" | "subagent" | "system";
	status: RunStatus;
	recovery_attempts: number;
	started_at: Date | string;
	finished_at: Date | string | null;
	metadata: JsonObject;
}

export class PostgresCoordinationStore {
	private readonly pool: PgPoolLike;
	private readonly sessionStorage: PostgresSessionStorage;
	private readonly defaultQuotas: DefaultQuotaConfig;

	constructor(
		pool: PgPoolLike,
		sessionStorage: PostgresSessionStorage,
		defaultQuotas: Partial<DefaultQuotaConfig> = {},
	) {
		this.pool = pool;
		this.sessionStorage = sessionStorage;
		this.defaultQuotas = { ...DEFAULT_QUOTA_CONFIG, ...defaultQuotas };
	}

	/** Configure or update tenant-level or user-level concurrency & queue quotas (`agent_quotas`). */
	async upsertQuota(input: UpsertQuotaInput): Promise<void> {
		if (input.scope === "tenant") {
			await this.pool.query(
				`INSERT INTO agent_quotas (
					tenant_id, scope_type, user_id,
					max_active_root_runs, max_active_child_runs, max_queued_commands,
					per_user_max_active_root_runs, per_user_max_active_child_runs, per_user_max_queued_commands,
					updated_at
				) VALUES ($1, 'tenant', '', $2, $3, $4, $5, $6, $7, clock_timestamp())
				ON CONFLICT (tenant_id, scope_type, user_id) DO UPDATE SET
					max_active_root_runs = EXCLUDED.max_active_root_runs,
					max_active_child_runs = EXCLUDED.max_active_child_runs,
					max_queued_commands = EXCLUDED.max_queued_commands,
					per_user_max_active_root_runs = EXCLUDED.per_user_max_active_root_runs,
					per_user_max_active_child_runs = EXCLUDED.per_user_max_active_child_runs,
					per_user_max_queued_commands = EXCLUDED.per_user_max_queued_commands,
					updated_at = clock_timestamp()`,
				[
					input.tenantId,
					input.maxActiveRootRuns,
					input.maxActiveChildRuns,
					input.maxQueuedCommands,
					input.perUserMaxActiveRootRuns ?? null,
					input.perUserMaxActiveChildRuns ?? null,
					input.perUserMaxQueuedCommands ?? null,
				],
			);
		} else {
			await this.pool.query(
				`INSERT INTO agent_quotas (
					tenant_id, scope_type, user_id,
					max_active_root_runs, max_active_child_runs, max_queued_commands,
					per_user_max_active_root_runs, per_user_max_active_child_runs, per_user_max_queued_commands,
					updated_at
				) VALUES ($1, 'user', $2, $3, $4, $5, NULL, NULL, NULL, clock_timestamp())
				ON CONFLICT (tenant_id, scope_type, user_id) DO UPDATE SET
					max_active_root_runs = EXCLUDED.max_active_root_runs,
					max_active_child_runs = EXCLUDED.max_active_child_runs,
					max_queued_commands = EXCLUDED.max_queued_commands,
					updated_at = clock_timestamp()`,
				[
					input.tenantId,
					input.userId,
					input.maxActiveRootRuns,
					input.maxActiveChildRuns,
					input.maxQueuedCommands,
				],
			);
		}
	}

	/** Resolve effective dual-layer (tenant + per-user) quotas for `(tenantId, userId)`. */
	async resolveEffectiveQuotas(
		client: PgQueryable,
		tenantId: string,
		userId: string,
	): Promise<EffectiveQuotaLimits> {
		const res = await client.query<QuotaRow>(
			`SELECT scope_type, max_active_root_runs, max_active_child_runs, max_queued_commands,
			        per_user_max_active_root_runs, per_user_max_active_child_runs, per_user_max_queued_commands
			 FROM agent_quotas
			 WHERE tenant_id = $1 AND ((scope_type = 'tenant' AND user_id = '') OR (scope_type = 'user' AND user_id = $2))`,
			[tenantId, userId],
		);

		let tenantRoot = this.defaultQuotas.tenantMaxActiveRootRuns;
		let tenantChild = this.defaultQuotas.tenantMaxActiveChildRuns;
		let tenantQueue = this.defaultQuotas.tenantMaxQueuedCommands;

		let userRoot = this.defaultQuotas.userMaxActiveRootRuns;
		let userChild = this.defaultQuotas.userMaxActiveChildRuns;
		let userQueue = this.defaultQuotas.userMaxQueuedCommands;

		const tenantRow = res.rows.find((r) => r.scope_type === "tenant");
		if (tenantRow) {
			tenantRoot = tenantRow.max_active_root_runs;
			tenantChild = tenantRow.max_active_child_runs;
			tenantQueue = tenantRow.max_queued_commands;
			if (tenantRow.per_user_max_active_root_runs !== null) {
				userRoot = tenantRow.per_user_max_active_root_runs;
			}
			if (tenantRow.per_user_max_active_child_runs !== null) {
				userChild = tenantRow.per_user_max_active_child_runs;
			}
			if (tenantRow.per_user_max_queued_commands !== null) {
				userQueue = tenantRow.per_user_max_queued_commands;
			}
		}

		const userRow = res.rows.find((r) => r.scope_type === "user");
		if (userRow) {
			userRoot = userRow.max_active_root_runs;
			userChild = userRow.max_active_child_runs;
			userQueue = userRow.max_queued_commands;
		}

		return {
			tenantMaxActiveRootRuns: tenantRoot,
			tenantMaxActiveChildRuns: tenantChild,
			tenantMaxQueuedCommands: tenantQueue,
			userMaxActiveRootRuns: Math.min(tenantRoot, userRoot),
			userMaxActiveChildRuns: Math.min(tenantChild, userChild),
			userMaxQueuedCommands: Math.min(tenantQueue, userQueue),
		};
	}

	/**
	 * Idempotently admit a session command (`prompt`, `steer`, `follow_up`, or `continuation`)
	 * with dual tenant/user queue backpressure checks and strict per-session `command_seq` ordering.
	 */
	async submitCommand(input: SubmitCommandInput): Promise<SubmitCommandResult> {
		const payloadHash = canonicalJsonHash(input.payload);

		const existingFast = await this.pool.query<CommandRow>(
			`SELECT * FROM agent_commands WHERE tenant_id = $1 AND command_id = $2`,
			[input.identity.tenantId, input.commandId],
		);
		if (existingFast.rows.length > 0) {
			const cmd = mapCommandRow(existingFast.rows[0]);
			if (cmd.payloadHash !== payloadHash) {
				throw new CommandPayloadConflictError(input.identity.tenantId, input.commandId);
			}
			return { command: cmd, deduplicated: true };
		}

		return withPgTransaction(this.pool, async (client) => {
			// Lock target session row to serialize `command_seq` allocation for this session.
			const sessRes = await client.query<{ origin_kind: string }>(
				`SELECT origin_kind FROM agent_sessions
				 WHERE tenant_id = $1 AND session_id = $2 AND user_id = $3
				 FOR UPDATE`,
				[input.identity.tenantId, input.identity.sessionId, input.identity.userId],
			);
			if (sessRes.rows.length === 0) {
				throw new SessionNotFoundError(input.identity);
			}

			// Re-check idempotency inside transaction in case of concurrent identical request.
			const existingTx = await client.query<CommandRow>(
				`SELECT * FROM agent_commands WHERE tenant_id = $1 AND command_id = $2`,
				[input.identity.tenantId, input.commandId],
			);
			if (existingTx.rows.length > 0) {
				const cmd = mapCommandRow(existingTx.rows[0]);
				if (cmd.payloadHash !== payloadHash) {
					throw new CommandPayloadConflictError(input.identity.tenantId, input.commandId);
				}
				return { command: cmd, deduplicated: true };
			}

			// Enforce queue backpressure on external commands (`continuation` wakeups bypass user queue cap).
			if (input.payload.kind !== "continuation") {
				const limits = await this.resolveEffectiveQuotas(
					client,
					input.identity.tenantId,
					input.identity.userId,
				);
				const countsRes = await client.query<{ tenant_queued: string | number; user_queued: string | number }>(
					`SELECT
						COUNT(*) FILTER (WHERE state = 'queued') AS tenant_queued,
						COUNT(*) FILTER (WHERE state = 'queued' AND user_id = $2) AS user_queued
					 FROM agent_commands
					 WHERE tenant_id = $1 AND state = 'queued'`,
					[input.identity.tenantId, input.identity.userId],
				);
				const tenantQueued = parsePgBigint(countsRes.rows[0].tenant_queued);
				const userQueued = parsePgBigint(countsRes.rows[0].user_queued);
				if (tenantQueued >= limits.tenantMaxQueuedCommands) {
					throw new QuotaExceededError(
						"tenant",
						input.identity.tenantId,
						input.identity.userId,
						limits.tenantMaxQueuedCommands,
					);
				}
				if (userQueued >= limits.userMaxQueuedCommands) {
					throw new QuotaExceededError(
						"user",
						input.identity.tenantId,
						input.identity.userId,
						limits.userMaxQueuedCommands,
					);
				}
			}

			const seqRes = await client.query<{ next_cmd_seq: string | number }>(
				`SELECT COALESCE(MAX(command_seq), 0) + 1 AS next_cmd_seq
				 FROM agent_commands
				 WHERE tenant_id = $1 AND session_id = $2`,
				[input.identity.tenantId, input.identity.sessionId],
			);
			const commandSeq = parsePgBigint(seqRes.rows[0].next_cmd_seq);
			const runId = input.payload.kind === "prompt" ? (input.runId ?? newRunId()) : null;
			const isChild = input.isChild ?? sessRes.rows[0].origin_kind === "subagent";
			const maxRecoveryAttempts = input.maxRecoveryAttempts ?? 3;

			const inserted = await client.query<CommandRow>(
				`INSERT INTO agent_commands (
					tenant_id, command_id, session_id, user_id, command_seq, kind,
					payload, payload_hash, run_id, is_child, state,
					recovery_attempts, max_recovery_attempts, accepted_at, updated_at
				) VALUES (
					$1, $2, $3, $4, $5, $6,
					$7::json, $8, $9, $10, 'queued',
					0, $11, clock_timestamp(), clock_timestamp()
				)
				RETURNING *`,
				[
					input.identity.tenantId,
					input.commandId,
					input.identity.sessionId,
					input.identity.userId,
					commandSeq,
					input.payload.kind,
					JSON.stringify(input.payload),
					payloadHash,
					runId,
					isChild,
					maxRecoveryAttempts,
				],
			);

			return { command: mapCommandRow(inserted.rows[0]), deduplicated: false };
		});
	}

	/** Persist a cross-worker cancellation intent on a command (`cancel_requested_at`). */
	async requestCancelCommand(tenantId: string, commandId: string, reason = "User requested cancellation"): Promise<boolean> {
		const res = await this.pool.query(
			`UPDATE agent_commands
			 SET cancel_requested_at = COALESCE(cancel_requested_at, clock_timestamp()),
			     cancel_reason = COALESCE(cancel_reason, $3),
			     state = CASE WHEN state = 'queued' THEN 'cancelled' ELSE state END,
			     finished_at = CASE WHEN state = 'queued' THEN clock_timestamp() ELSE finished_at END,
			     updated_at = clock_timestamp()
			 WHERE tenant_id = $1 AND command_id = $2
			   AND state IN ('queued', 'running', 'paused')`,
			[tenantId, commandId, reason],
		);
		return (res.rowCount ?? 0) > 0;
	}

	/** Check whether cancellation has been requested on `commandId` (or its paused parent run). */
	async checkCancelRequested(
		tenantId: string,
		commandId: string,
	): Promise<{ readonly requested: boolean; readonly reason: string | null }> {
		const res = await this.pool.query<{ cancel_requested_at: Date | string | null; cancel_reason: string | null }>(
			`SELECT cancel_requested_at, cancel_reason
			 FROM agent_commands
			 WHERE tenant_id = $1 AND command_id = $2`,
			[tenantId, commandId],
		);
		if (res.rows.length === 0) {
			return { requested: false, reason: null };
		}
		return {
			requested: res.rows[0].cancel_requested_at !== null,
			reason: res.rows[0].cancel_reason,
		};
	}

	/**
	 * Batch-renew active session leases in sorted `(tenantId, sessionId)` order (Iron Law #1 & #2)
	 * using a single SQL round-trip and PostgreSQL `clock_timestamp()`.
	 */
	async renewLeasesBatch(
		holder: string,
		leases: readonly { readonly tenantId: string; readonly sessionId: string; readonly fence: number }[],
		leaseTtlMs: number,
	): Promise<{
		readonly renewed: readonly SessionLeaseRecord[];
		readonly lost: readonly { readonly tenantId: string; readonly sessionId: string; readonly fence: number }[];
	}> {
		if (leases.length === 0) {
			return { renewed: [], lost: [] };
		}

		// Sort ascending by (tenantId, sessionId) to guarantee deadlock-free row lock acquisition.
		const sorted = [...leases].sort(
			(a, b) => a.tenantId.localeCompare(b.tenantId) || a.sessionId.localeCompare(b.sessionId),
		);
		const tenantIds = sorted.map((l) => l.tenantId);
		const sessionIds = sorted.map((l) => l.sessionId);
		const fences = sorted.map((l) => l.fence);

		const res = await this.pool.query<LeaseRow>(
			`UPDATE agent_session_leases AS l
			 SET renewed_at = clock_timestamp(),
			     expires_at = clock_timestamp() + ($5::bigint * interval '1 millisecond')
			 FROM UNNEST($2::text[], $3::text[], $4::bigint[]) AS input(tenant_id, session_id, fence)
			 WHERE l.tenant_id = input.tenant_id
			   AND l.session_id = input.session_id
			   AND l.holder = $1
			   AND l.fence = input.fence
			   AND l.expires_at > clock_timestamp()
			 RETURNING l.*`,
			[holder, tenantIds, sessionIds, fences, leaseTtlMs],
		);

		const renewedRecords = res.rows.map(mapLeaseRow);
		const renewedSet = new Set(renewedRecords.map((r) => `${r.tenantId}\0${r.sessionId}`));
		const lost = sorted.filter((item) => !renewedSet.has(`${item.tenantId}\0${item.sessionId}`));

		return { renewed: renewedRecords, lost };
	}

	/** Release a session lease without deleting the row so `fence` stays strictly monotonic forever. */
	async releaseLease(identity: SessionIdentity, holder: string, fence: number): Promise<void> {
		await this.pool.query(
			`UPDATE agent_session_leases
			 SET expires_at = clock_timestamp(),
			     renewed_at = clock_timestamp()
			 WHERE tenant_id = $1 AND session_id = $2 AND holder = $3 AND fence = $4`,
			[identity.tenantId, identity.sessionId, holder, fence],
		);
	}

	/**
	 * Claim either (1) an orphaned `running` command with an expired lease (applying poison-pill `dead_letter`
	 * circuit breaking when `recovery_attempts > max_recovery_attempts`), or (2) the oldest eligible `queued`
	 * `prompt`/`continuation` command respecting dual tenant & user active run quotas.
	 */
	async claimNextCommand(params: {
		readonly holder: string;
		readonly isChild: boolean;
		readonly leaseTtlMs: number;
	}): Promise<ClaimedExecution | null> {
		// Try up to 4 iterations in case a poisoned recovery candidate gets moved to `dead_letter`.
		for (let step = 0; step < 4; step++) {
			const outcome = await withPgTransaction(this.pool, async (client): Promise<
				{ readonly kind: "claimed"; readonly value: ClaimedExecution } | { readonly kind: "poisoned" } | { readonly kind: "empty" }
			> => {
				// 1. Priority: Recover crashed `running` commands whose lease has expired in PG clock.
				const orphanRes = await client.query<CommandRow>(
					`SELECT c.*
					 FROM agent_commands c
					 LEFT JOIN agent_session_leases l
					   ON l.tenant_id = c.tenant_id AND l.session_id = c.session_id
					 WHERE c.is_child = $1
					   AND c.state = 'running'
					   AND (l.expires_at IS NULL OR l.expires_at <= clock_timestamp())
					 ORDER BY c.updated_at ASC, c.tenant_id ASC, c.command_id ASC
					 LIMIT 1
					 FOR UPDATE OF c SKIP LOCKED`,
					[params.isChild],
				);

				if (orphanRes.rows.length > 0) {
					const cmd = mapCommandRow(orphanRes.rows[0]);
					const identity: SessionIdentity = {
						tenantId: cmd.tenantId,
						userId: cmd.userId,
						sessionId: cmd.sessionId,
					};
					const lease = await this.acquireOrTakeoverLeaseInTx(
						client,
						identity,
						params.holder,
						params.leaseTtlMs,
					);
					if (lease) {
						const nextAttempts = cmd.recoveryAttempts + 1;
						const targetRunId =
							cmd.runId ??
							(cmd.payload.kind === "continuation" ? cmd.payload.targetRunId : newRunId());

						if (nextAttempts > cmd.maxRecoveryAttempts) {
							// Poison-pill circuit breaker: transition to `dead_letter` and release lease immediately.
							const errMsg = `Command exceeded max_recovery_attempts (${String(cmd.maxRecoveryAttempts)})`;
							await this.sessionStorage.appendEventsInTx(client, {
								identity,
								expectedFence: lease.fence,
								events: [
									{
										eventId: newEventId(),
										runId: targetRunId,
										payload: {
											kind: "run_interrupted",
											runId: targetRunId,
											reason: "dead_letter",
											errorMessage: errMsg,
										},
									},
								],
								sessionMutation: {
									status: "idle",
									activeRunId: null,
								},
							});
							await client.query(
								`UPDATE agent_commands
								 SET state = 'dead_letter',
								     recovery_attempts = $3,
								     last_error = $4,
								     finished_at = clock_timestamp(),
								     updated_at = clock_timestamp()
								 WHERE tenant_id = $1 AND (command_id = $2 OR (session_id = $5 AND run_id = $6 AND state = 'paused'))`,
								[cmd.tenantId, cmd.commandId, nextAttempts, errMsg, cmd.sessionId, targetRunId],
							);
							await client.query(
								`UPDATE agent_runs
								 SET status = 'failed',
								     recovery_attempts = $4,
								     finished_at = clock_timestamp()
								 WHERE tenant_id = $1 AND session_id = $2 AND run_id = $3`,
								[cmd.tenantId, cmd.sessionId, targetRunId, nextAttempts],
							);
							await client.query(
								`UPDATE agent_session_leases
								 SET expires_at = clock_timestamp()
								 WHERE tenant_id = $1 AND session_id = $2 AND fence = $3`,
								[cmd.tenantId, cmd.sessionId, lease.fence],
							);
							return { kind: "poisoned" };
						}

						const updatedCmdRes = await client.query<CommandRow>(
							`UPDATE agent_commands
							 SET recovery_attempts = $3,
							     updated_at = clock_timestamp()
							 WHERE tenant_id = $1 AND command_id = $2
							 RETURNING *`,
							[cmd.tenantId, cmd.commandId, nextAttempts],
						);
						const runRes = await client.query<RunRow>(
							`UPDATE agent_runs
							 SET status = 'running',
							     recovery_attempts = $4
							 WHERE tenant_id = $1 AND session_id = $2 AND run_id = $3
							 RETURNING *`,
							[cmd.tenantId, cmd.sessionId, targetRunId, nextAttempts],
						);
						const rootRunId =
							runRes.rows[0]?.root_run_id ?? `${cmd.sessionId}/${targetRunId}`;

						return {
							kind: "claimed",
							value: {
								command: mapCommandRow(updatedCmdRes.rows[0]),
								lease,
								runId: targetRunId,
								rootRunId,
								isRecovery: true,
							},
						};
					}
				}

				// 2. Fair FIFO claim of eligible `queued` `prompt` or `continuation` commands respecting quotas.
				const candidatesRes = await client.query<CommandRow>(
					`SELECT c.*
					 FROM agent_commands c
					 WHERE c.is_child = $1
					   AND c.state = 'queued'
					   AND c.kind IN ('prompt', 'continuation')
					   AND NOT EXISTS (
					       SELECT 1 FROM agent_commands active
					       WHERE active.tenant_id = c.tenant_id
					         AND active.session_id = c.session_id
					         AND (
					             active.state = 'running'
					             OR (c.kind = 'prompt' AND active.state = 'paused')
					         )
					   )
					   AND NOT EXISTS (
					       SELECT 1 FROM agent_commands earlier
					       WHERE earlier.tenant_id = c.tenant_id
					         AND earlier.session_id = c.session_id
					         AND earlier.state = 'queued'
					         AND earlier.kind = c.kind
					         AND earlier.command_seq < c.command_seq
					   )
					 ORDER BY c.accepted_at ASC, c.command_id ASC
					 LIMIT 8
					 FOR UPDATE OF c SKIP LOCKED`,
					[params.isChild],
				);

				for (const row of candidatesRes.rows) {
					const candidate = mapCommandRow(row);
					const limits = await this.resolveEffectiveQuotas(client, candidate.tenantId, candidate.userId);
					const activeRes = await client.query<{ tenant_active: string | number; user_active: string | number }>(
						`SELECT
							COUNT(*) AS tenant_active,
							COUNT(*) FILTER (WHERE user_id = $2) AS user_active
						 FROM agent_runs
						 WHERE tenant_id = $1
						   AND status = 'running'
						   AND ($3::boolean = (source = 'subagent'))`,
						[candidate.tenantId, candidate.userId, params.isChild],
					);
					const tenantActive = parsePgBigint(activeRes.rows[0].tenant_active);
					const userActive = parsePgBigint(activeRes.rows[0].user_active);
					const maxTenant = params.isChild
						? limits.tenantMaxActiveChildRuns
						: limits.tenantMaxActiveRootRuns;
					const maxUser = params.isChild
						? limits.userMaxActiveChildRuns
						: limits.userMaxActiveRootRuns;

					if (tenantActive >= maxTenant || userActive >= maxUser) {
						continue;
					}

					const identity: SessionIdentity = {
						tenantId: candidate.tenantId,
						userId: candidate.userId,
						sessionId: candidate.sessionId,
					};
					const lease = await this.acquireOrTakeoverLeaseInTx(
						client,
						identity,
						params.holder,
						params.leaseTtlMs,
					);
					if (!lease) {
						continue;
					}

					// Lock session row in strict order after lease!
					const sessRes = await client.query<{ root_session_id: string; depth: number }>(
						`SELECT root_session_id, depth
						 FROM agent_sessions
						 WHERE tenant_id = $1 AND session_id = $2 AND user_id = $3
						 FOR UPDATE`,
						[identity.tenantId, identity.sessionId, identity.userId],
					);
					if (sessRes.rows.length === 0) {
						continue;
					}

					const updatedCmdRes = await client.query<CommandRow>(
						`UPDATE agent_commands
						 SET state = 'running',
						     started_at = COALESCE(started_at, clock_timestamp()),
						     updated_at = clock_timestamp()
						 WHERE tenant_id = $1 AND command_id = $2
						 RETURNING *`,
						[candidate.tenantId, candidate.commandId],
					);
					const claimedCmd = mapCommandRow(updatedCmdRes.rows[0]);

					let runId: string;
					let rootRunId: string;
					let isRecovery = false;

					if (claimedCmd.payload.kind === "prompt") {
						runId = claimedCmd.runId ?? newRunId();
						rootRunId =
							claimedCmd.payload.rootRunId ??
							`${sessRes.rows[0].root_session_id}/${runId}`;
						const depth = claimedCmd.payload.depth ?? Number(sessRes.rows[0].depth);
						await client.query(
							`INSERT INTO agent_runs (
								tenant_id, session_id, run_id, command_id, user_id, root_run_id,
								parent_session_id, parent_run_id, depth, source, status,
								recovery_attempts, started_at, metadata
							) VALUES (
								$1, $2, $3, $4, $5, $6,
								$7, $8, $9, $10, 'running',
								0, clock_timestamp(), '{}'
							)
							ON CONFLICT (tenant_id, session_id, run_id) DO UPDATE SET
								status = 'running'`,
							[
								claimedCmd.tenantId,
								claimedCmd.sessionId,
								runId,
								claimedCmd.commandId,
								claimedCmd.userId,
								rootRunId,
								claimedCmd.payload.parentSessionId ?? null,
								claimedCmd.payload.parentRunId ?? null,
								depth,
								claimedCmd.payload.source,
							],
						);
					} else if (claimedCmd.payload.kind === "continuation") {
						runId = claimedCmd.payload.targetRunId;
						isRecovery = true;
						const existingRunRes = await client.query<{ root_run_id: string }>(
							`UPDATE agent_runs
							 SET status = 'running'
							 WHERE tenant_id = $1 AND session_id = $2 AND run_id = $3
							 RETURNING root_run_id`,
							[claimedCmd.tenantId, claimedCmd.sessionId, runId],
						);
						rootRunId =
							existingRunRes.rows[0]?.root_run_id ??
							`${sessRes.rows[0].root_session_id}/${runId}`;
					} else {
						throw new Error(`Unexpected claimed command kind: ${claimedCmd.kind}`);
					}

					return {
						kind: "claimed",
						value: {
							command: claimedCmd,
							lease,
							runId,
							rootRunId,
							isRecovery,
						},
					};
				}

				return { kind: "empty" };
			});

			if (outcome.kind === "claimed") {
				return outcome.value;
			}
			if (outcome.kind === "empty") {
				return null;
			}
		}
		return null;
	}

	/** Drain queued `steer` or `follow_up` commands for an active session while verifying `fence`. */
	async drainQueuedSessionMessages(params: {
		readonly identity: SessionIdentity;
		readonly kind: "steer" | "follow_up";
		readonly expectedFence: number;
	}): Promise<readonly Message[]> {
		return withPgTransaction(this.pool, async (client) => {
			await this.assertLeaseFenceInTx(client, params.identity, params.expectedFence);

			const res = await client.query<CommandRow>(
				`UPDATE agent_commands
				 SET state = 'completed',
				     started_at = clock_timestamp(),
				     finished_at = clock_timestamp(),
				     updated_at = clock_timestamp()
				 WHERE tenant_id = $1
				   AND session_id = $2
				   AND state = 'queued'
				   AND kind = $3
				 RETURNING *`,
				[params.identity.tenantId, params.identity.sessionId, params.kind],
			);

			const sorted = res.rows.map(mapCommandRow).sort((a, b) => a.commandSeq - b.commandSeq);
			const messages: Message[] = [];
			for (const cmd of sorted) {
				if (cmd.payload.kind === "steer" || cmd.payload.kind === "follow_up") {
					messages.push(...cmd.payload.messages);
				}
			}
			return messages;
		});
	}

	/**
	 * Atomically settle a finished (`completed`, `cancelled`, or `failed`) command and its `agent_runs` row,
	 * and immediately release `agent_session_leases` (`expires_at = clock_timestamp()`).
	 */
	async settleExecution(params: {
		readonly identity: SessionIdentity;
		readonly commandId: string;
		readonly runId: string;
		readonly holder: string;
		readonly expectedFence: number;
		readonly status: "completed" | "cancelled" | "failed";
		readonly errorMessage?: string;
	}): Promise<void> {
		await withPgTransaction(this.pool, async (client) => {
			await this.assertLeaseFenceInTx(client, params.identity, params.expectedFence);

			// Also settle the original `paused` prompt command if this execution was a resuming `continuation` command!
			await client.query(
				`UPDATE agent_commands
				 SET state = $4,
				     last_error = $5,
				     finished_at = clock_timestamp(),
				     updated_at = clock_timestamp()
				 WHERE tenant_id = $1
				   AND session_id = $2
				   AND (command_id = $3 OR (run_id = $6 AND state IN ('running', 'paused')))`,
				[
					params.identity.tenantId,
					params.identity.sessionId,
					params.commandId,
					params.status,
					params.errorMessage ?? null,
					params.runId,
				],
			);

			await client.query(
				`UPDATE agent_runs
				 SET status = $4,
				     finished_at = clock_timestamp()
				 WHERE tenant_id = $1 AND session_id = $2 AND run_id = $3`,
				[params.identity.tenantId, params.identity.sessionId, params.runId, params.status],
			);

			// Consume any approved tickets used in this run.
			await client.query(
				`UPDATE agent_approvals
				 SET status = 'consumed',
				     consumed_at = clock_timestamp()
				 WHERE tenant_id = $1 AND session_id = $2 AND run_id = $3 AND status = 'approved'`,
				[params.identity.tenantId, params.identity.sessionId, params.runId],
			);

			await client.query(
				`UPDATE agent_session_leases
				 SET expires_at = clock_timestamp(),
				     renewed_at = clock_timestamp()
				 WHERE tenant_id = $1 AND session_id = $2 AND holder = $3 AND fence = $4`,
				[params.identity.tenantId, params.identity.sessionId, params.holder, params.expectedFence],
			);
		});
	}

	/**
	 * Persist pending `agent_approvals` tickets, mark the command `paused` and run `waiting_approval`,
	 * and immediately release `agent_session_leases` so the worker slot is freed (ADR-008).
	 */
	async parkExecutionForApprovals(params: {
		readonly identity: SessionIdentity;
		readonly commandId: string;
		readonly runId: string;
		readonly holder: string;
		readonly expectedFence: number;
		readonly approvals: readonly ApprovalRequestSpec[];
	}): Promise<readonly ApprovalRecord[]> {
		return withPgTransaction(this.pool, async (client) => {
			await this.assertLeaseFenceInTx(client, params.identity, params.expectedFence);

			await client.query(
				`UPDATE agent_commands
				 SET state = 'paused',
				     updated_at = clock_timestamp()
				 WHERE tenant_id = $1
				   AND session_id = $2
				   AND (command_id = $3 OR (run_id = $4 AND state = 'running'))`,
				[params.identity.tenantId, params.identity.sessionId, params.commandId, params.runId],
			);

			await client.query(
				`UPDATE agent_runs
				 SET status = 'waiting_approval'
				 WHERE tenant_id = $1 AND session_id = $2 AND run_id = $3`,
				[params.identity.tenantId, params.identity.sessionId, params.runId],
			);

			const createdApprovals: ApprovalRecord[] = [];
			for (const spec of params.approvals) {
				const res = await client.query<ApprovalRow>(
					`INSERT INTO agent_approvals (
						tenant_id, approval_id, session_id, user_id, run_id,
						operation_id, call_id, approver_id, tool_name,
						action_payload, action_hash, context, status,
						created_at, expires_at
					) VALUES (
						$1, $2, $3, $4, $5,
						$6, $7, $8, $9,
						$10::json, $11, $12::jsonb, 'pending',
						clock_timestamp(), clock_timestamp() + ($13::bigint * interval '1 millisecond')
					)
					ON CONFLICT (tenant_id, operation_id) DO UPDATE SET
						status = agent_approvals.status
					RETURNING *`,
					[
						spec.tenantId,
						spec.approvalId,
						spec.sessionId,
						spec.userId,
						spec.runId,
						spec.operationId,
						spec.callId,
						spec.approverId,
						spec.toolName,
						JSON.stringify(spec.actionPayload),
						spec.actionHash,
						JSON.stringify(spec.context),
						spec.ttlMs,
					],
				);
				createdApprovals.push(mapApprovalRow(res.rows[0]));
			}

			// Immediately release session lease so no worker holds a lock while waiting for a human!
			await client.query(
				`UPDATE agent_session_leases
				 SET expires_at = clock_timestamp(),
				     renewed_at = clock_timestamp()
				 WHERE tenant_id = $1 AND session_id = $2 AND holder = $3 AND fence = $4`,
				[params.identity.tenantId, params.identity.sessionId, params.holder, params.expectedFence],
			);

			return createdApprovals;
		});
	}

	/**
	 * Record an approver's decision (`approved` or `rejected`). Once all tickets of that `(session_id, run_id)`
	 * have settled, enqueue an `approval_decided` message into `agent_outbox` without locking `agent_sessions`.
	 */
	async decideApproval(params: {
		readonly tenantId: string;
		readonly approvalId: string;
		readonly decision: "approved" | "rejected";
		readonly decidedBy: string;
		readonly reason?: string;
	}): Promise<{ readonly approval: ApprovalRecord; readonly allSettledForRun: boolean }> {
		return withPgTransaction(this.pool, async (client) => {
			const res = await client.query<ApprovalRow>(
				`UPDATE agent_approvals
				 SET status = $3,
				     decided_by = $4,
				     decision_reason = $5,
				     decided_at = clock_timestamp()
				 WHERE tenant_id = $1 AND approval_id = $2 AND status = 'pending'
				 RETURNING *`,
				[
					params.tenantId,
					params.approvalId,
					params.decision,
					params.decidedBy,
					params.reason ?? null,
				],
			);
			if (res.rows.length === 0) {
				const existing = await client.query<ApprovalRow>(
					`SELECT * FROM agent_approvals WHERE tenant_id = $1 AND approval_id = $2`,
					[params.tenantId, params.approvalId],
				);
				if (existing.rows.length === 0) {
					throw new Error(`Approval ticket not found: ${params.tenantId}/${params.approvalId}`);
				}
				return { approval: mapApprovalRow(existing.rows[0]), allSettledForRun: false };
			}

			const updated = mapApprovalRow(res.rows[0]);
			const pendingCheck = await client.query<{ pending_count: string | number; approval_ids: string[] }>(
				`SELECT
					COUNT(*) FILTER (WHERE status = 'pending') AS pending_count,
					ARRAY_AGG(approval_id ORDER BY created_at ASC) AS approval_ids
				 FROM agent_approvals
				 WHERE tenant_id = $1 AND session_id = $2 AND run_id = $3`,
				[updated.tenantId, updated.sessionId, updated.runId],
			);

			const pendingCount = parsePgBigint(pendingCheck.rows[0].pending_count);
			const allSettled = pendingCount === 0;

			if (allSettled) {
				const outboxPayload: OutboxPayload = {
					kind: "approval_decided",
					runId: updated.runId,
					approvalIds: pendingCheck.rows[0].approval_ids ?? [updated.approvalId],
				};
				await this.enqueueOutboxInTx(client, {
					tenantId: updated.tenantId,
					userId: updated.userId,
					messageId: `approval_decided:${updated.sessionId}:${updated.runId}:${updated.approvalId}`,
					kind: "approval_decided",
					targetSessionId: updated.sessionId,
					payload: outboxPayload,
				});
			}

			return { approval: updated, allSettledForRun: allSettled };
		});
	}

	/** Expire overdue `pending` approval tickets and enqueue wakeup notifications when runs have no remaining pending tickets. */
	async expireOverdueApprovals(limit = 50): Promise<number> {
		return withPgTransaction(this.pool, async (client) => {
			const expiredRes = await client.query<ApprovalRow>(
				`UPDATE agent_approvals
				 SET status = 'expired',
				     decision_reason = 'Approval expired',
				     decided_at = clock_timestamp()
				 WHERE (tenant_id, approval_id) IN (
					 SELECT tenant_id, approval_id
					 FROM agent_approvals
					 WHERE status = 'pending' AND expires_at <= clock_timestamp()
					 ORDER BY expires_at ASC
					 LIMIT $1
					 FOR UPDATE SKIP LOCKED
				 )
				 RETURNING *`,
				[limit],
			);

			for (const row of expiredRes.rows) {
				const apr = mapApprovalRow(row);
				const remRes = await client.query<{ pending_count: string | number; approval_ids: string[] }>(
					`SELECT
						COUNT(*) FILTER (WHERE status = 'pending') AS pending_count,
						ARRAY_AGG(approval_id ORDER BY created_at ASC) AS approval_ids
					 FROM agent_approvals
					 WHERE tenant_id = $1 AND session_id = $2 AND run_id = $3`,
					[apr.tenantId, apr.sessionId, apr.runId],
				);
				if (parsePgBigint(remRes.rows[0].pending_count) === 0) {
					await this.enqueueOutboxInTx(client, {
						tenantId: apr.tenantId,
						userId: apr.userId,
						messageId: `approval_expired:${apr.sessionId}:${apr.runId}`,
						kind: "approval_decided",
						targetSessionId: apr.sessionId,
						payload: {
							kind: "approval_decided",
							runId: apr.runId,
							approvalIds: remRes.rows[0].approval_ids ?? [apr.approvalId],
						},
					});
				}
			}

			return expiredRes.rows.length;
		});
	}

	/** Insert a lock-free cross-session notification into `agent_outbox` (used by finished subagents and external signals). */
	async enqueueOutbox(params: {
		readonly tenantId: string;
		readonly userId: string;
		readonly messageId?: string;
		readonly kind: OutboxKind;
		readonly targetSessionId: string;
		readonly sourceSessionId?: string;
		readonly sourceRunId?: string;
		readonly payload: OutboxPayload;
	}): Promise<void> {
		await this.enqueueOutboxInTx(this.pool, params);
	}

	private async enqueueOutboxInTx(
		client: PgQueryable,
		params: {
			readonly tenantId: string;
			readonly userId: string;
			readonly messageId?: string;
			readonly kind: OutboxKind;
			readonly targetSessionId: string;
			readonly sourceSessionId?: string;
			readonly sourceRunId?: string;
			readonly payload: OutboxPayload;
		},
	): Promise<void> {
		await client.query(
			`INSERT INTO agent_outbox (
				tenant_id, user_id, message_id, kind, target_session_id,
				source_session_id, source_run_id, payload, state,
				attempts, available_at, created_at
			) VALUES (
				$1, $2, $3, $4, $5,
				$6, $7, $8::jsonb, 'pending',
				0, clock_timestamp(), clock_timestamp()
			)
			ON CONFLICT (tenant_id, message_id) DO NOTHING`,
			[
				params.tenantId,
				params.userId,
				params.messageId ?? newOutboxMessageId(),
				params.kind,
				params.targetSessionId,
				params.sourceSessionId ?? null,
				params.sourceRunId ?? null,
				JSON.stringify(params.payload),
			],
		);
	}

	/**
	 * Consume due `agent_outbox` rows via `FOR UPDATE SKIP LOCKED` and deliver them as `continuation`
	 * commands on their target sessions (with exponential backoff and `dead_letter` protection).
	 */
	async dispatchPendingOutboxBatch(limit = 25): Promise<number> {
		const claimed = await withPgTransaction(this.pool, async (client) => {
			const res = await client.query<OutboxRow>(
				`SELECT *
				 FROM agent_outbox
				 WHERE state = 'pending' AND available_at <= clock_timestamp()
				 ORDER BY available_at ASC, message_seq ASC
				 LIMIT $1
				 FOR UPDATE SKIP LOCKED`,
				[limit],
			);
			return res.rows.map(mapOutboxRow);
		});

		let deliveredCount = 0;
		for (const item of claimed) {
			try {
				await this.deliverSingleOutboxItem(item);
				await this.pool.query(
					`UPDATE agent_outbox
					 SET state = 'delivered',
					     delivered_at = clock_timestamp()
					 WHERE message_seq = $1`,
					[item.messageSeq],
				);
				deliveredCount += 1;
			} catch (err) {
				const nextAttempts = item.attempts + 1;
				const backoffMs = Math.min(60_000, 1_000 * 2 ** Math.min(6, nextAttempts));
				const nextState = nextAttempts >= 5 ? "dead_letter" : "pending";
				await this.pool.query(
					`UPDATE agent_outbox
					 SET state = $2,
					     attempts = $3,
					     last_error = $4,
					     available_at = clock_timestamp() + ($5::bigint * interval '1 millisecond')
					 WHERE message_seq = $1`,
					[
						item.messageSeq,
						nextState,
						nextAttempts,
						err instanceof Error ? err.message : String(err),
						backoffMs,
					],
				);
			}
		}

		return deliveredCount;
	}

	private async deliverSingleOutboxItem(item: OutboxRecord): Promise<void> {
		const targetIdentity: SessionIdentity = {
			tenantId: item.tenantId,
			userId: item.userId,
			sessionId: item.targetSessionId,
		};

		switch (item.payload.kind) {
			case "approval_decided": {
				const aprRes = await this.pool.query<ApprovalRow>(
					`SELECT * FROM agent_approvals
					 WHERE tenant_id = $1 AND session_id = $2 AND run_id = $3
					 ORDER BY created_at ASC`,
					[item.tenantId, item.targetSessionId, item.payload.runId],
				);
				const decisions: ApprovalDecision[] = aprRes.rows
					.map(mapApprovalRow)
					.filter((r): r is ApprovalRecord & { status: "approved" | "rejected" | "expired" } =>
						r.status === "approved" || r.status === "rejected" || r.status === "expired",
					)
					.map((r) => ({
						approvalId: r.approvalId,
						operationId: r.operationId,
						callId: r.callId,
						actionHash: r.actionHash,
						status: r.status,
						...(r.decidedBy ? { decidedBy: r.decidedBy } : {}),
						...(r.decisionReason ? { reason: r.decisionReason } : {}),
					}));

				await this.submitCommand({
					identity: targetIdentity,
					commandId: `cont_apr_${item.messageId}`,
					payload: {
						kind: "continuation",
						targetRunId: item.payload.runId,
						reason: "approval_decided",
						approvalDecisions: decisions,
					},
				});
				break;
			}

			case "child_result": {
				const reportMsg: Message = {
					role: "user",
					content:
						`[Subagent ${item.payload.childSessionId} (${item.payload.status})]:\n` +
						item.payload.summaryText,
					timestamp: Date.now(),
				};
				await this.submitCommand({
					identity: targetIdentity,
					commandId: `cont_child_${item.messageId}`,
					payload: {
						kind: "prompt",
						source: "system",
						messages: [reportMsg],
					},
				});
				break;
			}

			case "external_signal": {
				const sigMsg: Message = {
					role: "user",
					content: `[External Signal "${item.payload.signalName}"]: ${JSON.stringify(item.payload.data)}`,
					timestamp: Date.now(),
				};
				await this.submitCommand({
					identity: targetIdentity,
					commandId: `cont_sig_${item.messageId}`,
					payload: {
						kind: "continuation",
						targetRunId: item.payload.runId,
						reason: "external_signal",
						messages: [sigMsg],
					},
				});
				break;
			}
		}
	}

	/** TTL cleanup of terminal `agent_commands` and `delivered` `agent_outbox` rows (ADR-010 Category 2). */
	async purgeExpiredControlRecords(params: {
		readonly completedCommandsOlderThanMs: number;
		readonly deliveredOutboxOlderThanMs: number;
	}): Promise<{ readonly deletedCommands: number; readonly deletedOutbox: number }> {
		const [cmdRes, outboxRes] = await Promise.all([
			this.pool.query(
				`DELETE FROM agent_commands
				 WHERE state IN ('completed', 'cancelled', 'failed', 'dead_letter')
				   AND finished_at <= clock_timestamp() - ($1::bigint * interval '1 millisecond')`,
				[params.completedCommandsOlderThanMs],
			),
			this.pool.query(
				`DELETE FROM agent_outbox
				 WHERE state = 'delivered'
				   AND delivered_at <= clock_timestamp() - ($1::bigint * interval '1 millisecond')`,
				[params.deliveredOutboxOlderThanMs],
			),
		]);
		return {
			deletedCommands: cmdRes.rowCount ?? 0,
			deletedOutbox: outboxRes.rowCount ?? 0,
		};
	}

	/** List pending approvals for an approver (`agent_approvals_pending_approver_idx`). */
	async listPendingApprovalsForApprover(
		tenantId: string,
		approverId: string,
		limit = 50,
	): Promise<readonly ApprovalRecord[]> {
		const res = await this.pool.query<ApprovalRow>(
			`SELECT * FROM agent_approvals
			 WHERE tenant_id = $1 AND approver_id = $2 AND status = 'pending'
			 ORDER BY created_at DESC
			 LIMIT $3`,
			[tenantId, approverId, limit],
		);
		return res.rows.map(mapApprovalRow);
	}

	/** Read `agent_runs` record for `(tenantId, sessionId, runId)`. */
	async getRun(tenantId: string, sessionId: string, runId: string): Promise<RunRecord | null> {
		const res = await this.pool.query<RunRow>(
			`SELECT * FROM agent_runs WHERE tenant_id = $1 AND session_id = $2 AND run_id = $3`,
			[tenantId, sessionId, runId],
		);
		return res.rows.length > 0 ? mapRunRow(res.rows[0]) : null;
	}

	private async acquireOrTakeoverLeaseInTx(
		client: PgQueryable,
		identity: SessionIdentity,
		holder: string,
		leaseTtlMs: number,
	): Promise<SessionLeaseRecord | null> {
		const res = await client.query<LeaseRow>(
			`INSERT INTO agent_session_leases (
				tenant_id, session_id, user_id, holder, fence, acquired_at, renewed_at, expires_at
			) VALUES (
				$1, $2, $3, $4, 1,
				clock_timestamp(), clock_timestamp(),
				clock_timestamp() + ($5::bigint * interval '1 millisecond')
			)
			ON CONFLICT (tenant_id, session_id) DO UPDATE SET
				user_id = EXCLUDED.user_id,
				holder = EXCLUDED.holder,
				fence = agent_session_leases.fence + 1,
				acquired_at = clock_timestamp(),
				renewed_at = clock_timestamp(),
				expires_at = clock_timestamp() + ($5::bigint * interval '1 millisecond')
			WHERE agent_session_leases.expires_at <= clock_timestamp()
			   OR agent_session_leases.holder = EXCLUDED.holder
			RETURNING *`,
			[identity.tenantId, identity.sessionId, identity.userId, holder, leaseTtlMs],
		);
		return res.rows.length > 0 ? mapLeaseRow(res.rows[0]) : null;
	}

	private async assertLeaseFenceInTx(
		client: PgQueryable,
		identity: SessionIdentity,
		expectedFence: number,
	): Promise<void> {
		const res = await client.query<{ fence: string | number; is_valid: boolean }>(
			`SELECT fence, (expires_at > clock_timestamp()) AS is_valid
			 FROM agent_session_leases
			 WHERE tenant_id = $1 AND session_id = $2
			 FOR UPDATE`,
			[identity.tenantId, identity.sessionId],
		);
		if (res.rows.length === 0) {
			throw new FenceConflictError(identity.tenantId, identity.sessionId, expectedFence, "missing lease");
		}
		const dbFence = parsePgBigint(res.rows[0].fence);
		if (!res.rows[0].is_valid || dbFence !== expectedFence) {
			throw new FenceConflictError(
				identity.tenantId,
				identity.sessionId,
				expectedFence,
				`valid=${String(res.rows[0].is_valid)}, dbFence=${String(dbFence)}`,
			);
		}
	}
}

function mapLeaseRow(row: LeaseRow): SessionLeaseRecord {
	return {
		tenantId: row.tenant_id,
		sessionId: row.session_id,
		userId: row.user_id,
		holder: row.holder,
		fence: parsePgBigint(row.fence),
		acquiredAt: parsePgTimestamp(row.acquired_at),
		renewedAt: parsePgTimestamp(row.renewed_at),
		expiresAt: parsePgTimestamp(row.expires_at),
	};
}

function mapCommandRow(row: CommandRow): CommandRecord {
	return {
		tenantId: row.tenant_id,
		commandId: row.command_id,
		sessionId: row.session_id,
		userId: row.user_id,
		commandSeq: parsePgBigint(row.command_seq),
		kind: row.kind,
		payload: row.payload,
		payloadHash: row.payload_hash,
		runId: row.run_id,
		isChild: Boolean(row.is_child),
		state: row.state,
		recoveryAttempts: Number(row.recovery_attempts),
		maxRecoveryAttempts: Number(row.max_recovery_attempts),
		cancelRequestedAt: row.cancel_requested_at ? parsePgTimestamp(row.cancel_requested_at) : null,
		cancelReason: row.cancel_reason,
		lastError: row.last_error,
		acceptedAt: parsePgTimestamp(row.accepted_at),
		startedAt: row.started_at ? parsePgTimestamp(row.started_at) : null,
		finishedAt: row.finished_at ? parsePgTimestamp(row.finished_at) : null,
		updatedAt: parsePgTimestamp(row.updated_at),
	};
}

function mapApprovalRow(row: ApprovalRow): ApprovalRecord {
	return {
		tenantId: row.tenant_id,
		approvalId: row.approval_id,
		sessionId: row.session_id,
		userId: row.user_id,
		runId: row.run_id,
		operationId: row.operation_id,
		callId: row.call_id,
		approverId: row.approver_id,
		toolName: row.tool_name,
		actionPayload: row.action_payload,
		actionHash: row.action_hash,
		context: row.context ?? {},
		status: row.status,
		decidedBy: row.decided_by,
		decisionReason: row.decision_reason,
		createdAt: parsePgTimestamp(row.created_at),
		expiresAt: parsePgTimestamp(row.expires_at),
		decidedAt: row.decided_at ? parsePgTimestamp(row.decided_at) : null,
		consumedAt: row.consumed_at ? parsePgTimestamp(row.consumed_at) : null,
	};
}

function mapOutboxRow(row: OutboxRow): OutboxRecord {
	return {
		messageSeq: parsePgBigint(row.message_seq),
		tenantId: row.tenant_id,
		userId: row.user_id,
		messageId: row.message_id,
		kind: row.kind,
		targetSessionId: row.target_session_id,
		sourceSessionId: row.source_session_id,
		sourceRunId: row.source_run_id,
		payload: row.payload,
		state: row.state,
		attempts: Number(row.attempts),
		lastError: row.last_error,
		availableAt: parsePgTimestamp(row.available_at),
		createdAt: parsePgTimestamp(row.created_at),
		deliveredAt: row.delivered_at ? parsePgTimestamp(row.delivered_at) : null,
	};
}

function mapRunRow(row: RunRow): RunRecord {
	return {
		tenantId: row.tenant_id,
		sessionId: row.session_id,
		runId: row.run_id,
		commandId: row.command_id,
		userId: row.user_id,
		rootRunId: row.root_run_id,
		parentSessionId: row.parent_session_id,
		parentRunId: row.parent_run_id,
		depth: Number(row.depth),
		source: row.source,
		status: row.status,
		recoveryAttempts: Number(row.recovery_attempts),
		startedAt: parsePgTimestamp(row.started_at),
		finishedAt: row.finished_at ? parsePgTimestamp(row.finished_at) : null,
		metadata: row.metadata ?? {},
	};
}
