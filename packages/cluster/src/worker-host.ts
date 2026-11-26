// Clustered multi-worker scheduler host with isolated root/child slot pools, outbox pump, and graceful drain.

import { hostname } from "node:os";
import { uuidv7 } from "../../ai/src/index.ts";
import type { PostgresCoordinationStore, PostgresSessionStorage } from "../../postgres/src/index.ts";
import type { AuditSink, BlobStore } from "../../session/src/index.ts";
import type { TelemetryContext } from "../../telemetry/src/index.ts";
import { DurableExecutor, type SessionRuntimeResolver } from "./executor.ts";
import { LeaseSupervisor } from "./lease-supervisor.ts";

export interface WorkerHostOptions {
	readonly workerId?: string;
	readonly sessionStorage: PostgresSessionStorage;
	readonly coordinationStore: PostgresCoordinationStore;
	readonly runtimeResolver: SessionRuntimeResolver;
	readonly blobStore?: BlobStore;
	readonly blobThresholdBytes?: number;
	/** Maximum concurrent root/fork session runs on this worker node. Default: 16. */
	readonly maxRootConcurrency?: number;
	/** Maximum concurrent subagent child runs on this worker node (physically isolated pool). Default: 16. */
	readonly maxChildConcurrency?: number;
	/** Lease TTL in milliseconds. Default: 30000 (30s). */
	readonly leaseTtlMs?: number;
	/** Lease pre-dispatch safety margin in milliseconds. Default: 5000 (5s). */
	readonly leaseSafetyMarginMs?: number;
	/** Idle poll interval in milliseconds when no explicit wakeup arrives. Default: 500. */
	readonly pollIntervalMs?: number;
	/** Outbox & expired-approval sweep cadence in milliseconds. Default: 2000. */
	readonly outboxIntervalMs?: number;
	readonly auditSink?: AuditSink;
	readonly telemetry?: TelemetryContext;
	readonly onError?: (error: unknown) => void;
}

export class WorkerHost {
	readonly workerId: string;
	readonly maxRootConcurrency: number;
	readonly maxChildConcurrency: number;

	private readonly coordinationStore: PostgresCoordinationStore;
	private readonly leaseSupervisor: LeaseSupervisor;
	private readonly executor: DurableExecutor;
	private readonly pollIntervalMs: number;
	private readonly outboxIntervalMs: number;
	private readonly onError?: (error: unknown) => void;

	private readonly inFlightRoot = new Set<Promise<void>>();
	private readonly inFlightChild = new Set<Promise<void>>();
	private running = false;
	private pumpScheduled = false;
	private pollTimer: ReturnType<typeof setTimeout> | null = null;
	private outboxTimer: ReturnType<typeof setTimeout> | null = null;

	constructor(options: WorkerHostOptions) {
		this.workerId = options.workerId ?? `${hostname()}:${String(process.pid)}:${uuidv7()}`;
		this.maxRootConcurrency = options.maxRootConcurrency ?? 16;
		this.maxChildConcurrency = options.maxChildConcurrency ?? 16;
		this.coordinationStore = options.coordinationStore;
		this.pollIntervalMs = options.pollIntervalMs ?? 500;
		this.outboxIntervalMs = options.outboxIntervalMs ?? 2_000;
		this.onError = options.onError;

		this.leaseSupervisor = new LeaseSupervisor({
			holder: this.workerId,
			coordinationStore: options.coordinationStore,
			leaseTtlMs: options.leaseTtlMs,
			safetyMarginMs: options.leaseSafetyMarginMs,
			onError: options.onError,
		});

		this.executor = new DurableExecutor({
			sessionStorage: options.sessionStorage,
			coordinationStore: options.coordinationStore,
			leaseSupervisor: this.leaseSupervisor,
			runtimeResolver: options.runtimeResolver,
			blobStore: options.blobStore,
			blobThresholdBytes: options.blobThresholdBytes,
			auditSink: options.auditSink,
			telemetry: options.telemetry,
		});
	}

	/** Current active concurrency snapshot on this worker node. */
	get stats(): { readonly activeRootRuns: number; readonly activeChildRuns: number; readonly activeLeases: number } {
		return {
			activeRootRuns: this.inFlightRoot.size,
			activeChildRuns: this.inFlightChild.size,
			activeLeases: this.leaseSupervisor.activeCount,
		};
	}

	/** Start claiming commands, renewing leases, and pumping `agent_outbox`. */
	start(): void {
		if (this.running) return;
		this.running = true;
		this.leaseSupervisor.start();
		this.scheduleOutboxSweep(50);
		this.notifyWorkAvailable();
	}

	/** Trigger an immediate non-blocking claim pass (for local gateway submissions or Redis/PG wakeup listeners). */
	notifyWorkAvailable(): void {
		if (!this.running || this.pumpScheduled) {
			return;
		}
		this.pumpScheduled = true;
		if (this.pollTimer !== null) {
			clearTimeout(this.pollTimer);
			this.pollTimer = null;
		}
		queueMicrotask(() => {
			this.pumpScheduled = false;
			void this.pumpOnce();
		});
	}

	/**
	 * Run a single deterministic dispatch + claim cycle (useful both for background loops and deterministic tests).
	 */
	async pumpOnce(): Promise<number> {
		let claimedTotal = 0;

		try {
			while (this.running && this.inFlightRoot.size < this.maxRootConcurrency) {
				const claimed = await this.coordinationStore.claimNextCommand({
					holder: this.workerId,
					isChild: false,
					leaseTtlMs: this.leaseSupervisor.leaseTtlMs,
				});
				if (!claimed) break;
				claimedTotal += 1;
				this.launchInPool(this.inFlightRoot, claimed);
			}

			while (this.running && this.inFlightChild.size < this.maxChildConcurrency) {
				const claimed = await this.coordinationStore.claimNextCommand({
					holder: this.workerId,
					isChild: true,
					leaseTtlMs: this.leaseSupervisor.leaseTtlMs,
				});
				if (!claimed) break;
				claimedTotal += 1;
				this.launchInPool(this.inFlightChild, claimed);
			}
		} catch (err) {
			this.onError?.(err);
		} finally {
			if (this.running) {
				const jitter = Math.floor(Math.random() * 50);
				this.pollTimer = setTimeout(() => {
					this.pollTimer = null;
					void this.pumpOnce();
				}, this.pollIntervalMs + jitter);
			}
		}

		return claimedTotal;
	}

	/** Gracefully stop claiming new work, wait up to `drainTimeoutMs` for in-flight runs, then stop lease renewal. */
	async stop(options?: { readonly drainTimeoutMs?: number }): Promise<void> {
		this.running = false;
		if (this.pollTimer !== null) {
			clearTimeout(this.pollTimer);
			this.pollTimer = null;
		}
		if (this.outboxTimer !== null) {
			clearTimeout(this.outboxTimer);
			this.outboxTimer = null;
		}

		const drainTimeoutMs = options?.drainTimeoutMs ?? 15_000;
		const allInFlight = [...this.inFlightRoot, ...this.inFlightChild];
		if (allInFlight.length > 0) {
			await Promise.race([
				Promise.allSettled(allInFlight),
				new Promise<void>((resolve) => setTimeout(resolve, drainTimeoutMs)),
			]);
		}

		this.leaseSupervisor.stop();
	}

	private launchInPool(
		pool: Set<Promise<void>>,
		claimed: Parameters<DurableExecutor["executeClaimed"]>[0],
	): void {
		const task = this.executor
			.executeClaimed(claimed)
			.catch((err) => {
				this.onError?.(err);
			})
			.finally(() => {
				pool.delete(task);
				if (this.running) {
					this.notifyWorkAvailable();
				}
			});
		pool.add(task);
	}

	private scheduleOutboxSweep(delayMs: number): void {
		if (!this.running) return;
		this.outboxTimer = setTimeout(async () => {
			this.outboxTimer = null;
			try {
				await this.coordinationStore.expireOverdueApprovals(25);
				const delivered = await this.coordinationStore.dispatchPendingOutboxBatch(25);
				if (delivered > 0) {
					this.notifyWorkAvailable();
				}
			} catch (err) {
				this.onError?.(err);
			} finally {
				this.scheduleOutboxSweep(this.outboxIntervalMs);
			}
		}, delayMs);
	}
}
