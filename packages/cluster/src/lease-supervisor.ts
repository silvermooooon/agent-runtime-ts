// Fault-tolerant batch lease renewal supervisor with monotonic deadline tracking and remote cancel polling (ADR-007).

import type { PostgresCoordinationStore, SessionLeaseRecord } from "../../postgres/src/index.ts";
import { FenceConflictError, type SessionIdentity } from "../../session/src/index.ts";

export interface LeaseSupervisorOptions {
	readonly holder: string;
	readonly coordinationStore: PostgresCoordinationStore;
	/** Total lease TTL in milliseconds granted on claim and each renewal. Default: 30000 (30s). */
	readonly leaseTtlMs?: number;
	/** Minimum remaining lease validity required before dispatching an external call. Default: 5000 (5s). */
	readonly safetyMarginMs?: number;
	/** Background batch renewal cadence in milliseconds. Default: `Math.floor(leaseTtlMs / 3)`. */
	readonly renewIntervalMs?: number;
	readonly onError?: (error: unknown) => void;
}

interface TrackedLease {
	readonly identity: SessionIdentity;
	readonly commandId: string;
	fence: number;
	lastConfirmedMonoMs: number;
	readonly abortController: AbortController;
}

function leaseMapKey(tenantId: string, sessionId: string): string {
	return `${tenantId}\0${sessionId}`;
}

export class LeaseSupervisor {
	readonly holder: string;
	readonly leaseTtlMs: number;
	readonly safetyMarginMs: number;
	readonly renewIntervalMs: number;

	private readonly coordinationStore: PostgresCoordinationStore;
	private readonly onError?: (error: unknown) => void;
	private readonly tracked = new Map<string, TrackedLease>();
	private timer: ReturnType<typeof setTimeout> | null = null;
	private running = false;

	constructor(options: LeaseSupervisorOptions) {
		this.holder = options.holder;
		this.coordinationStore = options.coordinationStore;
		this.leaseTtlMs = options.leaseTtlMs ?? 30_000;
		this.safetyMarginMs = options.safetyMarginMs ?? 5_000;
		this.renewIntervalMs = options.renewIntervalMs ?? Math.max(1_000, Math.floor(this.leaseTtlMs / 3));
		this.onError = options.onError;
	}

	/** Register a newly claimed session lease and return its dedicated `AbortController`. */
	register(lease: SessionLeaseRecord, commandId: string): AbortController {
		const abortController = new AbortController();
		this.tracked.set(leaseMapKey(lease.tenantId, lease.sessionId), {
			identity: {
				tenantId: lease.tenantId,
				userId: lease.userId,
				sessionId: lease.sessionId,
			},
			commandId,
			fence: lease.fence,
			lastConfirmedMonoMs: performance.now(),
			abortController,
		});
		return abortController;
	}

	/** Unregister a completed, parked, or aborted session from background renewal. */
	unregister(identity: SessionIdentity): void {
		this.tracked.delete(leaseMapKey(identity.tenantId, identity.sessionId));
	}

	/** Number of currently tracked active leases on this worker. */
	get activeCount(): number {
		return this.tracked.size;
	}

	/** Start periodic batch renewal loop. */
	start(): void {
		if (this.running) return;
		this.running = true;
		this.scheduleNext(this.renewIntervalMs);
	}

	/** Stop periodic renewal loop. */
	stop(): void {
		this.running = false;
		if (this.timer !== null) {
			clearTimeout(this.timer);
			this.timer = null;
		}
	}

	/**
	 * Pre-dispatch guard (Iron Law #2): if remaining local monotonic lease validity is below `safetyMarginMs`,
	 * perform an immediate synchronous renewal before sending a model or tool request.
	 */
	async ensureLeaseSafe(identity: SessionIdentity): Promise<void> {
		const key = leaseMapKey(identity.tenantId, identity.sessionId);
		const entry = this.tracked.get(key);
		if (!entry) {
			throw new FenceConflictError(identity.tenantId, identity.sessionId, -1, "lease no longer tracked");
		}
		if (entry.abortController.signal.aborted) {
			throw new FenceConflictError(identity.tenantId, identity.sessionId, entry.fence, "session aborted");
		}

		const elapsedMs = performance.now() - entry.lastConfirmedMonoMs;
		const remainingMs = this.leaseTtlMs - elapsedMs;
		if (remainingMs > this.safetyMarginMs) {
			return;
		}

		const { renewed, lost } = await this.coordinationStore.renewLeasesBatch(
			this.holder,
			[{ tenantId: identity.tenantId, sessionId: identity.sessionId, fence: entry.fence }],
			this.leaseTtlMs,
		);
		if (lost.length > 0 || renewed.length === 0) {
			entry.abortController.abort(new FenceConflictError(identity.tenantId, identity.sessionId, entry.fence));
			this.tracked.delete(key);
			throw new FenceConflictError(identity.tenantId, identity.sessionId, entry.fence, "lost during pre-dispatch renewal");
		}
		entry.lastConfirmedMonoMs = performance.now();
	}

	/**
	 * Run one batch renewal + remote cancel check pass.
	 * Unlike naive supervisors, a transient network error does NOT mass-abort active sessions unless an individual
	 * session's monotonic clock has reached `leaseTtlMs - safetyMarginMs`.
	 */
	async tick(): Promise<void> {
		if (this.tracked.size === 0) {
			return;
		}

		const snapshot = [...this.tracked.entries()];
		const batchInput = snapshot.map(([, entry]) => ({
			tenantId: entry.identity.tenantId,
			sessionId: entry.identity.sessionId,
			fence: entry.fence,
		}));

		try {
			const { renewed, lost } = await this.coordinationStore.renewLeasesBatch(
				this.holder,
				batchInput,
				this.leaseTtlMs,
			);
			const nowMono = performance.now();
			for (const rec of renewed) {
				const entry = this.tracked.get(leaseMapKey(rec.tenantId, rec.sessionId));
				if (entry && entry.fence === rec.fence) {
					entry.lastConfirmedMonoMs = nowMono;
				}
			}

			// Abort ONLY sessions that explicitly lost their fence in PostgreSQL.
			for (const lostItem of lost) {
				const key = leaseMapKey(lostItem.tenantId, lostItem.sessionId);
				const entry = this.tracked.get(key);
				if (entry && entry.fence === lostItem.fence) {
					entry.abortController.abort(
						new FenceConflictError(lostItem.tenantId, lostItem.sessionId, lostItem.fence, "lease stolen or expired"),
					);
					this.tracked.delete(key);
				}
			}

			// Poll cross-worker cancellation intents for surviving sessions.
			await Promise.all(
				[...this.tracked.values()].map(async (entry) => {
					const cancelStatus = await this.coordinationStore.checkCancelRequested(
						entry.identity.tenantId,
						entry.commandId,
					);
					if (cancelStatus.requested && !entry.abortController.signal.aborted) {
						entry.abortController.abort(new Error(cancelStatus.reason ?? "Cancelled remotely"));
					}
				}),
			);
		} catch (err) {
			this.onError?.(err);
			// Transient DB/network blip: evict ONLY sessions whose local monotonic lease budget is exhausted!
			const nowMono = performance.now();
			const maxSafeElapsed = Math.max(1_000, this.leaseTtlMs - Math.floor(this.safetyMarginMs / 2));
			for (const [key, entry] of snapshot) {
				if (nowMono - entry.lastConfirmedMonoMs >= maxSafeElapsed) {
					entry.abortController.abort(
						new FenceConflictError(
							entry.identity.tenantId,
							entry.identity.sessionId,
							entry.fence,
							"lease renewal timed out past safety deadline",
						),
					);
					this.tracked.delete(key);
				}
			}
		}
	}

	private scheduleNext(delayMs: number): void {
		if (!this.running) return;
		this.timer = setTimeout(async () => {
			await this.tick();
			this.scheduleNext(this.renewIntervalMs);
		}, delayMs);
	}
}
