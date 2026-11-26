// Minimal PostgreSQL client/pool transaction interface structurally satisfied by `pg.Pool` and `pg.PoolClient`.

import type { Pool, QueryResult, QueryResultRow } from "pg";

export interface PgQueryable {
	query<R extends QueryResultRow = QueryResultRow>(text: string, values?: unknown[]): Promise<QueryResult<R>>;
}

export interface PgClientLike extends PgQueryable {
	release(err?: Error | boolean): void;
}

export interface PgPoolLike extends PgQueryable {
	connect(): Promise<PgClientLike>;
}

// Compile-time guarantee that a real `pg.Pool` instance satisfies `PgPoolLike` without wrappers.
export type AssertPgPoolCompatible = Pool extends PgPoolLike ? true : never;
const _ASSERT_PG_POOL_COMPATIBLE: AssertPgPoolCompatible = true;
void _ASSERT_PG_POOL_COMPATIBLE;

/** Execute `callback` inside an explicit `BEGIN ... COMMIT` transaction, rolling back cleanly on any error. */
export async function withPgTransaction<T>(
	pool: PgPoolLike,
	callback: (client: PgQueryable) => Promise<T>,
): Promise<T> {
	const client = await pool.connect();
	try {
		await client.query("BEGIN");
		const result = await callback(client);
		await client.query("COMMIT");
		return result;
	} catch (err) {
		try {
			await client.query("ROLLBACK");
		} catch {
			// Preserve original transaction failure.
		}
		throw err;
	} finally {
		client.release();
	}
}

/** Parse PostgreSQL `bigint` column (`string | number`) into a safe JS `number`. */
export function parsePgBigint(value: string | number): number {
	const parsed = typeof value === "number" ? value : Number(value);
	if (!Number.isSafeInteger(parsed)) {
		throw new RangeError(`PostgreSQL bigint value exceeds Number.MAX_SAFE_INTEGER: ${String(value)}`);
	}
	return parsed;
}

/** Parse PostgreSQL `timestamptz` (`Date | string`) into Unix epoch milliseconds. */
export function parsePgTimestamp(value: Date | string): number {
	return value instanceof Date ? value.getTime() : new Date(value).getTime();
}
