// Optional PostgreSQL `BlobStore` (`pg-blob://` scheme) backed by the optional `agent_blobs` extension table.

import {
	type BlobGetContext,
	type BlobPutContext,
	type BlobRef,
	type BlobStore,
	sha256Hex,
} from "../../session/src/index.ts";
import type { PgPoolLike } from "./client.ts";

export class PostgresBlobStore implements BlobStore {
	readonly scheme = "pg-blob";
	private readonly pool: PgPoolLike;

	constructor(pool: PgPoolLike) {
		this.pool = pool;
	}

	async put(data: Uint8Array, context: BlobPutContext): Promise<{ readonly uri: string }> {
		const blobId = `${context.sessionId}_${context.sha256}`;
		await this.pool.query(
			`INSERT INTO agent_blobs (
				tenant_id, blob_id, user_id, session_id, sha256, mime_type, byte_length, data, created_at
			) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, clock_timestamp())
			ON CONFLICT (tenant_id, blob_id) DO NOTHING`,
			[
				context.tenantId,
				blobId,
				context.userId,
				context.sessionId,
				context.sha256,
				context.mimeType,
				context.byteLength,
				Buffer.from(data.buffer, data.byteOffset, data.byteLength),
			],
		);
		return {
			uri: `pg-blob://${encodeURIComponent(context.tenantId)}/${encodeURIComponent(blobId)}`,
		};
	}

	async get(ref: BlobRef, context: BlobGetContext): Promise<Uint8Array> {
		const blobId = this.parseBlobId(ref.uri, context.tenantId);
		const res = await this.pool.query<{ data: Buffer; sha256: string }>(
			`SELECT data, sha256 FROM agent_blobs
			 WHERE tenant_id = $1 AND blob_id = $2 AND user_id = $3`,
			[context.tenantId, blobId, context.userId],
		);
		if (res.rows.length === 0) {
			throw new Error(`PostgresBlobStore blob not found: ${ref.uri}`);
		}
		const buf = res.rows[0].data;
		const bytes = new Uint8Array(buf.buffer, buf.byteOffset, buf.byteLength);
		const actualSha256 = sha256Hex(bytes);
		if (actualSha256 !== ref.sha256) {
			throw new Error(`PostgresBlobStore SHA-256 mismatch for ${ref.uri}: expected ${ref.sha256}, got ${actualSha256}`);
		}
		return bytes;
	}

	async delete(ref: BlobRef, context: BlobGetContext): Promise<void> {
		const blobId = this.parseBlobId(ref.uri, context.tenantId);
		await this.pool.query(
			`DELETE FROM agent_blobs WHERE tenant_id = $1 AND blob_id = $2 AND user_id = $3`,
			[context.tenantId, blobId, context.userId],
		);
	}

	private parseBlobId(uri: string, expectedTenantId: string): string {
		const prefix = "pg-blob://";
		if (!uri.startsWith(prefix)) {
			throw new Error(`Invalid pg-blob URI: ${uri}`);
		}
		const rest = uri.slice(prefix.length);
		const slashIndex = rest.indexOf("/");
		if (slashIndex <= 0) {
			throw new Error(`Malformed pg-blob URI: ${uri}`);
		}
		const tenantId = decodeURIComponent(rest.slice(0, slashIndex));
		if (tenantId !== expectedTenantId) {
			throw new Error(`Tenant mismatch on pg-blob access: expected ${expectedTenantId}, got ${tenantId}`);
		}
		return decodeURIComponent(rest.slice(slashIndex + 1));
	}
}
