// S3 / MinIO implementation of `BlobStore` (`s3://` URI scheme) requiring zero database tables.

import {
	type BlobGetContext,
	type BlobPutContext,
	type BlobRef,
	type BlobStore,
	sha256Hex,
} from "../../session/src/index.ts";
import { type S3ConnectionConfig, signS3Request } from "./sigv4.ts";

const EMPTY_PAYLOAD_SHA256 = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";

export class S3BlobStore implements BlobStore {
	readonly scheme = "s3";
	private readonly config: S3ConnectionConfig;
	private readonly fetchFn: typeof globalThis.fetch;

	constructor(config: S3ConnectionConfig) {
		this.config = config;
		this.fetchFn = config.fetch ?? globalThis.fetch;
	}

	canResolve(uri: string): boolean {
		return uri.startsWith(`s3://${this.config.bucket}/`);
	}

	async put(data: Uint8Array, context: BlobPutContext): Promise<{ readonly uri: string }> {
		const prefix = this.config.keyPrefix ? `${this.config.keyPrefix.replace(/^\/+|\/+$/g, "")}/` : "";
		const objectKey =
			`${prefix}blobs/${encodeURIComponent(context.tenantId)}/` +
			`${encodeURIComponent(context.userId)}/${encodeURIComponent(context.sessionId)}/${context.sha256}`;

		const signed = signS3Request({
			config: this.config,
			method: "PUT",
			objectKey,
			payloadSha256Hex: context.sha256,
			extraHeaders: {
				"content-type": context.mimeType,
				"content-length": String(context.byteLength),
			},
		});

		const bodyBuffer = Buffer.from(data.buffer, data.byteOffset, data.byteLength);
		const response = await this.fetchFn(signed.url, {
			method: "PUT",
			headers: signed.headers,
			body: bodyBuffer,
			signal: context.signal,
		});

		if (!response.ok) {
			const detail = await response.text().catch(() => "");
			throw new Error(`S3BlobStore PUT failed (HTTP ${String(response.status)}): ${detail}`);
		}

		return { uri: `s3://${this.config.bucket}/${objectKey}` };
	}

	async get(ref: BlobRef, context: BlobGetContext): Promise<Uint8Array> {
		const objectKey = this.extractObjectKey(ref.uri);
		const signed = signS3Request({
			config: this.config,
			method: "GET",
			objectKey,
			payloadSha256Hex: EMPTY_PAYLOAD_SHA256,
		});

		const response = await this.fetchFn(signed.url, {
			method: "GET",
			headers: signed.headers,
			signal: context.signal,
		});

		if (!response.ok) {
			throw new Error(`S3BlobStore GET failed for ${ref.uri} (HTTP ${String(response.status)})`);
		}

		const bytes = new Uint8Array(await response.arrayBuffer());
		const actualSha256 = sha256Hex(bytes);
		if (actualSha256 !== ref.sha256) {
			throw new Error(`S3BlobStore integrity error for ${ref.uri}: expected ${ref.sha256}, got ${actualSha256}`);
		}
		return bytes;
	}

	async delete(ref: BlobRef, context: BlobGetContext): Promise<void> {
		const objectKey = this.extractObjectKey(ref.uri);
		const signed = signS3Request({
			config: this.config,
			method: "DELETE",
			objectKey,
			payloadSha256Hex: EMPTY_PAYLOAD_SHA256,
		});

		const response = await this.fetchFn(signed.url, {
			method: "DELETE",
			headers: signed.headers,
			signal: context.signal,
		});

		if (!response.ok && response.status !== 404) {
			throw new Error(`S3BlobStore DELETE failed for ${ref.uri} (HTTP ${String(response.status)})`);
		}
	}

	private extractObjectKey(uri: string): string {
		const expectedPrefix = `s3://${this.config.bucket}/`;
		if (!uri.startsWith(expectedPrefix)) {
			throw new Error(`S3 URI ${uri} does not belong to configured bucket ${this.config.bucket}`);
		}
		return uri.slice(expectedPrefix.length);
	}
}
