// S3 / MinIO implementation of `ArchiveObjectStore` with mandatory read-back SHA-256 verification.

import {
	type ArchiveObjectStore,
	type ArchiveWriteInput,
	type ArchiveWriteReceipt,
	decodeArchiveEntries,
	encodeArchiveEntries,
	type JournalEntry,
	MemoryArchiveObjectStore,
} from "../../session/src/index.ts";
import { type S3ConnectionConfig, signS3Request } from "./sigv4.ts";

export {
	type ArchiveObjectStore,
	type ArchiveWriteInput,
	type ArchiveWriteReceipt,
	decodeArchiveEntries,
	encodeArchiveEntries,
	MemoryArchiveObjectStore,
};

const EMPTY_PAYLOAD_SHA256 = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";

export class S3ArchiveObjectStore implements ArchiveObjectStore {
	private readonly config: S3ConnectionConfig;
	private readonly fetchFn: typeof globalThis.fetch;

	constructor(config: S3ConnectionConfig) {
		this.config = config;
		this.fetchFn = config.fetch ?? globalThis.fetch;
	}

	async writeArchiveChunk(input: ArchiveWriteInput): Promise<ArchiveWriteReceipt> {
		const { gzBytes, sha256, uncompressedBytes } = encodeArchiveEntries(input.entries);
		const prefix = this.config.keyPrefix ? `${this.config.keyPrefix.replace(/^\/+|\/+$/g, "")}/` : "";
		const objectKey =
			`${prefix}archives/${encodeURIComponent(input.tenantId)}/` +
			`${encodeURIComponent(input.userId)}/${encodeURIComponent(input.sessionId)}/` +
			`${String(input.startSeq).padStart(12, "0")}-${String(input.endSeq).padStart(12, "0")}.jsonl.gz`;

		const putSigned = signS3Request({
			config: this.config,
			method: "PUT",
			objectKey,
			payloadSha256Hex: sha256,
			extraHeaders: {
				"content-type": "application/gzip",
				"content-length": String(gzBytes.byteLength),
			},
		});

		const putRes = await this.fetchFn(putSigned.url, {
			method: "PUT",
			headers: putSigned.headers,
			body: Buffer.from(gzBytes.buffer, gzBytes.byteOffset, gzBytes.byteLength),
			signal: input.signal,
		});
		if (!putRes.ok) {
			throw new Error(`S3ArchiveObjectStore PUT failed (HTTP ${String(putRes.status)})`);
		}
		const versionId = putRes.headers.get("x-amz-version-id");
		const objectUri = `s3://${this.config.bucket}/${objectKey}`;

		// Read back and verify SHA-256 before returning receipt so hot PG rows are never deleted on corrupt uploads.
		await this.readArchiveChunk({ objectUri, expectedSha256: sha256, signal: input.signal });

		return {
			objectUri,
			versionId,
			sha256,
			compressedBytes: gzBytes.byteLength,
			uncompressedBytes,
		};
	}

	async readArchiveChunk(params: {
		readonly objectUri: string;
		readonly expectedSha256: string;
		readonly signal?: AbortSignal;
	}): Promise<readonly JournalEntry[]> {
		const expectedPrefix = `s3://${this.config.bucket}/`;
		if (!params.objectUri.startsWith(expectedPrefix)) {
			throw new Error(`Archive URI ${params.objectUri} does not match bucket ${this.config.bucket}`);
		}
		const objectKey = params.objectUri.slice(expectedPrefix.length);
		const getSigned = signS3Request({
			config: this.config,
			method: "GET",
			objectKey,
			payloadSha256Hex: EMPTY_PAYLOAD_SHA256,
		});
		const res = await this.fetchFn(getSigned.url, {
			method: "GET",
			headers: getSigned.headers,
			signal: params.signal,
		});
		if (!res.ok) {
			throw new Error(`S3ArchiveObjectStore GET failed for ${params.objectUri} (HTTP ${String(res.status)})`);
		}
		const bytes = new Uint8Array(await res.arrayBuffer());
		return decodeArchiveEntries(bytes, params.expectedSha256);
	}
}
