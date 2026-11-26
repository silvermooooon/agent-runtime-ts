// Zero-dependency AWS Signature V4 signer using Node built-in `node:crypto` for S3 / MinIO / Ceph RGW.

import { createHash, createHmac } from "node:crypto";

export interface S3ConnectionConfig {
	/** Base endpoint URL, e.g. "https://minio.internal:9000" or "https://s3.us-east-1.amazonaws.com". */
	readonly endpoint: string;
	readonly region: string;
	readonly bucket: string;
	readonly accessKeyId: string;
	readonly secretAccessKey: string;
	readonly sessionToken?: string;
	/** Default `true` for intranet S3-compatible stores (MinIO / Ceph); set `false` for virtual-hosted style. */
	readonly forcePathStyle?: boolean;
	readonly keyPrefix?: string;
	readonly fetch?: typeof globalThis.fetch;
}

export interface SignedS3Request {
	readonly url: string;
	readonly headers: Record<string, string>;
}

/** Build and sign an S3 HTTP request (`PUT`, `GET`, `DELETE`, `HEAD`) with AWS Signature V4. */
export function signS3Request(params: {
	readonly config: S3ConnectionConfig;
	readonly method: "GET" | "PUT" | "DELETE" | "HEAD";
	readonly objectKey: string;
	readonly payloadSha256Hex: string;
	readonly extraHeaders?: Readonly<Record<string, string>>;
	readonly now?: Date;
}): SignedS3Request {
	const { config, method, objectKey, payloadSha256Hex } = params;
	const now = params.now ?? new Date();
	const amzDate = formatAmzDate(now);
	const dateStamp = amzDate.slice(0, 8);

	const base = new URL(config.endpoint);
	const forcePathStyle = config.forcePathStyle ?? true;
	const encodedKey = objectKey
		.split("/")
		.filter((seg) => seg.length > 0)
		.map(encodeRfc3986)
		.join("/");

	let host: string;
	let canonicalUri: string;
	let targetUrl: string;

	if (forcePathStyle) {
		host = base.host;
		const basePath = base.pathname.replace(/\/+$/, "");
		canonicalUri = `${basePath}/${encodeRfc3986(config.bucket)}/${encodedKey}`;
		targetUrl = `${base.protocol}//${host}${canonicalUri}`;
	} else {
		host = `${config.bucket}.${base.host}`;
		const basePath = base.pathname.replace(/\/+$/, "");
		canonicalUri = `${basePath}/${encodedKey}`;
		targetUrl = `${base.protocol}//${host}${canonicalUri}`;
	}

	const rawHeaders: Record<string, string> = {
		host,
		"x-amz-content-sha256": payloadSha256Hex,
		"x-amz-date": amzDate,
		...(config.sessionToken ? { "x-amz-security-token": config.sessionToken } : {}),
	};

	if (params.extraHeaders) {
		for (const [k, v] of Object.entries(params.extraHeaders)) {
			rawHeaders[k.toLowerCase()] = v.trim();
		}
	}

	const sortedHeaderNames = Object.keys(rawHeaders).sort();
	const canonicalHeaders = sortedHeaderNames.map((name) => `${name}:${rawHeaders[name]}\n`).join("");
	const signedHeaders = sortedHeaderNames.join(";");

	const canonicalRequest = [
		method,
		canonicalUri,
		"", // Empty query string
		canonicalHeaders,
		signedHeaders,
		payloadSha256Hex,
	].join("\n");

	const credentialScope = `${dateStamp}/${config.region}/s3/aws4_request`;
	const stringToSign = [
		"AWS4-HMAC-SHA256",
		amzDate,
		credentialScope,
		sha256Hex(canonicalRequest),
	].join("\n");

	const signingKey = deriveSigningKey(config.secretAccessKey, dateStamp, config.region, "s3");
	const signature = createHmac("sha256", signingKey).update(stringToSign, "utf8").digest("hex");

	const authorization =
		`AWS4-HMAC-SHA256 Credential=${config.accessKeyId}/${credentialScope}, ` +
		`SignedHeaders=${signedHeaders}, Signature=${signature}`;

	const requestHeaders: Record<string, string> = {};
	for (const [k, v] of Object.entries(rawHeaders)) {
		if (k !== "host") {
			requestHeaders[k] = v;
		}
	}
	requestHeaders.authorization = authorization;

	return { url: targetUrl, headers: requestHeaders };
}

function sha256Hex(data: string | Uint8Array): string {
	return createHash("sha256").update(data).digest("hex");
}

function hmacSha256(key: string | Buffer, data: string): Buffer {
	return createHmac("sha256", key).update(data, "utf8").digest();
}

function deriveSigningKey(secretAccessKey: string, dateStamp: string, region: string, service: string): Buffer {
	const kDate = hmacSha256(`AWS4${secretAccessKey}`, dateStamp);
	const kRegion = hmacSha256(kDate, region);
	const kService = hmacSha256(kRegion, service);
	return hmacSha256(kService, "aws4_request");
}

function formatAmzDate(date: Date): string {
	return date.toISOString().replace(/[:-]|\.\d{3}/g, "");
}

function encodeRfc3986(str: string): string {
	return encodeURIComponent(str).replace(/[!'()*]/g, (c) => `%${c.charCodeAt(0).toString(16).toUpperCase()}`);
}
