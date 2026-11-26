-- ============================================================================
-- Agent Runtime (TS) - Optional PostgreSQL Blob Store Extension Table
-- Create ONLY when selecting PostgresBlobStore (pg-blob://).
-- Not required when using S3BlobStore (s3://) or WorkspaceBlobStore (workspace://).
-- ============================================================================

CREATE TABLE agent_blobs (
    tenant_id text NOT NULL,
    blob_id text NOT NULL,
    user_id text NOT NULL,
    session_id text NOT NULL,
    sha256 text NOT NULL,
    mime_type text NOT NULL,
    byte_length bigint NOT NULL CHECK (byte_length >= 0),
    data bytea NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (tenant_id, blob_id)
);

CREATE INDEX agent_blobs_session_idx
    ON agent_blobs (tenant_id, session_id);

CREATE INDEX agent_blobs_user_created_idx
    ON agent_blobs (tenant_id, user_id, created_at DESC);
