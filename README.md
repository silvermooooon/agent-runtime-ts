# agent-runtime-ts

Multi-tenant, multi-user, clustered durable Agent Runtime in TypeScript, built with minimal external dependencies (`openai` + `pg`) for intranet SaaS deployments.

## Module Architecture (Strict Unidirectional DAG)

- **Layer 0** (`packages/schema`, `packages/telemetry`): Zero-dependency JSON Schema validation/coercion and telemetry contracts.
- **Layer 1** (`packages/ai`, `packages/agent`): Streaming model adapters (OpenAI & Azure OpenAI) and in-memory agent loop.
- **Layer 2** (`packages/session`): Pure durable session state machine (`SessionIdentity { tenantId, userId, sessionId }`, sliding-window Reducer, `DurableLoop`, `BlobStore` / `BlobRouter`, compaction, Memory & JSONL stores).
- **Layer 3** (`packages/postgres`, `packages/blob-s3`): PostgreSQL 14+ multi-tenant durable storage (11 core tables + 1 optional `agent_blobs` table) and zero-dependency AWS SigV4 S3/MinIO blob & cold-archive adapter.
- **Layer 4** (`packages/cluster`): Multi-worker `WorkerHost`, fault-tolerant `LeaseSupervisor`, `DurableExecutor`, `DurableApprovalService`, and `DurableSubagentCoordinator`.

## Documentation

- Architecture & Complete PostgreSQL DDL: [`docs/durable-architecture-and-ddl.md`](docs/durable-architecture-and-ddl.md)
- Architecture Decision Records (ADR): [`docs/architecture-decisions.md`](docs/architecture-decisions.md)
- Standalone SQL DDL:
  - [`docs/sql/001_durable_core.sql`](docs/sql/001_durable_core.sql) (11 core tables)
  - [`docs/sql/002_optional_pg_blobs.sql`](docs/sql/002_optional_pg_blobs.sql) (optional DB blob store table)

## Development & Verification

```bash
npm install --ignore-scripts
npm run check   # TypeScript typecheck + module layering DAG verification
npm test        # Build dist/ and run node:test suite
```
