-- ============================================================================
-- Agent Runtime (TS) - Multi-Tenant & Multi-User Core Durable Schema (PG 14+)
-- Total Core Tables: 11
-- Note: DDL lifecycle is managed by internal database tooling. Runtime never
--       executes DDL automatically.
-- ============================================================================

-- ----------------------------------------------------------------------------
-- 1. Session Core & Cold/Hot Journal Domain (4 Core Tables)
-- ----------------------------------------------------------------------------

CREATE TABLE agent_sessions (
    tenant_id text NOT NULL,
    session_id text NOT NULL,
    user_id text NOT NULL,
    root_session_id text NOT NULL,
    parent_session_id text,
    parent_event_id text,
    origin_kind text NOT NULL DEFAULT 'root'
        CHECK (origin_kind IN ('root', 'subagent', 'fork')),
    depth smallint NOT NULL DEFAULT 0 CHECK (depth >= 0),
    provider_session_id text NOT NULL,
    status text NOT NULL DEFAULT 'idle'
        CHECK (status IN ('idle', 'running', 'waiting_approval', 'archived')),
    active_run_id text,
    active_config_hash text,
    next_seq bigint NOT NULL DEFAULT 0 CHECK (next_seq >= 0),
    context_start_seq bigint NOT NULL DEFAULT 0
        CHECK (context_start_seq >= 0 AND context_start_seq <= next_seq),
    compaction_seq bigint
        CHECK (compaction_seq IS NULL OR (compaction_seq >= context_start_seq AND compaction_seq < next_seq)),
    archived_up_to_seq bigint NOT NULL DEFAULT -1
        CHECK (archived_up_to_seq >= -1 AND archived_up_to_seq < next_seq),
    metadata jsonb NOT NULL DEFAULT '{}',
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (tenant_id, session_id)
);

CREATE INDEX agent_sessions_user_created_idx
    ON agent_sessions (tenant_id, user_id, created_at DESC, session_id DESC);

CREATE INDEX agent_sessions_tenant_updated_idx
    ON agent_sessions (tenant_id, updated_at ASC)
    WHERE status <> 'archived';

CREATE INDEX agent_sessions_root_idx
    ON agent_sessions (tenant_id, root_session_id);

CREATE TABLE agent_session_configs (
    tenant_id text NOT NULL,
    session_id text NOT NULL,
    user_id text NOT NULL,
    config_hash text NOT NULL,
    model_spec jsonb NOT NULL,
    tools_schema json NOT NULL,
    options jsonb NOT NULL DEFAULT '{}',
    required_hooks text[] NOT NULL DEFAULT '{}',
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (tenant_id, session_id, config_hash),
    FOREIGN KEY (tenant_id, session_id) REFERENCES agent_sessions ON DELETE CASCADE
);

CREATE TABLE agent_session_events (
    tenant_id text NOT NULL,
    session_id text NOT NULL,
    user_id text NOT NULL,
    seq bigint NOT NULL CHECK (seq >= 0),
    event_id text NOT NULL,
    run_id text,
    event_type text NOT NULL,
    fence bigint CHECK (fence IS NULL OR fence >= 1),
    record json NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (tenant_id, session_id, seq),
    UNIQUE (tenant_id, session_id, event_id),
    FOREIGN KEY (tenant_id, session_id) REFERENCES agent_sessions ON DELETE CASCADE
);

CREATE INDEX agent_session_events_run_seq_idx
    ON agent_session_events (tenant_id, session_id, run_id, seq)
    WHERE run_id IS NOT NULL;

CREATE INDEX agent_session_events_compaction_idx
    ON agent_session_events (tenant_id, session_id, seq DESC)
    WHERE event_type = 'compaction';

CREATE TABLE agent_session_archives (
    tenant_id text NOT NULL,
    session_id text NOT NULL,
    user_id text NOT NULL,
    start_seq bigint NOT NULL CHECK (start_seq >= 0),
    end_seq bigint NOT NULL CHECK (end_seq >= start_seq),
    object_uri text NOT NULL,
    version_id text,
    sha256 text NOT NULL,
    format_version smallint NOT NULL DEFAULT 1 CHECK (format_version = 1),
    run_ids text[] NOT NULL DEFAULT '{}',
    checkpoint_seqs bigint[] NOT NULL DEFAULT '{}',
    metadata jsonb NOT NULL DEFAULT '{}',
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (tenant_id, session_id, start_seq),
    FOREIGN KEY (tenant_id, session_id) REFERENCES agent_sessions ON DELETE CASCADE
);

CREATE INDEX agent_session_archives_user_created_idx
    ON agent_session_archives (tenant_id, user_id, created_at DESC);

-- ----------------------------------------------------------------------------
-- 2. Cluster Coordination, Command Queue & Approval Control Domain (4 Tables)
-- ----------------------------------------------------------------------------

CREATE TABLE agent_session_leases (
    tenant_id text NOT NULL,
    session_id text NOT NULL,
    user_id text NOT NULL,
    holder text NOT NULL,
    fence bigint NOT NULL CHECK (fence >= 1),
    acquired_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    renewed_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    expires_at timestamptz NOT NULL,
    PRIMARY KEY (tenant_id, session_id),
    FOREIGN KEY (tenant_id, session_id) REFERENCES agent_sessions ON DELETE CASCADE
);

CREATE INDEX agent_session_leases_expires_idx
    ON agent_session_leases (expires_at ASC);

CREATE INDEX agent_session_leases_user_idx
    ON agent_session_leases (tenant_id, user_id);

CREATE TABLE agent_commands (
    tenant_id text NOT NULL,
    command_id text NOT NULL,
    session_id text NOT NULL,
    user_id text NOT NULL,
    command_seq bigint NOT NULL CHECK (command_seq >= 1),
    kind text NOT NULL CHECK (kind IN ('prompt', 'steer', 'follow_up', 'continuation')),
    payload json NOT NULL,
    payload_hash text NOT NULL,
    run_id text,
    is_child boolean NOT NULL DEFAULT false,
    state text NOT NULL DEFAULT 'queued'
        CHECK (state IN ('queued', 'running', 'paused', 'completed', 'cancelled', 'failed', 'dead_letter')),
    recovery_attempts integer NOT NULL DEFAULT 0 CHECK (recovery_attempts >= 0),
    max_recovery_attempts integer NOT NULL DEFAULT 3 CHECK (max_recovery_attempts >= 1),
    cancel_requested_at timestamptz,
    cancel_reason text,
    last_error text,
    accepted_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    started_at timestamptz,
    finished_at timestamptz,
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (tenant_id, command_id),
    UNIQUE (tenant_id, session_id, command_seq),
    UNIQUE (tenant_id, session_id, run_id),
    FOREIGN KEY (tenant_id, session_id) REFERENCES agent_sessions ON DELETE CASCADE
);

CREATE INDEX agent_commands_session_fifo_idx
    ON agent_commands (tenant_id, session_id, command_seq ASC)
    WHERE state IN ('queued', 'running', 'paused');

CREATE INDEX agent_commands_claim_queued_idx
    ON agent_commands (is_child, tenant_id, accepted_at ASC, command_id ASC)
    WHERE state = 'queued' AND kind IN ('prompt', 'continuation');

CREATE INDEX agent_commands_user_queued_idx
    ON agent_commands (tenant_id, user_id, accepted_at ASC)
    WHERE state = 'queued';

CREATE INDEX agent_commands_recovery_running_idx
    ON agent_commands (is_child, updated_at ASC, tenant_id, command_id)
    WHERE state = 'running';

CREATE TABLE agent_approvals (
    tenant_id text NOT NULL,
    approval_id text NOT NULL,
    session_id text NOT NULL,
    user_id text NOT NULL,
    run_id text NOT NULL,
    operation_id text NOT NULL,
    call_id text NOT NULL,
    approver_id text NOT NULL,
    tool_name text NOT NULL,
    action_payload json NOT NULL,
    action_hash text NOT NULL,
    context jsonb NOT NULL DEFAULT '{}',
    status text NOT NULL DEFAULT 'pending'
        CHECK (status IN ('pending', 'approved', 'rejected', 'expired', 'consumed')),
    decided_by text,
    decision_reason text,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    expires_at timestamptz NOT NULL,
    decided_at timestamptz,
    consumed_at timestamptz,
    PRIMARY KEY (tenant_id, approval_id),
    UNIQUE (tenant_id, operation_id),
    FOREIGN KEY (tenant_id, session_id) REFERENCES agent_sessions ON DELETE CASCADE
);

CREATE INDEX agent_approvals_pending_approver_idx
    ON agent_approvals (tenant_id, approver_id, created_at DESC)
    WHERE status = 'pending';

CREATE INDEX agent_approvals_user_created_idx
    ON agent_approvals (tenant_id, user_id, created_at DESC);

CREATE INDEX agent_approvals_pending_expiry_idx
    ON agent_approvals (expires_at ASC)
    WHERE status = 'pending';

CREATE TABLE agent_outbox (
    message_seq bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    tenant_id text NOT NULL,
    user_id text NOT NULL,
    message_id text NOT NULL,
    kind text NOT NULL CHECK (kind IN ('child_result', 'approval_decided', 'external_signal')),
    target_session_id text NOT NULL,
    source_session_id text,
    source_run_id text,
    payload jsonb NOT NULL,
    state text NOT NULL DEFAULT 'pending'
        CHECK (state IN ('pending', 'delivered', 'dead_letter')),
    attempts integer NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    last_error text,
    available_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    delivered_at timestamptz,
    UNIQUE (tenant_id, message_id)
);

CREATE INDEX agent_outbox_pending_dispatch_idx
    ON agent_outbox (available_at ASC, message_seq ASC)
    WHERE state = 'pending';

CREATE INDEX agent_outbox_target_session_idx
    ON agent_outbox (tenant_id, target_session_id, message_seq ASC)
    WHERE state = 'pending';

-- ----------------------------------------------------------------------------
-- 3. Multi-Tenant / Multi-User Quotas & Billing Accounting Domain (3 Tables)
-- ----------------------------------------------------------------------------

CREATE TABLE agent_quotas (
    tenant_id text NOT NULL,
    scope_type text NOT NULL CHECK (scope_type IN ('tenant', 'user')),
    user_id text NOT NULL DEFAULT '',
    max_active_root_runs integer NOT NULL CHECK (max_active_root_runs >= 0),
    max_active_child_runs integer NOT NULL CHECK (max_active_child_runs >= 0),
    max_queued_commands integer NOT NULL CHECK (max_queued_commands >= 0),
    per_user_max_active_root_runs integer
        CHECK (per_user_max_active_root_runs IS NULL OR per_user_max_active_root_runs >= 0),
    per_user_max_active_child_runs integer
        CHECK (per_user_max_active_child_runs IS NULL OR per_user_max_active_child_runs >= 0),
    per_user_max_queued_commands integer
        CHECK (per_user_max_queued_commands IS NULL OR per_user_max_queued_commands >= 0),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (tenant_id, scope_type, user_id),
    CHECK (
        (scope_type = 'tenant' AND user_id = '')
        OR (
            scope_type = 'user'
            AND user_id <> ''
            AND per_user_max_active_root_runs IS NULL
            AND per_user_max_active_child_runs IS NULL
            AND per_user_max_queued_commands IS NULL
        )
    )
);

CREATE TABLE agent_runs (
    tenant_id text NOT NULL,
    session_id text NOT NULL,
    run_id text NOT NULL,
    command_id text,
    user_id text NOT NULL,
    root_run_id text NOT NULL,
    parent_session_id text,
    parent_run_id text,
    depth smallint NOT NULL DEFAULT 0 CHECK (depth >= 0),
    source text NOT NULL CHECK (source IN ('user', 'subagent', 'system')),
    status text NOT NULL
        CHECK (status IN ('running', 'waiting_approval', 'completed', 'cancelled', 'failed')),
    recovery_attempts integer NOT NULL DEFAULT 0 CHECK (recovery_attempts >= 0),
    started_at timestamptz NOT NULL,
    finished_at timestamptz,
    metadata jsonb NOT NULL DEFAULT '{}',
    PRIMARY KEY (tenant_id, session_id, run_id)
);

CREATE INDEX agent_runs_tenant_active_idx
    ON agent_runs (tenant_id, source)
    WHERE status = 'running';

CREATE INDEX agent_runs_user_active_idx
    ON agent_runs (tenant_id, user_id, source)
    WHERE status = 'running';

CREATE INDEX agent_runs_user_started_idx
    ON agent_runs (tenant_id, user_id, started_at DESC, session_id DESC, run_id DESC);

CREATE INDEX agent_runs_user_finished_idx
    ON agent_runs (tenant_id, user_id, finished_at DESC)
    WHERE finished_at IS NOT NULL;

CREATE INDEX agent_runs_root_idx
    ON agent_runs (tenant_id, root_run_id, started_at ASC);

CREATE TABLE agent_model_usage (
    tenant_id text NOT NULL,
    attempt_id text NOT NULL,
    session_id text NOT NULL,
    run_id text,
    root_run_id text,
    user_id text NOT NULL,
    provider text NOT NULL,
    model text NOT NULL,
    api text NOT NULL,
    purpose text NOT NULL CHECK (purpose IN ('generation', 'compaction', 'memory_dream', 'classifier')),
    status text NOT NULL CHECK (status IN ('started', 'completed', 'aborted', 'failed')),
    usage_status text NOT NULL CHECK (usage_status IN ('known', 'unknown')),
    input_tokens bigint CHECK (input_tokens IS NULL OR input_tokens >= 0),
    output_tokens bigint CHECK (output_tokens IS NULL OR output_tokens >= 0),
    cache_read_tokens bigint CHECK (cache_read_tokens IS NULL OR cache_read_tokens >= 0),
    cache_write_tokens bigint CHECK (cache_write_tokens IS NULL OR cache_write_tokens >= 0),
    total_tokens bigint CHECK (total_tokens IS NULL OR total_tokens >= 0),
    provider_response_id text,
    started_at timestamptz NOT NULL,
    finished_at timestamptz,
    accounting_at timestamptz NOT NULL,
    metadata jsonb NOT NULL DEFAULT '{}',
    PRIMARY KEY (tenant_id, attempt_id)
);

CREATE INDEX agent_model_usage_user_accounting_idx
    ON agent_model_usage (tenant_id, user_id, accounting_at DESC);

CREATE INDEX agent_model_usage_tenant_accounting_idx
    ON agent_model_usage (tenant_id, accounting_at DESC);

CREATE INDEX agent_model_usage_root_idx
    ON agent_model_usage (tenant_id, root_run_id)
    WHERE root_run_id IS NOT NULL;
