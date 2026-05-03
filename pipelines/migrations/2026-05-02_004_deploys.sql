-- 2026-05-02_004_deploys
--
-- Deploy log written by the `studio` CLI on every deploy. Captures who,
-- when, what changed (git diff stat), what migrations applied, and whether
-- smoke tests passed. Forever queryable: "what version of the pipeline was
-- running on Studio between dates X and Y?"
--
-- The April outage was difficult to scope partly because we had no record
-- of what code was on Studio at any given point. This closes that gap.
--
-- Apply via `pipelines/migrations/migrate.py`.

BEGIN;

CREATE TABLE IF NOT EXISTS deploys (
    deploy_id              uuid PRIMARY KEY,
    project                text NOT NULL,                  -- 'form4' | 'pm' | 'tailorly' | ...
    env                    text NOT NULL DEFAULT 'production',
    git_sha_before         text,
    git_sha_after          text NOT NULL,
    deployer_user          text NOT NULL,
    deployer_host          text NOT NULL,
    deploy_started         timestamptz NOT NULL,
    deploy_finished        timestamptz,
    smoke_test_result      text,                            -- 'pass' | 'fail' | 'skipped'
    smoke_test_output      text,
    files_changed_json     jsonb,                           -- summary from `git diff --stat`
    migrations_applied     text[],                          -- list of migration versions
    rollback_target_sha    text,                            -- if this is a rollback, the sha being rolled to
    notes                  text
);

CREATE INDEX IF NOT EXISTS idx_deploys_project_started
    ON deploys (project, deploy_started DESC);

CREATE INDEX IF NOT EXISTS idx_deploys_finished
    ON deploys (deploy_finished DESC) WHERE deploy_finished IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_deploys_in_progress
    ON deploys (project, deploy_started DESC) WHERE deploy_finished IS NULL;

COMMENT ON TABLE deploys IS
    'Deploy provenance log. Every studio CLI deploy writes a row here.';

COMMIT;
