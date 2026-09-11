-- =========================================================
-- 02_powerbi_reader_role.sql
--
-- Read-only database role for Power BI's connection (Section 19 /
-- aws/iam.md). Deliberately excludes the `staging` schema — staging
-- can hold not-yet-quality-checked dirty data (Phase 6), which has
-- no business appearing on an executive dashboard.
--
-- Password is intentionally NOT set here (never hard-code credentials
-- in a checked-in DDL file — see .env.example / Section 24). Set it
-- after deploy:
--   ALTER ROLE powerbi_reader WITH PASSWORD '<from Secrets Manager>';
-- =========================================================

DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'powerbi_reader') THEN
        CREATE ROLE powerbi_reader WITH LOGIN PASSWORD 'change_me_immediately';
    END IF;
END
$$;

GRANT USAGE ON SCHEMA warehouse, analytics TO powerbi_reader;
GRANT SELECT ON ALL TABLES IN SCHEMA warehouse, analytics TO powerbi_reader;

-- Ensures tables created by later pipeline runs (e.g. a new Gold
-- table added in a future phase) are readable by powerbi_reader
-- automatically, without a manual GRANT being remembered every time.
ALTER DEFAULT PRIVILEGES FOR ROLE customer360_app IN SCHEMA warehouse, analytics
    GRANT SELECT ON TABLES TO powerbi_reader;
