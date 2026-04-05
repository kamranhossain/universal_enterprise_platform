-- Core extensions
CREATE EXTENSION IF NOT EXISTS timescaledb;
CREATE EXTENSION IF NOT EXISTS postgis;
CREATE EXTENSION IF NOT EXISTS postgis_topology;
CREATE EXTENSION IF NOT EXISTS "pg_trgm";
CREATE EXTENSION IF NOT EXISTS "ltree";
CREATE EXTENSION IF NOT EXISTS vector;          -- pgvector

-- PostgreSQL 18: uuidv7() built-in, no uuid-ossp needed

-- Verify critical extensions
DO $$
DECLARE
  missing text[] := ARRAY[]::text[];
  ext text;
BEGIN
  FOREACH ext IN ARRAY ARRAY['timescaledb','postgis','vector','ltree','pg_trgm']
  LOOP
    IF NOT EXISTS (
      SELECT 1 FROM pg_extension WHERE extname = ext
    ) THEN
      missing := missing || ext;
    END IF;
  END LOOP;

  IF array_length(missing, 1) > 0 THEN
    RAISE EXCEPTION 'Missing extensions: %. Re-run timescaledb-tune and check postgresql.conf', array_to_string(missing, ', ');
  END IF;
END;
$$;

-- Tenant RLS context function
CREATE OR REPLACE FUNCTION current_tenant_id()
RETURNS text LANGUAGE plpgsql STABLE AS $$
BEGIN
  RETURN current_setting('app.tenant_id', TRUE);
END;
$$;

-- RLS helper for future migrations
CREATE OR REPLACE FUNCTION enable_tenant_rls(table_name text)
RETURNS void LANGUAGE plpgsql AS $$
BEGIN
  EXECUTE format('ALTER TABLE %I ENABLE ROW LEVEL SECURITY', table_name);
  EXECUTE format('ALTER TABLE %I FORCE ROW LEVEL SECURITY',  table_name);
  EXECUTE format($$
    CREATE POLICY tenant_isolation ON %I
      USING (tenant_id::text = current_setting('app.tenant_id', TRUE))
  $$, table_name);
  EXECUTE format($$
    CREATE POLICY platform_admin_bypass ON %I
      USING (current_setting('app.bypass_rls', TRUE) = 'on')
  $$, table_name);
END;
$$;
