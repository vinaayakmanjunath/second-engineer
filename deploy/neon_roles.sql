-- Run once as the database owner (Neon: SQL editor, or psql). Safe to re-run.
-- Then set both passwords yourself, never in the repo:
--   alter role loader password '...';      -- GitHub Actions publishes gold with this role
--   alter role api_reader password '...';  -- the API on Render reads with this role
do $$
begin
    if not exists (select from pg_roles where rolname = 'loader') then
        create role loader login;
    end if;
    if not exists (select from pg_roles where rolname = 'api_reader') then
        create role api_reader login;
    end if;
    -- the loader creates and swaps the serving schemas; it owns nothing else
    execute format('grant create on database %I to loader', current_database());
end
$$;
-- the API can only read, and no query can run away with the database
alter role api_reader set default_transaction_read_only = on;
alter role api_reader set statement_timeout = '5s';
