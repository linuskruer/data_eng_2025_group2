-- Project 3: ClickHouse roles, users, and grants for full vs. limited access.

-- Schemas are created in 01_ and 02_ scripts.

CREATE ROLE IF NOT EXISTS analyst_full;
CREATE ROLE IF NOT EXISTS analyst_limited;

-- Full analysts can query every serving view (including gold tables if desired).
GRANT SELECT ON serving_views_full.* TO analyst_full;
GRANT SELECT ON default.* TO analyst_full;

-- Limited analysts can query masked views
-- Note: ClickHouse requires access to underlying tables/views that the masked views query from
-- The masking happens at the view level - limited users get masked data when querying masked views
GRANT SELECT ON serving_views_masked.* TO analyst_limited;
GRANT SELECT ON serving_views_full.* TO analyst_limited;  -- Required because masked views read from full views
GRANT SELECT ON default.* TO analyst_limited;  -- Required because views read from default schema tables

-- Example users for demos (update passwords in credentials store before production use).
CREATE USER IF NOT EXISTS analyst_full_user IDENTIFIED WITH plaintext_password BY 'Project3Full!';
CREATE USER IF NOT EXISTS analyst_limited_user IDENTIFIED WITH plaintext_password BY 'Project3Limited!';

GRANT analyst_full TO analyst_full_user;
GRANT analyst_limited TO analyst_limited_user;

-- Optional: revoke default role to force explicit assignment
ALTER USER analyst_full_user DEFAULT ROLE analyst_full;
ALTER USER analyst_limited_user DEFAULT ROLE analyst_limited;

-- Helper queries to validate access
-- SHOW GRANTS FOR analyst_full_user;
-- SHOW GRANTS FOR analyst_limited_user;

