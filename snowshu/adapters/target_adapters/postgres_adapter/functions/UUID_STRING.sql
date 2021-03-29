/* emulates snowflake's uuid_string() function for v4 UUIDs, v5 unsupported */
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

CREATE OR REPLACE FUNCTION UUID_STRING()
RETURNS TEXT AS
$$
SELECT
	uuid_generate_v4()::text;
$$
LANGUAGE SQL;