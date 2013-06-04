create extension postgis;

CREATE OR REPLACE FUNCTION 
to32time(time24 text, shift24 integer) RETURNS text AS $$
SELECT lpad(floor((total / 3600))::text, 2, '0')||':'||lpad(((total % 3600) / 60)::text, 2, '0')||':'||lpad((total % 60)::text, 2, '0') AS time
FROM
(SELECT
  (cast(split_part($1, ':', 1) as int4) * 3600)      -- hours
+ (cast(split_part($1, ':', 2) as int4) * 60)        -- minutes
+ CASE WHEN $1 similar to '%:%:%' THEN (cast(split_part($1, ':', 3) as int4)) ELSE 0 END -- seconds when applicable
+ (shift24 * 86400) as total --Add 24 hours (in seconds) when shift occured
) as xtotal
$$ LANGUAGE SQL;

CREATE OR REPLACE FUNCTION 
toseconds(time24 text, shift24 integer) RETURNS integer AS $$
SELECT total AS time
FROM
(SELECT
  (cast(split_part($1, ':', 1) as int4) * 3600)      -- hours
+ (cast(split_part($1, ':', 2) as int4) * 60)        -- minutes
+ CASE WHEN $1 similar to '%:%:%' THEN (cast(split_part($1, ':', 3) as int4)) ELSE 0 END -- seconds when applicable
+ (shift24 * 86400) as total --Add 24 hours (in seconds) when shift occured
) as xtotal
$$ LANGUAGE SQL;

