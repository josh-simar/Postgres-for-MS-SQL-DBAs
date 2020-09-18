CREATE OR REPLACE FUNCTION public.pg_whoisactive(show_own_spid boolean DEFAULT false, show_system_spids boolean DEFAULT false, show_sleeping_spids integer DEFAULT 1, OUT "dd hh:mm:ss.msssss" character varying, OUT session_id integer, OUT sql_text text, OUT parallelism_cnt bigint, OUT login_name name, OUT wait_info text, OUT blocking_session_id integer, OUT status text, OUT host_ip inet, OUT database_name name, OUT program_name text, OUT start_time timestamp with time zone, OUT login_time timestamp with time zone, OUT collection_time timestamp with time zone)
 RETURNS SETOF record
 LANGUAGE plpgsql
AS $function$
BEGIN

RETURN QUERY
WITH blocking AS (
SELECT
	blocked_locks.pid AS blocked_pid, blocked_activity.usename AS blocked_user, blocking_locks.pid AS blocking_pid, blocking_activity.usename AS blocking_user, blocked_activity.query AS blocked_statement, blocking_activity.query AS current_statement_in_blocking_process
FROM
	pg_catalog.pg_locks blocked_locks
JOIN pg_catalog.pg_stat_activity blocked_activity ON
	blocked_activity.pid = blocked_locks.pid
JOIN pg_catalog.pg_locks blocking_locks ON
	blocking_locks.locktype = blocked_locks.locktype
	AND blocking_locks.DATABASE IS NOT DISTINCT
FROM
	blocked_locks.DATABASE
	AND blocking_locks.relation IS NOT DISTINCT
FROM
	blocked_locks.relation
	AND blocking_locks.page IS NOT DISTINCT
FROM
	blocked_locks.page
	AND blocking_locks.tuple IS NOT DISTINCT
FROM
	blocked_locks.tuple
	AND blocking_locks.virtualxid IS NOT DISTINCT
FROM
	blocked_locks.virtualxid
	AND blocking_locks.transactionid IS NOT DISTINCT
FROM
	blocked_locks.transactionid
	AND blocking_locks.classid IS NOT DISTINCT
FROM
	blocked_locks.classid
	AND blocking_locks.objid IS NOT DISTINCT
FROM
	blocked_locks.objid
	AND blocking_locks.objsubid IS NOT DISTINCT
FROM
	blocked_locks.objsubid
	AND blocking_locks.pid != blocked_locks.pid
JOIN pg_catalog.pg_stat_activity blocking_activity ON
	blocking_activity.pid = blocking_locks.pid
WHERE
	NOT blocked_locks.GRANTED) ,
blocking2 AS (
SELECT
	DISTINCT blocked_pid, blocking_pid
FROM
	blocking)
, parallelism AS (SELECT datid, usesysid, backend_xmin, COUNT(*) AS parallelism_cnt FROM pg_stat_activity WHERE datid IS NOT NULL AND usesysid  IS NOT NULL AND backend_xmin IS NOT NULL GROUP BY datid, usesysid, backend_xmin HAVING COUNT(*) > 1)
SELECT
	REPLACE(CAST(AGE(NOW(), state_change) AS VARCHAR), 'day', '')::VARCHAR AS "dd hh:mm:ss.msssss",
	pid AS session_id,
	query AS sql_text,
	p.parallelism_cnt AS parallelism_cnt,
	usename AS login_name,
	wait_event_type || '(' || wait_event || ')' AS wait_info,
	b.blocking_pid AS blocking_session_id,
	state AS status,
	client_addr AS host_ip,
	datname AS database_name,
	application_name AS program_name,
	state_change AS start_time,
	backend_start AS login_time,
	NOW() AS collection_time
FROM
	pg_stat_activity sa
LEFT JOIN blocking2 b ON
	sa.pid = b.blocked_pid
LEFT JOIN parallelism p ON p.datid = sa.datid AND p.usesysid = sa.usesysid AND p.backend_xmin = sa.backend_xmin 
WHERE
	1 = 1
	AND (sa.pid != (SELECT pg_backend_pid()) OR show_own_spid)
	AND CASE WHEN show_sleeping_spids = 0::smallint AND sa.state = 'active' THEN 1
		WHEN show_sleeping_spids = 1::smallint AND sa.state IN ('active','idle in transaction') THEN 1
		WHEN show_sleeping_spids = 2::smallint THEN 1
		ELSE 0 END = 1
	AND sa.backend_type != 'parallel worker'
	AND ((backend_type NOT IN ('walwriter', 'logical replication launcher', 'autovacuum launcher', 'aurora runtime', 'background writer', 'checkpointer') AND usename != 'rdsadmin') OR show_system_spids)
ORDER BY
	AGE(NOW(), state_change) DESC;

END;
$function$
;
