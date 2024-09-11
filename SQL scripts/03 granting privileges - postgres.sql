/*	Creating ff_user to access bl_cl layers functions and procedures.	*/
DO 
$$

BEGIN
	
	IF NOT EXISTS (SELECT 1 FROM pg_user WHERE usename = 'ff_user')
		THEN 
			CREATE USER ff_user WITH PASSWORD 'frozenuserpassword';
	END IF;
	GRANT CONNECT ON DATABASE frozen_food TO ff_user;

	GRANT USAGE ON SCHEMA bl_cl TO ff_user;
	GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA bl_cl TO ff_user;
	GRANT EXECUTE ON ALL PROCEDURES IN SCHEMA bl_cl TO ff_user;

	GRANT USAGE ON SCHEMA bl_3nf TO ff_user;
	GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA bl_3nf TO ff_user;
	GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA bl_dm TO ff_user;	

END;
$$;

COMMIT;