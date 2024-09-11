CALL bl_cl.bl_3nf_load();
CALL bl_cl.bl_dm_load();

COMMIT;

SELECT * FROM bl_cl.log_procedures_inserts ORDER BY procedure_insert_number DESC;