CALL bl_cl.bl_3nf_load();
CALL bl_cl.bl_dm_load();

COMMIT;

SELECT	*
FROM 	bl_cl.log_procedures_inserts
ORDER BY procedure_insert_number DESC;

-- Presenting slowly changing dimension type 2
SELECT 	*
FROM	bl_3nf.ce_products_scd ce_pr
WHERE	ce_pr.product_src_id IN ('prod121', '231');

SELECT	*
FROM	bl_dm.dim_products_scd dim_pr
INNER JOIN bl_3nf.ce_products_scd ce_pr ON ce_pr.product_id::text = dim_pr.product_src_id
WHERE	ce_pr.product_src_id IN ('prod121', '231');