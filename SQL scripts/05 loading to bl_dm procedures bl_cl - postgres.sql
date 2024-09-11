CREATE OR REPLACE PROCEDURE bl_cl.dim_day_load(start_date text, end_date TEXT)
LANGUAGE plpgsql
AS
$$
BEGIN 
	EXECUTE 	FORMAT($ins_dim_day$
	
		INSERT INTO bl_dm.dim_day (event_dt, day_name, day_number_in_week, day_number_in_month, calendar_week_number, calendar_month_number)
		WITH days AS (SELECT generate_series(%L, %L, INTERVAL '1 day')::date AS event_dt)
		SELECT 	event_dt,
				to_char(event_dt, 'day') AS day_name,
				to_char(event_dt, 'ID')::int AS day_number_in_week,
				to_char(event_dt, 'DD')::int AS day_number_in_month,
				to_char(event_dt, 'WW')::int AS calendar_week_number,
				to_char(event_dt, 'MM')::int AS calendar_month_number
		FROM	days d
		WHERE 	NOT EXISTS (SELECT 1 FROM bl_dm.dim_day dd WHERE dd.event_dt = d.event_dt)
		
				$ins_dim_day$, start_date, end_date);
			
EXCEPTION
	WHEN OTHERS THEN
	RAISE NOTICE 'bl_cl.dim_day_load(start_date text, end_date TEXT) Error: %', SQLERRM;

END;
$$;

CREATE OR REPLACE PROCEDURE bl_cl.dim_products_scd_load()
LANGUAGE plpgsql
AS
$$
DECLARE
	rn_before bigint := 0;
	rn_after bigint := 0;
	procedure_begin_time timestamptz; 
BEGIN
	
	procedure_begin_time := clock_timestamp();
	rn_before := (SELECT count(*) FROM bl_dm.dim_products_scd);

	MERGE INTO bl_dm.dim_products_scd AS dim 
	USING (
		WITH dim_products_to_insert AS (
			SELECT	DISTINCT ce_pr.product_id::varchar AS p_id,
					ce_pr.product_name AS p_n,
					ce_pr.unit_gram_per_pack AS ugpp,
					ce_ca.product_category_id AS pc_id,
					ce_ca.product_category_name AS pc_name,
					ce_sca.product_subcategory_id AS psc_id,
					ce_sca.prod_subcat_name AS psc_name,
					start_dt,
					end_dt,
					is_active,
					ce_pr.insert_dt,
					'bl_3nf' AS src_sys,
					'ce_products_scd' AS src_ent
			FROM	bl_3nf.ce_products_scd ce_pr
			LEFT JOIN bl_3nf.ce_product_subcategories ce_sca USING (product_subcategory_id)
			LEFT JOIN bl_3nf.ce_product_categories ce_ca ON ce_ca.product_category_id = ce_sca.product_category_id)
		SELECT	nextval('bl_dm.seq_dim_products_scd_surr_id') AS p_surr_id,
				p_id,
				p_n,
				ugpp,
				pc_id,
				pc_name,
				psc_id,
				psc_name,
				start_dt,
				end_dt,
				is_active,
				insert_dt,
				src_sys,
				src_ent
		FROM	dim_products_to_insert dpti ) AS ce
		
		ON 	dim.product_src_id = ce.p_id AND
			dim.start_dt = ce.start_dt
		
		WHEN MATCHED THEN  
			UPDATE SET 	product_name = ce.p_n,
						unit_gram_per_pack = ce.ugpp,
						subcategory_id = ce.psc_id,
						end_dt = ce.end_dt,
						is_active = ce.is_active,
						insert_dt = ce.insert_dt
		WHEN NOT MATCHED THEN 
			INSERT (product_surr_id, product_src_id, product_name, unit_gram_per_pack, category_id, category_name,
						subcategory_id, subcategory_name, start_dt, end_dt, is_active, insert_dt, source_system, source_entity)
			VALUES (
				p_surr_id,
				p_id,
				p_n,
				ugpp,
				pc_id,
				pc_name,
				psc_id,
				psc_name,
				start_dt,
				end_dt,
				is_active,
				insert_dt,
				src_sys,
				src_ent );
				
	rn_after := (SELECT count(*) FROM bl_dm.dim_products_scd);
			
			-- logging inserted rows
	INSERT INTO bl_cl.log_procedures_inserts (procedure_insert_number, procedure_name, source_table, source_columns, target_table,
												procedure_start_time, inserted_rows_number)
		SELECT  (SELECT count (1) + 1 FROM bl_cl.log_procedures_inserts),
				'bl_cl.dim_products_scd_load()',
				'bl_3nf.ce_products_scd',
				'products',
				'bl_3nf.dim_products_scd',
				procedure_begin_time,
				rn_after - rn_before;
			
EXCEPTION
	WHEN OTHERS THEN
	RAISE NOTICE 'bl_cl.dim_products_scd_load() Error: %', SQLERRM;

END;
$$;

CREATE OR REPLACE PROCEDURE bl_cl.dim_customers_load()
LANGUAGE plpgsql
AS
$$
BEGIN
	WITH into_dm AS (INSERT INTO bl_dm.dim_customers (customer_surr_id, customer_src_id, first_name, last_name, gender, birth_dt, email, customer_address,
		postal_code, tel_number, city_id, city_name, province_id, province_name, country_id, country_name, continent_id, continent_name, 
		insert_dt, update_dt, source_system, source_entity)
		
		WITH customers_to_insert AS (
			SELECT 	DISTINCT customer_id::varchar AS c_src_id,
					first_name AS c_fn,
					last_name AS c_ln,
					gender AS c_g,
					birth_dt AS c_bdt,
					email AS c_e,
					customer_address AS c_a,
					postal_code AS c_pc,
					tel_number AS c_tn,
					city_id AS ci_id,
					city_name AS ci_n,
					province_id AS prov_id,
					province_name AS prov_n,
					country_id AS cou_id,
					country_name AS cou_n,
					continent_id AS con_id,
					continent_name AS con_n
			FROM	bl_3nf.ce_customers ce_c
			LEFT JOIN bl_3nf.ce_cities ce_cit USING (city_id)
			LEFT JOIN bl_3nf.ce_provincies ce_prov USING (province_id)
			LEFT JOIN bl_3nf.ce_countries ce_co USING (country_id)
			LEFT JOIN bl_3nf.ce_continents ce_cont USING (continent_id)	)
		SELECT 	nextval ('bl_dm.seq_dim_customers_surr_id'),
				c_src_id,
				c_fn,
				c_ln,
				c_g,
				c_bdt,
				c_e,
				c_a,
				c_pc,
				c_tn,
				ci_id,
				ci_n,
				prov_id,
				prov_n,
				cou_id,
				cou_n,
				con_id,
				con_n,
				current_date,
				current_date,
				'bl_3nf',
				'ce_customers'
		FROM	customers_to_insert cti
		WHERE NOT EXISTS (SELECT 1 FROM bl_dm.dim_customers dc WHERE dc.customer_src_id = cti.c_src_id)
		RETURNING *)
				-- logging inserted rows
	INSERT INTO bl_cl.log_procedures_inserts (procedure_insert_number, procedure_name, source_table, source_columns, target_table, inserted_rows_number)
		SELECT  (SELECT count (1) + 1 FROM bl_cl.log_procedures_inserts),
				'bl_cl.dim_customers_load()',
				'bl_3nf.ce_customers',
				'cities, provincies, countries, continents',
				'bl_dm.dim_customers',
				COALESCE((SELECT count(*) FROM into_dm), 0);
			
EXCEPTION
	WHEN OTHERS THEN
	RAISE NOTICE 'bl_cl.dim_customers_load() Error: %', SQLERRM;

END;
$$;

CREATE OR REPLACE PROCEDURE bl_cl.dim_employees_load()
LANGUAGE plpgsql
AS
$$
BEGIN
	WITH into_dm AS (INSERT INTO bl_dm.dim_employees (employee_surr_id, employee_src_id, first_name, last_name, birth_date, email, store_id, store_name, store_address,
		postal_code, city_id, city_name, province_id, province_name, country_id, country_name, continent_id, continent_name, 
		insert_dt, update_dt, source_system, source_entity)
		
		WITH employees_to_insert AS (
			SELECT 	DISTINCT employee_id::varchar AS e_src_id,
					first_name AS e_fn,
					last_name AS e_ln,
					birth_dt AS e_bdt,
					email AS e_e,
					store_id AS e_sid,
					store_name AS e_sn,
					store_address AS e_sa,
					postal_code AS e_pc,
					city_id AS ci_id,
					city_name AS ci_n,
					province_id AS prov_id,
					province_name AS prov_n,
					country_id AS cou_id,
					country_name AS cou_n,
					continent_id AS con_id,
					continent_name AS con_n
			FROM	bl_3nf.ce_employees ce_e
			LEFT JOIN bl_3nf.ce_stores ce_sto USING (store_id)
			LEFT JOIN bl_3nf.ce_cities ce_cit USING (city_id)
			LEFT JOIN bl_3nf.ce_provincies ce_prov USING (province_id)
			LEFT JOIN bl_3nf.ce_countries ce_co USING (country_id)
			LEFT JOIN bl_3nf.ce_continents ce_cont USING (continent_id)	)
		SELECT 	nextval ('bl_dm.seq_dim_employees_surr_id'),
				e_src_id,
				e_fn,
				e_ln,
				e_bdt,
				e_e,
				e_sid,
				e_sn,
				e_sa,
				e_pc,
				ci_id,
				ci_n,
				prov_id,
				prov_n,
				cou_id,
				cou_n,
				con_id,
				con_n,
				current_date,
				current_date,
				'bl_3nf',
				'ce_employees'
		FROM	employees_to_insert eti
		WHERE NOT EXISTS (SELECT 1 FROM bl_dm.dim_employees de WHERE de.employee_src_id = eti.e_src_id)
		RETURNING *)
				-- logging inserted rows
	INSERT INTO bl_cl.log_procedures_inserts (procedure_insert_number, procedure_name, source_table, source_columns, target_table, inserted_rows_number)
		SELECT  (SELECT count (1) + 1 FROM bl_cl.log_procedures_inserts),
				'bl_cl.dim_employees_load()',
				'bl_3nf.ce_employees',
				'stores, cities, provincies, countries, continents',
				'bl_dm.dim_employees',
				COALESCE((SELECT count(*) FROM into_dm), 0);
			
EXCEPTION
	WHEN OTHERS THEN
	RAISE NOTICE 'bl_cl.dim_employees_load() Error: %', SQLERRM;

END;
$$;

CREATE OR REPLACE PROCEDURE bl_cl.dim_manufacturers_load()
LANGUAGE plpgsql
AS
$$
BEGIN
	WITH into_dm AS (INSERT INTO bl_dm.dim_manufacturers (manufacturer_surr_id, manufacturer_src_id, manufacturer_address, postal_code, city_id,
		city_name, province_id, province_name, country_id, country_name, continent_id, continent_name, 
		insert_dt, update_dt, source_system, source_entity)
		
		WITH rows_to_insert AS (
			SELECT 	DISTINCT manufacturer_id::varchar AS src_id,
					manufacturer_address AS addr,
					postal_code AS pc,
					city_id AS ci_id,
					city_name AS ci_n,
					province_id AS prov_id,
					province_name AS prov_n,
					country_id AS cou_id,
					country_name AS cou_n,
					continent_id AS con_id,
					continent_name AS con_n
			FROM	bl_3nf.ce_manufacturers ce
			LEFT JOIN bl_3nf.ce_cities ce_cit USING (city_id)
			LEFT JOIN bl_3nf.ce_provincies ce_prov USING (province_id)
			LEFT JOIN bl_3nf.ce_countries ce_co USING (country_id)
			LEFT JOIN bl_3nf.ce_continents ce_cont USING (continent_id)	)
		SELECT 	nextval ('bl_dm.seq_dim_manufacturers_surr_id'),
				src_id,
				addr,
				pc,
				ci_id,
				ci_n,
				prov_id,
				prov_n,
				cou_id,
				cou_n,
				con_id,
				con_n,
				current_date,
				current_date,
				'bl_3nf',
				'ce_manufacturers'
		FROM	rows_to_insert rti
		WHERE NOT EXISTS (SELECT 1 FROM bl_dm.dim_manufacturers de WHERE de.manufacturer_src_id = rti.src_id)
		RETURNING *)
				-- logging inserted rows
	INSERT INTO bl_cl.log_procedures_inserts (procedure_insert_number, procedure_name, source_table, source_columns, target_table, inserted_rows_number)
		SELECT  (SELECT count (1) + 1 FROM bl_cl.log_procedures_inserts),
				'bl_cl.dim_manufacturers_load()',
				'bl_3nf.dim_manufacturers',
				'cities, provincies, countries, continents',
				'bl_dm.dim_manufacturers',
				COALESCE((SELECT count(*) FROM into_dm), 0);
			
EXCEPTION
	WHEN OTHERS THEN
	RAISE NOTICE 'bl_cl.dim_manufacturers_load() Error: %', SQLERRM;

END;
$$;

CREATE OR REPLACE PROCEDURE bl_cl.dim_sales_channels_load()
LANGUAGE plpgsql
AS
$$
BEGIN
	WITH into_dm AS (INSERT INTO bl_dm.dim_sales_channels (sale_channel_surr_id, channel_src_id, sale_channel_name, insert_dt, update_dt, source_system, source_entity)
		WITH rows_to_insert AS (
			SELECT 	DISTINCT sale_channel AS src_id,		/*	there is no distinct key argument for sale channel in this table	*/
					sale_channel AS ch_name
			FROM 	bl_3nf.ce_product_sales)
		SELECT 	nextval('bl_dm.seq_dim_sales_channels_surr_id'),
				src_id,
				ch_name,
				current_date,
				current_date,
				'bl_3nf',
				'ce_product_sales'
		FROM 	rows_to_insert rti
		WHERE NOT EXISTS (SELECT 1 FROM bl_dm.dim_sales_channels de WHERE de.channel_src_id = rti.src_id)
	RETURNING *)
				-- logging inserted rows
	INSERT INTO bl_cl.log_procedures_inserts (procedure_insert_number, procedure_name, source_table, source_columns, target_table, inserted_rows_number)
		SELECT  (SELECT count (1) + 1 FROM bl_cl.log_procedures_inserts),
				'bl_cl.dim_sales_channels_load()',
				'bl_3nf.ce_product_sales',
				'cities, provincies, countries, continents',
				'bl_dm.dim_sales_channels',
				COALESCE((SELECT count(*) FROM into_dm), 0);
			
EXCEPTION
	WHEN OTHERS THEN
	RAISE NOTICE 'bl_cl.dim_sales_channels_load() Error: %', SQLERRM;

END;
$$;

CREATE OR REPLACE PROCEDURE bl_cl.dim_fct_sales_dd_load()
LANGUAGE plpgsql
AS
$$
BEGIN
	WITH into_dm AS (
		INSERT INTO bl_dm.dim_fct_sales_dd (event_dt, manufacturer_surr_id, product_surr_id, customer_surr_id, employee_surr_id, sale_channel,
											fct_quantity_ordered, fct_cost, fct_amount_paid, insert_dt, update_dt)
			
			WITH rows_to_insert AS (
				SELECT 	DISTINCT ce_ps.sale_dt AS e_dt,
						dim_m.manufacturer_surr_id AS m_id,
						dim_pr.product_surr_id AS p_id,
						dim_cu.customer_surr_id AS c_id,
						dim_e.employee_surr_id AS e_id,
						ce_ps.sale_channel AS s_ch,
						ce_ps.quantity_ordered AS q_o,
						ce_ps.sale_cost AS s_c,
						ce_ps.amount_paid AS a_p
						
				FROM	bl_3nf.ce_product_sales ce_ps
				
				LEFT JOIN bl_3nf.ce_manufacturers_products ce_mp ON ce_mp.man_prod_id = ce_ps.man_prod_id
				LEFT JOIN bl_3nf.ce_products_scd ce_pr ON ce_pr.product_id = ce_mp.product_id
				LEFT JOIN bl_dm.dim_products_scd dim_pr ON dim_pr.product_src_id::bigint = ce_pr.product_id AND dim_pr.start_dt = ce_pr.start_dt 
				LEFT JOIN bl_3nf.ce_manufacturers ce_m ON ce_m.manufacturer_id = ce_mp.manufacturer_id
				LEFT JOIN bl_dm.dim_manufacturers dim_m ON dim_m.manufacturer_src_id::bigint = ce_m.manufacturer_id
				
				LEFT JOIN bl_3nf.ce_customers ce_cu ON ce_cu.customer_id = ce_ps.customer_id
				LEFT JOIN bl_dm.dim_customers dim_cu ON dim_cu.customer_src_id::bigint = ce_cu.customer_id
				
				LEFT JOIN bl_3nf.ce_employees ce_e ON ce_e.employee_id = ce_ps.employee_id
				LEFT JOIN bl_dm.dim_employees dim_e ON dim_e.employee_src_id::bigint = ce_e.employee_id
				
				WHERE 	dim_pr.product_src_id NOT IN ('n. a.') AND
						dim_m.manufacturer_src_id NOT IN ('n. a.') AND
						dim_cu.customer_src_id NOT IN ('n. a.') AND
						dim_e.employee_src_id NOT IN ('n. a.')
									)
			SELECT 	e_dt,
					m_id,
					p_id,
					c_id,
					e_id,
					s_ch,
					q_o,
					s_c,
					a_p,
					current_date,
					current_date
			FROM	rows_to_insert rti
			WHERE NOT EXISTS (SELECT 1 FROM bl_dm.dim_fct_sales_dd fct WHERE fct.event_dt = rti.e_dt)
			RETURNING * )
				-- logging inserted rows
	INSERT INTO bl_cl.log_procedures_inserts (procedure_insert_number, procedure_name, source_table, source_columns, target_table, inserted_rows_number)
		SELECT  (SELECT count (1) + 1 FROM bl_cl.log_procedures_inserts),
				'bl_cl.dim_fct_sales_dd_initial_load()',
				'bl_3nf.ce_product_sales',
				'',
				'bl_dm.dim_fct_sales_dd',
				COALESCE((SELECT count(*) FROM into_dm), 0);
			
EXCEPTION
	WHEN OTHERS THEN
	RAISE NOTICE 'bl_cl.dim_fct_sales_dd_load() Error: %', SQLERRM;

END;
$$;


CREATE OR REPLACE PROCEDURE bl_cl.bl_dm_load()
LANGUAGE plpgsql
AS
$$
BEGIN
				-- logging inserted rows
	INSERT INTO bl_cl.log_procedures_inserts (procedure_insert_number, procedure_name, source_table, source_columns, target_table, inserted_rows_number)
		SELECT  (SELECT count (1) + 1 FROM bl_cl.log_procedures_inserts),
				'bl_cl.bl_dm_load()',
				'bl_3nf layer',
				NULL,
				'bl_dm layer',
				NULL;
			
	CALL bl_cl.dim_day_load('2022-01-01', current_date::text);
	CALL bl_cl.dim_products_scd_load();
	CALL bl_cl.dim_customers_load();
	CALL bl_cl.dim_employees_load();
	CALL bl_cl.dim_manufacturers_load();
	CALL bl_cl.dim_sales_channels_load();
	CALL bl_cl.dim_fct_sales_dd_load();
			
EXCEPTION
	WHEN OTHERS THEN
	RAISE NOTICE 'bl_cl.bl_dm_load() Error: %', SQLERRM;

END;
$$;

COMMIT;
