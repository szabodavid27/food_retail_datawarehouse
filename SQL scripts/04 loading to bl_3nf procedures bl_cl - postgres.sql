CREATE OR REPLACE PROCEDURE bl_cl.ce_continents_load()
LANGUAGE plpgsql
AS 
$$
BEGIN
	
	WITH into_3nf AS (INSERT INTO bl_3nf.ce_continents (continent_id, continent_src_id, continent_name, insert_dt, update_dt, source_system, source_entity)
			SELECT	nextval('bl_3nf.seq_ce_continents_id'),
					man_continent_id,
					man_continent_name,
					current_date,
					current_date,
					'sa_online',
					'src_online_sales'
			FROM	sa_online.src_online_sales
			WHERE 	NOT EXISTS (SELECT 1 FROM bl_3nf.ce_continents WHERE continent_src_id = man_continent_id) AND 
					(man_continent_id IS NOT NULL AND man_continent_name IS NOT NULL)
			GROUP BY 	man_continent_id,
						man_continent_name
			RETURNING *)
			-- logging inserted rows
	INSERT INTO bl_cl.log_procedures_inserts (procedure_insert_number, procedure_name, source_table, source_columns, target_table, inserted_rows_number)
		SELECT  (SELECT count (1) + 1 FROM bl_cl.log_procedures_inserts),
				'bl_cl.ce_continents_load()',
				'sa_online.src_online_sales',
				'manufacturer continents',
				'bl_3nf.ce_contintents',
				COALESCE((SELECT count(*) FROM into_3nf), 0);
		
	WITH into_3nf AS (INSERT INTO bl_3nf.ce_continents (continent_id, continent_src_id, continent_name, insert_dt, update_dt, source_system, source_entity)
			SELECT	nextval('bl_3nf.seq_ce_continents_id'),
					cust_continent_id,
					cust_continent_name,
					current_date,
					current_date,
					'sa_online',
					'src_online_sales'
			FROM	sa_online.src_online_sales
			WHERE 	NOT EXISTS (SELECT 1 FROM bl_3nf.ce_continents WHERE continent_src_id = cust_continent_id) AND 
					(cust_continent_id IS NOT NULL OR cust_continent_name IS NOT NULL)
			GROUP BY 	cust_continent_id,
						cust_continent_name
			RETURNING *)
			-- logging inserted rows
	INSERT INTO bl_cl.log_procedures_inserts (procedure_insert_number, procedure_name, source_table, source_columns, target_table, inserted_rows_number)
		SELECT  (SELECT count (1) + 1 FROM bl_cl.log_procedures_inserts),
				'bl_cl.ce_continents_load()',
				'sa_online.src_online_sales',
				'customer continents',
				'bl_3nf.ce_contintents',
				COALESCE((SELECT count(*) FROM into_3nf), 0);
			
	WITH into_3nf AS (INSERT INTO bl_3nf.ce_continents (continent_id, continent_src_id, continent_name, insert_dt, update_dt, source_system, source_entity)
			SELECT	nextval('bl_3nf.seq_ce_continents_id'),
					store_continent_id,
					store_continent_name,
					current_date,
					current_date,
					'sa_offline',
					'src_offline_sales'
			FROM	sa_offline.src_offline_sales
			WHERE 	NOT EXISTS (SELECT 1 FROM bl_3nf.ce_continents WHERE continent_src_id = store_continent_id) AND 
					(store_continent_id IS NOT NULL OR store_continent_name IS NOT NULL)
			GROUP BY 	store_continent_id,
						store_continent_name
			RETURNING *)
				-- logging inserted rows
	INSERT INTO bl_cl.log_procedures_inserts (procedure_insert_number, procedure_name, source_table, source_columns, target_table, inserted_rows_number)
		SELECT  (SELECT count (1) + 1 FROM bl_cl.log_procedures_inserts),
				'bl_cl.ce_continents_load()',
				'sa_offline.src_offline_sales',
				'store continents',
				'bl_3nf.ce_contintents',
				COALESCE((SELECT count(*) FROM into_3nf), 0);
			
	WITH into_3nf AS (INSERT INTO bl_3nf.ce_continents (continent_id, continent_src_id, continent_name, insert_dt, update_dt, source_system, source_entity)
			SELECT	nextval('bl_3nf.seq_ce_continents_id'),
					man_continent_id,
					man_continent_name,
					current_date,
					current_date,
					'sa_offline',
					'src_offline_sales'
			FROM	sa_offline.src_offline_sales 
			WHERE 	NOT EXISTS (SELECT 1 FROM bl_3nf.ce_continents WHERE continent_src_id = man_continent_id) AND 
					(man_continent_id IS NOT NULL OR man_continent_name IS NOT NULL)
			GROUP BY 	man_continent_id,
						man_continent_name
			RETURNING *)			
			-- logging inserted rows
	INSERT INTO bl_cl.log_procedures_inserts (procedure_insert_number, procedure_name, source_table, source_columns, target_table, inserted_rows_number)
		SELECT  (SELECT count (1) + 1 FROM bl_cl.log_procedures_inserts),
				'bl_cl.ce_continents_load()',
				'sa_offline.src_offline_sales',
				'manufacturer continents',
				'bl_3nf.ce_contintents',
				COALESCE((SELECT count(*) FROM into_3nf), 0);
			
EXCEPTION
	WHEN OTHERS THEN
	RAISE NOTICE 'bl_cl.ce_continents_load() Error: %', SQLERRM;
END;
$$;


CREATE OR REPLACE PROCEDURE bl_cl.ce_countries_load()
LANGUAGE plpgsql
AS
$$
BEGIN

	WITH into_3nf AS (INSERT INTO bl_3nf.ce_countries (country_id, country_src_id, country_name, continent_id, insert_dt, update_dt, source_system, source_entity)
		SELECT	nextval('bl_3nf.seq_ce_countries_id'),
				man_country_id,
				man_country_name,
				COALESCE (continent_id, -1) AS continent_surr_id,
				current_date,
				current_date,
				'sa_offline',
				'src_offline_sales'
		FROM 	sa_offline.src_offline_sales
		LEFT JOIN	bl_3nf.ce_continents ON continent_src_id = man_continent_id
		
		WHERE 	NOT EXISTS (SELECT 1 FROM bl_3nf.ce_countries WHERE country_src_id = man_country_id) AND
				(man_country_id IS NOT NULL OR man_country_name IS NOT NULL)
		
		GROUP BY 	man_country_id,
					man_country_name,
					continent_surr_id
		RETURNING *)
			-- logging inserted rows
	INSERT INTO bl_cl.log_procedures_inserts (procedure_insert_number, procedure_name, source_table, source_columns, target_table, inserted_rows_number)
		SELECT  (SELECT count (1) + 1 FROM bl_cl.log_procedures_inserts),
				'bl_cl.ce_countries_load()',
				'sa_offline.src_offline_sales',
				'manufacturer countries',
				'bl_3nf.ce_countries',
				COALESCE((SELECT count(*) FROM into_3nf), 0);
			
	WITH into_3nf AS (INSERT INTO bl_3nf.ce_countries (country_id, country_src_id, country_name, continent_id, insert_dt, update_dt, source_system, source_entity)
		SELECT	nextval('bl_3nf.seq_ce_countries_id'),
				store_country_id,
				store_country_name,
				COALESCE (continent_id, -1) AS continent_surr_id,
				current_date,
				current_date,
				'sa_offline',
				'src_offline_sales'
		FROM 	sa_offline.src_offline_sales
		LEFT JOIN	bl_3nf.ce_continents ON continent_src_id = store_continent_id
		
		WHERE 	NOT EXISTS (SELECT 1 FROM bl_3nf.ce_countries WHERE country_src_id = store_country_id) AND
				(store_country_id IS NOT NULL OR store_country_name IS NOT NULL)
		
		GROUP BY 	store_country_id,
					store_country_name,
					continent_surr_id
		RETURNING *)
			-- logging inserted rows
	INSERT INTO bl_cl.log_procedures_inserts (procedure_insert_number, procedure_name, source_table, source_columns, target_table, inserted_rows_number)
		SELECT  (SELECT count (1) + 1 FROM bl_cl.log_procedures_inserts),
				'bl_cl.ce_countries_load()',
				'sa_offline.src_offline_sales',
				'store countries',
				'bl_3nf.ce_countries',
				COALESCE((SELECT count(*) FROM into_3nf), 0);
			
	WITH into_3nf AS (INSERT INTO bl_3nf.ce_countries (country_id, country_src_id, country_name, continent_id, insert_dt, update_dt, source_system, source_entity)
		SELECT	nextval('bl_3nf.seq_ce_countries_id'),
				man_country_id,
				man_country_name,
				COALESCE (continent_id, -1) AS continent_surr_id,
				current_date,
				current_date,
				'sa_online',
				'src_online_sales'
		FROM 	sa_online.src_online_sales
		LEFT JOIN	bl_3nf.ce_continents ON continent_src_id  = man_continent_id
		
		WHERE 	NOT EXISTS (SELECT 1 FROM bl_3nf.ce_countries WHERE country_src_id = man_country_id) AND
				(man_country_id IS NOT NULL OR man_country_name IS NOT NULL)
		
		GROUP BY 	man_country_id,
					man_country_name,
					continent_surr_id
		RETURNING *)
			-- logging inserted rows
	INSERT INTO bl_cl.log_procedures_inserts (procedure_insert_number, procedure_name, source_table, source_columns, target_table, inserted_rows_number)
		SELECT  (SELECT count (1) + 1 FROM bl_cl.log_procedures_inserts),
				'bl_cl.ce_countries_load()',
				'sa_online.src_online_sales',
				'manufacturer countries',
				'bl_3nf.ce_countries',
				COALESCE((SELECT count(*) FROM into_3nf), 0);
			
	WITH into_3nf AS (INSERT INTO bl_3nf.ce_countries (country_id, country_src_id, country_name, continent_id, insert_dt, update_dt, source_system, source_entity)
		SELECT	nextval('bl_3nf.seq_ce_countries_id'),
				cust_country_id,
				cust_country_name,
				COALESCE (continent_id, -1) AS continent_surr_id,
				current_date,
				current_date,
				'sa_online',
				'src_online_sales'
		FROM 	sa_online.src_online_sales
		LEFT JOIN	bl_3nf.ce_continents ON continent_src_id = cust_continent_id
		
		WHERE 	NOT EXISTS (SELECT 1 FROM bl_3nf.ce_countries WHERE country_src_id = cust_country_id) AND
				(cust_country_id IS NOT NULL OR cust_country_name IS NOT NULL)
				
		GROUP BY 	cust_country_id,
					cust_country_name,
					continent_surr_id
		RETURNING *)
			-- logging inserted rows
	INSERT INTO bl_cl.log_procedures_inserts (procedure_insert_number, procedure_name, source_table, source_columns, target_table, inserted_rows_number)
		SELECT  (SELECT count (1) + 1 FROM bl_cl.log_procedures_inserts),
				'bl_cl.ce_countries_load()',
				'sa_online.src_online_sales',
				'customer countries',
				'bl_3nf.ce_countries',
				COALESCE((SELECT count(*) FROM into_3nf), 0);
			
EXCEPTION
	WHEN OTHERS THEN
	RAISE NOTICE 'bl_cl.ce_countries_load() Error: %', SQLERRM;

END;
$$;

CREATE OR REPLACE PROCEDURE bl_cl.ce_provincies_load()
LANGUAGE plpgsql
AS
$$
BEGIN
WITH into_3nf AS (INSERT INTO bl_3nf.ce_provincies (province_id, province_src_id, province_name, country_id, insert_dt, update_dt, source_system, source_entity)
		SELECT	nextval('bl_3nf.seq_ce_provincies_id'),
				man_ctry_province_id,
				man_ctry_province_name,
				COALESCE (country_id, -1) AS country_surr_id,
				current_date,
				current_date,
				'sa_offline',
				'src_offline_sales'
		FROM 	sa_offline.src_offline_sales
		LEFT JOIN	bl_3nf.ce_countries ON country_src_id = man_country_id
		
		WHERE 	NOT EXISTS (SELECT 1 FROM bl_3nf.ce_provincies WHERE province_src_id = man_ctry_province_id) AND
				(man_ctry_province_id IS NOT NULL OR man_ctry_province_name IS NOT NULL)
		
		GROUP BY 	man_ctry_province_id,
					man_ctry_province_name,
					country_surr_id
		RETURNING *)
			-- logging inserted rows
	INSERT INTO bl_cl.log_procedures_inserts (procedure_insert_number, procedure_name, source_table, source_columns, target_table, inserted_rows_number)
		SELECT  (SELECT count (1) + 1 FROM bl_cl.log_procedures_inserts),
				'bl_cl.ce_provincies_load()',
				'sa_offline.src_offline_sales',
				'manufacturer provincies',
				'bl_3nf.ce_provincies',
				COALESCE((SELECT count(*) FROM into_3nf), 0);
			
	WITH into_3nf AS (INSERT INTO bl_3nf.ce_provincies (province_id, province_src_id, province_name, country_id, insert_dt, update_dt, source_system, source_entity)
		SELECT	nextval('bl_3nf.seq_ce_provincies_id'),
				man_ctry_province_id,
				man_ctry_province_name,
				COALESCE (ce_ctry.country_id, -1) AS country_surr_id,
				current_date,
				current_date,
				'sa_online',
				'src_online_sales'
		FROM 	sa_online.src_online_sales sons
		LEFT JOIN	bl_3nf.ce_countries ce_ctry ON ce_ctry.country_src_id = sons.man_country_id 
		
		WHERE 	NOT EXISTS (SELECT 1 FROM bl_3nf.ce_provincies ce_prov WHERE ce_prov.province_src_id = man_ctry_province_id) AND
				(man_ctry_province_id IS NOT NULL OR man_ctry_province_name IS NOT NULL)
		
		GROUP BY 	man_ctry_province_id,
					man_ctry_province_name,
					country_surr_id
		RETURNING *)
			-- logging inserted rows
	INSERT INTO bl_cl.log_procedures_inserts (procedure_insert_number, procedure_name, source_table, source_columns, target_table, inserted_rows_number)
		SELECT  (SELECT count (1) + 1 FROM bl_cl.log_procedures_inserts),
				'bl_cl.ce_provincies_load()',
				'sa_online.src_online_sales',
				'manufacturer provincies',
				'bl_3nf.ce_provincies',
				COALESCE((SELECT count(*) FROM into_3nf), 0);
	
	WITH into_3nf AS (INSERT INTO bl_3nf.ce_provincies (province_id, province_src_id, province_name, country_id, insert_dt, update_dt, source_system, source_entity)
		SELECT	nextval('bl_3nf.seq_ce_provincies_id'),
				cust_ctry_province_id,
				cust_ctry_province_name,
				COALESCE (ce_ctry.country_id, -1) AS country_surr_id,
				current_date,
				current_date,
				'sa_online',
				'src_online_sales'
		FROM 	sa_online.src_online_sales sons
		LEFT JOIN	bl_3nf.ce_countries ce_ctry ON ce_ctry.country_src_id = sons.cust_country_id
		
		WHERE 	NOT EXISTS (SELECT 1 FROM bl_3nf.ce_provincies ce_prov WHERE ce_prov.province_src_id = cust_ctry_province_id) AND
				(cust_ctry_province_id IS NOT NULL OR cust_ctry_province_name IS NOT NULL)
		
		GROUP BY 	cust_ctry_province_id,
					cust_ctry_province_name,
					country_surr_id
		RETURNING *)
			-- logging inserted rows
	INSERT INTO bl_cl.log_procedures_inserts (procedure_insert_number, procedure_name, source_table, source_columns, target_table, inserted_rows_number)
		SELECT  (SELECT count (1) + 1 FROM bl_cl.log_procedures_inserts),
				'bl_cl.ce_provincies_load()',
				'sa_online.src_online_sales',
				'customer provincies',
				'bl_3nf.ce_provincies',
				COALESCE((SELECT count(*) FROM into_3nf), 0);
				
	WITH into_3nf AS (INSERT INTO bl_3nf.ce_provincies (province_id, province_src_id, province_name, country_id, insert_dt, update_dt, source_system, source_entity)
		SELECT	nextval('bl_3nf.seq_ce_provincies_id'),
				store_ctry_province_id,
				store_ctry_province_name,
				COALESCE (ce_ctry.country_id, -1) AS country_surr_id,
				current_date,
				current_date,
				'sa_offline',
				'src_offline_sales'
		FROM 	sa_offline.src_offline_sales soffs
		LEFT JOIN	bl_3nf.ce_countries ce_ctry ON ce_ctry.country_src_id = soffs.store_country_id 
		
		WHERE 	NOT EXISTS (SELECT 1 FROM bl_3nf.ce_provincies ce_prov WHERE ce_prov.province_src_id = store_ctry_province_id) AND
				(store_ctry_province_id IS NOT NULL OR store_ctry_province_name IS NOT NULL)
		
		GROUP BY 	store_ctry_province_id,
					store_ctry_province_name,
					country_surr_id
		RETURNING *)
			-- logging inserted rows
	INSERT INTO bl_cl.log_procedures_inserts (procedure_insert_number, procedure_name, source_table, source_columns, target_table, inserted_rows_number)
		SELECT  (SELECT count (1) + 1 FROM bl_cl.log_procedures_inserts),
				'bl_cl.ce_provincies_load()',
				'sa_offline.src_offline_sales',
				'store provincies',
				'bl_3nf.ce_provincies',
				COALESCE((SELECT count(*) FROM into_3nf), 0);
			
EXCEPTION
	WHEN OTHERS THEN
	RAISE NOTICE 'bl_cl.ce_provincies_load() Error: %', SQLERRM;

END;
$$;

CREATE OR REPLACE PROCEDURE bl_cl.ce_cities_load()
LANGUAGE plpgsql
AS
$$
BEGIN

	WITH into_3nf AS (INSERT INTO bl_3nf.ce_cities (city_id, city_src_id, city_name, province_id, insert_dt, update_dt, source_system, source_entity)
		SELECT	nextval('bl_3nf.seq_ce_cities_id'),
				man_city_id,	
				man_city_name,
				COALESCE (ce_prov.province_id, -1) AS province_surr_id,
				current_date,
				current_date,
				'sa_offline',
				'src_offline_sales'
		FROM 	sa_offline.src_offline_sales soffs
		LEFT JOIN	bl_3nf.ce_provincies ce_prov ON ce_prov.province_src_id = soffs.man_ctry_province_id
		
		WHERE 	NOT EXISTS (SELECT 1 FROM bl_3nf.ce_cities ce_city WHERE ce_city.city_src_id = man_city_id) AND
				(man_city_id IS NOT NULL OR man_city_name IS NOT NULL)
		
		GROUP BY 	man_city_id,
					man_city_name,
					province_surr_id
		RETURNING *)
			-- logging inserted rows
	INSERT INTO bl_cl.log_procedures_inserts (procedure_insert_number, procedure_name, source_table, source_columns, target_table, inserted_rows_number)
		SELECT  (SELECT count (1) + 1 FROM bl_cl.log_procedures_inserts),
				'bl_cl.ce_cities_load()',
				'sa_offline.src_offline_sales',
				'manufacturer cities',
				'bl_3nf.ce_cities',
				COALESCE((SELECT count(*) FROM into_3nf), 0);

	WITH into_3nf AS (INSERT INTO bl_3nf.ce_cities (city_id, city_src_id, city_name, province_id, insert_dt, update_dt, source_system, source_entity)
		SELECT	nextval('bl_3nf.seq_ce_cities_id'),
				man_city_id,	
				man_city_name,
				COALESCE (ce_prov.province_id, -1) AS province_surr_id,
				current_date,
				current_date,
				'sa_online',
				'src_online_sales'
		FROM 	sa_online.src_online_sales sons
		LEFT JOIN	bl_3nf.ce_provincies ce_prov ON ce_prov.province_src_id = sons.man_ctry_province_id
		
		WHERE 	NOT EXISTS (SELECT 1 FROM bl_3nf.ce_cities ce_city WHERE ce_city.city_src_id = man_city_id) AND
				(man_city_id IS NOT NULL OR man_city_name IS NOT NULL)
		
		GROUP BY 	man_city_id,
					man_city_name,
					province_surr_id
		RETURNING *)
			-- logging inserted rows
	INSERT INTO bl_cl.log_procedures_inserts (procedure_insert_number, procedure_name, source_table, source_columns, target_table, inserted_rows_number)
		SELECT  (SELECT count (1) + 1 FROM bl_cl.log_procedures_inserts),
				'bl_cl.ce_cities_load()',
				'sa_online.src_online_sales',
				'manufacturer cities',
				'bl_3nf.ce_cities',
				COALESCE((SELECT count(*) FROM into_3nf), 0);
					
	WITH into_3nf AS (INSERT INTO bl_3nf.ce_cities (city_id, city_src_id, city_name, province_id, insert_dt, update_dt, source_system, source_entity)
		SELECT	nextval('bl_3nf.seq_ce_cities_id'),
				cust_city_id,	
				cust_city_name,
				COALESCE (ce_prov.province_id, -1) AS province_surr_id,
				current_date,
				current_date,
				'sa_online',
				'src_online_sales'
		FROM 	sa_online.src_online_sales sons
		LEFT JOIN	bl_3nf.ce_provincies ce_prov ON ce_prov.province_src_id = sons.cust_ctry_province_id 
		
		WHERE 	NOT EXISTS (SELECT 1 FROM bl_3nf.ce_cities ce_city WHERE ce_city.city_src_id = sons.cust_city_id) AND
				(cust_city_id IS NOT NULL OR cust_city_name IS NOT NULL)
		
		GROUP BY 	cust_city_id,	
					cust_city_name,
					province_surr_id
		RETURNING *)
			-- logging inserted rows
	INSERT INTO bl_cl.log_procedures_inserts (procedure_insert_number, procedure_name, source_table, source_columns, target_table, inserted_rows_number)
		SELECT  (SELECT count (1) + 1 FROM bl_cl.log_procedures_inserts),
				'bl_cl.ce_cities_load()',
				'sa_online.src_online_sales',
				'customer cities',
				'bl_3nf.ce_cities',
				COALESCE((SELECT count(*) FROM into_3nf), 0);
					
	WITH into_3nf AS (INSERT INTO bl_3nf.ce_cities (city_id, city_src_id, city_name, province_id, insert_dt, update_dt, source_system, source_entity)
		SELECT	nextval('bl_3nf.seq_ce_cities_id'),
				store_city_id,
				store_city_name,
				COALESCE (ce_prov.province_id, -1) AS province_surr_id,
				current_date,
				current_date,
				'sa_offline',
				'src_offline_sales'
		FROM 	sa_offline.src_offline_sales soffs
		LEFT JOIN	bl_3nf.ce_provincies ce_prov ON ce_prov.province_src_id = soffs.store_ctry_province_id 
		
		WHERE 	NOT EXISTS (SELECT 1 FROM bl_3nf.ce_cities ce_city WHERE ce_city.city_src_id = soffs.store_city_id) AND
				(store_city_id IS NOT NULL OR store_city_name IS NOT NULL)
		
		GROUP BY 	store_city_id,
					store_city_name,
					province_surr_id
		RETURNING *)
			-- logging inserted rows
	INSERT INTO bl_cl.log_procedures_inserts (procedure_insert_number, procedure_name, source_table, source_columns, target_table, inserted_rows_number)
		SELECT  (SELECT count (1) + 1 FROM bl_cl.log_procedures_inserts),
				'bl_cl.ce_cities_load()',
				'sa_offline.src_offline_sales',
				'store cities',
				'bl_3nf.ce_cities',
				COALESCE((SELECT count(*) FROM into_3nf), 0);
			
EXCEPTION
	WHEN OTHERS THEN
	RAISE NOTICE 'bl_cl.ce_cities_load() Error: %', SQLERRM;

END;
$$;

CREATE OR REPLACE PROCEDURE bl_cl.ce_customers_load()
LANGUAGE plpgsql
AS
$$
BEGIN

	WITH into_3nf AS (INSERT INTO bl_3nf.ce_customers (customer_id, customer_src_id, first_name, last_name, gender, birth_dt, email, customer_address, city_id, postal_code, tel_number, insert_dt, update_dt, source_system, source_entity)
		WITH 	email_occurencies AS (
					SELECT	subq.cust_email AS email_name,	/*	in the dataset falsely one e-mail can belong to more, then one customer	*/
							count(1) AS email_occur_number	/*	this field counts these occurences.	*/
					FROM 	(SELECT		customer_id,
										cust_email
								FROM 	sa_online.src_online_sales
								GROUP BY 	customer_id,
											cust_email) AS subq
					GROUP BY subq.cust_email )
		
		SELECT 	nextval('bl_3nf.seq_ce_customers_id')c_surr_id,
				COALESCE (customer_id, 'n. a.') AS c_src_id,
				COALESCE (cust_first_name, 'n. a.') AS c_f_name,
				COALESCE (cust_last_name, 'n. a.') AS c_l_name,
				COALESCE (cust_gender, 'n. a.') AS c_g,
				COALESCE (cust_dob, '1900-01-01')::date AS c_dob,
			
			/*	If an email occurs more, than once, this will concatenate the customer_id to it, in order to be unique.	*/
				COALESCE 	(CASE WHEN email_occur_number > 1 THEN sons.cust_email || ' _' || sons.customer_id
								ELSE cust_email												
							END, customer_id || ' missing email') AS c_email,
				
				COALESCE (cust_addr, 'n. a.') AS c_addr,
				COALESCE (ce_cit.city_id, -1) AS c_city_id,
				COALESCE (cust_postal_code, 'n. a.') AS c_p_code,
				COALESCE (cust_tel_number, 'n. a.') AS c_t_num,
				current_date,
				current_date,
				'sa_online',
				'src_online_sales'
		FROM 	sa_online.src_online_sales sons
		LEFT JOIN email_occurencies e_occ ON e_occ.email_name = sons.cust_email
		LEFT JOIN bl_3nf.ce_cities ce_cit ON ce_cit.city_src_id = sons.cust_city_id
		
		WHERE	NOT EXISTS (SELECT 1 FROM bl_3nf.ce_customers ce_cust WHERE	ce_cust.customer_src_id = sons.customer_id)
	
		GROUP BY 	c_src_id,
					c_f_name,
					c_l_name,
					c_g,
					c_dob,
					c_email,
					c_addr,
					c_city_id,
					c_p_code,
					c_t_num
		RETURNING *)
			-- logging inserted rows
	INSERT INTO bl_cl.log_procedures_inserts (procedure_insert_number, procedure_name, source_table, source_columns, target_table, inserted_rows_number)
		SELECT  (SELECT count (1) + 1 FROM bl_cl.log_procedures_inserts),
				'bl_cl.ce_customers_load()',
				'sa_online.src_online_sales',
				'customers',
				'bl_3nf.ce_customers',
				COALESCE((SELECT count(*) FROM into_3nf), 0);
			
EXCEPTION
	WHEN OTHERS THEN
	RAISE NOTICE 'bl_cl.ce_customers_load() Error: %', SQLERRM;

END;
$$;

CREATE OR REPLACE PROCEDURE bl_cl.ce_stores_load()
LANGUAGE plpgsql
AS
$$
BEGIN

	WITH into_3nf AS (INSERT INTO bl_3nf.ce_stores (store_id, store_src_id, store_name, store_address, postal_code, city_id, insert_dt, update_dt, source_system, source_entity)
		SELECT	nextval ('bl_3nf.seq_ce_stores_id'),
				COALESCE (emp_store_id, 'n. a.') AS s_src_id,
				COALESCE (emp_store_name, 'n. a.') AS s_name,
				COALESCE (store_addr, 'n. a.') AS s_addr,
				COALESCE (store_postal_code, 'n. a.') AS s_post,
				COALESCE(ce_cit.city_id, -1) AS s_cit_id,
				current_date,
				current_date,
				'sa_online',
				'src_online_sales'
		FROM	sa_offline.src_offline_sales soffs
		LEFT JOIN	bl_3nf.ce_cities ce_cit ON ce_cit.city_src_id = soffs.store_city_id
		
		WHERE 	NOT EXISTS (SELECT 1 FROM bl_3nf.ce_stores ce_s WHERE ce_s.store_src_id = soffs.emp_store_id)
		
		GROUP BY 	s_src_id,
					s_name,
					s_addr,
					s_post,
					s_cit_id
		RETURNING *)
			-- logging inserted rows
	INSERT INTO bl_cl.log_procedures_inserts (procedure_insert_number, procedure_name, source_table, source_columns, target_table, inserted_rows_number)
		SELECT  (SELECT count (1) + 1 FROM bl_cl.log_procedures_inserts),
				'bl_cl.ce_stores_load()',
				'sa_online.src_online_sales',
				'stores ',
				'bl_3nf.ce_stores',
				COALESCE((SELECT count(*) FROM into_3nf), 0);

EXCEPTION
	WHEN OTHERS THEN
	RAISE NOTICE 'bl_cl.ce_stores_load() Error: %', SQLERRM;

END;
$$;

CREATE OR REPLACE PROCEDURE bl_cl.ce_employees_load()
LANGUAGE plpgsql
AS
$$
BEGIN

	WITH into_3nf AS (INSERT INTO bl_3nf.ce_employees (employee_id, employee_src_id, first_name, last_name, birth_dt, email, store_id, insert_dt, update_dt, source_system, source_entity)
 			
	SELECT	nextval ('bl_3nf.seq_ce_employees_id') AS e_surr_id,
			COALESCE (emp_id, 'n. a.') AS e_src_id,
			COALESCE (split_part (emp_name, ' ', 1), 'n. a.') AS f_name,	/*	splitting names to different parts	*/
			COALESCE (split_part (emp_name, ' ', 2), 'n. a.') AS l_name,
			COALESCE (emp_dob, '1900-01-01')::date AS e_dob,
			COALESCE (REPLACE (emp_email, ' ', ''), 'n. a.') AS e_mail,		/*	there were falsely spaces inside email addresses	*/
			COALESCE (ce_sto.store_id, -1) AS s_id,
			current_date,
			current_date,
			'sa_offline',
			'src_offline_sales'			
	FROM	sa_offline.src_offline_sales soffs
	LEFT JOIN bl_3nf.ce_stores ce_sto ON ce_sto.store_src_id = soffs.emp_store_id
	
	WHERE NOT EXISTS (SELECT 1 FROM bl_3nf.ce_employees ce_emp WHERE ce_emp.employee_src_id = soffs.emp_id)
	
	GROUP BY 	e_src_id,
				f_name,
				l_name,
				e_dob,
				e_mail,
				s_id
	RETURNING *)
			-- logging inserted rows
	INSERT INTO bl_cl.log_procedures_inserts (procedure_insert_number, procedure_name, source_table, source_columns, target_table, inserted_rows_number)
		SELECT  (SELECT count (1) + 1 FROM bl_cl.log_procedures_inserts),
				'bl_cl.ce_employees_load()',
				'sa_offline.src_offline_sales',
				'employees',
				'bl_3nf.ce_employees',
				COALESCE((SELECT count(*) FROM into_3nf), 0);
			
EXCEPTION
	WHEN OTHERS THEN
	RAISE NOTICE 'bl_cl.ce_employees_load() Error: %', SQLERRM;

END;
$$;

CREATE OR REPLACE PROCEDURE bl_cl.ce_manufacturers_load()
LANGUAGE plpgsql
AS
$$
BEGIN

	WITH into_3nf AS (INSERT INTO bl_3nf.ce_manufacturers (manufacturer_id, manufacturer_src_id, manufacturer_name, manufacturer_address, postal_code, city_id, insert_dt, update_dt, source_system, source_entity)
		WITH manufacturers_to_insert AS (
			SELECT	COALESCE (sons.man_id, 'n. a.') AS m_src_id,
					COALESCE (sons.man_name, 'n. a.') AS m_name,
					COALESCE (sons.man_addr, 'n. a.') AS m_addr,
					COALESCE (ce_cit.city_id, -1) AS m_cit_id,
					COALESCE (sons.man_postal_code, 'n. a.') AS m_post,
					current_date AS ins_dt,
					current_date AS upd_dt,
					'sa_online' AS m_src_sys,
					'src_online_sales' AS m_src_ent
			FROM 	sa_online.src_online_sales sons
			LEFT JOIN bl_3nf.ce_cities ce_cit ON ce_cit.city_src_id = sons.man_city_id	
		
			GROUP BY 	m_src_id,
						m_name,
						m_addr,
						m_cit_id,
						m_post)
		
		SELECT 		nextval ('bl_3nf.seq_ce_manufacturers_id') AS m_surr_id,
					m_src_id,
					m_name,
					m_addr,
					m_post,
					m_cit_id,
					ins_dt,
					upd_dt,
					m_src_sys,
					m_src_ent
		FROM 		manufacturers_to_insert
		WHERE NOT EXISTS (SELECT 1 FROM bl_3nf.ce_manufacturers ce_man WHERE ce_man.manufacturer_src_id = m_src_id)
		RETURNING *)
			-- logging inserted rows
	INSERT INTO bl_cl.log_procedures_inserts (procedure_insert_number, procedure_name, source_table, source_columns, target_table, inserted_rows_number)
		SELECT  (SELECT count (1) + 1 FROM bl_cl.log_procedures_inserts),
				'bl_cl.ce_manufacturers_load()',
				'sa_online.src_online_sales',
				'manufacturers',
				'bl_3nf.ce_manufacturers',
				COALESCE((SELECT count(*) FROM into_3nf), 0);

	WITH into_3nf AS (INSERT INTO bl_3nf.ce_manufacturers (manufacturer_id, manufacturer_src_id, manufacturer_name, manufacturer_address, postal_code, city_id, insert_dt, update_dt, source_system, source_entity)
		WITH manufacturers_to_insert AS (
			SELECT	COALESCE (soffs.man_id, 'n. a.') AS m_src_id,
					COALESCE (soffs.man_name, 'n. a.') AS m_name,
					COALESCE (soffs.man_addr, 'n. a.') AS m_addr,
					COALESCE (ce_cit.city_id, -1) AS m_cit_id,
					COALESCE (soffs.man_postal_code, 'n. a.') AS m_post,
					current_date AS ins_dt,
					current_date AS upd_dt,
					'sa_offline' AS m_src_sys,
					'src_offline_sales' AS m_src_ent
			FROM 	sa_offline.src_offline_sales soffs
			LEFT JOIN bl_3nf.ce_cities ce_cit ON ce_cit.city_src_id = soffs.man_city_id	
		
			GROUP BY 	m_src_id,
						m_name,
						m_addr,
						m_cit_id,
						m_post)
		
		SELECT 		nextval ('bl_3nf.seq_ce_manufacturers_id') AS m_surr_id,
					m_src_id,
					m_name,
					m_addr,
					m_post,
					m_cit_id,
					ins_dt,
					upd_dt,
					m_src_sys,
					m_src_ent
		FROM 		manufacturers_to_insert
		WHERE NOT EXISTS (SELECT 1 FROM bl_3nf.ce_manufacturers ce_man WHERE ce_man.manufacturer_src_id = m_src_id)
		RETURNING *)
			-- logging inserted rows
	INSERT INTO bl_cl.log_procedures_inserts (procedure_insert_number, procedure_name, source_table, source_columns, target_table, inserted_rows_number)
		SELECT  (SELECT count (1) + 1 FROM bl_cl.log_procedures_inserts),
				'bl_cl.ce_manufacturers_load()',
				'sa_offline.src_offline_sales',
				'manufacturers',
				'bl_3nf.ce_manufacturers',
				COALESCE((SELECT count(*) FROM into_3nf), 0);
			
EXCEPTION
	WHEN OTHERS THEN
	RAISE NOTICE 'bl_cl.ce_manufacturers_load() Error: %', SQLERRM;
		
END;
$$;

CREATE OR REPLACE PROCEDURE bl_cl.ce_product_categories_load()
LANGUAGE plpgsql
AS
$$
BEGIN

	WITH into_3nf AS (INSERT INTO bl_3nf.ce_product_categories (product_category_id, prod_category_src_id, product_category_name, insert_dt, update_dt, source_system, source_entity)
		WITH categories_to_insert AS (
			SELECT 	COALESCE (sons.product_cat_id, 'n. a.') AS c_src_id,
					COALESCE (sons.prod_cat_name, 'n. a.') AS c_name,
					current_date AS ins_dt,
					current_date AS upd_dt,
					'sa_online' AS c_src_sys,
					'src_online_sales' AS c_src_ent
			FROM	sa_online.src_online_sales sons
			GROUP BY 	c_src_id,
						c_name
						)
		
		SELECT	nextval ('bl_3nf.seq_ce_categories_id') AS c_surr_id,
				c_src_id,
				c_name,
				ins_dt,
				upd_dt,
				c_src_sys,
				c_src_ent
		FROM	categories_to_insert
		WHERE NOT EXISTS (SELECT 1 FROM bl_3nf.ce_product_categories ce_pc WHERE ce_pc.prod_category_src_id = c_src_id)
		RETURNING *)
			-- logging inserted rows
	INSERT INTO bl_cl.log_procedures_inserts (procedure_insert_number, procedure_name, source_table, source_columns, target_table, inserted_rows_number)
		SELECT  (SELECT count (1) + 1 FROM bl_cl.log_procedures_inserts),
				'bl_cl.ce_product_categories_load()',
				'sa_online.src_online_sales',
				'categories',
				'bl_3nf.ce_product_categories',
				COALESCE((SELECT count(*) FROM into_3nf), 0);

	WITH into_3nf AS (INSERT INTO bl_3nf.ce_product_categories (product_category_id, prod_category_src_id, product_category_name, insert_dt, update_dt, source_system, source_entity)
		WITH categories_to_insert AS (
			SELECT 	COALESCE (soffs.product_cat_id, 'n. a.') AS c_src_id,
					COALESCE (soffs.prod_cat_name, 'n. a.') AS c_name,
					current_date AS ins_dt,
					current_date AS upd_dt,
					'sa_offline' AS c_src_sys,
					'src_offline_sales' AS c_src_ent
			FROM	sa_offline.src_offline_sales soffs
			GROUP BY 	c_src_id,
						c_name
						)
		
		SELECT	nextval ('bl_3nf.seq_ce_categories_id') AS c_surr_id,
				c_src_id,
				c_name,
				ins_dt,
				upd_dt,
				c_src_sys,
				c_src_ent
		FROM	categories_to_insert
		WHERE NOT EXISTS (SELECT 1 FROM bl_3nf.ce_product_categories ce_pc WHERE ce_pc.prod_category_src_id = c_src_id)
		RETURNING *)
			-- logging inserted rows
	INSERT INTO bl_cl.log_procedures_inserts (procedure_insert_number, procedure_name, source_table, source_columns, target_table, inserted_rows_number)
		SELECT  (SELECT count (1) + 1 FROM bl_cl.log_procedures_inserts),
				'bl_cl.ce_product_categories_load()',
				'sa_offline.src_offline_sales',
				'categories',
				'bl_3nf.ce_product_categories',
				COALESCE((SELECT count(*) FROM into_3nf), 0);
			
EXCEPTION
	WHEN OTHERS THEN
	RAISE NOTICE 'bl_cl.ce_product_categories_load() Error: %', SQLERRM;

END;
$$;

CREATE OR REPLACE PROCEDURE bl_cl.ce_product_subcategories_load()
LANGUAGE plpgsql
AS
$$
BEGIN

	WITH into_3nf AS (INSERT INTO bl_3nf.ce_product_subcategories (product_subcategory_id, prod_subcat_src_id, prod_subcat_name, product_category_id, insert_dt, update_dt, source_system, source_entity)
		WITH subcategories_to_insert AS (
			SELECT	COALESCE (sons.pr_subcat_id, 'n. a.') AS sc_src_id,
					COALESCE (sons.pr_subcat_name, 'n. a.') AS sc_name,
					COALESCE (ce_pc.product_category_id, -1) AS cat_id,
					current_date AS ins_dt,
					current_date AS upd_dt,
					'sa_online' AS sc_src_sys,
					'src_online_sales' AS sc_src_ent
			FROM 	sa_online.src_online_sales sons
			LEFT JOIN bl_3nf.ce_product_categories ce_pc ON ce_pc.prod_category_src_id = sons.product_cat_id
			GROUP BY 	sc_src_id,
						sc_name,
						cat_id 
						)						
		SELECT	nextval ('bl_3nf.seq_ce_subcategories_id') AS sc_surr_id,
				sc_src_id,
				sc_name,
				cat_id,
				ins_dt,
				upd_dt,
				sc_src_sys,
				sc_src_ent
		FROM	subcategories_to_insert				
		WHERE NOT EXISTS (SELECT 1 FROM bl_3nf.ce_product_subcategories ce_sc WHERE ce_sc.prod_subcat_src_id = sc_src_id)
		RETURNING *)
			-- logging inserted rows
	INSERT INTO bl_cl.log_procedures_inserts (procedure_insert_number, procedure_name, source_table, source_columns, target_table, inserted_rows_number)
		SELECT  (SELECT count (1) + 1 FROM bl_cl.log_procedures_inserts),
				'bl_cl.ce_product_subcategories_load()',
				'sa_online.src_online_sales',
				'subcategories',
				'bl_3nf.ce_product_subcategories',
				COALESCE((SELECT count(*) FROM into_3nf), 0);
	
	WITH into_3nf AS (INSERT INTO bl_3nf.ce_product_subcategories (product_subcategory_id, prod_subcat_src_id, prod_subcat_name, product_category_id, insert_dt, update_dt, source_system, source_entity)
		WITH subcategories_to_insert AS (
			SELECT	COALESCE (soffs.pr_subcat_id, 'n. a.') AS sc_src_id,
					COALESCE (soffs.pr_subcat_name, 'n. a.') AS sc_name,
					COALESCE (ce_pc.product_category_id, -1) AS cat_id,
					current_date AS ins_dt,
					current_date AS upd_dt,
					'sa_offline' AS sc_src_sys,
					'src_offline_sales' AS sc_src_ent
			FROM 	sa_offline.src_offline_sales soffs
			LEFT JOIN bl_3nf.ce_product_categories ce_pc ON ce_pc.prod_category_src_id = soffs.product_cat_id
			GROUP BY 	sc_src_id,
						sc_name,
						cat_id 
						)						
		SELECT	nextval ('bl_3nf.seq_ce_subcategories_id') AS sc_surr_id,
				sc_src_id,
				sc_name,
				cat_id,
				ins_dt,
				upd_dt,
				sc_src_sys,
				sc_src_ent
		FROM	subcategories_to_insert				
		WHERE NOT EXISTS (SELECT 1 FROM bl_3nf.ce_product_subcategories ce_sc WHERE ce_sc.prod_subcat_src_id = sc_src_id)
		RETURNING *)
			-- logging inserted rows
	INSERT INTO bl_cl.log_procedures_inserts (procedure_insert_number, procedure_name, source_table, source_columns, target_table, inserted_rows_number)
		SELECT  (SELECT count (1) + 1 FROM bl_cl.log_procedures_inserts),
				'bl_cl.ce_product_subcategories_load()',
				'sa_offline.src_offline_sales',
				'subcategories',
				'bl_3nf.ce_product_subcategories',
				COALESCE((SELECT count(*) FROM into_3nf), 0);
			
EXCEPTION
	WHEN OTHERS THEN
	RAISE NOTICE 'bl_cl.ce_product_subcategories_load() Error: %', SQLERRM;

END;
$$;

CREATE OR REPLACE PROCEDURE bl_cl.ce_products_scd_load()
LANGUAGE plpgsql
AS
$$
DECLARE
	rn_before bigint := 0;
	rn_after bigint := 0;
	procedure_begin_time timestamptz; 
BEGIN
	procedure_begin_time := clock_timestamp();
	rn_before := (SELECT count(*) FROM bl_3nf.ce_products_scd);
	-- inserting totally new rows from online source and changing end_date and is_active values of expired rows
	MERGE INTO bl_3nf.ce_products_scd AS ce
	USING (
		WITH products_to_insert AS (
				SELECT 	DISTINCT COALESCE (sons.prod_id, 'n. a.') AS p_src_id,
						COALESCE (sons.product_name, 'n. a.') AS p_name,
						COALESCE (ce_psc.product_subcategory_id, -1) AS psc_id,
						COALESCE (sons.unit_gram_per_pack::int, -1) AS ugp,
						COALESCE (ce_pr.start_dt, '1990-01-01'::date) AS s_dt,
						COALESCE (ce_pr.end_dt, '9999-12-31':: date) AS e_dt,
						COALESCE (ce_pr.is_active, TRUE) AS is_a,
						COALESCE (ce_pr.insert_dt, current_date) AS i_dt,
						'sa_online' AS p_src_sys,
						'src_online_sales' AS p_src_ent
				FROM 	sa_online.src_online_sales sons
				LEFT JOIN bl_3nf.ce_product_subcategories ce_psc ON ce_psc.prod_subcat_src_id = sons.pr_subcat_id 
				LEFT JOIN bl_3nf.ce_products_scd ce_pr ON ce_pr.product_src_id = sons.prod_id	)
			SELECT	nextval ('bl_3nf.seq_ce_products_scd_id') AS p_surr_id,
					p_src_id,
					p_name,
					psc_id,
					ugp,
					s_dt,
					e_dt,
					is_a,
					i_dt,
					p_src_sys,
					p_src_ent
			FROM 	products_to_insert	) AS src
			
		ON  	ce.product_src_id = src.p_src_id AND
				ce.start_dt = src.s_dt
	
		WHEN MATCHED AND ce.product_name <> src.p_name OR 		-- expired rows
						ce.product_subcategory_id <> src.psc_id OR
						ce.unit_gram_per_pack <> src.ugp OR
						ce.end_dt <> src.e_dt OR
						ce.is_active <> src.is_a OR
						ce.insert_dt <> src.i_dt
			THEN UPDATE SET 	end_dt = current_date - INTERVAL '1 day',
								is_active = FALSE				
							
		WHEN NOT MATCHED THEN 
			INSERT (product_id, product_src_id, product_name, product_subcategory_id, unit_gram_per_pack, start_dt,
						end_dt, is_active, insert_dt, source_system, source_entity)
			VALUES ( 
				p_surr_id,
				p_src_id,
				p_name,
				psc_id,
				ugp,
				s_dt,
				e_dt,
				is_a,
				i_dt,
				p_src_sys,
				p_src_ent);
	-- inserting changed records as a new row after registering changes of regarding expired rows		
	MERGE INTO bl_3nf.ce_products_scd AS ce
	USING (
		WITH products_to_insert AS (
				SELECT 	DISTINCT COALESCE (sons.prod_id, 'n. a.') AS p_src_id,
						COALESCE (sons.product_name, 'n. a.') AS p_name,
						COALESCE (ce_psc.product_subcategory_id, -1) AS psc_id,
						COALESCE (sons.unit_gram_per_pack::int, -1) AS ugp,
						COALESCE (ce_pr.start_dt, '1990-01-01'::date) AS s_dt,
						COALESCE (ce_pr.end_dt, '9999-12-31':: date) AS e_dt,
						COALESCE (ce_pr.is_active, TRUE) AS is_a,
						COALESCE (ce_pr.insert_dt, current_date) AS i_dt,
						'sa_online' AS p_src_sys,
						'src_online_sales' AS p_src_ent
				FROM 	sa_online.src_online_sales sons
				LEFT JOIN bl_3nf.ce_product_subcategories ce_psc ON ce_psc.prod_subcat_src_id = sons.pr_subcat_id 
				LEFT JOIN bl_3nf.ce_products_scd ce_pr ON ce_pr.product_src_id = sons.prod_id	)
			SELECT	nextval ('bl_3nf.seq_ce_products_scd_id') AS p_surr_id,
					p_src_id,
					p_name,
					psc_id,
					ugp,
					s_dt,
					e_dt,
					is_a,
					i_dt,
					p_src_sys,
					p_src_ent
			FROM 	products_to_insert	) AS src
			
		ON  	ce.product_src_id = src.p_src_id AND
				ce.start_dt = src.s_dt AND
				ce.product_name = src.p_name AND 		
				ce.product_subcategory_id = src.psc_id AND
				ce.unit_gram_per_pack = src.ugp AND
				ce.end_dt = src.e_dt AND
				ce.is_active = src.is_a AND
				ce.insert_dt = src.i_dt
							
		WHEN NOT MATCHED
		
			THEN INSERT (product_id, product_src_id, product_name, product_subcategory_id, unit_gram_per_pack, start_dt,
						end_dt, is_active, insert_dt, source_system, source_entity)
			VALUES ( 
				p_surr_id,
				p_src_id,
				p_name,
				psc_id,
				ugp,
				current_date,
				'9999-12-31',
				TRUE,
				i_dt,
				p_src_sys,
				p_src_ent);
			
	rn_after := (SELECT count(*) FROM bl_3nf.ce_products_scd);
			
			-- logging inserted rows
	INSERT INTO bl_cl.log_procedures_inserts (procedure_insert_number, procedure_name, source_table, source_columns, target_table,
												procedure_start_time, inserted_rows_number)
		SELECT  (SELECT count (1) + 1 FROM bl_cl.log_procedures_inserts),
				'bl_cl.ce_products_scd_load()',
				'sa_online.src_online_sales',
				'products',
				'bl_3nf.ce_products_scd',
				procedure_begin_time,
				rn_after - rn_before;
			
	-- for further logging calculation
	rn_before := rn_after;
	procedure_begin_time := clock_timestamp();
	-- inserting totally new rows from offline source and changing end_date and is_active values of expired rows
	MERGE INTO bl_3nf.ce_products_scd AS ce
	USING (
		WITH products_to_insert AS (
				SELECT 	DISTINCT COALESCE (soffs.prod_id, 'n. a.') AS p_src_id,
						COALESCE (soffs.product_name, 'n. a.') AS p_name,
						COALESCE (ce_psc.product_subcategory_id, -1) AS psc_id,
						COALESCE (soffs.unit_gram_per_pack::int, -1) AS ugp,
						COALESCE (ce_pr.start_dt, '1990-01-01'::date) AS s_dt,
						COALESCE (ce_pr.end_dt, '9999-12-31':: date) AS e_dt,
						COALESCE (ce_pr.is_active, TRUE) AS is_a,
						COALESCE (ce_pr.insert_dt, current_date) AS i_dt,
						'sa_offline' AS p_src_sys,
						'src_offline_sales' AS p_src_ent
				FROM 	sa_offline.src_offline_sales soffs
				LEFT JOIN bl_3nf.ce_product_subcategories ce_psc ON ce_psc.prod_subcat_src_id = soffs.pr_subcat_id 
				LEFT JOIN bl_3nf.ce_products_scd ce_pr ON ce_pr.product_src_id = soffs.prod_id	)
			SELECT	nextval ('bl_3nf.seq_ce_products_scd_id') AS p_surr_id,
					p_src_id,
					p_name,
					psc_id,
					ugp,
					s_dt,
					e_dt,
					is_a,
					i_dt,
					p_src_sys,
					p_src_ent
			FROM 	products_to_insert	) AS src
			
		ON  	ce.product_src_id = src.p_src_id AND
				ce.start_dt = src.s_dt
	
		WHEN MATCHED AND ce.product_name <> src.p_name OR 		-- expired rows
						ce.product_subcategory_id <> src.psc_id OR
						ce.unit_gram_per_pack <> src.ugp OR
						ce.end_dt <> src.e_dt OR
						ce.is_active <> src.is_a OR
						ce.insert_dt <> src.i_dt
			THEN UPDATE SET 	end_dt = current_date - INTERVAL '1 day',
								is_active = FALSE				
							
		WHEN NOT MATCHED THEN 
			INSERT (product_id, product_src_id, product_name, product_subcategory_id, unit_gram_per_pack, start_dt,
						end_dt, is_active, insert_dt, source_system, source_entity)
			VALUES ( 
				p_surr_id,
				p_src_id,
				p_name,
				psc_id,
				ugp,
				s_dt,
				e_dt,
				is_a,
				i_dt,
				p_src_sys,
				p_src_ent);
	-- inserting changed records as a new row after registering changes of regarding expired rows		
	MERGE INTO bl_3nf.ce_products_scd AS ce
	USING (
		WITH products_to_insert AS (
				SELECT 	DISTINCT COALESCE (soffs.prod_id, 'n. a.') AS p_src_id,
						COALESCE (soffs.product_name, 'n. a.') AS p_name,
						COALESCE (ce_psc.product_subcategory_id, -1) AS psc_id,
						COALESCE (soffs.unit_gram_per_pack::int, -1) AS ugp,
						COALESCE (ce_pr.start_dt, '1990-01-01'::date) AS s_dt,
						COALESCE (ce_pr.end_dt, '9999-12-31':: date) AS e_dt,
						COALESCE (ce_pr.is_active, TRUE) AS is_a,
						COALESCE (ce_pr.insert_dt, current_date) AS i_dt,
						'sa_offline' AS p_src_sys,
						'src_offline_sales' AS p_src_ent
				FROM 	sa_offline.src_offline_sales soffs
				LEFT JOIN bl_3nf.ce_product_subcategories ce_psc ON ce_psc.prod_subcat_src_id = soffs.pr_subcat_id 
				LEFT JOIN bl_3nf.ce_products_scd ce_pr ON ce_pr.product_src_id = soffs.prod_id	)
			SELECT	nextval ('bl_3nf.seq_ce_products_scd_id') AS p_surr_id,
					p_src_id,
					p_name,
					psc_id,
					ugp,
					s_dt,
					e_dt,
					is_a,
					i_dt,
					p_src_sys,
					p_src_ent
			FROM 	products_to_insert	) AS src
			
		ON  	ce.product_src_id = src.p_src_id AND
				ce.start_dt = src.s_dt AND
				ce.product_name = src.p_name AND 		
				ce.product_subcategory_id = src.psc_id AND
				ce.unit_gram_per_pack = src.ugp AND
				ce.end_dt = src.e_dt AND
				ce.is_active = src.is_a AND
				ce.insert_dt = src.i_dt
							
		WHEN NOT MATCHED
		
			THEN INSERT (product_id, product_src_id, product_name, product_subcategory_id, unit_gram_per_pack, start_dt,
						end_dt, is_active, insert_dt, source_system, source_entity)
			VALUES ( 
				p_surr_id,
				p_src_id,
				p_name,
				psc_id,
				ugp,
				current_date,
				'9999-12-31',
				TRUE,
				i_dt,
				p_src_sys,
				p_src_ent);
			
		rn_after := count(*) FROM bl_3nf.ce_products_scd;
			
			-- logging inserted rows
	INSERT INTO bl_cl.log_procedures_inserts (procedure_insert_number, procedure_name, source_table, source_columns, target_table,
												procedure_start_time, inserted_rows_number)
		SELECT  (SELECT count (1) + 1 FROM bl_cl.log_procedures_inserts),
				'bl_cl.ce_products_scd_load()',
				'sa_online.src_online_sales',
				'products',
				'bl_3nf.ce_products_scd',
				procedure_begin_time,
				rn_after - rn_before;
	
EXCEPTION
	WHEN OTHERS THEN
	RAISE NOTICE 'bl_cl.ce_products_scd_load() Error: %', SQLERRM;

END;
$$;

CREATE OR REPLACE PROCEDURE bl_cl.ce_manufacturers_products_load()
LANGUAGE plpgsql
AS
$$
BEGIN

	WITH into_3nf AS (INSERT INTO bl_3nf.ce_manufacturers_products (man_prod_id, source_id, manufacturer_id, product_id, procurement_price, insert_dt, update_dt, source_system, source_entity)
		WITH manufacturers_products_to_insert AS (
					/*	making composite src_id from product and manufacturer src_id-s	*/
			SELECT 	COALESCE (sons.prod_id || '_' || sons.man_id, 'n. a.') AS src_id,
					COALESCE (ce_m.manufacturer_id, -1) AS m_id,
					COALESCE (ce_pr.product_id, -1) AS p_id,
					COALESCE (sons.procurement_per_unit::NUMERIC, -1) AS p_p,
					current_date AS ins_dt,
					current_date AS upd_dt,
					'sa_online' AS mp_src_sys,
					'src_online_sales' AS mp_src_ent
			FROM	sa_online.src_online_sales sons
			LEFT JOIN bl_3nf.ce_manufacturers ce_m ON ce_m.manufacturer_src_id = sons.man_id
			LEFT JOIN bl_3nf.ce_products_scd ce_pr ON ce_pr.product_src_id = sons.prod_id
			GROUP BY 	src_id,
						m_id,
						p_id,
						p_p 
						)
		SELECT	nextval ('bl_3nf.seq_ce_manufacturers_products_id') AS mp_surr_id,
				src_id,
				m_id,
				p_id,
				p_p,
				ins_dt,
				upd_dt,
				mp_src_sys,
				mp_src_ent
		FROM 	manufacturers_products_to_insert
		WHERE NOT EXISTS (SELECT 1 FROM bl_3nf.ce_manufacturers_products ce_mp WHERE ce_mp.source_id = src_id)
		RETURNING *)
			-- logging inserted rows
	INSERT INTO bl_cl.log_procedures_inserts (procedure_insert_number, procedure_name, source_table, source_columns, target_table, inserted_rows_number)
		SELECT  (SELECT count (1) + 1 FROM bl_cl.log_procedures_inserts),
				'bl_cl.ce_manufacturers_products_load()',
				'sa_online.src_online_sales',
				'manufacuters & products',
				'bl_3nf.ce_manufacturers_products',
				COALESCE((SELECT count(*) FROM into_3nf), 0);
	
	WITH into_3nf AS (INSERT INTO bl_3nf.ce_manufacturers_products (man_prod_id, source_id, manufacturer_id, product_id, procurement_price, insert_dt, update_dt, source_system, source_entity)
		WITH manufacturers_products_to_insert AS (
					/*	making composite src_id from product and manufacturer src_id-s	*/
			SELECT 	COALESCE (soffs.prod_id || '_' || soffs.man_id, 'n. a.') AS src_id,
					COALESCE (ce_m.manufacturer_id, -1) AS m_id,
					COALESCE (ce_pr.product_id, -1) AS p_id,
					COALESCE (soffs.procurement_per_unit::NUMERIC, -1) AS p_p,
					current_date AS ins_dt,
					current_date AS upd_dt,
					'sa_offline' AS mp_src_sys,
					'src_offline_sales' AS mp_src_ent
			FROM	sa_offline.src_offline_sales soffs
			LEFT JOIN bl_3nf.ce_manufacturers ce_m ON ce_m.manufacturer_src_id = soffs.man_id
			LEFT JOIN bl_3nf.ce_products_scd ce_pr ON ce_pr.product_src_id = soffs.prod_id
			GROUP BY 	src_id,
						m_id,
						p_id,
						p_p 
						)
		SELECT	nextval ('bl_3nf.seq_ce_manufacturers_products_id') AS mp_surr_id,
				src_id,
				m_id,
				p_id,
				p_p,
				ins_dt,
				upd_dt,
				mp_src_sys,
				mp_src_ent
		FROM 	manufacturers_products_to_insert
		WHERE NOT EXISTS (SELECT 1 FROM bl_3nf.ce_manufacturers_products ce_mp WHERE ce_mp.source_id = src_id)
		RETURNING *)
			-- logging inserted rows
	INSERT INTO bl_cl.log_procedures_inserts (procedure_insert_number, procedure_name, source_table, source_columns, target_table, inserted_rows_number)
		SELECT  (SELECT count (1) + 1 FROM bl_cl.log_procedures_inserts),
				'bl_cl.ce_manufacturers_products_load()',
				'sa_offline.src_offline_sales',
				'manufacuters & products',
				'bl_3nf.ce_manufacturers_products',
				COALESCE((SELECT count(*) FROM into_3nf), 0);
			
EXCEPTION
	WHEN OTHERS THEN
	RAISE NOTICE 'bl_cl.ce_manufacturers_products_load() Error: %', SQLERRM;

END;
$$;

CREATE OR REPLACE PROCEDURE bl_cl.ce_product_sales_load()
LANGUAGE plpgsql
AS
$$
BEGIN

	WITH into_3nf AS (INSERT INTO bl_3nf.ce_product_sales (product_sale_id, product_sale_src_id, sale_dt, man_prod_id, customer_id, employee_id, quantity_ordered,
											sale_cost, amount_paid, sale_channel, insert_dt, update_dt, source_system, source_entity)
		WITH sales_to_insert AS (
			SELECT	COALESCE (sons.sale_id, 'n. a.') AS src_id,
					COALESCE (sons.sale_date::date, '1900-01-01'::date) AS s_dt,
					COALESCE (ce_mp.man_prod_id, -1) AS mp_id,
					COALESCE (ce_cu.customer_id, -1) AS cu_id,
					-1 AS e_id,	/*	in case of online sale, no employee is registered	*/
					COALESCE (sons.quantity_ordered::NUMERIC, -1) AS q_o,
					COALESCE (sons.order_cost::NUMERIC, -1) AS s_c,
					COALESCE (sons.paid_amount::NUMERIC, -1) AS a_p,
					'online' AS s_ch,
					current_date AS ins_dt,
					current_date AS upd_dt,
					'sa_online' AS s_src_sys,
					'src_online_sales' AS s_src_ent
			FROM	sa_online.src_online_sales sons
			LEFT JOIN bl_3nf.ce_manufacturers_products  ce_mp ON ce_mp.source_id = sons.prod_id || '_' || sons.man_id
			LEFT JOIN bl_3nf.ce_customers ce_cu ON ce_cu.customer_src_id = sons.customer_id
			GROUP BY 	src_id,
						s_dt,
						mp_id,
						cu_id,
						e_id,
						q_o,
						s_c,
						a_p
			ORDER BY 	s_dt
						)
		SELECT	nextval ('bl_3nf.seq_ce_product_sales_id') AS s_surr_id,
				src_id,
				s_dt,
				mp_id,
				cu_id,
				e_id,
				q_o,
				s_c,
				a_p,
				s_ch,
				ins_dt,
				upd_dt,
				s_src_sys,
				s_src_ent
		FROM	sales_to_insert
		WHERE NOT EXISTS (SELECT 1 FROM bl_3nf.ce_product_sales ce_ps WHERE ce_ps.product_sale_src_id = src_id)
		RETURNING *)
			-- logging inserted rows
	INSERT INTO bl_cl.log_procedures_inserts (procedure_insert_number, procedure_name, source_table, source_columns, target_table, inserted_rows_number)
		SELECT  (SELECT count (1) + 1 FROM bl_cl.log_procedures_inserts),
				'bl_cl.ce_product_sales_load()',
				'sa_online.src_online_sales',
				'sales',
				'bl_3nf.ce_product_sales',
				COALESCE((SELECT count(*) FROM into_3nf), 0);
	
	WITH into_3nf AS (INSERT INTO bl_3nf.ce_product_sales (product_sale_id, product_sale_src_id, sale_dt, man_prod_id, customer_id, employee_id, quantity_ordered,
											sale_cost, amount_paid, sale_channel, insert_dt, update_dt, source_system, source_entity)
		WITH sales_to_insert AS (
			SELECT	COALESCE (soffs.sale_id, 'n. a.') AS off_src_id,
					COALESCE (soffs.sale_date::date, '1900-01-01'::date) AS s_dt,
					COALESCE (ce_mp.man_prod_id, -1) AS mp_id,
					-1 AS cu_id,	/*	in case of online sale, no customer is not registered	*/
					COALESCE (ce_e.employee_id, -1) AS e_id,
					COALESCE (soffs.buyed_number::NUMERIC, -1) AS q_o,
					COALESCE (soffs.sale_cost::NUMERIC, -1) AS s_c,
					COALESCE (soffs.paid_amount::NUMERIC, -1) AS a_p,
					'offline' AS s_ch,
					current_date AS ins_dt,
					current_date AS upd_dt,
					'sa_offline' AS s_src_sys,
					'src_offline_sales' AS s_src_ent
			FROM	sa_offline.src_offline_sales soffs
			LEFT JOIN bl_3nf.ce_manufacturers_products  ce_mp ON ce_mp.source_id = soffs.prod_id || '_' || soffs.man_id
			LEFT JOIN bl_3nf.ce_employees ce_e ON ce_e.employee_src_id = soffs.emp_id
			GROUP BY 	off_src_id,
						s_dt,
						mp_id,
						cu_id,
						e_id,
						q_o,
						s_c,
						a_p
			ORDER BY 	s_dt
						)
		SELECT	nextval ('bl_3nf.seq_ce_product_sales_id') AS s_surr_id,
				off_src_id,
				s_dt,
				mp_id,
				cu_id,
				e_id,
				q_o,
				s_c,
				a_p,
				s_ch,
				ins_dt,
				upd_dt,
				s_src_sys,
				s_src_ent
		FROM	sales_to_insert
		WHERE NOT EXISTS (SELECT 1 FROM bl_3nf.ce_product_sales ce_ps WHERE ce_ps.product_sale_src_id = off_src_id)
		RETURNING *)
			-- logging inserted rows
	INSERT INTO bl_cl.log_procedures_inserts (procedure_insert_number, procedure_name, source_table, source_columns, target_table, inserted_rows_number)
		SELECT  (SELECT count (1) + 1 FROM bl_cl.log_procedures_inserts),
				'bl_cl.ce_product_sales_load()',
				'sa_offline.src_offline_sales',
				'sales',
				'bl_3nf.ce_product_sales',
				COALESCE((SELECT count(*) FROM into_3nf), 0);
			
EXCEPTION
	WHEN OTHERS THEN
	RAISE NOTICE 'bl_cl.ce_product_sales_load() Error: %', SQLERRM;

END;
$$;

CREATE OR REPLACE PROCEDURE bl_cl.bl_3nf_load()
LANGUAGE plpgsql
AS
$$
BEGIN
		-- logging procedure
INSERT INTO bl_cl.log_procedures_inserts (procedure_insert_number, procedure_name, source_table, source_columns, target_table, inserted_rows_number)
	SELECT  (SELECT count (1) + 1 FROM bl_cl.log_procedures_inserts),
			'bl_cl.bl_3nf_load()',
			'SA layer',
			NULL,
			'BL 3NF layer',
			NULL;
	CALL bl_cl.ce_continents_load();
	CALL bl_cl.ce_countries_load();
	CALL bl_cl.ce_provincies_load();
	CALL bl_cl.ce_cities_load();
	CALL bl_cl.ce_customers_load();
	CALL bl_cl.ce_manufacturers_load();
	CALL bl_cl.ce_stores_load();
	CALL bl_cl.ce_employees_load();
	CALL bl_cl.ce_manufacturers_load();
	CALL bl_cl.ce_product_categories_load();
	CALL bl_cl.ce_product_subcategories_load();
	CALL bl_cl.ce_products_scd_load();
	CALL bl_cl.ce_manufacturers_products_load();
	CALL bl_cl.ce_product_sales_load();
			
EXCEPTION
	WHEN OTHERS THEN
	RAISE NOTICE 'bl_cl.bl_3nf_load() Error: %', SQLERRM;

END;
$$;

COMMIT;