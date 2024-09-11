/*	Filling bl_3nf tables with default rows for NULL value handling	*/

INSERT INTO bl_3nf.ce_continents (continent_id, continent_src_id, continent_name, insert_dt, update_dt, source_system, source_entity)
	SELECT -1, 'n. a.', 'n. a.', current_date, current_date, 'manual', 'manual'
	WHERE NOT EXISTS (SELECT 1 FROM bl_3nf.ce_continents WHERE continent_id = -1);

INSERT INTO bl_3nf.ce_countries (country_id, country_src_id, country_name, continent_id, insert_dt, update_dt, source_system, source_entity)
	SELECT -1, 'n. a.', 'n. a.', -1, current_date, current_date, 'manual', 'manual'
	WHERE NOT EXISTS (SELECT 1 FROM bl_3nf.ce_countries WHERE country_id = -1);
	
INSERT INTO bl_3nf.ce_provincies (province_id, province_src_id, province_name, country_id, insert_dt, update_dt, source_system, source_entity)
	SELECT -1, 'n. a.', 'n. a.', -1, current_date, current_date, 'manual', 'manual'
	WHERE NOT EXISTS (SELECT 1 FROM bl_3nf.ce_provincies WHERE province_id = -1);
	
INSERT INTO bl_3nf.ce_cities (city_id, city_src_id, city_name, province_id, insert_dt, update_dt, source_system, source_entity)
	SELECT -1, 'n. a.', 'n. a.', -1, current_date, current_date, 'manual', 'manual'
	WHERE NOT EXISTS (SELECT 1 FROM bl_3nf.ce_cities WHERE city_id = -1);
	
INSERT INTO bl_3nf.ce_customers (customer_id, customer_src_id, first_name, last_name, gender, birth_dt, email, customer_address, city_id, postal_code, tel_number, insert_dt, update_dt, source_system, source_entity)
	SELECT -1, 'n. a.', 'n. a.', 'n. a.', 'n. a.', '1900-01-01'::date, 'n. a.', 'n. a.', -1, 'n. a.', 'n. a.', current_date, current_date, 'manual', 'manual'
	WHERE NOT EXISTS (SELECT 1 FROM bl_3nf.ce_customers WHERE customer_id = -1);
	
INSERT INTO bl_3nf.ce_stores (store_id, store_src_id, store_name, store_address, postal_code, city_id, insert_dt, update_dt, source_system, source_entity)
	SELECT -1, 'n. a.', 'n. a.', 'n. a.', 'n. a.', -1, current_date, current_date, 'manual', 'manual'
	WHERE NOT EXISTS (SELECT 1 FROM bl_3nf.ce_stores WHERE store_id = -1);

INSERT INTO bl_3nf.ce_employees (employee_id, employee_src_id, first_name, last_name, birth_dt, email, store_id, insert_dt, update_dt, source_system, source_entity)
	SELECT -1, 'n. a.', 'n. a.', 'n. a.', '1900-01-01'::date, 'n. a.', -1, current_date, current_date, 'manual', 'manual'
	WHERE NOT EXISTS (SELECT 1 FROM bl_3nf.ce_employees WHERE employee_id = -1);

INSERT INTO bl_3nf.ce_manufacturers (manufacturer_id, manufacturer_src_id, manufacturer_name, manufacturer_address, postal_code, city_id, insert_dt, update_dt, source_system, source_entity)
	SELECT -1, 'n. a.', 'n. a.', 'n. a.', 'n. a.', -1, current_date, current_date, 'manual', 'manual'
	WHERE NOT EXISTS (SELECT 1 FROM bl_3nf.ce_manufacturers WHERE manufacturer_id = -1);

INSERT INTO bl_3nf.ce_product_categories (product_category_id, prod_category_src_id, product_category_name, insert_dt, update_dt, source_system, source_entity)
	SELECT -1, 'n. a.', 'n. a.', current_date, current_date, 'manual', 'manual'
	WHERE NOT EXISTS (SELECT 1 FROM bl_3nf.ce_product_categories WHERE product_category_id = -1);

INSERT INTO bl_3nf.ce_product_subcategories (product_subcategory_id, prod_subcat_src_id, prod_subcat_name, product_category_id, insert_dt, update_dt, source_system, source_entity)
	SELECT -1, 'n. a.', 'n. a.', -1, current_date, current_date, 'manual', 'manual'
	WHERE NOT EXISTS (SELECT 1 FROM bl_3nf.ce_product_subcategories WHERE product_subcategory_id = -1);

INSERT INTO bl_3nf.ce_products_scd (product_id, product_src_id, product_name, product_subcategory_id, unit_gram_per_pack, start_dt, end_dt, is_active, insert_dt, source_system, source_entity)
	SELECT -1, 'n. a.', 'n. a.', -1, -1, '1900-01-01'::date, '9999-12-31'::date, TRUE, '1900-01-01', 'manual', 'manual'
	WHERE NOT EXISTS (SELECT 1 FROM bl_3nf.ce_products_scd WHERE product_id = -1);

INSERT INTO bl_3nf.ce_manufacturers_products (man_prod_id, source_id, manufacturer_id, product_id, procurement_price, insert_dt, update_dt, source_system, source_entity)
	SELECT -1, 'n. a.', -1, -1, -1, current_date, current_date, 'manual', 'manual'
	WHERE NOT EXISTS (SELECT 1 FROM bl_3nf.ce_manufacturers_products WHERE man_prod_id = -1);
	
/*	Filling bl_dm tables with default rows for NULL value handling	*/

INSERT INTO bl_dm.dim_employees (	employee_surr_id, 
									employee_src_id, 
									first_name, last_name, 
									birth_date, email, 
									store_id, 
									store_name, 
									store_address, 
									postal_code, 
									city_id, 
									city_name, 
									province_id, 
									province_name, 
									country_id, 
									country_name, 
									continent_id, 
									continent_name,
									insert_dt,
									update_dt,
									source_system, 
									source_entity)
									
	SELECT -1, 'n. a.', 'n. a.', 'n. a.', '1900-01-01'::date, 'n. a.', -1, 'n. a.', 'n. a.', 
				'n. a.', -1, 'n. a.', -1, 'n. a.', -1, 'n. a.', -1, 'n. a.', current_date, current_date, 'manual', 'manual'
	WHERE NOT EXISTS (SELECT 1 FROM bl_dm.dim_employees WHERE employee_surr_id = -1);

INSERT INTO bl_dm.dim_manufacturers (	manufacturer_surr_id, 
										manufacturer_src_id, 
										manufacturer_address, 
										postal_code, 
										city_id, 
										city_name, 
										province_id,
										province_name,
										country_id,
										country_name,
										continent_id,
										continent_name,
										insert_dt,
										update_dt,
										source_system,
										source_entity)
	
	SELECT -1, 'n. a.', 'n. a.', 'n. a.', -1, 'n. a.', -1, 'n. a.', -1, 'n. a.', -1, 'n. a.', current_date, current_date, 'manual', 'manual'
	WHERE NOT EXISTS (SELECT 1 FROM bl_dm.dim_manufacturers WHERE manufacturer_surr_id = -1);

INSERT INTO bl_dm.dim_day (event_dt, day_name, day_number_in_week, day_number_in_month, calendar_week_number, calendar_month_number)
	SELECT '1900-01-01'::date, 'n. a.', 'n. a.', 'n. a.', 'n. a.', 'n. a.'
	WHERE NOT EXISTS (SELECT * FROM bl_dm.dim_day WHERE event_dt = '1900-01-01')
		UNION
	SELECT '9999-12-31'::date, 'n. a.', 'n. a.', 'n. a.', 'n. a.', 'n. a.'
	WHERE NOT EXISTS (SELECT * FROM bl_dm.dim_day WHERE event_dt = '9999-12-31');

INSERT INTO bl_dm.dim_sales_channels (sale_channel_surr_id, channel_src_id, sale_channel_name, insert_dt, update_dt, source_system, source_entity)
	SELECT -1, 'n. a.', 'n. a.', current_date, current_date, 'manual', 'manual'
	WHERE NOT EXISTS (SELECT 1 FROM bl_dm.dim_sales_channels WHERE sale_channel_surr_id = -1);

INSERT INTO bl_dm.dim_products_scd (	product_surr_id,
										product_src_id,
										product_name,
										unit_gram_per_pack,
										category_id,
										category_name,
										subcategory_id,
										subcategory_name,
										start_dt,
										end_dt,
										is_active,
										insert_dt,
										source_system,
										source_entity)
	SELECT -1, 'n. a.', 'n. a.', -1, -1, 'n. a.', -1, 'n. a.', '1900-01-01'::date, '9999-12-31'::date, 
			TRUE, '1900-01-01'::date, 'manual', 'manual'
	WHERE NOT EXISTS (SELECT 1 FROM bl_dm.dim_products_scd WHERE product_surr_id = -1);

INSERT INTO bl_dm.dim_customers (	customer_surr_id,
									customer_src_id,
									first_name,
									last_name,
									gender,
									birth_dt,
									email,
									customer_address,
									postal_code,
									tel_number,
									city_id,
									city_name,
									province_id,
									province_name,
									country_id,
									country_name,
									continent_id,
									continent_name,
									insert_dt,
									update_dt,
									source_system,
									source_entity)
	SELECT -1, 'n. a.', 'n. a.', 'n. a.', 'n. a.', '1900-01-01'::date, 'n. a.', 'n. a.', 'n. a.', 'n. a.', 
			-1, 'n. a.', -1, 'n. a.', -1, 'n. a.', -1, 'n. a.', current_date, current_date, 'manual', 'manual'
	WHERE NOT EXISTS (SELECT 1 FROM bl_dm.dim_customers WHERE customer_surr_id = -1);
			
COMMIT;

