CREATE SCHEMA sa_offline;

CREATE SCHEMA sa_online;

CREATE SCHEMA bl_3nf;

CREATE SCHEMA bl_cl;

CREATE SCHEMA bl_dm;

-- ddl script of tables of SA layers:

CREATE EXTENSION IF NOT EXISTS file_fdw;
CREATE SERVER IF NOT EXISTS file_server FOREIGN DATA WRAPPER file_fdw;

DO
$$
DECLARE
	path_to_offline_sales TEXT := 'd:\datasource_offline_reduced.csv';
	path_to_online_sales TEXT := 'd:\datasource_online_reduced.csv';
BEGIN
	EXECUTE FORMAT ($ex$
	CREATE FOREIGN TABLE sa_offline.ext_offline_sales (
		sale_id varchar(4000),
		sale_date varchar(4000),
		prod_id varchar(4000),
		emp_id varchar(4000),
		emp_store_city_id varchar(4000),
		buyed_number varchar(4000),
		man_id varchar(4000),
		procurement_per_unit varchar(4000),
		sale_cost varchar(4000),
		paid_amount varchar(4000),
		sale_channel varchar(4000),
		product_name varchar(4000),
		product_cat_id varchar(4000),
		prod_cat_name varchar(4000),
		pr_subcat_id varchar(4000),
		pr_subcat_name varchar(4000),
		unit_gram_per_pack varchar(4000),
		man_name varchar(4000),
		man_addr varchar(4000),
		man_city_id varchar(4000),
		man_city_name varchar(4000),
		man_postal_code varchar(4000),
		man_country_id varchar(4000),
		man_country_name varchar(4000),
		man_ctry_province_id varchar(4000),
		man_ctry_province_name varchar(4000),
		man_continent_id varchar(4000),
		man_continent_name varchar(4000),
		emp_name varchar(4000),
		emp_dob varchar(4000),
		emp_email varchar(4000),
		emp_store_id varchar(4000),
		emp_store_name varchar(4000),
		store_city_id varchar(4000),
		store_addr varchar(4000),
		store_city_name varchar(4000),
		store_postal_code varchar(4000),
		store_country_id varchar(4000),
		store_country_name varchar(4000),
		store_ctry_province_id varchar(4000),
		store_ctry_province_name varchar(4000),
		store_continent_id varchar(4000),
		store_continent_name varchar(4000)
		) SERVER file_server
		OPTIONS (FORMAT 'csv', filename '%s',
		HEADER 'true', DELIMITER ',')$ex$, path_to_offline_sales);
	
	EXECUTE FORMAT ($ex$
	CREATE FOREIGN TABLE sa_online.ext_online_sales (
		sale_id varchar(4000),
		sale_date varchar(4000),
		prod_id varchar(4000),
		customer_id varchar(4000),
		quantity_ordered varchar(4000),
		man_id varchar(4000),
		procurement_per_unit varchar(4000),
		order_cost varchar(4000),
		paid_amount varchar(4000),
		delivery_hour varchar(4000),
		sale_channel varchar(4000),
		product_name varchar(4000),
		product_cat_id varchar(4000),
		prod_cat_name varchar(4000),
		pr_subcat_id varchar(4000),
		pr_subcat_name varchar(4000),
		unit_gram_per_pack varchar(4000),
		cust_first_name varchar(4000),
		cust_last_name varchar(4000),
		cust_gender varchar(4000),
		cust_dob varchar(4000),
		cust_addr varchar(4000),
		cust_tel_number varchar(4000),
		cust_email varchar(4000),
		cust_city_id varchar(4000),
		cust_city_name varchar(4000),
		cust_postal_code varchar(4000),
		cust_country_id varchar(4000),
		cust_country_name varchar(4000),
		cust_ctry_province_id varchar(4000),
		cust_ctry_province_name varchar(4000),
		cust_continent_id varchar(4000),
		cust_continent_name varchar(4000),
		man_name varchar(4000),
		man_addr varchar(4000),
		man_city_id varchar(4000),
		man_city_name varchar(4000),
		man_postal_code varchar(4000),
		man_country_id varchar(4000),
		man_country_name varchar(4000),
		man_ctry_province_id varchar(4000),
		man_ctry_province_name varchar(4000),
		man_continent_id varchar(4000),
		man_continent_name varchar(4000)
		) SERVER file_server
		OPTIONS (FORMAT 'csv', filename '%s',
		HEADER 'true', DELIMITER ',')$ex$, path_to_online_sales);

END;
$$;

-- ddl script of tables of sa layers
CREATE TABLE sa_offline.src_offline_sales AS 
	SELECT * FROM sa_offline.ext_offline_sales;

CREATE TABLE sa_online.src_online_sales AS
	SELECT * FROM sa_online.ext_online_sales;

-- adding primary keys to sa layer tables
ALTER TABLE sa_offline.src_offline_sales ADD PRIMARY KEY (sale_id);

ALTER TABLE sa_online.src_online_sales ADD PRIMARY KEY (sale_id);

-- ddl script of tables of bl_3nf layer:
CREATE TABLE bl_3nf.ce_continents (
	continent_id SMALLINT NOT NULL,		-- there is only 6 continent on earth, smallint data type size is enough
	continent_src_id varchar(100) NOT NULL,
	continent_name varchar(100) NOT NULL,
	insert_dt date NOT NULL,
	update_dt date NOT NULL,
	source_system varchar(100) NOT NULL,
	source_entity varchar(100) NOT NULL,
	CONSTRAINT pk_ce_continents_continent_id PRIMARY KEY (continent_id)
	);
	 
CREATE TABLE bl_3nf.ce_countries (
	country_id int NOT NULL,		-- there is no more then 200 country on earth, int data type size is enough
	country_src_id varchar(100) NOT NULL,
	country_name varchar(100) NOT NULL,
	continent_id smallint NOT NULL,
	insert_dt date NOT NULL,
	update_dt date NOT NULL,
	source_system varchar(100) NOT NULL,
	source_entity varchar(100) NOT NULL,
	CONSTRAINT pk_ce_countries_country_id PRIMARY KEY (country_id)
	);
	
CREATE TABLE bl_3nf.ce_provincies (
	province_id bigint NOT NULL,
	province_src_id varchar(100) NOT NULL,
	province_name varchar(100) NOT NULL,
	country_id int NOT NULL,
	insert_dt date NOT NULL,
	update_dt date NOT NULL,
	source_system varchar(100) NOT NULL,
	source_entity varchar(100) NOT NULL,
	CONSTRAINT pk_ce_provincies PRIMARY KEY (province_id)
	);
	
CREATE TABLE bl_3nf.ce_cities (
	city_id bigint NOT NULL,
	city_src_id varchar(100) NOT NULL,
	city_name varchar(100) NOT NULL,
	province_id bigint NOT NULL,
	insert_dt date NOT NULL,
	update_dt date NOT NULL,
	source_system varchar(100) NOT NULL,
	source_entity varchar(100) NOT NULL,
	CONSTRAINT pk_ce_cities_city_id PRIMARY KEY (city_id)
	);
	
CREATE TABLE bl_3nf.ce_customers (
	customer_id bigint NOT NULL,
	customer_src_id varchar(100) NOT NULL,
	first_name varchar(100) NOT NULL,
	last_name varchar(100) NOT NULL,
	gender varchar(5) NOT NULL,
	birth_dt date NOT NULL,
	email varchar(100) NOT NULL,
	customer_address varchar(100) NOT NULL,
	city_id bigint NOT NULL,
	postal_code varchar(100) NOT NULL,
	tel_number varchar(100) NOT NULL,
	insert_dt date NOT NULL,
	update_dt date NOT NULL,
	source_system varchar(100) NOT NULL,
	source_entity varchar(100) NOT NULL,
	CONSTRAINT pk_ce_customers_customer_id PRIMARY KEY (customer_id),
	-- Customer's email is unique value
	CONSTRAINT uni_ce_customers_email UNIQUE (email)
	);

CREATE TABLE bl_3nf.ce_stores (
	store_id bigint NOT NULL,
	store_src_id varchar(100) NOT NULL,
	store_name varchar(100) NOT NULL,
	store_address varchar(100) NOT NULL,
	postal_code varchar(100) NOT NULL,
	city_id bigint NOT NULL,
	insert_dt date NOT NULL,
	update_dt date NOT NULL,
	source_system varchar(100) NOT NULL,
	source_entity varchar(100) NOT NULL,
	CONSTRAINT pk_ce_stores_store_id PRIMARY KEY (store_id)
	);
	
CREATE TABLE bl_3nf.ce_employees (
	employee_id bigint NOT NULL,
	employee_src_id varchar(100) NOT NULL,
	first_name varchar(100) NOT NULL,
	last_name varchar(100) NOT NULL,
	birth_dt date NOT NULL,
	email varchar(100) NOT NULL,
	store_id bigint NOT NULL,
	insert_dt date NOT NULL,
	update_dt date NOT NULL,
	source_system varchar(100) NOT NULL,
	source_entity varchar(100) NOT NULL,
	CONSTRAINT pk_ce_employees_employee_id PRIMARY KEY (employee_id),
	-- Employee's email is unique value
	CONSTRAINT uni_ce_employee_email UNIQUE (email)
	);
	
CREATE TABLE bl_3nf.ce_manufacturers (
	manufacturer_id bigint NOT NULL,
	manufacturer_src_id varchar(100) NOT NULL,
	manufacturer_name varchar(100) NOT NULL,
	manufacturer_address varchar(100) NOT NULL,
	postal_code varchar(100) NOT NULL,
	city_id bigint NOT NULL,
	insert_dt date NOT NULL,
	update_dt date NOT NULL,
	source_system varchar(100) NOT NULL,
	source_entity varchar(100) NOT NULL,
	CONSTRAINT pk_ce_manufacturers_manufacturer_id PRIMARY KEY (manufacturer_id)
	);
	
CREATE TABLE bl_3nf.ce_product_categories (
	product_category_id bigint NOT NULL,
	prod_category_src_id varchar(100) NOT NULL,
	product_category_name varchar(100) NOT NULL,
	insert_dt date NOT NULL,
	update_dt date NOT NULL,
	source_system varchar(100) NOT NULL,
	source_entity varchar(100) NOT NULL,
	CONSTRAINT pk_ce_product_categories_product_category_id PRIMARY KEY (product_category_id)
	);

CREATE TABLE bl_3nf.ce_product_subcategories (
	product_subcategory_id bigint NOT NULL,
	prod_subcat_src_id varchar(100) NOT NULL,
	prod_subcat_name varchar(100) NOT NULL,
	product_category_id bigint NOT NULL,
	insert_dt date NOT NULL,
	update_dt date NOT NULL,
	source_system varchar(100) NOT NULL,
	source_entity varchar(100) NOT NULL,
	CONSTRAINT pk_ce_product_subcategories_product_subcategory_id PRIMARY KEY (product_subcategory_id)
	);

CREATE TABLE bl_3nf.ce_products_scd (
	product_id bigint NOT NULL,
	product_src_id varchar(100) NOT NULL,
	product_name varchar(100) NOT NULL,
	product_subcategory_id bigint NOT NULL,
	unit_gram_per_pack int NOT NULL,
	start_dt date NOT NULL,
	end_dt date NOT NULL,
	is_active boolean NOT NULL,
	insert_dt date NOT NULL,
	source_system varchar(100) NOT NULL,
	source_entity varchar(100) NOT NULL,
	CONSTRAINT pk_ce_products_scd_product_src_id_start_dt PRIMARY KEY (product_src_id, start_dt),
	-- in order to use product_id field as an unique identifier
	CONSTRAINT uni_ce_products_scd_product_id UNIQUE (product_id)
	);

CREATE TABLE bl_3nf.ce_manufacturers_products (
	man_prod_id bigint NOT NULL,
	source_id varchar(100) NOT NULL,
	manufacturer_id bigint NOT NULL,
	product_id bigint NOT NULL,
	procurement_price decimal NOT NULL,
	insert_dt date NOT NULL,
	update_dt date NOT NULL,
	source_system varchar(100) NOT NULL,
	source_entity varchar(100) NOT NULL,
	CONSTRAINT pk_ce_manufacturers_products_man_prod_id PRIMARY KEY (man_prod_id)
	);

CREATE TABLE bl_3nf.ce_product_sales (
	product_sale_id bigint NOT NULL,
	product_sale_src_id varchar(100) NOT NULL,
	sale_dt date NOT NULL,
	man_prod_id bigint NOT NULL,
	customer_id bigint NOT NULL,
	employee_id bigint NOT NULL,
	quantity_ordered int NOT NULL,
	sale_cost decimal NOT NULL,
	amount_paid decimal NOT NULL,
	sale_channel varchar(100) NOT NULL,
	insert_dt date NOT NULL,
	update_dt date NOT NULL,
	source_system varchar(100) NOT NULL,
	source_entity varchar(100) NOT NULL,
	CONSTRAINT pk_ce_product_sales_product_sale_id PRIMARY KEY (product_sale_id)
	);

	/*	Creating sequences	*/
--continent
CREATE SEQUENCE bl_3nf.seq_ce_continents_id
	AS SMALLINT
	OWNED BY bl_3nf.ce_continents.continent_id;

--country
CREATE SEQUENCE bl_3nf.seq_ce_countries_id
	AS int 
	OWNED BY bl_3nf.ce_countries.country_id;

--province
CREATE SEQUENCE bl_3nf.seq_ce_provincies_id
	OWNED BY bl_3nf.ce_provincies.province_id;

--city
CREATE SEQUENCE bl_3nf.seq_ce_cities_id
	OWNED BY bl_3nf.ce_cities.city_id;

--customers
CREATE SEQUENCE bl_3nf.seq_ce_customers_id
	OWNED BY bl_3nf.ce_customers.customer_id;

--stores
CREATE SEQUENCE bl_3nf.seq_ce_stores_id
	OWNED BY bl_3nf.ce_stores.store_id;

--employees
CREATE SEQUENCE bl_3nf.seq_ce_employees_id
	OWNED BY bl_3nf.ce_employees.employee_id;

--manufacturers
CREATE SEQUENCE bl_3nf.seq_ce_manufacturers_id
	OWNED BY bl_3nf.ce_manufacturers.manufacturer_id;

--product categories
CREATE SEQUENCE bl_3nf.seq_ce_categories_id
	OWNED BY bl_3nf.ce_product_categories.product_category_id;

--product subcategories
CREATE SEQUENCE bl_3nf.seq_ce_subcategories_id
	OWNED BY bl_3nf.ce_product_subcategories.product_subcategory_id;

--products
CREATE SEQUENCE bl_3nf.seq_ce_products_scd_id
	OWNED BY bl_3nf.ce_products_scd.product_id;

--manufacturers-products
CREATE SEQUENCE bl_3nf.seq_ce_manufacturers_products_id
	OWNED BY bl_3nf.ce_manufacturers_products.man_prod_id;

--product sales
CREATE SEQUENCE bl_3nf.seq_ce_product_sales_id
	OWNED BY bl_3nf.ce_product_sales.product_sale_id;

	/*	Adding foreign keys to tables	*/
	
ALTER TABLE bl_3nf.ce_countries 
	ADD CONSTRAINT fk_ce_countries_ce_continents_continent_id FOREIGN KEY (continent_id) REFERENCES bl_3nf.ce_continents(continent_id);
	
ALTER TABLE bl_3nf.ce_provincies 
	ADD CONSTRAINT fk_ce_provincies_ce_countries_country_id FOREIGN KEY (country_id) REFERENCES bl_3nf.ce_countries(country_id);
	
ALTER TABLE bl_3nf.ce_cities 
	ADD CONSTRAINT fk_ce_cities_ce_provincies_province_id FOREIGN KEY (province_id) REFERENCES bl_3nf.ce_provincies(province_id);

ALTER TABLE bl_3nf.ce_customers
	ADD CONSTRAINT fk_ce_customer_ce_cities_city_id FOREIGN KEY (city_id) REFERENCES bl_3nf.ce_cities(city_id);

ALTER TABLE bl_3nf.ce_stores 
	ADD CONSTRAINT fk_ce_stores_ce_cities_city_id FOREIGN KEY (city_id) REFERENCES bl_3nf.ce_cities(city_id);

ALTER TABLE bl_3nf.ce_employees
	ADD CONSTRAINT fk_ce_employees_ce_stores_store_id FOREIGN KEY (store_id) REFERENCES bl_3nf.ce_stores(store_id);

ALTER TABLE bl_3nf.ce_product_subcategories
	ADD CONSTRAINT fk_ce_product_subcategories_ce_categories_category_id FOREIGN KEY (product_category_id) REFERENCES bl_3nf.ce_product_categories (product_category_id);

ALTER TABLE bl_3nf.ce_products_scd
	ADD CONSTRAINT fk_ce_products_scd_subcategory_product_subcategory_id FOREIGN KEY (product_subcategory_id) REFERENCES bl_3nf.ce_product_subcategories (product_subcategory_id);

ALTER TABLE bl_3nf.ce_manufacturers 
	ADD CONSTRAINT fk_ce_manufacturers_ce_cities_city_id FOREIGN KEY (city_id) REFERENCES bl_3nf.ce_cities(city_id);

ALTER TABLE bl_3nf.ce_manufacturers_products
	ADD CONSTRAINT fk_ce_manufacturers_products_ce_products_product_id FOREIGN KEY (product_id) REFERENCES bl_3nf.ce_products_scd(product_id),
	ADD CONSTRAINT fk_ce_manufacturers_products_ce_manufacturers_manufacturer_id FOREIGN KEY (manufacturer_id) REFERENCES bl_3nf.ce_manufacturers(manufacturer_id);

ALTER TABLE bl_3nf.ce_product_sales 
	ADD CONSTRAINT fk_ce_product_sales_ce_manufacturers_products_man_prod_id FOREIGN KEY (man_prod_id) REFERENCES bl_3nf.ce_manufacturers_products(man_prod_id),
	ADD CONSTRAINT fk_ce_product_sales_ce_customers_customer_id FOREIGN KEY (customer_id) REFERENCES bl_3nf.ce_customers(customer_id),
	ADD CONSTRAINT fk_ce_product_sales_ce_employees_employee_id FOREIGN KEY (employee_id) REFERENCES bl_3nf.ce_employees(employee_id);

-- ddl script of tables of bl_cl layer:

-- ddl script of tables of bl_dm layer:
CREATE TABLE bl_dm.dim_employees (
	employee_surr_id bigint NOT NULL,
	employee_src_id varchar(100) NOT NULL,
	first_name varchar(100) NOT NULL,
	last_name varchar(100) NOT NULL,
	birth_date date NOT NULL,
	email varchar(100) NOT NULL,
	store_id bigint NOT NULL,
	store_name varchar(100) NOT NULL,
	store_address varchar(100) NOT NULL,
	postal_code varchar(100) NOT NULL,
	city_id bigint NOT NULL,
	city_name varchar(100) NOT NULL,
	province_id bigint NOT NULL,
	province_name varchar(100) NOT NULL,
	country_id bigint NOT NULL,
	country_name varchar(100) NOT NULL,
	continent_id SMALLINT NOT NULL,
	continent_name varchar(100) NOT NULL,
	insert_dt date NOT NULL,
	update_dt date NOT NULL,
	source_system varchar(100) NOT NULL,
	source_entity varchar(100) NOT NULL,
	CONSTRAINT pk_dim_employees_employee_surr_id PRIMARY KEY (employee_surr_id)
	);

CREATE TABLE bl_dm.dim_manufacturers (
	manufacturer_surr_id bigint NOT NULL,
	manufacturer_src_id varchar(100) NOT NULL,
	manufacturer_address varchar(100) NOT NULL,
	postal_code varchar(100) NOT NULL,
	city_id bigint NOT NULL,
	city_name varchar(100) NOT NULL,
	province_id bigint NOT NULL,
	province_name varchar(100) NOT NULL,
	country_id bigint NOT NULL,
	country_name varchar(100) NOT NULL,
	continent_id SMALLINT NOT NULL,
	continent_name varchar(100) NOT NULL,
	insert_dt date NOT NULL,
	update_dt date NOT NULL,
	source_system varchar(100) NOT NULL,
	source_entity varchar(100) NOT NULL,
	CONSTRAINT pk_dim_manufacturers_manufacturer_surr_id PRIMARY KEY (manufacturer_surr_id)
	);

CREATE TABLE bl_dm.dim_day (
    event_dt date,
    day_name varchar(9),
    day_number_in_week varchar(5),
    day_number_in_month varchar(5),
    calendar_week_number varchar(5),
    calendar_month_number varchar(5),
    CONSTRAINT pk_dim_day_event_dt PRIMARY KEY (event_dt)
    );
   
CREATE TABLE bl_dm.dim_sales_channels (
	sale_channel_surr_id bigint NOT NULL,
	channel_src_id varchar(100) NOT NULL,
	sale_channel_name varchar(100) NOT NULL,
	insert_dt date NOT NULL,
	update_dt date NOT NULL,
	source_system varchar(100) NOT NULL,
	source_entity varchar(100) NOT NULL,
	CONSTRAINT pk_dim_sales_channels_surr_id PRIMARY KEY (sale_channel_surr_id)
	);

CREATE TABLE bl_dm.dim_products_scd (
	product_surr_id bigint NOT NULL,
	product_src_id varchar(100) NOT NULL,
	product_name varchar(100) NOT NULL,
	unit_gram_per_pack int NOT NULL,
	category_id bigint NOT NULL,
	category_name varchar(100) NOT NULL,
	subcategory_id bigint NOT NULL,
	subcategory_name varchar(100) NOT NULL,
	start_dt date NOT NULL,
	end_dt date NOT NULL,
	is_active boolean NOT NULL,
	insert_dt date NOT NULL,
	source_system varchar(100) NOT NULL,
	source_entity varchar(100) NOT NULL,
	CONSTRAINT pk_dim_products_scd_src_id_start_dt PRIMARY KEY (product_src_id, start_dt)
	);
	
CREATE TABLE bl_dm.dim_customers (
	customer_surr_id bigint NOT NULL,
	customer_src_id varchar(100) NOT NULL,
	first_name varchar(100) NOT NULL,
	last_name varchar(100) NOT NULL,
	gender varchar(5) NOT NULL,
	birth_dt date NOT NULL,
	email varchar(100) NOT NULL,
	customer_address varchar(100) NOT NULL,
	postal_code varchar(100) NOT NULL,
	tel_number varchar(100) NOT NULL,
	city_id bigint NOT NULL,
	city_name varchar(100) NOT NULL,
	province_id bigint NOT NULL,
	province_name varchar(100) NOT NULL,
	country_id int NOT NULL,
	country_name varchar(100) NOT NULL,
	continent_id SMALLINT NOT NULL,
	continent_name varchar(100) NOT NULL,
	insert_dt date NOT NULL,
	update_dt date NOT NULL,
	source_system varchar(100) NOT NULL,
	source_entity varchar(100) NOT NULL,
	CONSTRAINT pk_dim_customers_customer_surr_id PRIMARY KEY (customer_surr_id)
	);

-- creating yearly partitioned fact tables
CREATE TABLE bl_dm.dim_fct_sales_dd (
	event_dt date NOT NULL,
	manufacturer_surr_id bigint NOT NULL,
	product_surr_id bigint NOT NULL,
	customer_surr_id bigint NOT NULL,
	employee_surr_id bigint NOT NULL,
	sale_channel varchar NOT NULL,
	fct_quantity_ordered int NOT NULL,
	fct_cost decimal NOT NULL,
	fct_amount_paid decimal NOT NULL,
	insert_dt date NOT NULL,
	update_dt date NOT NULL
	) PARTITION BY RANGE (event_dt);

	CREATE TABLE bl_dm.dim_fct_sales_dd_2021 PARTITION OF bl_dm.dim_fct_sales_dd
		FOR VALUES FROM ('2021-01-01') TO ('2022-01-01');
	
	CREATE TABLE bl_dm.dim_fct_sales_dd_2022 PARTITION OF bl_dm.dim_fct_sales_dd
		FOR VALUES FROM ('2022-01-01') TO ('2023-01-01');
	
	CREATE TABLE bl_dm.dim_fct_sales_dd_2023 PARTITION OF bl_dm.dim_fct_sales_dd
		FOR VALUES FROM ('2023-01-01') TO ('2024-01-01');
	
	CREATE TABLE bl_dm.dim_fct_sales_dd_2024 PARTITION OF bl_dm.dim_fct_sales_dd
		FOR VALUES FROM ('2024-01-01') TO ('2025-01-01');

-- adding check constraints to the partitioned fact table
ALTER TABLE bl_dm.dim_fct_sales_dd_2021 ADD CONSTRAINT dim_fct_sales_2021_check
	CHECK (event_dt >= '2021-01-01' AND event_dt < '2022-01-01');

ALTER TABLE bl_dm.dim_fct_sales_dd_2022 ADD CONSTRAINT dim_fct_sales_2022_check
	CHECK (event_dt >= '2022-01-01' AND event_dt < '2023-01-01');

ALTER TABLE bl_dm.dim_fct_sales_dd_2023 ADD CONSTRAINT dim_fct_sales_2023_check
	CHECK (event_dt >= '2023-01-01' AND event_dt < '2024-01-01');

ALTER TABLE bl_dm.dim_fct_sales_dd_2024 ADD CONSTRAINT dim_fct_sales_2024_check
	CHECK (event_dt >= '2024-01-01' AND event_dt < '2025-01-01');


	/*	Creating sequences to surrogate fields.	*/
CREATE SEQUENCE bl_dm.seq_dim_employees_surr_id
	OWNED BY bl_dm.dim_employees.employee_surr_id;

CREATE SEQUENCE bl_dm.seq_dim_manufacturers_surr_id
	OWNED BY bl_dm.dim_manufacturers.manufacturer_surr_id;

CREATE SEQUENCE bl_dm.seq_dim_sales_channels_surr_id
	OWNED BY bl_dm.dim_sales_channels.sale_channel_surr_id;
	
CREATE SEQUENCE bl_dm.seq_dim_products_scd_surr_id
	OWNED BY bl_dm.dim_products_scd.product_surr_id;
	
CREATE SEQUENCE bl_dm.seq_dim_customers_surr_id
	OWNED BY bl_dm.dim_customers.customer_surr_id;


CREATE TABLE bl_cl.log_procedures_inserts (
	procedure_insert_number bigint,
	procedure_name TEXT,
	source_table TEXT,
	source_columns TEXT,
	target_table TEXT,
	transaction_start_time timestamptz DEFAULT current_timestamp,
	procedure_start_time timestamptz DEFAULT clock_timestamp(),
	inserted_rows_number int
	);

COMMIT;
