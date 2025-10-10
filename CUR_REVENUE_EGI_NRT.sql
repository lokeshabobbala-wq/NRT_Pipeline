-- curated.cur_revenue_egi_nrt definition

-- Drop table

-- DROP TABLE curated.cur_revenue_egi_nrt;

--DROP TABLE curated.cur_revenue_egi_nrt;
CREATE TABLE IF NOT EXISTS curated.cur_revenue_egi_nrt
(
	cp_backlog_sni_revenue VARCHAR(100)   ENCODE lzo
	,final_rtm VARCHAR(100)   ENCODE lzo
	,"region" VARCHAR(100)   ENCODE lzo
	,customer_po_number VARCHAR(500)   ENCODE lzo
	,eg_end_customer_name VARCHAR(500)   ENCODE lzo
	,sales_order_line_item_number VARCHAR(100)   ENCODE lzo
	,delivery_identifier VARCHAR(100)   ENCODE lzo
	,material_number VARCHAR(500)   ENCODE lzo
	,financial_close_calendar_year_month_code VARCHAR(100)   ENCODE lzo
	,include_exclude VARCHAR(100)   ENCODE lzo
	,order_create_calendar_date DATE   ENCODE az64
	,order_type VARCHAR(500)   ENCODE lzo
	,profit_center_hierarchy_level_3_description VARCHAR(500)   ENCODE lzo
	,profit_center_hierarchy_level_4_description VARCHAR(500)   ENCODE lzo
	,business_area_code VARCHAR(100)   ENCODE lzo
	,sku_description VARCHAR(500)   ENCODE lzo
	,route_to_market VARCHAR(500)   ENCODE lzo
	,sales_order_completion_status_code VARCHAR(100)   ENCODE lzo
	,sales_order_identifier VARCHAR(100)   ENCODE lzo
	,sc_bklg_catg VARCHAR(100)   ENCODE lzo
	,ship_to_customer_sta VARCHAR(500)   ENCODE lzo
	,shipment_date DATE   ENCODE az64
	,sold_to_customer_sta VARCHAR(500)   ENCODE lzo
	,source_type VARCHAR(100)   ENCODE lzo
	,sum_of_base_quantity NUMERIC(30,15)   ENCODE az64
	,sum_of_sales_order_base_quantity NUMERIC(30,15)   ENCODE az64
	,"sum_of_cp_enterprise_standard_cost_(usd)" NUMERIC(30,15)   ENCODE az64
	,"sum_of_cp_factory_margin_(usd)" NUMERIC(30,15)   ENCODE az64
	,"sum_of_cp_gross_margin_(usd)" NUMERIC(30,15)   ENCODE az64
	,"sum_of_cp_gross_revenue_(usd)" NUMERIC(30,15)   ENCODE az64
	,"sum_of_cp_total_cost_of_sales_(usd)" NUMERIC(30,15)   ENCODE az64
	,sum_of_cp_unit_quantity NUMERIC(30,15)   ENCODE az64
	,"sum_of_gross_revenue_(usd)" NUMERIC(30,15)   ENCODE az64
	,sum_of_net_revenue NUMERIC(30,15)   ENCODE az64
	,secured_position_net_usd NUMERIC(30,15)   ENCODE az64
	,"sum_of_order_item_net_value_(usd)" NUMERIC(30,15)   ENCODE az64
	,sum_of_order_item_net_value_document_currency NUMERIC(30,15)   ENCODE az64
	,process_timestamp TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,record_hash VARCHAR(128)   ENCODE lzo
	,source_file_name VARCHAR(512)   ENCODE lzo
	,batch_run_dt TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,created_by VARCHAR(100)   ENCODE lzo
	,create_dt TIMESTAMP WITHOUT TIME ZONE  DEFAULT ('now'::character varying)::timestamp without time zone ENCODE az64
	,source_nm VARCHAR(100)   ENCODE lzo
	
)
DISTSTYLE AUTO
 SORTKEY (
	sales_order_identifier
	)
;
ALTER TABLE curated.cur_revenue_egi_nrt owner to arubauser;
