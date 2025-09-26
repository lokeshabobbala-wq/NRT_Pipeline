-- curated.CUR_REVENUE_EGI_NRT definition

-- Drop table

-- DROP TABLE curated.CUR_REVENUE_EGI_NRT;

--DROP TABLE curated.CUR_REVENUE_EGI_NRT;
CREATE TABLE IF NOT EXISTS curated.CUR_REVENUE_EGI_NRT
(
	r_revenue_egi_nrt_id VARCHAR(100)   ENCODE lzo
	,delivery_identifier VARCHAR(100)   ENCODE lzo
	,"sum_of_net_revenue_(usd)" NUMERIC(30,15)   ENCODE az64
	,segment_level_2_description VARCHAR(100)   ENCODE lzo
	,sales_order_identifier VARCHAR(100)   ENCODE lzo
	,sales_order_line_item_identifier VARCHAR(100)   ENCODE lzo
	,order_create_date DATE   ENCODE az64
	,shipment_actual_goods_movement_date DATE   ENCODE az64
	,end_customer_party_name VARCHAR(500)   ENCODE lzo
	,cp_route_to_market VARCHAR(100)   ENCODE lzo
	,"sum_of_cp_net_revenue_(usd)" NUMERIC(18,4)   ENCODE az64
	,sc_risk_assessment_category VARCHAR(100)   ENCODE lzo
	,profit_center_hierarchy_level_7_description VARCHAR(500)   ENCODE lzo
	,profit_center_hierarchy_level_7 VARCHAR(500)   ENCODE lzo
	,include_exclude VARCHAR(100)   ENCODE lzo
	,customer_po_number VARCHAR(500)   ENCODE lzo
	,financial_close_calendar_year_month_code VARCHAR(500)   ENCODE lzo
	,order_type VARCHAR(500)   ENCODE lzo
	,profit_center_hierarchy_level_3_description VARCHAR(500)   ENCODE lzo
	,profit_center_hierarchy_level_4_description VARCHAR(500)   ENCODE lzo
	,route_to_market VARCHAR(500)   ENCODE lzo
	,sales_order_completion_status_code VARCHAR(500)   ENCODE lzo
	,material_number VARCHAR(500)   ENCODE lzo
	,ship_to_party_name VARCHAR(500)   ENCODE lzo
	,sold_to_party_name VARCHAR(500)   ENCODE lzo
	,source_type VARCHAR(500)   ENCODE lzo
	,sum_of_base_quantity NUMERIC(30,15)   ENCODE az64
	,sum_of_cp_unit_quantity NUMERIC(30,15)   ENCODE az64
	,"sum_of_cp_enterprise_standard_cost_(usd)" NUMERIC(30,15)   ENCODE az64
	,"sum_of_cp_factory_margin_(usd)" NUMERIC(30,15)   ENCODE az64
	,"sum_of_cp_gross_margin_(usd)" NUMERIC(30,15)   ENCODE az64
	,"sum_of_cp_gross_revenue_(usd)" NUMERIC(30,15)   ENCODE az64
	,"sum_of_cp_total_cost_of_sales_(usd)" NUMERIC(30,15)   ENCODE az64
	,"sum_of_gross_revenue_(usd)" NUMERIC(30,15)   ENCODE az64
	,sum_of_net_revenue NUMERIC(30,15)   ENCODE az64
	,"sum_of_order_item_net_value_(usd)" NUMERIC(30,15)   ENCODE az64
	,batch_run_dt DATE   ENCODE az64
	,source_nm VARCHAR(500)   ENCODE lzo
	,created_by VARCHAR(500)   ENCODE lzo
	,create_dt TIMESTAMP WITHOUT TIME ZONE  DEFAULT ('now'::character varying)::timestamp without time zone ENCODE az64
	,sum_of_sales_order_base_quantity NUMERIC(30,15)   ENCODE az64
	,sum_of_order_item_net_value_document_currency NUMERIC(30,15)   ENCODE az64
	,process_timestamp TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
)
DISTSTYLE AUTO
 SORTKEY (
	"sum_of_net_revenue_(usd)"
	)
;
ALTER TABLE curated.CUR_REVENUE_EGI_NRT owner to arubauser;
