CREATE OR REPLACE PROCEDURE published.sp_curtopub_r_revenue_egi_scd2(v_region varchar)
	LANGUAGE plpgsql
AS $$
	
	
	
	
	
	


DECLARE

BEGIN

RAISE LOG 'Procedure PUBLISHED.SP_CURTOPUB_R_REVENUE_EGI_SCD2 Started';

TRUNCATE TABLE INTERMEDIATE.I_REVENUE_EGI_SCD2 ;

IF v_region = 'AMS' THEN
	
	RAISE LOG 'Data Ingestion to INTERMEDIATE.I_REVENUE_EGI_SCD2 table started for AMS Region';

	INSERT INTO INTERMEDIATE.I_REVENUE_EGI_SCD2
	(		
		cp_backlog_sni_revenue,
		delivery_identifier,
		sum_of_sales_order_base_quantity,
		process_timestamp,
		sum_of_order_item_net_value_document_currency,
		"sum_of_net_revenue_(usd)",
		segment_level_2_description,
		sales_order_identifier,
		sales_order_line_item_identifier,
		order_create_date,
		shipment_actual_goods_movement_date,
		end_customer_party_name,
		cp_route_to_market,
		"sum_of_cp_net_revenue_(usd)",
		sc_risk_assessment_category,
		profit_center_hierarchy_level_7_description,
		profit_center_hierarchy_level_7,
		include_exclude,
		customer_po_number,
		financial_close_calendar_year_month_code,
		order_type,
		profit_center_hierarchy_level_3_description,
		profit_center_hierarchy_level_4_description,
		route_to_market,
		sales_order_completion_status_code,
		material_number,
		ship_to_party_name,
		sold_to_party_name,
		source_type,
		sum_of_base_quantity,
		sum_of_cp_unit_quantity,
		"sum_of_cp_enterprise_standard_cost_(usd)",
		"sum_of_cp_factory_margin_(usd)",
		"sum_of_cp_gross_margin_(usd)",
		"sum_of_cp_gross_revenue_(usd)",
		"sum_of_cp_total_cost_of_sales_(usd)",
		"sum_of_gross_revenue_(usd)",
		sum_of_net_revenue,
		"sum_of_order_item_net_value_(usd)",
		batch_run_dt,
		source_nm,
		created_by,
		create_dt
		)
		SELECT 
		cp_backlog_sni_revenue,
		delivery_identifier,
		sum_of_sales_order_base_quantity,
		process_timestamp,
		sum_of_order_item_net_value_document_currency,
		"sum_of_net_revenue_(usd)",
		segment_level_2_description,
		TRIM(LEADING '0' FROM SALES_ORDER_IDENTIFIER) AS Sales_Order_Identifier,
		TRIM(LEADING '0' FROM Sales_Order_Line_Item_Identifier) AS Sales_Order_Line_Item_Identifier,			
		order_create_date,
		shipment_actual_goods_movement_date,
		end_customer_party_name,
		cp_route_to_market,
		"sum_of_cp_net_revenue_(usd)",
		sc_risk_assessment_category,
		profit_center_hierarchy_level_7_description,
		profit_center_hierarchy_level_7,
		include_exclude,
		customer_po_number,
		financial_close_calendar_year_month_code,
		order_type,
		profit_center_hierarchy_level_3_description,
		profit_center_hierarchy_level_4_description,
		route_to_market,
		sales_order_completion_status_code,
		material_number,
		ship_to_party_name,
		sold_to_party_name,
		source_type,
		sum_of_base_quantity,
		sum_of_cp_unit_quantity,
		"sum_of_cp_enterprise_standard_cost_(usd)",
		"sum_of_cp_factory_margin_(usd)",
		"sum_of_cp_gross_margin_(usd)",
		"sum_of_cp_gross_revenue_(usd)",
		"sum_of_cp_total_cost_of_sales_(usd)",
		"sum_of_gross_revenue_(usd)",
		sum_of_net_revenue,
		"sum_of_order_item_net_value_(usd)",
		batch_run_dt,
		source_nm,
		created_by,
		create_dt
		FROM 
		(Select * from 
		(Select X.*
			 , row_number() over 
			   (partition by Order_Type, Order_Create_Date, SALES_ORDER_IDENTIFIER, Sales_Order_Line_Item_Identifier, Source_Type, Shipment_Actual_Goods_Movement_Date, delivery_identifier, segment_level_2_description
				order by CP_Type_Priority, Quantity_Priority) as row_num
		From (Select *
				   , case when sum_of_sales_order_base_quantity <> 0 and "Sum_of_CP_Net_Revenue_(USD)" <> 0
						  then 1
						  when sum_of_sales_order_base_quantity = 0 and "Sum_of_CP_Net_Revenue_(USD)" <> 0
						  then 2
						  else 3
					 end as Quantity_Priority
				   , case when cp_backlog_sni_revenue = 'REV'
						  then 1
						  when cp_backlog_sni_revenue = 'BACKLOG'
						  then 2
						  else 3
					 end as CP_Type_Priority
		from CURATED.CUR_REVENUE_EGI_AMS_INC
		) X	) Y Where Y.row_num = 1) A;

	RAISE LOG 'Data Ingestion to Intermediate table completed successfully for AMS';
		
	/* Checking Data Quality and passing the rejected rows to REJECTED_EGI_DATA table */
	call published.sp_dq_check('EGI');
		
	-- Step 1: Update the current records to set them to inactive	
	RAISE LOG 'Update the current records to set them to inactive for AMS region';		

	UPDATE published.R_REVENUE_EGI_AMS AS target
	SET 
		is_current_fg = 'N',
		end_date = CURRENT_DATE,
		LAST_UPDATE_DT = SYSDATE
	FROM INTERMEDIATE.I_REVENUE_EGI_SCD2 AS delt
	WHERE 
		TRIM(delt.segment_level_2_description)='Americas' AND TRIM(target.is_current_fg) = 'Y'
	--	target.Order_Type IS NOT NULL AND delt.Order_Type IS NOT NULL
	--    AND target.order_create_calendar_date IS NOT NULL AND delt.Order_Create_Date IS NOT NULL
	--    AND target.SALES_ORDER_IDENTIFIER IS NOT NULL AND delt.SALES_ORDER_IDENTIFIER IS NOT NULL
	--   AND target.sales_order_line_item_number IS NOT NULL AND delt.Sales_Order_Line_Item_Identifier IS NOT NULL
	--    AND target.Source_Type IS NOT NULL AND delt.Source_Type IS NOT NULL
	--    AND target.shipment_date IS NOT NULL AND delt.Shipment_Actual_Goods_Movement_Date IS NOT NULL
	--    AND target.delivery_identifier IS NOT NULL AND delt.delivery_identifier IS NOT NULL
	--    AND target.segment_level_2_description IS NOT NULL AND delt.segment_level_2_description IS NOT NULL
	--    AND 
		AND COALESCE(TRIM(target.Order_Type),'') = COALESCE(TRIM(delt.Order_Type),'')
		AND COALESCE(TRIM(target.order_create_calendar_date),'1900-01-01') = COALESCE(TRIM(delt.Order_Create_Date),'1900-01-01')
		AND COALESCE(TRIM(target.SALES_ORDER_IDENTIFIER),'') = COALESCE(TRIM(delt.SALES_ORDER_IDENTIFIER),'')
		AND COALESCE(TRIM(target.sales_order_line_item_number),'') = COALESCE(TRIM(delt.Sales_Order_Line_Item_Identifier),'')
		AND COALESCE(TRIM(target.Source_Type),'') = COALESCE(TRIM(delt.Source_Type),'')
		AND COALESCE(TRIM(target.shipment_date),'1900-01-01') = COALESCE(TRIM(delt.Shipment_Actual_Goods_Movement_Date),'1900-01-01')
		AND COALESCE(TRIM(target.delivery_identifier),'') = COALESCE(TRIM(delt.delivery_identifier),'')
		AND TRIM(target.process_timestamp) <> TRIM(delt.process_timestamp);
	
	-- Step 2: Insert new or changed records from the delta table into the TARGET table
	RAISE LOG 'Insert new or changed records from the INTERMEDIATE.I_REVENUE_EGI_SCD2 table into the PUBLISHED.R_REVENUE_EGI_AMS table for AMS region';			

	-- Insert into final Revenue Table PUBLISHED.R_REVENUE_EGI_AMS
	INSERT INTO PUBLISHED.R_REVENUE_EGI_AMS 
	(
		cp_backlog_sni_revenue
		,delivery_identifier
		,sum_of_sales_order_base_quantity
		,process_timestamp
		,sum_of_order_item_net_value_document_currency
		,fiscal_quarter
		,eg_net_revenue
		,region
		,region_id
		,sales_order_identifier
		,sales_order_line_item_number
		,sap_order_key
		,order_create_calendar_date
		,shipment_date
		,eg_end_customer_name
		,final_rtm
		,secured_position_net_usd
		,sc_bklg_catg
		,sku_description
		,include_exclude
		,business_area_code
		,"Customer_PO_Number" ,
		"Financial_Close_Calendar_Year_Month_Code",
		"Order_Type" ,
		"Profit_Center_Hierarchy_Level_3_Description",
		"Profit_Center_Hierarchy_Level_4_Description" ,
		"Route_To_Market",
		"Sales_Order_Completion_Status_Code"  ,
		"Material_Number",
		"Source_Type" ,
		"Sum_of_Base_Quantity"  ,
		"Sum_of_CP_Unit_Quantity"  ,
		"Sum_of_CP_Enterprise_Standard_Cost_(USD)" ,
		"Sum_of_CP_Factory_Margin_(USD)" ,
		"Sum_of_CP_Gross_Margin_(USD)" ,
		"Sum_of_CP_Gross_Revenue_(USD)"  ,
		"Sum_of_CP_Total_Cost_of_Sales_(USD)" ,
		"Sum_of_Gross_Revenue_(USD)"  ,
		"Sum_of_Net_Revenue" ,
		"Sum_of_Order_Item_Net_Value_(USD)" ,
		sold_to_customer_sta 
		,ship_to_customer_sta
		,is_current_fg
		,batch_run_dt
		,source_nm
		,created_by
		,start_date 
		,end_date			
	)
	SELECT 
		cp_backlog_sni_revenue,
		delivery_identifier,
		sum_of_sales_order_base_quantity,
		process_timestamp,
		sum_of_order_item_net_value_document_currency,
		(B.FISCAL_YEAR_QUARTER + RIGHT (B.FISCAL_YEAR,2)) AS FISCAL_QUARTER, 
		"Sum_of_CP_Net_Revenue_(USD)",
		'AMS' AS REGION,
		503 AS REGION_ID,
		-- Remove the Leading Zero's from SAP_ORDER_NO and SAP_ITEM_NO for AMS
		TRIM(LEADING '0' FROM SALES_ORDER_IDENTIFIER) AS Sales_Order_Identifier,
		TRIM(LEADING '0' FROM Sales_Order_Line_Item_Identifier) AS Sales_Order_Line_Item_Identifier,
		(TRIM(LEADING '0' FROM SALES_ORDER_IDENTIFIER) + '-' + TRIM(LEADING '0' FROM Sales_Order_Line_Item_Identifier)) AS SAP_ORDER_KEY,
		Order_Create_Date,
		Shipment_Actual_Goods_Movement_Date,
		End_Customer_Party_Name,
		CP_Route_To_Market,
		"Sum_of_Net_Revenue_(USD)",
		SC_Risk_Assessment_Category,
		Profit_Center_Hierarchy_Level_7_Description,
		Include_Exclude,
		"Profit_Center_Hierarchy_Level_7",
		Customer_PO_Number,
		Financial_Close_Calendar_Year_Month_Code,	
		"Order_Type" ,
		"Profit_Center_Hierarchy_Level_3_Description",
		"Profit_Center_Hierarchy_Level_4_Description" ,
		"Route_To_Market",
		"Sales_Order_Completion_Status_Code"  ,
		"Material_Number",
		"Source_Type" ,
		"Sum_of_Base_Quantity"  ,
		"Sum_of_CP_Unit_Quantity"  ,
		"Sum_of_CP_Enterprise_Standard_Cost_(USD)" ,
		"Sum_of_CP_Factory_Margin_(USD)" ,
		"Sum_of_CP_Gross_Margin_(USD)" ,
		"Sum_of_CP_Gross_Revenue_(USD)"  ,
		"Sum_of_CP_Total_Cost_of_Sales_(USD)" ,
		"Sum_of_Gross_Revenue_(USD)"  ,
		"Sum_of_Net_Revenue" ,
		"Sum_of_Order_Item_Net_Value_(USD)" ,
		Sold_To_Party_Name
		,Ship_To_Party_Name ,
		'Y' AS IS_CURRENT_FG,
		A.BATCH_RUN_DT,
		A.SOURCE_NM, 
		A.CREATED_BY,
		CURRENT_DATE AS start_date, 
		NULL AS end_date
	FROM 
		INTERMEDIATE.I_REVENUE_EGI_SCD2 AS A
		LEFT JOIN PUBLISHED.D_DATE_CALENDAR B ON A.BATCH_RUN_DT = B.DATE 
		-- Logic as provided by the SC Analytics Team
		WHERE 
		TRIM(A.segment_level_2_description)='Americas' AND
		"Sum_of_Net_Revenue_(USD)" >= 0 AND
		Sales_Order_Identifier NOT LIKE '%Grand%' AND
		Sales_Order_Line_Item_Identifier NOT LIKE '%?%' AND 
		Sales_Order_Identifier NOT LIKE '%?%' AND
		NOT EXISTS (
			SELECT 1 
			FROM published.R_REVENUE_EGI_AMS AS target
			WHERE 
				TRIM(target.IS_CURRENT_FG) = 'Y'
				AND COALESCE(TRIM(target.Order_Type),'') = COALESCE(TRIM(A.Order_Type),'')
				AND COALESCE(TRIM(target.order_create_calendar_date),'1900-01-01') = COALESCE(TRIM(A.Order_Create_Date),'1900-01-01')
				AND COALESCE(TRIM(target.SALES_ORDER_IDENTIFIER),'') = COALESCE(TRIM(A.SALES_ORDER_IDENTIFIER),'')
				AND COALESCE(TRIM(target.sales_order_line_item_number),'') = COALESCE(TRIM(A.Sales_Order_Line_Item_Identifier),'')
				AND COALESCE(TRIM(target.Source_Type),'') = COALESCE(TRIM(A.Source_Type),'')
				AND COALESCE(TRIM(target.shipment_date),'1900-01-01') = COALESCE(TRIM(A.Shipment_Actual_Goods_Movement_Date),'1900-01-01')
				AND COALESCE(TRIM(target.delivery_identifier),'') = COALESCE(TRIM(A.delivery_identifier),'')
				AND TRIM(target.process_timestamp) = TRIM(A.process_timestamp)
	);

	RAISE LOG 'Data Inserted into PUBLISED.R_REVENUE_EGI_AMS from the INTERMEDIATE.I_REVENUE_EGI_SCD2 table';
		
	--Doing the Aggregation and move the EG value to L1 table
	TRUNCATE TABLE PUBLISHED.RPT_EGI_AMS_DATA;

	RAISE LOG 'Data Ingestion to PUBLISHED.RPT_EGI_AMS_DATA table started';
	
	INSERT INTO PUBLISHED.RPT_EGI_AMS_DATA 
	SELECT SAP_ORDER_KEY,SUM(SECURED_POSITION_NET_USD) ,SUM(eg_net_revenue)
	FROM PUBLISHED.R_REVENUE_EGI_AMS WHERE IS_CURRENT_FG = 'Y' 
	GROUP BY SAP_ORDER_KEY;
	
	RAISE LOG 'Data Inserted into PUBLISHED.RPT_EGI_AMS_DATA from PUBLISHED.R_REVENUE_EGI_AMS';

END IF ;

IF v_region = 'APJ' THEN

-----------------------APJ-----------------------
	
	RAISE LOG 'Data Ingestion to INTERMEDIATE.I_REVENUE_EGI_SCD2 table started for APJ region';

	INSERT INTO INTERMEDIATE.I_REVENUE_EGI_SCD2
	(		
		cp_backlog_sni_revenue,
		delivery_identifier,
		sum_of_sales_order_base_quantity,
		process_timestamp,
		sum_of_order_item_net_value_document_currency,
		"sum_of_net_revenue_(usd)",
		segment_level_2_description,
		sales_order_identifier,
		sales_order_line_item_identifier,
		order_create_date,
		shipment_actual_goods_movement_date,
		end_customer_party_name,
		cp_route_to_market,
		"sum_of_cp_net_revenue_(usd)",
		sc_risk_assessment_category,
		profit_center_hierarchy_level_7_description,
		profit_center_hierarchy_level_7,
		include_exclude,
		customer_po_number,
		financial_close_calendar_year_month_code,
		order_type,
		profit_center_hierarchy_level_3_description,
		profit_center_hierarchy_level_4_description,
		route_to_market,
		sales_order_completion_status_code,
		material_number,
		ship_to_party_name,
		sold_to_party_name,
		source_type,
		sum_of_base_quantity,
		sum_of_cp_unit_quantity,
		"sum_of_cp_enterprise_standard_cost_(usd)",
		"sum_of_cp_factory_margin_(usd)",
		"sum_of_cp_gross_margin_(usd)",
		"sum_of_cp_gross_revenue_(usd)",
		"sum_of_cp_total_cost_of_sales_(usd)",
		"sum_of_gross_revenue_(usd)",
		sum_of_net_revenue,
		"sum_of_order_item_net_value_(usd)",
		batch_run_dt,
		source_nm,
		created_by,
		create_dt
		)
		SELECT 
		cp_backlog_sni_revenue,
		delivery_identifier,
		sum_of_sales_order_base_quantity,
		process_timestamp,
		sum_of_order_item_net_value_document_currency,
		"sum_of_net_revenue_(usd)",
		segment_level_2_description,
		TRIM(LEADING '0' FROM SALES_ORDER_IDENTIFIER) AS Sales_Order_Identifier,
		TRIM(LEADING '0' FROM Sales_Order_Line_Item_Identifier) AS Sales_Order_Line_Item_Identifier,			
		order_create_date,
		shipment_actual_goods_movement_date,
		end_customer_party_name,
		cp_route_to_market,
		"sum_of_cp_net_revenue_(usd)",
		sc_risk_assessment_category,
		profit_center_hierarchy_level_7_description,
		profit_center_hierarchy_level_7,
		include_exclude,
		customer_po_number,
		financial_close_calendar_year_month_code,
		order_type,
		profit_center_hierarchy_level_3_description,
		profit_center_hierarchy_level_4_description,
		route_to_market,
		sales_order_completion_status_code,
		material_number,
		ship_to_party_name,
		sold_to_party_name,
		source_type,
		sum_of_base_quantity,
		sum_of_cp_unit_quantity,
		"sum_of_cp_enterprise_standard_cost_(usd)",
		"sum_of_cp_factory_margin_(usd)",
		"sum_of_cp_gross_margin_(usd)",
		"sum_of_cp_gross_revenue_(usd)",
		"sum_of_cp_total_cost_of_sales_(usd)",
		"sum_of_gross_revenue_(usd)",
		sum_of_net_revenue,
		"sum_of_order_item_net_value_(usd)",
		batch_run_dt,
		source_nm,
		created_by,
		create_dt
		FROM 
		(Select * from 
		(Select X.*
			 , row_number() over 
			   (partition by Order_Type, Order_Create_Date, SALES_ORDER_IDENTIFIER, Sales_Order_Line_Item_Identifier, Source_Type, Shipment_Actual_Goods_Movement_Date, delivery_identifier, segment_level_2_description
				order by CP_Type_Priority, Quantity_Priority) as row_num
		From (Select *
				   , case when sum_of_sales_order_base_quantity <> 0 and "Sum_of_CP_Net_Revenue_(USD)" <> 0
						  then 1
						  when sum_of_sales_order_base_quantity = 0 and "Sum_of_CP_Net_Revenue_(USD)" <> 0
						  then 2
						  else 3
					 end as Quantity_Priority
				   , case when cp_backlog_sni_revenue = 'REV'
						  then 1
						  when cp_backlog_sni_revenue = 'BACKLOG'
						  then 2
						  else 3
					 end as CP_Type_Priority
		from CURATED.CUR_REVENUE_EGI_APJ_INC
		) X	) Y Where Y.row_num = 1) A;

	RAISE LOG 'Data Ingestion to Intermediate table completed successfully for APJ region';
	
	/* Checking Data Quality and passing the rejected rows to REJECTED_EGI_DATA table */
	call published.sp_dq_check('EGI');		

	-- Step 1: Update the current records to set them to inactive
	RAISE LOG 'Update the current records to set them to inactive for APJ region';		

	UPDATE published.R_REVENUE_EGI_APJ AS target
	SET 
		is_current_fg = 'N',
		end_date = CURRENT_DATE,
		LAST_UPDATE_DT = SYSDATE
	FROM INTERMEDIATE.I_REVENUE_EGI_SCD2 AS delt
	WHERE 
		TRIM(delt.segment_level_2_description)='Asia Pacific' AND TRIM(target.is_current_fg) = 'Y'
	--	target.Order_Type IS NOT NULL AND delt.Order_Type IS NOT NULL
	--    AND target.order_create_calendar_date IS NOT NULL AND delt.Order_Create_Date IS NOT NULL
	--    AND target.SALES_ORDER_IDENTIFIER IS NOT NULL AND delt.SALES_ORDER_IDENTIFIER IS NOT NULL
	--   AND target.sales_order_line_item_number IS NOT NULL AND delt.Sales_Order_Line_Item_Identifier IS NOT NULL
	--    AND target.Source_Type IS NOT NULL AND delt.Source_Type IS NOT NULL
	--    AND target.shipment_date IS NOT NULL AND delt.Shipment_Actual_Goods_Movement_Date IS NOT NULL
	--    AND target.delivery_identifier IS NOT NULL AND delt.delivery_identifier IS NOT NULL
	--    AND target.segment_level_2_description IS NOT NULL AND delt.segment_level_2_description IS NOT NULL
	--    AND 
		AND COALESCE(TRIM(target.Order_Type),'') = COALESCE(TRIM(delt.Order_Type),'')
		AND COALESCE(TRIM(target.order_create_calendar_date),'1900-01-01') = COALESCE(TRIM(delt.Order_Create_Date),'1900-01-01')
		AND COALESCE(TRIM(target.SALES_ORDER_IDENTIFIER),'') = COALESCE(TRIM(delt.SALES_ORDER_IDENTIFIER),'')
		AND COALESCE(TRIM(target.sales_order_line_item_number),'') = COALESCE(TRIM(delt.Sales_Order_Line_Item_Identifier),'')
		AND COALESCE(TRIM(target.Source_Type),'') = COALESCE(TRIM(delt.Source_Type),'')
		AND COALESCE(TRIM(target.shipment_date),'1900-01-01') = COALESCE(TRIM(delt.Shipment_Actual_Goods_Movement_Date),'1900-01-01')
		AND COALESCE(TRIM(target.delivery_identifier),'') = COALESCE(TRIM(delt.delivery_identifier),'')
		AND TRIM(target.process_timestamp) <> TRIM(delt.process_timestamp);
	
	-- Step 2: Insert new or changed records from the delt table into the TARGET table	
	RAISE LOG 'Insert new or changed records from the INTERMEDIATE.I_REVENUE_EGI_SCD2 table into the PUBLISHED.R_REVENUE_EGI_APJ table for APJ region';			

	-- Insert into final Revenue Table PUBLISHED.R_REVENUE_EGI_APJ
	INSERT INTO PUBLISHED.R_REVENUE_EGI_APJ 
	(
		cp_backlog_sni_revenue
		,delivery_identifier
		,sum_of_sales_order_base_quantity
		,process_timestamp
		,sum_of_order_item_net_value_document_currency
		,fiscal_quarter
		,eg_net_revenue
		,region
		,region_id
		,sales_order_identifier
		,sales_order_line_item_number
		,sap_order_key
		,order_create_calendar_date
		,shipment_date
		,eg_end_customer_name
		,final_rtm
		,secured_position_net_usd
		,sc_bklg_catg
		,sku_description
		,include_exclude
		,business_area_code
		,"Customer_PO_Number" ,
		"Financial_Close_Calendar_Year_Month_Code",
		"Order_Type" ,
		"Profit_Center_Hierarchy_Level_3_Description",
		"Profit_Center_Hierarchy_Level_4_Description" ,
		"Route_To_Market",
		"Sales_Order_Completion_Status_Code"  ,
		"Material_Number",
		"Source_Type" ,
		"Sum_of_Base_Quantity"  ,
		"Sum_of_CP_Unit_Quantity"  ,
		"Sum_of_CP_Enterprise_Standard_Cost_(USD)" ,
		"Sum_of_CP_Factory_Margin_(USD)" ,
		"Sum_of_CP_Gross_Margin_(USD)" ,
		"Sum_of_CP_Gross_Revenue_(USD)"  ,
		"Sum_of_CP_Total_Cost_of_Sales_(USD)" ,
		"Sum_of_Gross_Revenue_(USD)"  ,
		"Sum_of_Net_Revenue" ,
		"Sum_of_Order_Item_Net_Value_(USD)" ,
		sold_to_customer_sta 
		,ship_to_customer_sta
		,is_current_fg
		,batch_run_dt
		,source_nm
		,created_by
		,start_date 
		,end_date			
		)
		SELECT 
			cp_backlog_sni_revenue,
			delivery_identifier,
			sum_of_sales_order_base_quantity,
			process_timestamp,
			sum_of_order_item_net_value_document_currency,
			(B.FISCAL_YEAR_QUARTER + RIGHT (B.FISCAL_YEAR,2)) AS FISCAL_QUARTER, 
			"Sum_of_CP_Net_Revenue_(USD)",
			'APJ' AS REGION,
			502 AS REGION_ID,
			TRIM(LEADING '0' FROM SALES_ORDER_IDENTIFIER) AS SALES_ORDER_IDENTIFIER,
			TRIM(LEADING '0' FROM Sales_Order_Line_Item_Identifier) AS Sales_Order_Line_Item_Identifier,
			(TRIM(LEADING '0' FROM SALES_ORDER_IDENTIFIER) + '-' + TRIM(LEADING '0' FROM Sales_Order_Line_Item_Identifier)) AS SAP_ORDER_KEY,
			Order_Create_Date,
			Shipment_Actual_Goods_Movement_Date,
			End_Customer_Party_Name,
			CP_Route_To_Market,
			"Sum_of_Net_Revenue_(USD)",
			SC_Risk_Assessment_Category,
			Profit_Center_Hierarchy_Level_7_Description,
			Include_Exclude,
			"Profit_Center_Hierarchy_Level_7",
			Customer_PO_Number,
			Financial_Close_Calendar_Year_Month_Code,	
			"Order_Type" ,
			"Profit_Center_Hierarchy_Level_3_Description",
			"Profit_Center_Hierarchy_Level_4_Description" ,
			"Route_To_Market",
			"Sales_Order_Completion_Status_Code"  ,
			"Material_Number",
			"Source_Type" ,
			"Sum_of_Base_Quantity"  ,
			"Sum_of_CP_Unit_Quantity"  ,
			"Sum_of_CP_Enterprise_Standard_Cost_(USD)" ,
			"Sum_of_CP_Factory_Margin_(USD)" ,
			"Sum_of_CP_Gross_Margin_(USD)" ,
			"Sum_of_CP_Gross_Revenue_(USD)"  ,
			"Sum_of_CP_Total_Cost_of_Sales_(USD)" ,
			"Sum_of_Gross_Revenue_(USD)"  ,
			"Sum_of_Net_Revenue" ,
			"Sum_of_Order_Item_Net_Value_(USD)" ,
			Sold_To_Party_Name
			,Ship_To_Party_Name ,
			'Y' AS IS_CURRENT_FG,
			A.BATCH_RUN_DT,
			A.SOURCE_NM, 
			A.CREATED_BY,
			CURRENT_DATE AS start_date, 
			NULL AS end_date
		FROM 
		INTERMEDIATE.I_REVENUE_EGI_SCD2 AS A
		LEFT JOIN PUBLISHED.D_DATE_CALENDAR B ON A.BATCH_RUN_DT = B.DATE 
		-- Logic as provided by the SC Analytics Team
		WHERE 
		TRIM(A.segment_level_2_description)='Asia Pacific' AND
		"Sum_of_Net_Revenue_(USD)" >= 0 AND
		Sales_Order_Identifier NOT LIKE '%Grand%' AND
		Sales_Order_Line_Item_Identifier NOT LIKE '%?%' AND 
		Sales_Order_Identifier NOT LIKE '%?%' AND
		NOT EXISTS (
			SELECT 1 
			FROM published.R_REVENUE_EGI_APJ AS target
			WHERE 
				TRIM(target.IS_CURRENT_FG) = 'Y'
				AND COALESCE(TRIM(target.Order_Type),'') = COALESCE(TRIM(A.Order_Type),'')
				AND COALESCE(TRIM(target.order_create_calendar_date),'1900-01-01') = COALESCE(TRIM(A.Order_Create_Date),'1900-01-01')
				AND COALESCE(TRIM(target.SALES_ORDER_IDENTIFIER),'') = COALESCE(TRIM(A.SALES_ORDER_IDENTIFIER),'')
				AND COALESCE(TRIM(target.sales_order_line_item_number),'') = COALESCE(TRIM(A.Sales_Order_Line_Item_Identifier),'')
				AND COALESCE(TRIM(target.Source_Type),'') = COALESCE(TRIM(A.Source_Type),'')
				AND COALESCE(TRIM(target.shipment_date),'1900-01-01') = COALESCE(TRIM(A.Shipment_Actual_Goods_Movement_Date),'1900-01-01')
				AND COALESCE(TRIM(target.delivery_identifier),'') = COALESCE(TRIM(A.delivery_identifier),'')
				AND TRIM(target.process_timestamp) = TRIM(A.process_timestamp)
		);


	RAISE LOG 'Data Inserted into PUBLISED.R_REVENUE_EGI_APJ from the the INTERMEDIATE.I_REVENUE_EGI_SCD2 table';
	
	--Doing the Aggregation and move the EG value to L1 table
	TRUNCATE TABLE PUBLISHED.RPT_EGI_APJ_DATA;

	RAISE LOG 'Data Ingestion to PUBLISHED.RPT_EGI_APJ_DATA table started';

	INSERT INTO PUBLISHED.RPT_EGI_APJ_DATA 
	SELECT SAP_ORDER_KEY,SUM(secured_position_net_usd) ,SUM(eg_net_revenue)
	FROM PUBLISHED.R_REVENUE_EGI_APJ 
	WHERE IS_CURRENT_FG = 'Y'
	GROUP BY SAP_ORDER_KEY;

	RAISE LOG 'Data Inserted into PUBLISHED.RPT_EGI_APJ_DATA from PUBLISHED.R_REVENUE_EGI_APJ';


End If;

IF v_region = 'EMEA' THEN 
	
	RAISE LOG 'Data Ingestion to INTERMEDIATE.I_REVENUE_EGI_SCD2 table started for EMEA region';

	INSERT INTO INTERMEDIATE.I_REVENUE_EGI_SCD2
	(		
		cp_backlog_sni_revenue,
		delivery_identifier,
		sum_of_sales_order_base_quantity,
		process_timestamp,
		sum_of_order_item_net_value_document_currency,
		"sum_of_net_revenue_(usd)",
		segment_level_2_description,
		sales_order_identifier,
		sales_order_line_item_identifier,
		order_create_date,
		shipment_actual_goods_movement_date,
		end_customer_party_name,
		cp_route_to_market,
		"sum_of_cp_net_revenue_(usd)",
		sc_risk_assessment_category,
		profit_center_hierarchy_level_7_description,
		profit_center_hierarchy_level_7,
		include_exclude,
		customer_po_number,
		financial_close_calendar_year_month_code,
		order_type,
		profit_center_hierarchy_level_3_description,
		profit_center_hierarchy_level_4_description,
		route_to_market,
		sales_order_completion_status_code,
		material_number,
		ship_to_party_name,
		sold_to_party_name,
		source_type,
		sum_of_base_quantity,
		sum_of_cp_unit_quantity,
		"sum_of_cp_enterprise_standard_cost_(usd)",
		"sum_of_cp_factory_margin_(usd)",
		"sum_of_cp_gross_margin_(usd)",
		"sum_of_cp_gross_revenue_(usd)",
		"sum_of_cp_total_cost_of_sales_(usd)",
		"sum_of_gross_revenue_(usd)",
		sum_of_net_revenue,
		"sum_of_order_item_net_value_(usd)",
		batch_run_dt,
		source_nm,
		created_by,
		create_dt
		)
		SELECT 
		cp_backlog_sni_revenue,
		delivery_identifier,
		sum_of_sales_order_base_quantity,
		process_timestamp,
		sum_of_order_item_net_value_document_currency,
		"sum_of_net_revenue_(usd)",
		segment_level_2_description,
		TRIM(LEADING '0' FROM SALES_ORDER_IDENTIFIER) AS Sales_Order_Identifier,
		TRIM(LEADING '0' FROM Sales_Order_Line_Item_Identifier) AS Sales_Order_Line_Item_Identifier,			
		order_create_date,
		shipment_actual_goods_movement_date,
		end_customer_party_name,
		cp_route_to_market,
		"sum_of_cp_net_revenue_(usd)",
		sc_risk_assessment_category,
		profit_center_hierarchy_level_7_description,
		profit_center_hierarchy_level_7,
		include_exclude,
		customer_po_number,
		financial_close_calendar_year_month_code,
		order_type,
		profit_center_hierarchy_level_3_description,
		profit_center_hierarchy_level_4_description,
		route_to_market,
		sales_order_completion_status_code,
		material_number,
		ship_to_party_name,
		sold_to_party_name,
		source_type,
		sum_of_base_quantity,
		sum_of_cp_unit_quantity,
		"sum_of_cp_enterprise_standard_cost_(usd)",
		"sum_of_cp_factory_margin_(usd)",
		"sum_of_cp_gross_margin_(usd)",
		"sum_of_cp_gross_revenue_(usd)",
		"sum_of_cp_total_cost_of_sales_(usd)",
		"sum_of_gross_revenue_(usd)",
		sum_of_net_revenue,
		"sum_of_order_item_net_value_(usd)",
		batch_run_dt,
		source_nm,
		created_by,
		create_dt
		FROM 
		(Select * from 
		(Select X.*
			 , row_number() over 
			   (partition by Order_Type, Order_Create_Date, SALES_ORDER_IDENTIFIER, Sales_Order_Line_Item_Identifier, Source_Type, Shipment_Actual_Goods_Movement_Date, delivery_identifier, segment_level_2_description
				order by CP_Type_Priority, Quantity_Priority) as row_num
		From (Select *
				   , case when sum_of_sales_order_base_quantity <> 0 and "Sum_of_CP_Net_Revenue_(USD)" <> 0
						  then 1
						  when sum_of_sales_order_base_quantity = 0 and "Sum_of_CP_Net_Revenue_(USD)" <> 0
						  then 2
						  else 3
					 end as Quantity_Priority
				   , case when cp_backlog_sni_revenue = 'REV'
						  then 1
						  when cp_backlog_sni_revenue = 'BACKLOG'
						  then 2
						  else 3
					 end as CP_Type_Priority
		from CURATED.CUR_REVENUE_EGI_EMEA_INC
		) X	) Y Where Y.row_num = 1) A;

	RAISE LOG 'Data Ingestion to Intermediate table completed successfully for EMEA region';

	/* Checking Data Quality and passing the rejected rows to REJECTED_EGI_DATA table */
	call published.sp_dq_check('EGI');	

	-- Step 1: Update the current records to set them to inactive
	RAISE LOG 'Update the current records to set them to inactive for EMEA region';		

	UPDATE published.R_REVENUE_EGI_EMEA AS target
	SET 
		is_current_fg = 'N',
		end_date = CURRENT_DATE,
		LAST_UPDATE_DT = SYSDATE
	FROM INTERMEDIATE.I_REVENUE_EGI_SCD2 AS delt
	WHERE 
		TRIM(delt.segment_level_2_description)='EMEA' AND TRIM(target.is_current_fg) = 'Y'
	--	target.Order_Type IS NOT NULL AND delt.Order_Type IS NOT NULL
	--    AND target.order_create_calendar_date IS NOT NULL AND delt.Order_Create_Date IS NOT NULL
	--    AND target.SALES_ORDER_IDENTIFIER IS NOT NULL AND delt.SALES_ORDER_IDENTIFIER IS NOT NULL
	--   AND target.sales_order_line_item_number IS NOT NULL AND delt.Sales_Order_Line_Item_Identifier IS NOT NULL
	--    AND target.Source_Type IS NOT NULL AND delt.Source_Type IS NOT NULL
	--    AND target.shipment_date IS NOT NULL AND delt.Shipment_Actual_Goods_Movement_Date IS NOT NULL
	--    AND target.delivery_identifier IS NOT NULL AND delt.delivery_identifier IS NOT NULL
	--    AND target.segment_level_2_description IS NOT NULL AND delt.segment_level_2_description IS NOT NULL
	--    AND 
		AND COALESCE(TRIM(target.Order_Type),'') = COALESCE(TRIM(delt.Order_Type),'')
		AND COALESCE(TRIM(target.order_create_calendar_date),'1900-01-01') = COALESCE(TRIM(delt.Order_Create_Date),'1900-01-01')
		AND COALESCE(TRIM(target.SALES_ORDER_IDENTIFIER),'') = COALESCE(TRIM(delt.SALES_ORDER_IDENTIFIER),'')
		AND COALESCE(TRIM(target.sales_order_line_item_number),'') = COALESCE(TRIM(delt.Sales_Order_Line_Item_Identifier),'')
		AND COALESCE(TRIM(target.Source_Type),'') = COALESCE(TRIM(delt.Source_Type),'')
		AND COALESCE(TRIM(target.shipment_date),'1900-01-01') = COALESCE(TRIM(delt.Shipment_Actual_Goods_Movement_Date),'1900-01-01')
		AND COALESCE(TRIM(target.delivery_identifier),'') = COALESCE(TRIM(delt.delivery_identifier),'')
		AND TRIM(target.process_timestamp) <> TRIM(delt.process_timestamp);
	

	RAISE LOG 'Insert new or changed records from the INTERMEDIATE.I_REVENUE_EGI_SCD2 table into the PUBLISHED.R_REVENUE_EGI_EMEA table for EMEA region';			
	-- Step 2: Insert new or changed records from the delt table into the TARGET table

	-- Insert into final Revenue Table PUBLISHED.R_REVENUE_EGI_EMEA
	INSERT INTO PUBLISHED.R_REVENUE_EGI_EMEA 
	(
		cp_backlog_sni_revenue
		,delivery_identifier
		,sum_of_sales_order_base_quantity
		,process_timestamp
		,sum_of_order_item_net_value_document_currency
		,fiscal_quarter
		,eg_net_revenue
		,region
		,region_id
		,sales_order_identifier
		,sales_order_line_item_number
		,sap_order_key
		,order_create_calendar_date
		,shipment_date
		,eg_end_customer_name
		,final_rtm
		,secured_position_net_usd
		,sc_bklg_catg
		,sku_description
		,include_exclude
		,business_area_code
		,"Customer_PO_Number" ,
		"Financial_Close_Calendar_Year_Month_Code",
		"Order_Type" ,
		"Profit_Center_Hierarchy_Level_3_Description",
		"Profit_Center_Hierarchy_Level_4_Description" ,
		"Route_To_Market",
		"Sales_Order_Completion_Status_Code"  ,
		"Material_Number",
		"Source_Type" ,
		"Sum_of_Base_Quantity"  ,
		"Sum_of_CP_Unit_Quantity"  ,
		"Sum_of_CP_Enterprise_Standard_Cost_(USD)" ,
		"Sum_of_CP_Factory_Margin_(USD)" ,
		"Sum_of_CP_Gross_Margin_(USD)" ,
		"Sum_of_CP_Gross_Revenue_(USD)"  ,
		"Sum_of_CP_Total_Cost_of_Sales_(USD)" ,
		"Sum_of_Gross_Revenue_(USD)"  ,
		"Sum_of_Net_Revenue" ,
		"Sum_of_Order_Item_Net_Value_(USD)" ,
		sold_to_customer_sta 
		,ship_to_customer_sta
		,is_current_fg
		,batch_run_dt
		,source_nm
		,created_by
		,start_date 
		,end_date			
	)
	SELECT 
		cp_backlog_sni_revenue,
		delivery_identifier,
		sum_of_sales_order_base_quantity,
		process_timestamp,
		sum_of_order_item_net_value_document_currency,
		(B.FISCAL_YEAR_QUARTER + RIGHT (B.FISCAL_YEAR,2)) AS FISCAL_QUARTER, 
		"Sum_of_CP_Net_Revenue_(USD)",
		'EMEA' AS REGION,
		500 AS REGION_ID,
		TRIM(LEADING '0' FROM SALES_ORDER_IDENTIFIER) AS SALES_ORDER_IDENTIFIER,
		TRIM(LEADING '0' FROM Sales_Order_Line_Item_Identifier) AS Sales_Order_Line_Item_Identifier,
		(TRIM(LEADING '0' FROM SALES_ORDER_IDENTIFIER) + '-' + TRIM(LEADING '0' FROM Sales_Order_Line_Item_Identifier) ) AS SAP_ORDER_KEY,
		Order_Create_Date,
		Shipment_Actual_Goods_Movement_Date,
		End_Customer_Party_Name,
		CP_Route_To_Market,
		"Sum_of_Net_Revenue_(USD)",
		SC_Risk_Assessment_Category,
		Profit_Center_Hierarchy_Level_7_Description,
		Include_Exclude,
		"Profit_Center_Hierarchy_Level_7",
		Customer_PO_Number,
		Financial_Close_Calendar_Year_Month_Code,	
		"Order_Type" ,
		"Profit_Center_Hierarchy_Level_3_Description",
		"Profit_Center_Hierarchy_Level_4_Description" ,
		"Route_To_Market",
		"Sales_Order_Completion_Status_Code"  ,
		"Material_Number",
		"Source_Type" ,
		"Sum_of_Base_Quantity"  ,
		"Sum_of_CP_Unit_Quantity"  ,
		"Sum_of_CP_Enterprise_Standard_Cost_(USD)" ,
		"Sum_of_CP_Factory_Margin_(USD)" ,
		"Sum_of_CP_Gross_Margin_(USD)" ,
		"Sum_of_CP_Gross_Revenue_(USD)"  ,
		"Sum_of_CP_Total_Cost_of_Sales_(USD)" ,
		"Sum_of_Gross_Revenue_(USD)"  ,
		"Sum_of_Net_Revenue" ,
		"Sum_of_Order_Item_Net_Value_(USD)" ,
		Sold_To_Party_Name
		,Ship_To_Party_Name ,
		'Y' AS IS_CURRENT_FG,
		A.BATCH_RUN_DT,
		A.SOURCE_NM, 
		A.CREATED_BY,
		CURRENT_DATE AS start_date, 
		NULL AS end_date
	FROM 
	INTERMEDIATE.I_REVENUE_EGI_SCD2 AS A
	LEFT JOIN PUBLISHED.D_DATE_CALENDAR B ON A.BATCH_RUN_DT = B.DATE 
	-- Logic as provided by the SC Analytics Team
	WHERE 
	TRIM(A.segment_level_2_description)='EMEA' AND
	Sales_Order_Identifier NOT LIKE '%Grand%' AND
	Sales_Order_Line_Item_Identifier NOT LIKE '%?%' AND
	Sales_Order_Identifier NOT LIKE '%?%' AND
	NOT EXISTS (
		SELECT 1 
		FROM published.R_REVENUE_EGI_EMEA AS target
		WHERE 
			TRIM(target.IS_CURRENT_FG) = 'Y'
			AND COALESCE(TRIM(target.Order_Type),'') = COALESCE(TRIM(A.Order_Type),'')
			AND COALESCE(TRIM(target.order_create_calendar_date),'1900-01-01') = COALESCE(TRIM(A.Order_Create_Date),'1900-01-01')
			AND COALESCE(TRIM(target.SALES_ORDER_IDENTIFIER),'') = COALESCE(TRIM(A.SALES_ORDER_IDENTIFIER),'')
			AND COALESCE(TRIM(target.sales_order_line_item_number),'') = COALESCE(TRIM(A.Sales_Order_Line_Item_Identifier),'')
			AND COALESCE(TRIM(target.Source_Type),'') = COALESCE(TRIM(A.Source_Type),'')
			AND COALESCE(TRIM(target.shipment_date),'1900-01-01') = COALESCE(TRIM(A.Shipment_Actual_Goods_Movement_Date),'1900-01-01')
			AND COALESCE(TRIM(target.delivery_identifier),'') = COALESCE(TRIM(A.delivery_identifier),'')
			AND TRIM(target.process_timestamp) = TRIM(A.process_timestamp)
	);

	RAISE LOG 'Data cleansing process Completed';
	RAISE LOG 'Data Inserted into PUBLISED.R_REVENUE_EGI_EMEA from the the INTERMEDIATE.I_REVENUE_EGI_SCD2 table';

	--Doing the Aggregation and move the EG value to L1 table
	TRUNCATE TABLE PUBLISHED.RPT_EGI_EMEA_DATA;

	RAISE LOG 'Data Ingestion to PUBLISHED.RPT_EGI_EMEA_DATA table started';

	INSERT INTO PUBLISHED.RPT_EGI_EMEA_DATA 
	SELECT SAP_ORDER_KEY,SUM(secured_position_net_usd) ,SUM(eg_net_revenue)
	FROM PUBLISHED.R_REVENUE_EGI_EMEA WHERE IS_CURRENT_FG = 'Y'
	GROUP BY sap_order_key;

	RAISE LOG 'Data Inserted into PUBLISHED.RPT_EGI_EMEA_DATA from PUBLISHED.R_REVENUE_EGI_EMEA';
	
End If;		

RAISE LOG 'Procedure PUBLISHED.SP_CURTOPUB_R_REVENUE_EGI_SCD2 Completed';

EXCEPTION
  WHEN OTHERS THEN
  RAISE log 'Inside PUBLISHED.SP_CURTOPUB_R_REVENUE_EGI_SCD2 exception block';
  RAISE EXCEPTION 'error message SQLERRM % SQLSTATE %',  SQLERRM, SQLSTATE;
END;




















$$
;
