import cx_Oracle
import os
import configparser
import pandas as pd
#import logging
import sqlalchemy
from datetime import date
from datetime import time
from datetime import datetime
from flex_to_phub_dml import *


"""
Set log path and level of logging
"""
#LOG_FILENAME = 'C:\flex_to_phub_etl.log'

#etl_logger = logging.getLogger('ETLLogger')
#etl_logger.setLevel(logging.

"""
Run the pre sql statement(s) needed before extracting data from
Flex to the Product HUB
"""

def pre_sql(flex_cur,conn):
    for query in presql:
        flex_cur.execute(query)
        conn.commit()
        
"""
Convert NaN, and NaT to None value = NULL
This will allow Oracle to accept updates and inserts to 
attributes that have NULL
"""

def convert_to_null(v_num):
    x =pd.isnull(v_num)
    if x == True:
        v_num = None
    return v_num

def etl_process(flexconn,hubconn,flex_cur,hub_cur):
    
    """
    Load data from Flex and HUB into dataframes
    Then merge the dataframe based on left join on flex_id
    """
    
    dfHUB = pd.read_sql('select flex_id from CDC_FLEX_MAKEABLESELLABLE', hubconn)
    dfFlex = pd.read_sql('select * from WRK_CARTERS_MAKEABLESELLABLE', flexconn )
    dfDelta = pd.merge(dfFlex, dfHUB, on='FLEX_ID' , how='left', indicator=True)
    
    inscount =0
    updcount =0
    etl_status = None
    
    """
    Check to see if there's any data to process
    """
    data_cnt = dfDelta.FLEX_ID.count()
    
    if data_cnt == 0:
        print('No data found to be processed ... ')
        print('ETL process will end')
        return       
    
    
    """
    Loop though the dataframe and apply insert if _merge = left_only
    update if _merge=both
    """
    
    for index, row in dfDelta.iterrows():

        v_capxtime = datetime.now()
        v_capxtime=v_capxtime.strftime("%Y" + "%m" + "%d" +"%I" + "%M" + "%S") + "00000"

        if row._merge == 'left_only':
            try:
                hub_cur.execute(sql_insert,{'b_ACTION_FLAG' : convert_to_null(row.ACTION_FLAG),
                                        'b_AGE_CODE' : convert_to_null(row.AGE_CODE),
                                        'b_AGE_KEY' : convert_to_null(row.AGE_KEY),
                                        'b_AGE_VALUE' : convert_to_null(row.AGE_VALUE),
                                        'b_AGENT' : convert_to_null(row.AGENT),
                                        'b_AGENT_ID' : convert_to_null(row.AGENT_ID),
                                        'b_ARTWK_PLACEMENT_COMP' : convert_to_null(row.ARTWK_PLACEMENT_COMP),
                                        'b_BENCHMARK_STYLE' : convert_to_null(row.BENCHMARK_STYLE),
                                        'b_BRAND_CODE' : convert_to_null(row.BRAND_CODE),
                                        'b_BRAND_KEY' : convert_to_null(row.BRAND_KEY),
                                        'b_BRAND_VALUE' : convert_to_null(row.BRAND_VALUE),
                                        'b_BUSINESS_UNIT_CODE' : convert_to_null(row.BUSINESS_UNIT_CODE),
                                        'b_BUSINESS_UNIT_KEY' : convert_to_null(row.BUSINESS_UNIT_KEY),
                                        'b_BUSINESS_UNIT_VALUE' : convert_to_null(row.BUSINESS_UNIT_VALUE),
                                        'b_CLIMATE_CODE' : convert_to_null(row.CLIMATE_CODE),
                                        'b_CLIMATE_KEY' : convert_to_null(row.CLIMATE_KEY),
                                        'b_CLIMATE_VALUE' : convert_to_null(row.CLIMATE_VALUE),
                                        'b_CLIMATE_ZONE_CODE' : convert_to_null(row.CLIMATE_ZONE_CODE),
                                        'b_CLIMATE_ZONE_KEY' : convert_to_null(row.CLIMATE_ZONE_KEY),
                                        'b_CLIMATE_ZONE_VALUE' : convert_to_null(row.CLIMATE_ZONE_VALUE),
                                        'b_COLLECTION_CODE' : convert_to_null(row.COLLECTION_CODE),
                                        'b_COLLECTION_KEY' : convert_to_null(row.COLLECTION_KEY),
                                        'b_COLLECTION_VALUE' : convert_to_null(row.COLLECTION_VALUE),
                                        'b_COLOR' : convert_to_null(row.COLOR),
                                        'b_COLORWAY_NAME' : convert_to_null(row.COLORWAY_NAME),
                                        'b_COLORWAY_SEQUENCE' : convert_to_null(row.COLORWAY_SEQUENCE),
                                        'b_COLORWAY_STATUS_CODE' : convert_to_null(row.COLORWAY_STATUS_CODE),
                                        'b_COLORWAY_STATUS_KEY' : convert_to_null(row.COLORWAY_STATUS_KEY),
                                        'b_COLORWAY_STATUS_VALUE' : convert_to_null(row.COLORWAY_STATUS_VALUE),
                                        'b_COLORWAY_THUMBNAIL_URL' : convert_to_null(row.COLORWAY_THUMBNAIL_URL),
                                        'b_COMPONENT1_SILH' : convert_to_null(row.COMPONENT1_SILH),
                                        'b_COMPONENT2_SILH' : convert_to_null(row.COMPONENT2_SILH),
                                        'b_COMPONENT3_SILH' : convert_to_null(row.COMPONENT3_SILH),
                                        'b_COMPONENT4_SILH' : convert_to_null(row.COMPONENT4_SILH),
                                        'b_COMPONENT5_SILH' : convert_to_null(row.COMPONENT5_SILH),
                                        'b_COMPONENT6_SILH' : convert_to_null(row.COMPONENT6_SILH),
                                        'b_COMPONENT7_SILH' : convert_to_null(row.COMPONENT7_SILH),
                                        'b_CORE_BODY_CODE' : convert_to_null(row.CORE_BODY_CODE),
                                        'b_CORE_BODY_DESC_CODE' : convert_to_null(row.CORE_BODY_DESC_CODE),
                                        'b_CORE_BODY_DESC_KEY' : convert_to_null(row.CORE_BODY_DESC_KEY),
                                        'b_CORE_BODY_DESC_VALUE' : convert_to_null(row.CORE_BODY_DESC_VALUE),
                                        'b_CORE_BODY_KEY' : convert_to_null(row.CORE_BODY_KEY),
                                        'b_CORE_BODY_VALUE' : convert_to_null(row.CORE_BODY_VALUE),
                                        'b_CURRENCY_CODE' : convert_to_null(row.CURRENCY_CODE),
                                        'b_CURRENCY_KEY' : convert_to_null(row.CURRENCY_KEY),
                                        'b_CURRENCY_VALUE' : convert_to_null(row.CURRENCY_VALUE),
                                        'b_DELETE_FLAG' : convert_to_null(row.DELETE_FLAG),
                                        'b_DELIVERY_DATE' : convert_to_null(row.DELIVERY_DATE),
                                        'b_DELIVERY_GROUP_CODE' : convert_to_null(row.DELIVERY_GROUP_CODE),
                                        'b_DELIVERY_GROUP_KEY' : convert_to_null(row.DELIVERY_GROUP_KEY),
                                        'b_DELIVERY_GROUP_VALUE' : convert_to_null(row.DELIVERY_GROUP_VALUE),
                                        'b_DESCRIPTION' : convert_to_null(row.DESCRIPTION),
                                        'b_DESCRIPTION_FOR_TAGS_CODE' : convert_to_null(row.DESCRIPTION_FOR_TAGS_CODE),
                                        'b_DESCRIPTION_FOR_TAGS_KEY' : convert_to_null(row.DESCRIPTION_FOR_TAGS_KEY),
                                        'b_DESCRIPTION_FOR_TAGS_VALUE' : convert_to_null(row.DESCRIPTION_FOR_TAGS_VALUE),
                                        'b_DESIGN_SAMPLE' : convert_to_null(row.DESIGN_SAMPLE),
                                        'b_DESIGNER' : convert_to_null(row.DESIGNER),
                                        'b_DISCONTINUE_DATE' : convert_to_null(row.DISCONTINUE_DATE),
                                        'b_ECOMM_LAUNCH_DATE' : convert_to_null(row.ECOMM_LAUNCH_DATE),
                                        'b_EXCESS_INVENTORY_DATE' : convert_to_null(row.EXCESS_INVENTORY_DATE),
                                        'b_EXCLUSIVE_CHANNEL_CODE' : convert_to_null(row.EXCLUSIVE_CHANNEL_CODE),
                                        'b_EXCLUSIVE_CHANNEL_KEY' : convert_to_null(row.EXCLUSIVE_CHANNEL_KEY),
                                        'b_EXCLUSIVE_CHANNEL_VALUE' : convert_to_null(row.EXCLUSIVE_CHANNEL_VALUE),
                                        'b_FABRIC_CONTENT_CODE' : convert_to_null(row.FABRIC_CONTENT_CODE),
                                        'b_FABRIC_CONTENT_KEY' : convert_to_null(row.FABRIC_CONTENT_KEY),
                                        'b_FABRIC_CONTENT_VALUE' : convert_to_null(row.FABRIC_CONTENT_VALUE),
                                        'b_FABRIC_FAMILY_CODE' : convert_to_null(row.FABRIC_FAMILY_CODE),
                                        'b_FABRIC_FAMILY_KEY' : convert_to_null(row.FABRIC_FAMILY_KEY),
                                        'b_FABRIC_FAMILY_VALUE' : convert_to_null(row.FABRIC_FAMILY_VALUE),
                                        'b_FABRIC_GROUP_CODE' : convert_to_null(row.FABRIC_GROUP_CODE),
                                        'b_FABRIC_GROUP_KEY' : convert_to_null(row.FABRIC_GROUP_KEY),
                                        'b_FABRIC_GROUP_VALUE' : convert_to_null(row.FABRIC_GROUP_VALUE),
                                        'b_FACTORY' : convert_to_null(row.FACTORY),
                                        'b_FACTORY_ID' : convert_to_null(row.FACTORY_ID),
                                        'b_FR_CODE' : convert_to_null(row.FR_CODE),
                                        'b_FR_KEY' : convert_to_null(row.FR_KEY),
                                        'b_FR_VALUE' : convert_to_null(row.FR_VALUE),
                                        'b_IS_CARRYOVER' : convert_to_null(row.IS_CARRYOVER),
                                        'b_IS_QA_COMMENTS' : convert_to_null(row.IS_QA_COMMENTS),
                                        'b_IS_SET_CODE' : convert_to_null(row.IS_SET_CODE),
                                        'b_IS_SET_KEY' : convert_to_null(row.IS_SET_KEY),
                                        'b_IS_SET_VALUE' : convert_to_null(row.IS_SET_VALUE),
                                        'b_LAST_UPDATED' : convert_to_null(row.LAST_UPDATED),
                                        'b_LAUNCH_DATE_OVERWRITE' : convert_to_null(row.LAUNCH_DATE_OVERWRITE),
                                        'b_LIFECYCLE_STATE_CODE' : convert_to_null(row.LIFECYCLE_STATE_CODE),
                                        'b_LIFECYCLE_STATE_KEY' : convert_to_null(row.LIFECYCLE_STATE_KEY),
                                        'b_LIFECYCLE_STATE_VALUE' : convert_to_null(row.LIFECYCLE_STATE_VALUE),
                                        'b_LIFESTYLE_CODE' : convert_to_null(row.LIFESTYLE_CODE),
                                        'b_LIFESTYLE_KEY' : convert_to_null(row.LIFESTYLE_KEY),
                                        'b_LIFESTYLE_VALUE' : convert_to_null(row.LIFESTYLE_VALUE),
                                        'b_MANUFACTURING_PIECES' : convert_to_null(row.MANUFACTURING_PIECES),
                                        'b_MSRP' : convert_to_null(row.MSRP),
                                        'b_NRF_COLOR_CODE_CODE' : convert_to_null(row.NRF_COLOR_CODE_CODE),
                                        'b_NRF_COLOR_CODE_KEY' : convert_to_null(row.NRF_COLOR_CODE_KEY),
                                        'b_NRF_COLOR_CODE_VALUE' : convert_to_null(row.NRF_COLOR_CODE_VALUE),
                                        'b_NRF_COLOR_NAME_CODE' : convert_to_null(row.NRF_COLOR_NAME_CODE),
                                        'b_NRF_COLOR_NAME_KEY' : convert_to_null(row.NRF_COLOR_NAME_KEY),
                                        'b_NRF_COLOR_NAME_VALUE' : convert_to_null(row.NRF_COLOR_NAME_VALUE),
                                        'b_PARENT_SUPPLIER' : convert_to_null(row.PARENT_SUPPLIER),
                                        'b_PARENT_SUPPLIER_ID' : convert_to_null(row.PARENT_SUPPLIER_ID),
                                        'b_PARENT_VENDOR_ID' : convert_to_null(row.PARENT_VENDOR_ID),
                                        'b_PLANABLE_GENDER_CODE' : convert_to_null(row.PLANABLE_GENDER_CODE),
                                        'b_PLANABLE_GENDER_KEY' : convert_to_null(row.PLANABLE_GENDER_KEY),
                                        'b_PLANABLE_GENDER_VALUE' : convert_to_null(row.PLANABLE_GENDER_VALUE),
                                        'b_PRICE_POINT_CODE' : convert_to_null(row.PRICE_POINT_CODE),
                                        'b_PRICE_POINT_KEY' : convert_to_null(row.PRICE_POINT_KEY),
                                        'b_PRICE_POINT_VALUE' : convert_to_null(row.PRICE_POINT_VALUE),
                                        'b_PRIMARY_MATL_REF' : convert_to_null(row.PRIMARY_MATL_REF),
                                        'b_PRIMARY_SOURCE_PRODUCT' : convert_to_null(row.PRIMARY_SOURCE_PRODUCT),
                                        'b_PRIMARY_SOURCE_SEASON' : convert_to_null(row.PRIMARY_SOURCE_SEASON),
                                        'b_PRODUCT_CLASS_CODE' : convert_to_null(row.PRODUCT_CLASS_CODE),
                                        'b_PRODUCT_CLASS_KEY' : convert_to_null(row.PRODUCT_CLASS_KEY),
                                        'b_PRODUCT_CLASS_VALUE' : convert_to_null(row.PRODUCT_CLASS_VALUE),
                                        'b_PRODUCT_DESCRIPTION' : convert_to_null(row.PRODUCT_DESCRIPTION),
                                        'b_PRODUCT_MANAGER' : convert_to_null(row.PRODUCT_MANAGER),
                                        'b_PRODUCT_NAME' : convert_to_null(row.PRODUCT_NAME),
                                        'b_PRODUCT_NUM' : convert_to_null(row.PRODUCT_NUM),
                                        'b_PRODUCT_SEQ' : convert_to_null(row.PRODUCT_SEQ),
                                        'b_PRODUCT_STATUS_CODE' : convert_to_null(row.PRODUCT_STATUS_CODE),
                                        'b_PRODUCT_STATUS_KEY' : convert_to_null(row.PRODUCT_STATUS_KEY),
                                        'b_PRODUCT_STATUS_VALUE' : convert_to_null(row.PRODUCT_STATUS_VALUE),
                                        'b_PRODUCT_SUB_CLASS_CODE' : convert_to_null(row.PRODUCT_SUB_CLASS_CODE),
                                        'b_PRODUCT_SUB_CLASS_KEY' : convert_to_null(row.PRODUCT_SUB_CLASS_KEY),
                                        'b_PRODUCT_SUB_CLASS_VALUE' : convert_to_null(row.PRODUCT_SUB_CLASS_VALUE),
                                        'b_PRODUCT_THUMB_URL' : convert_to_null(row.PRODUCT_THUMB_URL),
                                        'b_PRODUCTION_GROUP_CODE' : convert_to_null(row.PRODUCTION_GROUP_CODE),
                                        'b_PRODUCTION_GROUP_KEY' : convert_to_null(row.PRODUCTION_GROUP_KEY),
                                        'b_PRODUCTION_GROUP_VALUE' : convert_to_null(row.PRODUCTION_GROUP_VALUE),
                                        'b_PROTO_SAMPLE' : convert_to_null(row.PROTO_SAMPLE),
                                        'b_QA_COMMENTS' : convert_to_null(row.QA_COMMENTS),
                                        'b_RDA_CODE_AGENT' : convert_to_null(row.RDA_CODE_AGENT),
                                        'b_RDA_CODE_FACTORY' : convert_to_null(row.RDA_CODE_FACTORY),
                                        'b_RDA_CODE_PARENT_VENDOR' : convert_to_null(row.RDA_CODE_PARENT_VENDOR),
                                        'b_REPLENISHMENT_CODE' : convert_to_null(row.REPLENISHMENT_CODE),
                                        'b_REPLENISHMENT_KEY' : convert_to_null(row.REPLENISHMENT_KEY),
                                        'b_REPLENISHMENT_VALUE' : convert_to_null(row.REPLENISHMENT_VALUE),
                                        'b_REPORTING_GENDER_CODE' : convert_to_null(row.REPORTING_GENDER_CODE),
                                        'b_REPORTING_GENDER_KEY' : convert_to_null(row.REPORTING_GENDER_KEY),
                                        'b_REPORTING_GENDER_VALUE' : convert_to_null(row.REPORTING_GENDER_VALUE),
                                        'b_RETAIL_CLIMATE_CODE' : convert_to_null(row.RETAIL_CLIMATE_CODE),
                                        'b_RETAIL_CLIMATE_KEY' : convert_to_null(row.RETAIL_CLIMATE_KEY),
                                        'b_RETAIL_CLIMATE_VALUE' : convert_to_null(row.RETAIL_CLIMATE_VALUE),
                                        'b_RETAIL_LAUNCH_DATE' : convert_to_null(row.RETAIL_LAUNCH_DATE),
                                        'b_RETAIL_PRESTN_TYPE_CODE' : convert_to_null(row.RETAIL_PRESTN_TYPE_CODE),
                                        'b_RETAIL_PRESTN_TYPE_KEY' : convert_to_null(row.RETAIL_PRESTN_TYPE_KEY),
                                        'b_RETAIL_PRESTN_TYPE_VALUE' : convert_to_null(row.RETAIL_PRESTN_TYPE_VALUE),
                                        'b_RETAIL_SEASON_CODE' : convert_to_null(row.RETAIL_SEASON_CODE),
                                        'b_RETAIL_SEASON_KEY' : convert_to_null(row.RETAIL_SEASON_KEY),
                                        'b_RETAIL_SEASON_VALUE' : convert_to_null(row.RETAIL_SEASON_VALUE),
                                        'b_RTL_SUBCLASS_CODE' : convert_to_null(row.RTL_SUBCLASS_CODE),
                                        'b_RTL_SUBCLASS_KEY' : convert_to_null(row.RTL_SUBCLASS_KEY),
                                        'b_RTL_SUBCLASS_VALUE' : convert_to_null(row.RTL_SUBCLASS_VALUE),
                                        'b_SBU_CODE' : convert_to_null(row.SBU_CODE),
                                        'b_SBU_KEY' : convert_to_null(row.SBU_KEY),
                                        'b_SBU_VALUE' : convert_to_null(row.SBU_VALUE),
                                        'b_SEASON_NAME' : convert_to_null(row.SEASON_NAME),
                                        'b_SEASON_TYPE_CODE' : convert_to_null(row.SEASON_TYPE_CODE),
                                        'b_SEASON_TYPE_KEY' : convert_to_null(row.SEASON_TYPE_KEY),
                                        'b_SEASON_TYPE_VALUE' : convert_to_null(row.SEASON_TYPE_VALUE),
                                        'b_SELECTED_SIZES_CODE' : convert_to_null(row.SELECTED_SIZES_CODE),
                                        'b_SELECTED_SIZES_KEY' : convert_to_null(row.SELECTED_SIZES_KEY),
                                        'b_SELECTED_SIZES_VALUE' : convert_to_null(row.SELECTED_SIZES_VALUE),
                                        'b_SIZE_RANGE_CODE' : convert_to_null(row.SIZE_RANGE_CODE),
                                        'b_SIZE_RANGE_KEY' : convert_to_null(row.SIZE_RANGE_KEY),
                                        'b_SIZE_RANGE_VALUE' : convert_to_null(row.SIZE_RANGE_VALUE),
                                        'b_SOURCE_NAME' : convert_to_null(row.SOURCE_NAME),
                                        'b_SOURCING_MANAGER_CODE' : convert_to_null(row.SOURCING_MANAGER_CODE),
                                        'b_SOURCING_MANAGER_KEY' : convert_to_null(row.SOURCING_MANAGER_KEY),
                                        'b_SOURCING_MANAGER_VALUE' : convert_to_null(row.SOURCING_MANAGER_VALUE),
                                        'b_STC_OVERWRITE' : convert_to_null(row.STC_OVERWRITE),
                                        'b_STORE_SET_DATE' : convert_to_null(row.STORE_SET_DATE),
                                        'b_STRATEGY_LAUNCH_DATE' : convert_to_null(row.STRATEGY_LAUNCH_DATE),
                                        'b_STYLE_NUM' : convert_to_null(row.STYLE_NUM),
                                        'b_SUPPLIER_ID' : convert_to_null(row.SUPPLIER_ID),
                                        'b_TARGET_COST' : convert_to_null(row.TARGET_COST),
                                        'b_TARGET_WHOLESALE_PRICE' : convert_to_null(row.TARGET_WHOLESALE_PRICE),
                                        'b_TECHNICAL_DESIGNER' : convert_to_null(row.TECHNICAL_DESIGNER),
                                        'b_TOTAL_FORECAST' : convert_to_null(row.TOTAL_FORECAST),
                                        'b_TRGET_RETL_MARGN_PERCNT' : convert_to_null(row.TRGET_RETL_MARGN_PERCNT),
                                        'b_TRGET_WHOLSLE_MRGIN_PERCNT' : convert_to_null(row.TRGET_WHOLSLE_MRGIN_PERCNT),
                                        'b_WAREHOUSE_CODE' : convert_to_null(row.WAREHOUSE_CODE),
                                        'b_WAREHOUSE_KEY' : convert_to_null(row.WAREHOUSE_KEY),
                                        'b_WAREHOUSE_VALUE' : convert_to_null(row.WAREHOUSE_VALUE),
                                        'b_WHOLESALE' : convert_to_null(row.WHOLESALE),
                                        'b_FLEX_WHOLESALE_LAUNCH_DATE' : convert_to_null(row.FLEX_WHOLESALE_LAUNCH_DATE),
                                        'b_WHOLESALE_PRESTN_TYPE_CODE' : convert_to_null(row.WHOLESALE_PRESTN_TYPE_CODE),
                                        'b_WHOLESALE_PRESTN_TYPE_KEY' : convert_to_null(row.WHOLESALE_PRESTN_TYPE_KEY),
                                        'b_WHOLESALE_PRESTN_TYPE_VALUE' : convert_to_null(row.WHOLESALE_PRESTN_TYPE_VALUE),
                                        'b_YEAR_CODE' : convert_to_null(row.YEAR_CODE),
                                        'b_YEAR_KEY' : convert_to_null(row.YEAR_KEY),
                                        'b_YEAR_VALUE' : convert_to_null(row.YEAR_VALUE),
                                        'b_FIT_SAMPLE_APPROVED' : convert_to_null(row.FIT_SAMPLE_APPROVED),
                                        'b_MARKET_SAMPLE_KEY' : convert_to_null(row.MARKET_SAMPLE_KEY),
                                        'b_MARKET_SAMPLE_CODE' : convert_to_null(row.MARKET_SAMPLE_CODE),
                                        'b_MARKET_SAMPLE_VALUE' : convert_to_null(row.MARKET_SAMPLE_VALUE),
                                        'b_PP_SAMPEL_APPROVED' : convert_to_null(row.PP_SAMPEL_APPROVED),
                                        'b_SOURCING_REGION_KEY' : convert_to_null(row.SOURCING_REGION_KEY),
                                        'b_SOURCING_REGION_CODE' : convert_to_null(row.SOURCING_REGION_CODE),
                                        'b_SOURCING_REGION_VALUE' : convert_to_null(row.SOURCING_REGION_VALUE),
                                        'b_SOURCE_ACTIVE_KEY' : convert_to_null(row.SOURCE_ACTIVE_KEY),
                                        'b_SOURCE_ACTIVE_CODE' : convert_to_null(row.SOURCE_ACTIVE_CODE),
                                        'b_SOURCE_ACTIVE_VALUE' : convert_to_null(row.SOURCE_ACTIVE_VALUE),
                                        'b_STYLE_SORT_KEY' : convert_to_null(row.STYLE_SORT_KEY),
                                        'b_STYLE_SORT_CODE' : convert_to_null(row.STYLE_SORT_CODE),
                                        'b_STYLE_SORT_VALUE' : convert_to_null(row.STYLE_SORT_VALUE),
                                        'b_MARKET_SAMPLE_VENDOR' : convert_to_null(row.MARKET_SAMPLE_VENDOR),
                                        'b_SUNSET_DATE' : convert_to_null(row.SUNSET_DATE),
                                        'b_POS_REPORTING_GROUP_KEY' : convert_to_null(row.POS_REPORTING_GROUP_KEY),
                                        'b_POS_REPORTING_GROUP_VALUE' : convert_to_null(row.POS_REPORTING_GROUP_VALUE),
                                        'b_POS_REPORTING_GROUP_CODE' : convert_to_null(row.POS_REPORTING_GROUP_CODE),
                                        'b_RETAIL_SBU_VALUE' : convert_to_null(row.RETAIL_SBU_VALUE),
                                        'b_RETAIL_SBU_CODE' : convert_to_null(row.RETAIL_SBU_CODE),
                                        'b_RETAIL_SBU_KEY' : convert_to_null(row.RETAIL_SBU_KEY),
                                        'b_RTL_PLANNABLE_GENDER_VALUE' : convert_to_null(row.RETAIL_PLANNABLE_GENDER_VALUE),
                                        'b_RTL_PLANNABLE_GENDER_CODE' : convert_to_null(row.RETAIL_PLANNABLE_GENDER_CODE),
                                        'b_RTL_PLANNABLE_GENDER_KEY' : convert_to_null(row.RETAIL_PLANNABLE_GENDER_KEY),
                                        'b_RETAIL_DEPT_VALUE' : convert_to_null(row.RETAIL_DEPT_VALUE),
                                        'b_RETAIL_DEPT_CODE' : convert_to_null(row.RETAIL_DEPT_CODE),
                                        'b_RETAIL_DEPT_KEY' : convert_to_null(row.RETAIL_DEPT_KEY),
                                        'b_RETAIL_CLASS_VALUE' : convert_to_null(row.RETAIL_CLASS_VALUE),
                                        'b_RETAIL_CLASS_CODE' : convert_to_null(row.RETAIL_CLASS_CODE),
                                        'b_RETAIL_CLASS_KEY' : convert_to_null(row.RETAIL_CLASS_KEY),
                                        'b_RETAIL_SUB_CLASS_VALUE' : convert_to_null(row.RETAIL_SUB_CLASS_VALUE),
                                        'b_RETAIL_SUB_CLASS_CODE' : convert_to_null(row.RETAIL_SUB_CLASS_CODE),
                                        'b_RETAIL_SUB_CLASS_KEY' : convert_to_null(row.RETAIL_SUB_CLASS_KEY),
                                        'b_RETAIL_DESCRIPTION' : convert_to_null(row.RETAIL_DESCRIPTION),
                                        'b_RETAIL_SHARED_EXCLUSIVE_CODE' : convert_to_null(row.RETAIL_SHARED_EXCLUSIVE_CODE),
                                        'b_RETAIL_SHARED_EXCLUSIVE_VAL' : convert_to_null(row.RETAIL_SHARED_EXCLUSIVE_VALUE),
                                        'b_RETAIL_SHARED_EXCLUSIVE_KEY' : convert_to_null(row.RETAIL_SHARED_EXCLUSIVE_KEY),
                                        'b_STORE_LAUNCH_DATE' : convert_to_null(row.STORE_LAUNCH_DATE),
                                        'b_STORE_PLANNED_CLR_DATE' : convert_to_null(row.STORE_PLANNED_CLR_DATE),
                                        'b_RETAIL_COLLECTION_CODE' : convert_to_null(row.RETAIL_COLLECTION_CODE),
                                        'b_RETAIL_COLLECTION_VALUE' : convert_to_null(row.RETAIL_COLLECTION_VALUE),
                                        'b_RETAIL_COLLECTION_KEY' : convert_to_null(row.RETAIL_COLLECTION_KEY),
                                        'b_RETAIL_LIFECYCLE_VALUE' : convert_to_null(row.RETAIL_LIFECYCLE_VALUE),
                                        'b_RETAIL_LIFECYCLE_CODE' : convert_to_null(row.RETAIL_LIFECYCLE_CODE),
                                        'b_RETAIL_LIFECYCLE_KEY' : convert_to_null(row.RETAIL_LIFECYCLE_KEY),
                                        'b_RETAIL_PRICING_VALUE' : convert_to_null(row.RETAIL_PRICING_VALUE),
                                        'b_RETAIL_PRICING_CODE' : convert_to_null(row.RETAIL_PRICING_CODE),
                                        'b_RETAIL_PRICING_KEY' : convert_to_null(row.RETAIL_PRICING_KEY),
                                        'b_RETAIL_LENGTH_VALUE' : convert_to_null(row.RETAIL_LENGTH_VALUE),
                                        'b_RETAIL_LENGTH_CODE' : convert_to_null(row.RETAIL_LENGTH_CODE),
                                        'b_RETAIL_LENGTH_KEY' : convert_to_null(row.RETAIL_LENGTH_KEY),
                                        'b_RETAIL_WEAR_NOW_VALUE' : convert_to_null(row.RETAIL_WEAR_NOW_VALUE),
                                        'b_RETAIL_WEAR_NOW_CODE' : convert_to_null(row.RETAIL_WEAR_NOW_CODE),
                                        'b_RETAIL_WEAR_NOW_KEY' : convert_to_null(row.RETAIL_WEAR_NOW_KEY),
                                        'b_RETAIL_COLOR_FAMILY_VALUE' : convert_to_null(row.RETAIL_COLOR_FAMILY_VALUE),
                                        'b_RETAIL_COLOR_FAMILY_CODE' : convert_to_null(row.RETAIL_COLOR_FAMILY_CODE),
                                        'b_RETAIL_COLOR_FAMILY_KEY' : convert_to_null(row.RETAIL_COLOR_FAMILY_KEY),
                                        'b_RETAIL_ACC_AGE_SEG_VALUE' : convert_to_null(row.RETAIL_ACC_AGE_SEG_VALUE),
                                        'b_RETAIL_ACC_AGE_SEG_CODE' : convert_to_null(row.RETAIL_ACC_AGE_SEG_CODE),
                                        'b_RETAIL_ACC_AGE_SEG_KEY' : convert_to_null(row.RETAIL_ACC_AGE_SEG_KEY),
                                        'b_RETAIL_PARENT_STYLE_VALUE' : convert_to_null(row.RETAIL_PARENT_STYLE_VALUE),
                                        'b_RETAIL_PARENT_STYLE_CODE' : convert_to_null(row.RETAIL_PARENT_STYLE_CODE),
                                        'b_RETAIL_PARENT_STYLE_KEY' : convert_to_null(row.RETAIL_PARENT_STYLE_KEY),
                                        'b_STORE_ZONE_VALUE' : convert_to_null(row.STORE_ZONE_VALUE),
                                        'b_STORE_ZONE_CODE' : convert_to_null(row.STORE_ZONE_CODE),
                                        'b_STORE_ZONE_KEY' : convert_to_null(row.STORE_ZONE_KEY),
                                        'b_RETAIL_RELEASED_VALUE' : convert_to_null(row.RETAIL_RELEASED_VALUE),
                                        'b_RETAIL_RELEASED_CODE' : convert_to_null(row.RETAIL_RELEASED_CODE),
                                        'b_RETAIL_RELEASED_KEY' : convert_to_null(row.RETAIL_RELEASED_KEY),
                                        'b_PRIMARY_VENDOR' : convert_to_null(row.PRIMARY_VENDOR),
                                        'b_SECONDARY_VENDOR' : convert_to_null(row.SECONDARY_VENDOR),
                                        'b_RETAIL_OSV_COST' : convert_to_null(row.RETAIL_OSV_COST),
                                        'b_ECOMM_PLANNED_CL_DATE' : convert_to_null(row.ECOMM_PLANNED_CL_DATE),
                                        'b_ECOMM_WEB_COLOR_KEY' : convert_to_null(row.ECOMM_WEB_COLOR_KEY),
                                        'b_ECOMM_WEB_COLOR_VALUE' : convert_to_null(row.ECOMM_WEB_COLOR_VALUE),
                                        'b_ECOMM_WEB_COLOR_CODE' : convert_to_null(row.ECOMM_WEB_COLOR_CODE),
                                        'b_ECOMM_PHOTOSTYLE' : convert_to_null(row.ECOMM_PHOTOSTYLE),
                                        'b_ECOMM_WEB_VAR_MASTER' : convert_to_null(row.ECOMM_WEB_VAR_MASTER),
                                        'b_ECOMM_RELEASED_VALUE' : convert_to_null(row.ECOMM_RELEASED_VALUE),
                                        'b_ECOMM_RELEASED_CODE' : convert_to_null(row.ECOMM_RELEASED_CODE),
                                        'b_ECOMM_RELEASED_KEY' : convert_to_null(row.ECOMM_RELEASED_KEY),
                                        'b_VENDOR_STYLE_ID' : convert_to_null(row.VENDOR_STYLE_ID),
                                        'b_PRODUCT_TYPE_CODE' : convert_to_null(row.PRODUCT_TYPE_CODE),
                                        'b_PRODUCT_TYPE_KEY' : convert_to_null(row.PRODUCT_TYPE_KEY),
                                        'b_PRODUCT_TYPE_VALUE' : convert_to_null(row.PRODUCT_TYPE_VALUE),
                                        'b_UPC' : convert_to_null(row.UPC),
                                        'b_PRIMARY_VENDOR_ID' : convert_to_null(row.PRIMARY_VENDOR_ID),
                                        'b_SECONDARY_VENDOR_ID' : convert_to_null(row.SECONDARY_VENDOR_ID),
                                        'b_RETAIL_COMPOSITION_CODE' : convert_to_null(row.RETAIL_COMPOSITION_CODE),
                                        'b_RETAIL_COMPOSITION_KEY' : convert_to_null(row.RETAIL_COMPOSITION_KEY),
                                        'b_RETAIL_COMPOSITION_VALUE' : convert_to_null(row.RETAIL_COMPOSITION_VALUE),
                                        'b_ECOMM_PLANNING_ATTRIBUTE_CD' : convert_to_null(row.ECOMM_PLANNING_ATTRIBUTE_CODE),
                                        'b_ECOMM_PLANNING_ATTRIBUTE_KEY' : convert_to_null(row.ECOMM_PLANNING_ATTRIBUTE_KEY),
                                        'b_ECOMM_PLANNING_ATTRIBUTE_VAL' : convert_to_null(row.ECOMM_PLANNING_ATTRIBUTE_VALUE),
                                        'b_RETAIL_BRAND_VALUE' : convert_to_null(row.RETAIL_BRAND_VALUE),
                                        'b_RETAIL_BRAND_CODE' : convert_to_null(row.RETAIL_BRAND_CODE),
                                        'b_RETAIL_BRAND_KEY' : convert_to_null(row.RETAIL_BRAND_KEY),
                                        'b_COLORWAY_SOURCING_STATUS' : convert_to_null(row.COLORWAY_SOURCING_STATUS),
                                        'b_SOURCE_ACTIVE' : convert_to_null(row.SOURCE_ACTIVE),
                                        'b_CHARACTER_VALUE' : convert_to_null(row.CHARACTER_VALUE),
                                        'b_CHARACTER_CODE' : convert_to_null(row.CHARACTER_CODE),
                                        'b_CHARACTER_KEY' : convert_to_null(row.CHARACTER_KEY),
                                        'b_CATEGORY_VALUE' : convert_to_null(row.CATEGORY_VALUE),
                                        'b_CATEGORY_CODE' : convert_to_null(row.CATEGORY_CODE),
                                        'b_CATEGORY_KEY' : convert_to_null(row.CATEGORY_KEY),
                                        'b_PRINT_VALUE' : convert_to_null(row.PRINT_VALUE),
                                        'b_PRINT_KEY' : convert_to_null(row.PRINT_KEY),
                                        'b_PRINT_CODE' : convert_to_null(row.PRINT_CODE),
                                        'b_ITEM_CATEGORY_VALUE' : convert_to_null(row.ITEM_CATEGORY_VALUE),
                                        'b_ITEM_CATEGORY_CODE' : convert_to_null(row.ITEM_CATEGORY_CODE),
                                        'b_ITEM_CATEGORY_KEY' : convert_to_null(row.ITEM_CATEGORY_KEY),
                                        'b_SEASON_CREATED_KEY' : convert_to_null(row.SEASON_CREATED_KEY),
                                        'b_SEASON_CREATED_VALUE' : convert_to_null(row.SEASON_CREATED_VALUE),
                                        'b_SEASON_CREATED_CODE' : convert_to_null(row.SEASON_CREATED_CODE),
                                        'b_YEAR_CREATED_KEY' : convert_to_null(row.YEAR_CREATED_KEY),
                                        'b_YEAR_CREATED_VALUE' : convert_to_null(row.YEAR_CREATED_VALUE),
                                        'b_ALT_IMAGE' : convert_to_null(row.ALT_IMAGE),
                                        'b_ORIGINAL_PRICE' : convert_to_null(row.ORIGINAL_PRICE),
                                        'b_PROGRAM' : convert_to_null(row.PROGRAM),
                                        'b_STORE_RELAUNCH_DATE' : convert_to_null(row.STORE_RELAUNCH_DATE),
                                        'b_STORE_RELAUNCH_CLR_DATE' : convert_to_null(row.STORE_RELAUNCH_CLR_DATE),
                                        'b_ECOM_RELAUNCH_DATE' : convert_to_null(row.ECOM_RELAUNCH_DATE),
                                        'b_ECOM_RELAUNCH_CLR_DATE' : convert_to_null(row.ECOM_RELAUNCH_CLR_DATE),
                                        'b_SKIPHOP_COMMENTS' : convert_to_null(row.SKIPHOP_COMMENTS),
                                        'b_LANDED_COST' : convert_to_null(row.LANDED_COST),
                                        'b_SUB_CATEGORY_VALUE' : convert_to_null(row.SUB_CATEGORY_VALUE),
                                        'b_SUB_CATEGORY_CODE' : convert_to_null(row.SUB_CATEGORY_CODE),
                                        'b_SUB_CATEGORY_KEY' : convert_to_null(row.SUB_CATEGORY_KEY),
                                        'b_INTL_LIFECYCLE_VALUE' : convert_to_null(row.INTL_LIFECYCLE_VALUE),
                                        'b_INTL_LIFECYCLE_CODE' : convert_to_null(row.INTL_LIFECYCLE_CODE),
                                        'b_INTL_LIFECYCLE_KEY' : convert_to_null(row.INTL_LIFECYCLE_KEY),
                                        'b_DOMESTIC_LIFECYCLE_VALUE' : convert_to_null(row.DOMESTIC_LIFECYCLE_VALUE),
                                        'b_DOMESTIC_LIFECYCLE_CODE' : convert_to_null(row.DOMESTIC_LIFECYCLE_CODE),
                                        'b_DOMESTIC_LIFECYCLE_KEY' : convert_to_null(row.DOMESTIC_LIFECYCLE_KEY),
                                        'b_REPORTABLE_AGE_VALUE' : convert_to_null(row.REPORTABLE_AGE_VALUE),
                                        'b_REPORTABLE_AGE_CODE' : convert_to_null(row.REPORTABLE_AGE_CODE),
                                        'b_REPORTABLE_AGE_KEY' : convert_to_null(row.REPORTABLE_AGE_KEY),
                                        'b_INTL_PARTNERS_VALUE' : convert_to_null(row.INTL_PARTNERS_VALUE),
                                        'b_INTL_PARTNERS_CODE' : convert_to_null(row.INTL_PARTNERS_CODE),
                                        'b_INTL_PARTNERS_KEY' : convert_to_null(row.INTL_PARTNERS_KEY),
                                        'b_MERCHANDISER' : convert_to_null(row.MERCHANDISER),
                                        'b_FLEX_ID' : convert_to_null(row.FLEX_ID),
                                        'b_PROC_IND' : 0,
                                        'b_SRC_SYS_CD' : 'FLEX',
                                        'b_DTL__CAPXUSER' : 'INFA_CDC_BULK',
                                        'b_DTL__CAPXTIMESTAMP' : v_capxtime,
                                        'b_DTL__CAPXACTION' : 'I'
                                       })
                inscount = inscount +1
                hubconn.commit()
                etl_status = 'Success'
            except Exception as e:
                print('FLEX_ID: ' + row.FLEX_ID + ' error: ' + str(e))
                hubconn.rollback()
                etl_status = 'Fail'
                break

        if row._merge =='both':
            try:
                
                # Archive data from cdc_flex_makeablesellable table 
                # Data only get archived when the data assoicated with the flex id was updated
                
                hub_cur.execute(sql_insert_arc,{'b_FLEX_ID' : row.FLEX_ID})
                
                #Update data in the cdc_flex_makeablesellabe to capture changes from FLEX db
                
                hub_cur.execute(sql_update,{
                    'b_ACTION_FLAG' : convert_to_null(row.ACTION_FLAG),
                    'b_AGE_CODE' : convert_to_null(row.AGE_CODE),
                    'b_AGE_KEY' : convert_to_null(row.AGE_KEY),
                    'b_AGE_VALUE' : convert_to_null(row.AGE_VALUE),
                    'b_AGENT' : convert_to_null(row.AGENT),
                    'b_AGENT_ID' : convert_to_null(row.AGENT_ID),
                    'b_ARTWK_PLACEMENT_COMP' : convert_to_null(row.ARTWK_PLACEMENT_COMP),
                    'b_BENCHMARK_STYLE' : convert_to_null(row.BENCHMARK_STYLE),
                    'b_BRAND_CODE' : convert_to_null(row.BRAND_CODE),
                    'b_BRAND_KEY' : convert_to_null(row.BRAND_KEY),
                    'b_BRAND_VALUE' : convert_to_null(row.BRAND_VALUE),
                    'b_BUSINESS_UNIT_CODE' : convert_to_null(row.BUSINESS_UNIT_CODE),
                    'b_BUSINESS_UNIT_KEY' : convert_to_null(row.BUSINESS_UNIT_KEY),
                    'b_BUSINESS_UNIT_VALUE' : convert_to_null(row.BUSINESS_UNIT_VALUE),
                    'b_CLIMATE_CODE' : convert_to_null(row.CLIMATE_CODE),
                    'b_CLIMATE_KEY' : convert_to_null(row.CLIMATE_KEY),
                    'b_CLIMATE_VALUE' : convert_to_null(row.CLIMATE_VALUE),
                    'b_CLIMATE_ZONE_CODE' : convert_to_null(row.CLIMATE_ZONE_CODE),
                    'b_CLIMATE_ZONE_KEY' : convert_to_null(row.CLIMATE_ZONE_KEY),
                    'b_CLIMATE_ZONE_VALUE' : convert_to_null(row.CLIMATE_ZONE_VALUE),
                    'b_COLLECTION_CODE' : convert_to_null(row.COLLECTION_CODE),
                    'b_COLLECTION_KEY' : convert_to_null(row.COLLECTION_KEY),
                    'b_COLLECTION_VALUE' : convert_to_null(row.COLLECTION_VALUE),
                    'b_COLOR' : convert_to_null(row.COLOR),
                    'b_COLORWAY_NAME' : convert_to_null(row.COLORWAY_NAME),
                    'b_COLORWAY_SEQUENCE' : convert_to_null(row.COLORWAY_SEQUENCE),
                    'b_COLORWAY_STATUS_CODE' : convert_to_null(row.COLORWAY_STATUS_CODE),
                    'b_COLORWAY_STATUS_KEY' : convert_to_null(row.COLORWAY_STATUS_KEY),
                    'b_COLORWAY_STATUS_VALUE' : convert_to_null(row.COLORWAY_STATUS_VALUE),
                    'b_COLORWAY_THUMBNAIL_URL' : convert_to_null(row.COLORWAY_THUMBNAIL_URL),
                    'b_COMPONENT1_SILH' : convert_to_null(row.COMPONENT1_SILH),
                    'b_COMPONENT2_SILH' : convert_to_null(row.COMPONENT2_SILH),
                    'b_COMPONENT3_SILH' : convert_to_null(row.COMPONENT3_SILH),
                    'b_COMPONENT4_SILH' : convert_to_null(row.COMPONENT4_SILH),
                    'b_COMPONENT5_SILH' : convert_to_null(row.COMPONENT5_SILH),
                    'b_COMPONENT6_SILH' : convert_to_null(row.COMPONENT6_SILH),
                    'b_COMPONENT7_SILH' : convert_to_null(row.COMPONENT7_SILH),
                    'b_CORE_BODY_CODE' : convert_to_null(row.CORE_BODY_CODE),
                    'b_CORE_BODY_DESC_CODE' : convert_to_null(row.CORE_BODY_DESC_CODE),
                    'b_CORE_BODY_DESC_KEY' : convert_to_null(row.CORE_BODY_DESC_KEY),
                    'b_CORE_BODY_DESC_VALUE' : convert_to_null(row.CORE_BODY_DESC_VALUE),
                    'b_CORE_BODY_KEY' : convert_to_null(row.CORE_BODY_KEY),
                    'b_CORE_BODY_VALUE' : convert_to_null(row.CORE_BODY_VALUE),
                    'b_CURRENCY_CODE' : convert_to_null(row.CURRENCY_CODE),
                    'b_CURRENCY_KEY' : convert_to_null(row.CURRENCY_KEY),
                    'b_CURRENCY_VALUE' : convert_to_null(row.CURRENCY_VALUE),
                    'b_DELETE_FLAG' : convert_to_null(row.DELETE_FLAG),
                    'b_DELIVERY_DATE' : convert_to_null(row.DELIVERY_DATE),
                    'b_DELIVERY_GROUP_CODE' : convert_to_null(row.DELIVERY_GROUP_CODE),
                    'b_DELIVERY_GROUP_KEY' : convert_to_null(row.DELIVERY_GROUP_KEY),
                    'b_DELIVERY_GROUP_VALUE' : convert_to_null(row.DELIVERY_GROUP_VALUE),
                    'b_DESCRIPTION' : convert_to_null(row.DESCRIPTION),
                    'b_DESCRIPTION_FOR_TAGS_CODE' : convert_to_null(row.DESCRIPTION_FOR_TAGS_CODE),
                    'b_DESCRIPTION_FOR_TAGS_KEY' : convert_to_null(row.DESCRIPTION_FOR_TAGS_KEY),
                    'b_DESCRIPTION_FOR_TAGS_VALUE' : convert_to_null(row.DESCRIPTION_FOR_TAGS_VALUE),
                    'b_DESIGN_SAMPLE' : convert_to_null(row.DESIGN_SAMPLE),
                    'b_DESIGNER' : convert_to_null(row.DESIGNER),
                    'b_DISCONTINUE_DATE' : convert_to_null(row.DISCONTINUE_DATE),
                    'b_ECOMM_LAUNCH_DATE' : convert_to_null(row.ECOMM_LAUNCH_DATE),
                    'b_EXCESS_INVENTORY_DATE' : convert_to_null(row.EXCESS_INVENTORY_DATE),
                    'b_EXCLUSIVE_CHANNEL_CODE' : convert_to_null(row.EXCLUSIVE_CHANNEL_CODE),
                    'b_EXCLUSIVE_CHANNEL_KEY' : convert_to_null(row.EXCLUSIVE_CHANNEL_KEY),
                    'b_EXCLUSIVE_CHANNEL_VALUE' : convert_to_null(row.EXCLUSIVE_CHANNEL_VALUE),
                    'b_FABRIC_CONTENT_CODE' : convert_to_null(row.FABRIC_CONTENT_CODE),
                    'b_FABRIC_CONTENT_KEY' : convert_to_null(row.FABRIC_CONTENT_KEY),
                    'b_FABRIC_CONTENT_VALUE' : convert_to_null(row.FABRIC_CONTENT_VALUE),
                    'b_FABRIC_FAMILY_CODE' : convert_to_null(row.FABRIC_FAMILY_CODE),
                    'b_FABRIC_FAMILY_KEY' : convert_to_null(row.FABRIC_FAMILY_KEY),
                    'b_FABRIC_FAMILY_VALUE' : convert_to_null(row.FABRIC_FAMILY_VALUE),
                    'b_FABRIC_GROUP_CODE' : convert_to_null(row.FABRIC_GROUP_CODE),
                    'b_FABRIC_GROUP_KEY' : convert_to_null(row.FABRIC_GROUP_KEY),
                    'b_FABRIC_GROUP_VALUE' : convert_to_null(row.FABRIC_GROUP_VALUE),
                    'b_FACTORY' : convert_to_null(row.FACTORY),
                    'b_FACTORY_ID' : convert_to_null(row.FACTORY_ID),
                    'b_FR_CODE' : convert_to_null(row.FR_CODE),
                    'b_FR_KEY' : convert_to_null(row.FR_KEY),
                    'b_FR_VALUE' : convert_to_null(row.FR_VALUE),
                    'b_IS_CARRYOVER' : convert_to_null(row.IS_CARRYOVER),
                    'b_IS_QA_COMMENTS' : convert_to_null(row.IS_QA_COMMENTS),
                    'b_IS_SET_CODE' : convert_to_null(row.IS_SET_CODE),
                    'b_IS_SET_KEY' : convert_to_null(row.IS_SET_KEY),
                    'b_IS_SET_VALUE' : convert_to_null(row.IS_SET_VALUE),
                    'b_LAST_UPDATED' : convert_to_null(row.LAST_UPDATED),
                    'b_LAUNCH_DATE_OVERWRITE' : convert_to_null(row.LAUNCH_DATE_OVERWRITE),
                    'b_LIFECYCLE_STATE_CODE' : convert_to_null(row.LIFECYCLE_STATE_CODE),
                    'b_LIFECYCLE_STATE_KEY' : convert_to_null(row.LIFECYCLE_STATE_KEY),
                    'b_LIFECYCLE_STATE_VALUE' : convert_to_null(row.LIFECYCLE_STATE_VALUE),
                    'b_LIFESTYLE_CODE' : convert_to_null(row.LIFESTYLE_CODE),
                    'b_LIFESTYLE_KEY' : convert_to_null(row.LIFESTYLE_KEY),
                    'b_LIFESTYLE_VALUE' : convert_to_null(row.LIFESTYLE_VALUE),
                    'b_MANUFACTURING_PIECES' : convert_to_null(row.MANUFACTURING_PIECES),
                    'b_MSRP' : convert_to_null(row.MSRP),
                    'b_NRF_COLOR_CODE_CODE' : convert_to_null(row.NRF_COLOR_CODE_CODE),
                    'b_NRF_COLOR_CODE_KEY' : convert_to_null(row.NRF_COLOR_CODE_KEY),
                    'b_NRF_COLOR_CODE_VALUE' : convert_to_null(row.NRF_COLOR_CODE_VALUE),
                    'b_NRF_COLOR_NAME_CODE' : convert_to_null(row.NRF_COLOR_NAME_CODE),
                    'b_NRF_COLOR_NAME_KEY' : convert_to_null(row.NRF_COLOR_NAME_KEY),
                    'b_NRF_COLOR_NAME_VALUE' : convert_to_null(row.NRF_COLOR_NAME_VALUE),
                    'b_PARENT_SUPPLIER' : convert_to_null(row.PARENT_SUPPLIER),
                    'b_PARENT_SUPPLIER_ID' : convert_to_null(row.PARENT_SUPPLIER_ID),
                    'b_PARENT_VENDOR_ID' : convert_to_null(row.PARENT_VENDOR_ID),
                    'b_PLANABLE_GENDER_CODE' : convert_to_null(row.PLANABLE_GENDER_CODE),
                    'b_PLANABLE_GENDER_KEY' : convert_to_null(row.PLANABLE_GENDER_KEY),
                    'b_PLANABLE_GENDER_VALUE' : convert_to_null(row.PLANABLE_GENDER_VALUE),
                    'b_PRICE_POINT_CODE' : convert_to_null(row.PRICE_POINT_CODE),
                    'b_PRICE_POINT_KEY' : convert_to_null(row.PRICE_POINT_KEY),
                    'b_PRICE_POINT_VALUE' : convert_to_null(row.PRICE_POINT_VALUE),
                    'b_PRIMARY_MATL_REF' : convert_to_null(row.PRIMARY_MATL_REF),
                    'b_PRIMARY_SOURCE_PRODUCT' : convert_to_null(row.PRIMARY_SOURCE_PRODUCT),
                    'b_PRIMARY_SOURCE_SEASON' : convert_to_null(row.PRIMARY_SOURCE_SEASON),
                    'b_PRODUCT_CLASS_CODE' : convert_to_null(row.PRODUCT_CLASS_CODE),
                    'b_PRODUCT_CLASS_KEY' : convert_to_null(row.PRODUCT_CLASS_KEY),
                    'b_PRODUCT_CLASS_VALUE' : convert_to_null(row.PRODUCT_CLASS_VALUE),
                    'b_PRODUCT_DESCRIPTION' : convert_to_null(row.PRODUCT_DESCRIPTION),
                    'b_PRODUCT_MANAGER' : convert_to_null(row.PRODUCT_MANAGER),
                    'b_PRODUCT_NAME' : convert_to_null(row.PRODUCT_NAME),
                    'b_PRODUCT_NUM' : convert_to_null(row.PRODUCT_NUM),
                    'b_PRODUCT_SEQ' : convert_to_null(row.PRODUCT_SEQ),
                    'b_PRODUCT_STATUS_CODE' : convert_to_null(row.PRODUCT_STATUS_CODE),
                    'b_PRODUCT_STATUS_KEY' : convert_to_null(row.PRODUCT_STATUS_KEY),
                    'b_PRODUCT_STATUS_VALUE' : convert_to_null(row.PRODUCT_STATUS_VALUE),
                    'b_PRODUCT_SUB_CLASS_CODE' : convert_to_null(row.PRODUCT_SUB_CLASS_CODE),
                    'b_PRODUCT_SUB_CLASS_KEY' : convert_to_null(row.PRODUCT_SUB_CLASS_KEY),
                    'b_PRODUCT_SUB_CLASS_VALUE' : convert_to_null(row.PRODUCT_SUB_CLASS_VALUE),
                    'b_PRODUCT_THUMB_URL' : convert_to_null(row.PRODUCT_THUMB_URL),
                    'b_PRODUCTION_GROUP_CODE' : convert_to_null(row.PRODUCTION_GROUP_CODE),
                    'b_PRODUCTION_GROUP_KEY' : convert_to_null(row.PRODUCTION_GROUP_KEY),
                    'b_PRODUCTION_GROUP_VALUE' : convert_to_null(row.PRODUCTION_GROUP_VALUE),
                    'b_PROTO_SAMPLE' : convert_to_null(row.PROTO_SAMPLE),
                    'b_QA_COMMENTS' : convert_to_null(row.QA_COMMENTS),
                    'b_RDA_CODE_AGENT' : convert_to_null(row.RDA_CODE_AGENT),
                    'b_RDA_CODE_FACTORY' : convert_to_null(row.RDA_CODE_FACTORY),
                    'b_RDA_CODE_PARENT_VENDOR' : convert_to_null(row.RDA_CODE_PARENT_VENDOR),
                    'b_REPLENISHMENT_CODE' : convert_to_null(row.REPLENISHMENT_CODE),
                    'b_REPLENISHMENT_KEY' : convert_to_null(row.REPLENISHMENT_KEY),
                    'b_REPLENISHMENT_VALUE' : convert_to_null(row.REPLENISHMENT_VALUE),
                    'b_REPORTING_GENDER_CODE' : convert_to_null(row.REPORTING_GENDER_CODE),
                    'b_REPORTING_GENDER_KEY' : convert_to_null(row.REPORTING_GENDER_KEY),
                    'b_REPORTING_GENDER_VALUE' : convert_to_null(row.REPORTING_GENDER_VALUE),
                    'b_RETAIL_CLIMATE_CODE' : convert_to_null(row.RETAIL_CLIMATE_CODE),
                    'b_RETAIL_CLIMATE_KEY' : convert_to_null(row.RETAIL_CLIMATE_KEY),
                    'b_RETAIL_CLIMATE_VALUE' : convert_to_null(row.RETAIL_CLIMATE_VALUE),
                    'b_RETAIL_LAUNCH_DATE' : convert_to_null(row.RETAIL_LAUNCH_DATE),
                    'b_RETAIL_PRESTN_TYPE_CODE' : convert_to_null(row.RETAIL_PRESTN_TYPE_CODE),
                    'b_RETAIL_PRESTN_TYPE_KEY' : convert_to_null(row.RETAIL_PRESTN_TYPE_KEY),
                    'b_RETAIL_PRESTN_TYPE_VALUE' : convert_to_null(row.RETAIL_PRESTN_TYPE_VALUE),
                    'b_RETAIL_SEASON_CODE' : convert_to_null(row.RETAIL_SEASON_CODE),
                    'b_RETAIL_SEASON_KEY' : convert_to_null(row.RETAIL_SEASON_KEY),
                    'b_RETAIL_SEASON_VALUE' : convert_to_null(row.RETAIL_SEASON_VALUE),
                    'b_RTL_SUBCLASS_CODE' : convert_to_null(row.RTL_SUBCLASS_CODE),
                    'b_RTL_SUBCLASS_KEY' : convert_to_null(row.RTL_SUBCLASS_KEY),
                    'b_RTL_SUBCLASS_VALUE' : convert_to_null(row.RTL_SUBCLASS_VALUE),
                    'b_SBU_CODE' : convert_to_null(row.SBU_CODE),
                    'b_SBU_KEY' : convert_to_null(row.SBU_KEY),
                    'b_SBU_VALUE' : convert_to_null(row.SBU_VALUE),
                    'b_SEASON_NAME' : convert_to_null(row.SEASON_NAME),
                    'b_SEASON_TYPE_CODE' : convert_to_null(row.SEASON_TYPE_CODE),
                    'b_SEASON_TYPE_KEY' : convert_to_null(row.SEASON_TYPE_KEY),
                    'b_SEASON_TYPE_VALUE' : convert_to_null(row.SEASON_TYPE_VALUE),
                    'b_SELECTED_SIZES_CODE' : convert_to_null(row.SELECTED_SIZES_CODE),
                    'b_SELECTED_SIZES_KEY' : convert_to_null(row.SELECTED_SIZES_KEY),
                    'b_SELECTED_SIZES_VALUE' : convert_to_null(row.SELECTED_SIZES_VALUE),
                    'b_SIZE_RANGE_CODE' : convert_to_null(row.SIZE_RANGE_CODE),
                    'b_SIZE_RANGE_KEY' : convert_to_null(row.SIZE_RANGE_KEY),
                    'b_SIZE_RANGE_VALUE' : convert_to_null(row.SIZE_RANGE_VALUE),
                    'b_SOURCE_NAME' : convert_to_null(row.SOURCE_NAME),
                    'b_SOURCING_MANAGER_CODE' : convert_to_null(row.SOURCING_MANAGER_CODE),
                    'b_SOURCING_MANAGER_KEY' : convert_to_null(row.SOURCING_MANAGER_KEY),
                    'b_SOURCING_MANAGER_VALUE' : convert_to_null(row.SOURCING_MANAGER_VALUE),
                    'b_STC_OVERWRITE' : convert_to_null(row.STC_OVERWRITE),
                    'b_STORE_SET_DATE' : convert_to_null(row.STORE_SET_DATE),
                    'b_STRATEGY_LAUNCH_DATE' : convert_to_null(row.STRATEGY_LAUNCH_DATE),
                    'b_STYLE_NUM' : convert_to_null(row.STYLE_NUM),
                    'b_SUPPLIER_ID' : convert_to_null(row.SUPPLIER_ID),
                    'b_TARGET_COST' : convert_to_null(row.TARGET_COST),
                    'b_TARGET_WHOLESALE_PRICE' : convert_to_null(row.TARGET_WHOLESALE_PRICE),
                    'b_TECHNICAL_DESIGNER' : convert_to_null(row.TECHNICAL_DESIGNER),
                    'b_TOTAL_FORECAST' : convert_to_null(row.TOTAL_FORECAST),
                    'b_TRGET_RETL_MARGN_PERCNT' : convert_to_null(row.TRGET_RETL_MARGN_PERCNT),
                    'b_TRGET_WHOLSLE_MRGIN_PERCNT' : convert_to_null(row.TRGET_WHOLSLE_MRGIN_PERCNT),
                    'b_WAREHOUSE_CODE' : convert_to_null(row.WAREHOUSE_CODE),
                    'b_WAREHOUSE_KEY' : convert_to_null(row.WAREHOUSE_KEY),
                    'b_WAREHOUSE_VALUE' : convert_to_null(row.WAREHOUSE_VALUE),
                    'b_WHOLESALE' : convert_to_null(row.WHOLESALE),
                    'b_FLEX_WHOLESALE_LAUNCH_DATE' : convert_to_null(row.FLEX_WHOLESALE_LAUNCH_DATE),
                    'b_WHOLESALE_PRESTN_TYPE_CODE' : convert_to_null(row.WHOLESALE_PRESTN_TYPE_CODE),
                    'b_WHOLESALE_PRESTN_TYPE_KEY' : convert_to_null(row.WHOLESALE_PRESTN_TYPE_KEY),
                    'b_WHOLESALE_PRESTN_TYPE_VALUE' : convert_to_null(row.WHOLESALE_PRESTN_TYPE_VALUE),
                    'b_YEAR_CODE' : convert_to_null(row.YEAR_CODE),
                    'b_YEAR_KEY' : convert_to_null(row.YEAR_KEY),
                    'b_YEAR_VALUE' : convert_to_null(row.YEAR_VALUE),
                    'b_FIT_SAMPLE_APPROVED' : convert_to_null(row.FIT_SAMPLE_APPROVED),
                    'b_MARKET_SAMPLE_KEY' : convert_to_null(row.MARKET_SAMPLE_KEY),
                    'b_MARKET_SAMPLE_CODE' : convert_to_null(row.MARKET_SAMPLE_CODE),
                    'b_MARKET_SAMPLE_VALUE' : convert_to_null(row.MARKET_SAMPLE_VALUE),
                    'b_PP_SAMPEL_APPROVED' : convert_to_null(row.PP_SAMPEL_APPROVED),
                    'b_SOURCING_REGION_KEY' : convert_to_null(row.SOURCING_REGION_KEY),
                    'b_SOURCING_REGION_CODE' : convert_to_null(row.SOURCING_REGION_CODE),
                    'b_SOURCING_REGION_VALUE' : convert_to_null(row.SOURCING_REGION_VALUE),
                    'b_SOURCE_ACTIVE_KEY' : convert_to_null(row.SOURCE_ACTIVE_KEY),
                    'b_SOURCE_ACTIVE_CODE' : convert_to_null(row.SOURCE_ACTIVE_CODE),
                    'b_SOURCE_ACTIVE_VALUE' : convert_to_null(row.SOURCE_ACTIVE_VALUE),
                    'b_STYLE_SORT_KEY' : convert_to_null(row.STYLE_SORT_KEY),
                    'b_STYLE_SORT_CODE' : convert_to_null(row.STYLE_SORT_CODE),
                    'b_STYLE_SORT_VALUE' : convert_to_null(row.STYLE_SORT_VALUE),
                    'b_MARKET_SAMPLE_VENDOR' : convert_to_null(row.MARKET_SAMPLE_VENDOR),
                    'b_SUNSET_DATE' : convert_to_null(row.SUNSET_DATE),
                    'b_POS_REPORTING_GROUP_KEY' : convert_to_null(row.POS_REPORTING_GROUP_KEY),
                    'b_POS_REPORTING_GROUP_VALUE' : convert_to_null(row.POS_REPORTING_GROUP_VALUE),
                    'b_POS_REPORTING_GROUP_CODE' : convert_to_null(row.POS_REPORTING_GROUP_CODE),
                    'b_RETAIL_SBU_VALUE' : convert_to_null(row.RETAIL_SBU_VALUE),
                    'b_RETAIL_SBU_CODE' : convert_to_null(row.RETAIL_SBU_CODE),
                    'b_RETAIL_SBU_KEY' : convert_to_null(row.RETAIL_SBU_KEY),
                    'b_RTL_PLANNABLE_GENDER_VALUE' : convert_to_null(row.RETAIL_PLANNABLE_GENDER_VALUE),
                    'b_RTL_PLANNABLE_GENDER_CODE' : convert_to_null(row.RETAIL_PLANNABLE_GENDER_CODE),
                    'b_RTL_PLANNABLE_GENDER_KEY' : convert_to_null(row.RETAIL_PLANNABLE_GENDER_KEY),
                    'b_RETAIL_DEPT_VALUE' : convert_to_null(row.RETAIL_DEPT_VALUE),
                    'b_RETAIL_DEPT_CODE' : convert_to_null(row.RETAIL_DEPT_CODE),
                    'b_RETAIL_DEPT_KEY' : convert_to_null(row.RETAIL_DEPT_KEY),
                    'b_RETAIL_CLASS_VALUE' : convert_to_null(row.RETAIL_CLASS_VALUE),
                    'b_RETAIL_CLASS_CODE' : convert_to_null(row.RETAIL_CLASS_CODE),
                    'b_RETAIL_CLASS_KEY' : convert_to_null(row.RETAIL_CLASS_KEY),
                    'b_RETAIL_SUB_CLASS_VALUE' : convert_to_null(row.RETAIL_SUB_CLASS_VALUE),
                    'b_RETAIL_SUB_CLASS_CODE' : convert_to_null(row.RETAIL_SUB_CLASS_CODE),
                    'b_RETAIL_SUB_CLASS_KEY' : convert_to_null(row.RETAIL_SUB_CLASS_KEY),
                    'b_RETAIL_DESCRIPTION' : convert_to_null(row.RETAIL_DESCRIPTION),
                    'b_RETAIL_SHARED_EXCLUSIVE_CODE' : convert_to_null(row.RETAIL_SHARED_EXCLUSIVE_CODE),
                    'b_RETAIL_SHARED_EXCLUSIVE_VAL' : convert_to_null(row.RETAIL_SHARED_EXCLUSIVE_VALUE),
                    'b_RETAIL_SHARED_EXCLUSIVE_KEY' : convert_to_null(row.RETAIL_SHARED_EXCLUSIVE_KEY),
                    'b_STORE_LAUNCH_DATE' : convert_to_null(row.STORE_LAUNCH_DATE),
                    'b_STORE_PLANNED_CLR_DATE' : convert_to_null(row.STORE_PLANNED_CLR_DATE),
                    'b_RETAIL_COLLECTION_CODE' : convert_to_null(row.RETAIL_COLLECTION_CODE),
                    'b_RETAIL_COLLECTION_VALUE' : convert_to_null(row.RETAIL_COLLECTION_VALUE),
                    'b_RETAIL_COLLECTION_KEY' : convert_to_null(row.RETAIL_COLLECTION_KEY),
                    'b_RETAIL_LIFECYCLE_VALUE' : convert_to_null(row.RETAIL_LIFECYCLE_VALUE),
                    'b_RETAIL_LIFECYCLE_CODE' : convert_to_null(row.RETAIL_LIFECYCLE_CODE),
                    'b_RETAIL_LIFECYCLE_KEY' : convert_to_null(row.RETAIL_LIFECYCLE_KEY),
                    'b_RETAIL_PRICING_VALUE' : convert_to_null(row.RETAIL_PRICING_VALUE),
                    'b_RETAIL_PRICING_CODE' : convert_to_null(row.RETAIL_PRICING_CODE),
                    'b_RETAIL_PRICING_KEY' : convert_to_null(row.RETAIL_PRICING_KEY),
                    'b_RETAIL_LENGTH_VALUE' : convert_to_null(row.RETAIL_LENGTH_VALUE),
                    'b_RETAIL_LENGTH_CODE' : convert_to_null(row.RETAIL_LENGTH_CODE),
                    'b_RETAIL_LENGTH_KEY' : convert_to_null(row.RETAIL_LENGTH_KEY),
                    'b_RETAIL_WEAR_NOW_VALUE' : convert_to_null(row.RETAIL_WEAR_NOW_VALUE),
                    'b_RETAIL_WEAR_NOW_CODE' : convert_to_null(row.RETAIL_WEAR_NOW_CODE),
                    'b_RETAIL_WEAR_NOW_KEY' : convert_to_null(row.RETAIL_WEAR_NOW_KEY),
                    'b_RETAIL_COLOR_FAMILY_VALUE' : convert_to_null(row.RETAIL_COLOR_FAMILY_VALUE),
                    'b_RETAIL_COLOR_FAMILY_CODE' : convert_to_null(row.RETAIL_COLOR_FAMILY_CODE),
                    'b_RETAIL_COLOR_FAMILY_KEY' : convert_to_null(row.RETAIL_COLOR_FAMILY_KEY),
                    'b_RETAIL_ACC_AGE_SEG_VALUE' : convert_to_null(row.RETAIL_ACC_AGE_SEG_VALUE),
                    'b_RETAIL_ACC_AGE_SEG_CODE' : convert_to_null(row.RETAIL_ACC_AGE_SEG_CODE),
                    'b_RETAIL_ACC_AGE_SEG_KEY' : convert_to_null(row.RETAIL_ACC_AGE_SEG_KEY),
                    'b_RETAIL_PARENT_STYLE_VALUE' : convert_to_null(row.RETAIL_PARENT_STYLE_VALUE),
                    'b_RETAIL_PARENT_STYLE_CODE' : convert_to_null(row.RETAIL_PARENT_STYLE_CODE),
                    'b_RETAIL_PARENT_STYLE_KEY' : convert_to_null(row.RETAIL_PARENT_STYLE_KEY),
                    'b_STORE_ZONE_VALUE' : convert_to_null(row.STORE_ZONE_VALUE),
                    'b_STORE_ZONE_CODE' : convert_to_null(row.STORE_ZONE_CODE),
                    'b_STORE_ZONE_KEY' : convert_to_null(row.STORE_ZONE_KEY),
                    'b_RETAIL_RELEASED_VALUE' : convert_to_null(row.RETAIL_RELEASED_VALUE),
                    'b_RETAIL_RELEASED_CODE' : convert_to_null(row.RETAIL_RELEASED_CODE),
                    'b_RETAIL_RELEASED_KEY' : convert_to_null(row.RETAIL_RELEASED_KEY),
                    'b_PRIMARY_VENDOR' : convert_to_null(row.PRIMARY_VENDOR),
                    'b_SECONDARY_VENDOR' : convert_to_null(row.SECONDARY_VENDOR),
                    'b_RETAIL_OSV_COST' : convert_to_null(row.RETAIL_OSV_COST),
                    'b_ECOMM_PLANNED_CL_DATE' : convert_to_null(row.ECOMM_PLANNED_CL_DATE),
                    'b_ECOMM_WEB_COLOR_KEY' : convert_to_null(row.ECOMM_WEB_COLOR_KEY),
                    'b_ECOMM_WEB_COLOR_VALUE' : convert_to_null(row.ECOMM_WEB_COLOR_VALUE),
                    'b_ECOMM_WEB_COLOR_CODE' : convert_to_null(row.ECOMM_WEB_COLOR_CODE),
                    'b_ECOMM_PHOTOSTYLE' : convert_to_null(row.ECOMM_PHOTOSTYLE),
                    'b_ECOMM_WEB_VAR_MASTER' : convert_to_null(row.ECOMM_WEB_VAR_MASTER),
                    'b_ECOMM_RELEASED_VALUE' : convert_to_null(row.ECOMM_RELEASED_VALUE),
                    'b_ECOMM_RELEASED_CODE' : convert_to_null(row.ECOMM_RELEASED_CODE),
                    'b_ECOMM_RELEASED_KEY' : convert_to_null(row.ECOMM_RELEASED_KEY),
                    'b_VENDOR_STYLE_ID' : convert_to_null(row.VENDOR_STYLE_ID),
                    'b_PRODUCT_TYPE_CODE' : convert_to_null(row.PRODUCT_TYPE_CODE),
                    'b_PRODUCT_TYPE_KEY' : convert_to_null(row.PRODUCT_TYPE_KEY),
                    'b_PRODUCT_TYPE_VALUE' : convert_to_null(row.PRODUCT_TYPE_VALUE),
                    'b_UPC' : convert_to_null(row.UPC),
                    'b_PRIMARY_VENDOR_ID' : convert_to_null(row.PRIMARY_VENDOR_ID),
                    'b_SECONDARY_VENDOR_ID' : convert_to_null(row.SECONDARY_VENDOR_ID),
                    'b_RETAIL_COMPOSITION_CODE' : convert_to_null(row.RETAIL_COMPOSITION_CODE),
                    'b_RETAIL_COMPOSITION_KEY' : convert_to_null(row.RETAIL_COMPOSITION_KEY),
                    'b_RETAIL_COMPOSITION_VALUE' : convert_to_null(row.RETAIL_COMPOSITION_VALUE),
                    'b_ECOMM_PLANNING_ATTRIBUTE_CD' : convert_to_null(row.ECOMM_PLANNING_ATTRIBUTE_CODE),
                    'b_ECOMM_PLANNING_ATTRIBUTE_KEY' : convert_to_null(row.ECOMM_PLANNING_ATTRIBUTE_KEY),
                    'b_ECOMM_PLANNING_ATTRIBUTE_VAL' : convert_to_null(row.ECOMM_PLANNING_ATTRIBUTE_VALUE),
                    'b_RETAIL_BRAND_VALUE' : convert_to_null(row.RETAIL_BRAND_VALUE),
                    'b_RETAIL_BRAND_CODE' : convert_to_null(row.RETAIL_BRAND_CODE),
                    'b_RETAIL_BRAND_KEY' : convert_to_null(row.RETAIL_BRAND_KEY),
                    'b_COLORWAY_SOURCING_STATUS' : convert_to_null(row.COLORWAY_SOURCING_STATUS),
                    'b_SOURCE_ACTIVE' : convert_to_null(row.SOURCE_ACTIVE),
                    'b_CHARACTER_VALUE' : convert_to_null(row.CHARACTER_VALUE),
                    'b_CHARACTER_CODE' : convert_to_null(row.CHARACTER_CODE),
                    'b_CHARACTER_KEY' : convert_to_null(row.CHARACTER_KEY),
                    'b_CATEGORY_VALUE' : convert_to_null(row.CATEGORY_VALUE),
                    'b_CATEGORY_CODE' : convert_to_null(row.CATEGORY_CODE),
                    'b_CATEGORY_KEY' : convert_to_null(row.CATEGORY_KEY),
                    'b_PRINT_VALUE' : convert_to_null(row.PRINT_VALUE),
                    'b_PRINT_KEY' : convert_to_null(row.PRINT_KEY),
                    'b_PRINT_CODE' : convert_to_null(row.PRINT_CODE),
                    'b_ITEM_CATEGORY_VALUE' : convert_to_null(row.ITEM_CATEGORY_VALUE),
                    'b_ITEM_CATEGORY_CODE' : convert_to_null(row.ITEM_CATEGORY_CODE),
                    'b_ITEM_CATEGORY_KEY' : convert_to_null(row.ITEM_CATEGORY_KEY),
                    'b_SEASON_CREATED_KEY' : convert_to_null(row.SEASON_CREATED_KEY),
                    'b_SEASON_CREATED_VALUE' : convert_to_null(row.SEASON_CREATED_VALUE),
                    'b_SEASON_CREATED_CODE' : convert_to_null(row.SEASON_CREATED_CODE),
                    'b_YEAR_CREATED_KEY' : convert_to_null(row.YEAR_CREATED_KEY),
                    'b_YEAR_CREATED_VALUE' : convert_to_null(row.YEAR_CREATED_VALUE),
                    'b_ALT_IMAGE' : convert_to_null(row.ALT_IMAGE),
                    'b_ORIGINAL_PRICE' : convert_to_null(row.ORIGINAL_PRICE),
                    'b_PROGRAM' : convert_to_null(row.PROGRAM),
                    'b_STORE_RELAUNCH_DATE' : convert_to_null(row.STORE_RELAUNCH_DATE),
                    'b_STORE_RELAUNCH_CLR_DATE' : convert_to_null(row.STORE_RELAUNCH_CLR_DATE),
                    'b_ECOM_RELAUNCH_DATE' : convert_to_null(row.ECOM_RELAUNCH_DATE),
                    'b_ECOM_RELAUNCH_CLR_DATE' : convert_to_null(row.ECOM_RELAUNCH_CLR_DATE),
                    'b_SKIPHOP_COMMENTS' : convert_to_null(row.SKIPHOP_COMMENTS),
                    'b_LANDED_COST' : convert_to_null(row.LANDED_COST),
                    'b_SUB_CATEGORY_VALUE' : convert_to_null(row.SUB_CATEGORY_VALUE),
                    'b_SUB_CATEGORY_CODE' : convert_to_null(row.SUB_CATEGORY_CODE),
                    'b_SUB_CATEGORY_KEY' : convert_to_null(row.SUB_CATEGORY_KEY),
                    'b_INTL_LIFECYCLE_VALUE' : convert_to_null(row.INTL_LIFECYCLE_VALUE),
                    'b_INTL_LIFECYCLE_CODE' : convert_to_null(row.INTL_LIFECYCLE_CODE),
                    'b_INTL_LIFECYCLE_KEY' : convert_to_null(row.INTL_LIFECYCLE_KEY),
                    'b_DOMESTIC_LIFECYCLE_VALUE' : convert_to_null(row.DOMESTIC_LIFECYCLE_VALUE),
                    'b_DOMESTIC_LIFECYCLE_CODE' : convert_to_null(row.DOMESTIC_LIFECYCLE_CODE),
                    'b_DOMESTIC_LIFECYCLE_KEY' : convert_to_null(row.DOMESTIC_LIFECYCLE_KEY),
                    'b_REPORTABLE_AGE_VALUE' : convert_to_null(row.REPORTABLE_AGE_VALUE),
                    'b_REPORTABLE_AGE_CODE' : convert_to_null(row.REPORTABLE_AGE_CODE),
                    'b_REPORTABLE_AGE_KEY' : convert_to_null(row.REPORTABLE_AGE_KEY),
                    'b_INTL_PARTNERS_VALUE' : convert_to_null(row.INTL_PARTNERS_VALUE),
                    'b_INTL_PARTNERS_CODE' : convert_to_null(row.INTL_PARTNERS_CODE),
                    'b_INTL_PARTNERS_KEY' : convert_to_null(row.INTL_PARTNERS_KEY),
                    'b_MERCHANDISER' : convert_to_null(row.MERCHANDISER),
                    'b_DTL__CAPXTIMESTAMP' : v_capxtime,
                    'v_FLEX_ID':row.FLEX_ID  
                })

                updcount = updcount +1
                hubconn.commit()
                etl_status = 'Success'

            except Exception as e:
                print('FLEX_ID: ' + row.FLEX_ID + ' error: ' + str(e))
                hubconn.rollback()
                etl_status = 'Fail'
                break
                
    print('Inserted total row(s): ' + str(inscount))            
    print('Updated total row(s): ' + str(updcount))    
    print('Archived total row(s): ' + str(updcount))            
                
    """
    Update the action flag to 0 on 
    the FLEX database
    """
    if etl_status == 'Success':    
        try:
            flex_cur.execute(update_flex,{'b_FLEX_ID':row.FLEX_ID,
                                          'b_LAST_UPDATED':row.LAST_UPDATED,
                                          'b_ACTION_FLAG':row.ACTION_FLAG})
            flexconn.commit()

        except Exception as e:
            print('FLEX_ID: ' + row.FLEX_ID + ' error: ' + str(e) + '.  Error happened during update to flex action flag')
            flexconn.rollback()      
        
    
def post_sql(hub_cur):
    #Post SQL Runs procedure in the HUB
    
    proc_nm ='PKG_RUN_JOBS.PROC_RUN_JOB_STEPS'
    job_nm = 'POST_BULK_CDC_FLEX'
    load_type ='BULK'
    job_seq = 'NULL'

    hub_cur.callproc(proc_nm,[job_nm,load_type,job_seq])
    

def main():
    
    start_time = datetime.now()
    
    print('Start flow')
    print('>>> Start ETL process @ ' + str(start_time) + ' <<<')
    #read db cfg file to connect to the databases
    config = configparser.ConfigParser()
    config.read_file(open('db.cfg'))
    trg_db=config.get('HUB','dbname')
    trg_usr=config.get('HUB','username')
    trg_pswd= config.get('HUB','password')

    src_db=config.get('FLEX','dbname')
    src_usr=config.get('FLEX','username')
    src_pswd= config.get('FLEX','password')
    
    #Apply connection string to connect to FLEX db and PHUB db
    
    hubconn = cx_Oracle.connect("{}/{}@{}".format(trg_usr,trg_pswd,trg_db))
    flexconn = cx_Oracle.connect("{}/{}@{}".format(src_usr,src_pswd,src_db))
    
    hub_cur = hubconn.cursor()
    flex_cur = flexconn.cursor()
    
    #call processes needed to execute ETL
    print('Execute pre sql logic: @' + str(datetime.now()))
          
    pre_sql(flex_cur,flexconn)
   
    print('Execute etl process: @' + str(datetime.now()))
    etl_process(flexconn,hubconn,flex_cur,hub_cur)
    
    print('Execute post sql: @' + str(datetime.now()))
    post_sql(hub_cur)
    
    end_time =datetime.now()
    print('>>> End ETL process @ ' + str(end_time) + ' <<<')
    
    run_time = end_time - start_time
    
    print('Total runtime: ' + str(run_time))
    
    #close all connections and cursors
    flex_cur.close()
    hub_cur.close()
    hubconn.close()
    flexconn.close()
    
    print('All cursors and database connections closed')

if __name__ == "__main__":
    main()