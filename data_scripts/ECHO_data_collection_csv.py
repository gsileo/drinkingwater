import pandas as pd
import os
import math
import time
import sys
import numpy as np
import sqlalchemy

import pickle

os.chdir('/home/gsileo/repos/water')

from db_utils import DatabaseConfiguration, PostgresConnectionManager

DB_CONFIG = DatabaseConfiguration(
    host = 'localhost',
    port = 5432,
    username = os.environ.get('POSTGRES_USERNAME'),
    password = os.environ.get('POSTGRES_PASSWORD'),
    database_name = 'water'
)

###############################################
### Collect all of the FSDWIS Data from API ###
###############################################

SDWIS_tables = ['WATER_SYSTEM', 'ENFORCEMENT_ACTION', 'GEOGRAPHIC_AREA', 'LCR_SAMPLE_RESULT', 'LCR_SAMPLE', 'SERVICE_AREA', 'TREATMENT', 'WATER_SYSTEM_FACILITY', 
                'VIOLATION', 'VIOLATION_ENF_ASSOC']

#####################
### water system ###

water_system_calls = []
count = 100000
call = 0
while count == 100000:
    #first construct the url
    row_return_start = 1 + call*100000
    row_return_end = 100000 + call*100000
    
    #construct the url
    water_system_url = f'https://enviro.epa.gov/enviro/efservice/{SDWIS_tables[0]}/ROWS/{row_return_start}:{row_return_end}/CSV'
        
    #get data and save as dataframe
    water_system_df = pd.read_csv(water_system_url)
    
    #add to the list of calls
    water_system_calls.append(water_system_df)

    #update the count to reflect number of records returned
    count = len(water_system_df)
        
    #update call counter
    call = call + 1    
    
for i in range(len(water_system_calls)):
    #drop extra column
    water_system_calls[i].drop(columns='Unnamed: 47', inplace=True)
    #rename columns
    water_system_calls[i].columns = ['PWSID', 'PWS_NAME','NPM_CANDIDATE', 'PRIMACY_AGENCY_CODE', 'EPA_REGION', 'SEASON_BEGIN_DATE', 'SEASON_END_DATE', 'PWS_ACTIVITY_CODE',
                                     'PWS_DEACTIVATION_DATE', 'PWS_TYPE_CODE', 'DBPR_SCHEDULE_CAT_CODE', 'CDS_ID', 'GW_SW_CODE', 'LT2_SCHEDULE_CAT_CODE', 'OWNER_TYPE_CODE',
                                     'POPULATION_SERVED_COUNT', 'POP_CAT_2_CODE', 'POP_CAT_3_CODE', 'POP_CAT_4_CODE', 'POP_CAT_5_CODE', 'POP_CAT_11_CODE', 'PRIMACY_TYPE', 
                                     'PRIMARY_SOURCE_CODE', 'IS_GRANT_ELIGIBLE_IND', 'IS_WHOLESALER_IND', 'IS_SCHOOL_OR_DAYCARE_IND', 'SERVICE_CONNECTIONS_COUNT','SUBMISSION_STATUS_CODE',
                                     'ORG_NAME', 'ADMIN_NAME', 'EMAIL_ADDR', 'PHONE_NUMBER', 'PHONE_EXT_NUMBER', 'FAX_NUMBER', 'ALT_PHONE_NUMBER', 'ADDRESS_LINE1', 'ADDRESS_LINE2', 
                                     'CITY_NAME', 'ZIP_CODE', 'COUNTRY_CODE', 'STATE_CODE', 'SOURCE_WATER_PROTECTION_CODE', 'SOURCE_PROTECTION_BEGIN_DATE', 'OUTSTANDING_PERFORMER',
                                     'OUTSTANDING_PERFORM_BEGIN_DATE', 'CITIES_SERVED', 'COUNTIES_SERVED']
    #send to lowercase
    water_system_calls[i].columns.str.lower()
    
    with PostgresConnectionManager(DB_CONFIG) as conn:
        water_system_calls[i].to_sql('water_system', conn, schema='fsdwis', if_exists ='append', index=False)
        
        
##########################
### enforcement action ###

enforcement_action_calls = []
count = 100000
call = 0
while count == 100000:
    #first construct the url
    row_return_start = 1 + call*100000
    row_return_end = 100000 + call*100000
    
    #set the url
    enforcement_action_url = f'https://enviro.epa.gov/enviro/efservice/{SDWIS_tables[1]}/ROWS/{row_return_start}:{row_return_end}/CSV'
    
    #get data and save as dataframe
    enforcement_action_df = pd.read_csv(enforcement_action_url)
    
    #save to list
    enforcement_action_calls.append(enforcement_action_df)
    
    #update the count to reflect number of records returned
    count = len(enforcement_action_df)
        
    #update call counter
    call = call + 1
    
for i in range(len(enforcement_action_calls)):
    #drop extra column
    enforcement_action_calls[i].drop(columns='Unnamed: 6', inplace=True)
    #rename columns
    enforcement_action_calls[i].columns = ['PWSID', 'ENFORCEMENT_ID', 'ORIGINATOR_CODE', 'ENFORCEMENT_DATE', 'ENFORCEMENT_ACTION_TYPE_CODE','ENFORCEMENT_COMMENT_TEXT']
    #send to lowercase
    enforcement_action_calls[i].columns.str.lower()
    
    with PostgresConnectionManager(DB_CONFIG) as conn:
        enforcement_action_calls[i].to_sql('enforcement_action', conn, schema='fsdwis', if_exists ='append', index=False)
        

#######################
### geographic area ###    

geographic_area_calls = []
count = 100000
call = 0
while count == 100000:
    #first construct the url
    row_return_start = 1 + call*100000
    row_return_end = 100000 + call*100000
    
    #set the url
    geographic_area_url = f'https://enviro.epa.gov/enviro/efservice/{SDWIS_tables[2]}/ROWS/{row_return_start}:{row_return_end}/CSV'
    
    #get data and save as dataframe
    geographic_area_df = pd.read_csv(geographic_area_url)
    
    #save to list
    geographic_area_calls.append(geographic_area_df)
    
    #update the count to reflect number of records returned
    count = len(geographic_area_df)
        
    #update call counter
    call = call + 1
    
for i in range(len(geographic_area_calls)):
    #drop extra column
    geographic_area_calls[i].drop(columns='Unnamed: 13', inplace=True)
    #rename columns
    geographic_area_calls[i].columns = ['PWSID', 'GEO_ID', 'PRIMACY_AGENCY_CODE', 'EPA_REGION', 'PWS_ACTIVITY_CODE', 'PWS_TYPE_CODE', 'TRIBAL_CODE', 'STATE_SERVED','ANSI_ENTITY_CODE', 
                                        'ZIP_CODE_SERVED', 'CITY_SERVED', 'AREA_TYPE_CODE', 'COUNTY_SERVED']
    #send to lowercase
    geographic_area_calls[i].columns.str.lower()
    
    with PostgresConnectionManager(DB_CONFIG) as conn:
        geographic_area_calls[i].to_sql('geographic_area', conn, schema='fsdwis', if_exists ='append', index=False)
        
        
#########################
### lcr sample result ###    

lcr_sample_result_calls = []
count = 100000
call = 0
while count == 100000:
    #first construct the url
    row_return_start = 1 + call*100000
    row_return_end = 100000 + call*100000
    
    #set the url
    lcr_sample_result_url = f'https://enviro.epa.gov/enviro/efservice/{SDWIS_tables[3]}/ROWS/{row_return_start}:{row_return_end}/CSV'
    
    #get data and save as dataframe
    lcr_sample_result_df = pd.read_csv(lcr_sample_result_url)
    
    #save to list
    lcr_sample_result_calls.append(lcr_sample_result_df)
    
    #update the count to reflect number of records returned
    count = len(lcr_sample_result_df)
        
    #update call counter
    call = call + 1
    
for i in range(len(lcr_sample_result_calls)):
    #drop extra column
    lcr_sample_result_calls[i].drop(columns='Unnamed: 9', inplace=True)
    #rename columns
    lcr_sample_result_calls[i].columns = ['PWSID', 'SAMPLE_ID', 'PRIMACY_AGENCY_CODE', 'EPA_REGION', 'SAR_ID', 'CONTAMINANT_CODE', 'RESULT_SIGN_CODE', 'SAMPLE_MEASURE',
                                          'UNIT_OF_MEASURE']
    #send to lowercase
    lcr_sample_result_calls[i].columns.str.lower()
    
    with PostgresConnectionManager(DB_CONFIG) as conn:
        lcr_sample_result_calls[i].to_sql('lcr_sample_result', conn, schema='fsdwis', if_exists ='append', index=False)

        
##################
### lcr sample ###   

lcr_sample_calls = []
count = 100000
call = 0
while count == 100000:
    #first construct the url
    row_return_start = 1 + call*100000
    row_return_end = 100000 + call*100000
    
    #set the url
    lcr_sample_url = f'https://enviro.epa.gov/enviro/efservice/{SDWIS_tables[4]}/ROWS/{row_return_start}:{row_return_end}/CSV'
    
    #get data and save as dataframe
    lcr_sample_df = pd.read_csv(lcr_sample_url)
    
    #save to list
    lcr_sample_calls.append(lcr_sample_df)
    
    #update the count to reflect number of records returned
    count = len(lcr_sample_df)
    
    #update call counter
    call = call + 1
    
for i in range(len(lcr_sample_calls)):
    #drop extra column
    lcr_sample_calls[i].drop(columns='Unnamed: 7', inplace=True)
    #rename columns
    lcr_sample_calls[i].columns = ['PWSID', 'SAMPLE_ID', 'SAMPLING_END_DATE', 'SAMPLING_START_DATE', 'RECONCILIATION_ID', 'PRIMACY_AGENCY_CODE', 'EPA_REGION']
    #send to lowercase
    lcr_sample_calls[i].columns.str.lower()
    
    with PostgresConnectionManager(DB_CONFIG) as conn:
        lcr_sample_calls[i].to_sql('lcr_sample', conn, schema='fsdwis', if_exists ='append', index=False)
        
        
####################
### service area ###

service_area_calls = []
count = 100000
call = 0 
while count == 100000:
    #first construct the url
    row_return_start = 1 + call*100000
    row_return_end = 100000 + call*100000
    
    #set the url
    service_area_url = f'https://enviro.epa.gov/enviro/efservice/{SDWIS_tables[5]}/ROWS/{row_return_start}:{row_return_end}/CSV'
    
    #get data and save as dataframe
    service_area_df = pd.read_csv(service_area_url)
    
    #save to list
    service_area_calls.append(service_area_df)
    
    #update the count to reflect number of records returned
    count = len(service_area_df)
    
    #update call counter
    call = call + 1
    
for i in range(len(service_area_calls)):
    #drop extra column
    service_area_calls[i].drop(columns='Unnamed: 7', inplace=True)
    #rename columns
    service_area_calls[i].columns = ['PWSID', 'PRIMACY_AGENCY_CODE', 'EPA_REGION', 'PWS_ACTIVITY_CODE', 'PWS_TYPE_CODE', 'SERVICE_AREA_TYPE_CODE','IS_PRIMARY_SERVICE_AREA_CODE']
    #send to lowercase
    service_area_calls[i].columns.str.lower()
    
    with PostgresConnectionManager(DB_CONFIG) as conn:
        service_area_calls[i].to_sql('service_area', conn, schema='fsdwis', if_exists ='append', index=False)
        
        
#################
### treatment ###

treatment_calls = []
count = 100000
call = 0 
while count == 100000:
    #first construct the url
    row_return_start = 1 + call*100000
    row_return_end = 100000 + call*100000
    
    #set the url
    treatment_url = f'https://enviro.epa.gov/enviro/efservice/{SDWIS_tables[6]}/ROWS/{row_return_start}:{row_return_end}/CSV'
    
    #get data and save as dataframe
    treatment_df = pd.read_csv(treatment_url)
    
    #save to list
    treatment_calls.append(treatment_df)
    
    #update the count to reflect number of records returned
    count = len(treatment_df)
    
    #update call counter
    call = call + 1
    
for i in range(len(treatment_calls)):
    #drop extra column
    treatment_calls[i].drop(columns='Unnamed: 6', inplace=True)
    #rename columns
    treatment_calls[i].columns = ['PWSID', 'FACILITY_ID', 'TREATMENT_ID', 'COMMENTS_TEXT', 'TREATMENT_OBJECTIVE_CODE', 'TREATMENT_PROCESS_CODE']
    #send to lowercase
    treatment_calls[i].columns.str.lower()
    
    with PostgresConnectionManager(DB_CONFIG) as conn:
        treatment_calls[i].to_sql('treatment', conn, schema='fsdwis', if_exists ='append', index=False)
        
        
#############################
### water system facility ###

water_system_facility_calls = []
count = 100000
call = 0 
while count == 100000:
    #first construct the url
    row_return_start = 1 + call*100000
    row_return_end = 100000 + call*100000
    
    #set the url
    water_system_facility_url = f'https://enviro.epa.gov/enviro/efservice/{SDWIS_tables[7]}/ROWS/{row_return_start}:{row_return_end}/CSV'
    
    #get data and save as dataframe
    water_system_facility_df = pd.read_csv(water_system_facility_url)
    
    #save to list
    water_system_facility_calls.append(water_system_facility_df)
    
    #update the count to reflect number of records returned
    count = len(water_system_facility_df)
    
    #update call counter
    call = call + 1
    
for i in range(len(water_system_facility_calls)):
    #drop extra column
    water_system_facility_calls[i].drop(columns='Unnamed: 21', inplace=True)
    #rename columns
    water_system_facility_calls[i].columns = ['PWSID', 'PRIMACY_AGENCY_CODE', 'EPA_REGION', 'FACILITY_ID','FACILITY_NAME', 'STATE_FACILITY_ID', 'FACILITY_ACTIVITY_CODE', 
                                              'FACILITY_DEACTIVATION_DATE', 'FACILITY_TYPE_CODE', 'SUBMISSION_STATUS_CODE', 'IS_SOURCE_IND', 'WATER_TYPE_CODE', 'AVAILABILITY_CODE', 
                                              'SELLER_TREATMENT_CODE', 'SELLER_PWSID', 'SELLER_PWS_NAME', 'FILTRATION_STATUS_CODE', 'PWS_ACTIVITY_CODE', 'PWS_DEACTIVATION_DATE', 
                                              'PWS_TYPE_CODE', 'IS_SOURCE_TREATED_IND']
    #send to lowercase
    water_system_facility_calls[i].columns.str.lower()
    
    with PostgresConnectionManager(DB_CONFIG) as conn:
        water_system_facility_calls[i].to_sql('water_system_facility', conn, schema='fsdwis', if_exists ='append', index=False)
        

#################
### violation ###

violation_calls = []
count = 100000
call = 0 
while count == 100000:
    #first construct the url
    row_return_start = 1 + call*100000
    row_return_end = 100000 + call*100000
    
    #set the url
    violation_url = f'https://enviro.epa.gov/enviro/efservice/{SDWIS_tables[8]}/ROWS/{row_return_start}:{row_return_end}/CSV'
    
    #get data and save as dataframe
    violation_df = pd.read_csv(violation_url)
    
    #save to list
    violation_calls.append(violation_df)
    
    #update the count to reflect number of records returned
    count = len(violation_df)
    
    #update call counter
    call = call + 1
    
for i in range(len(violation_calls)):
    #drop extra column
    violation_calls[i].drop(columns='Unnamed: 34', inplace=True)
    #rename columns
    violation_calls[i].columns = ['PWSID', 'VIOLATION_ID', 'FACILITY_ID', 'POPULATION_SERVED_COUNT', 'NPM_CANDIDATE', 'PWS_ACTIVITY_CODE', 'PWS_DEACTIVATION_DATE', 
                                  'PRIMARY_SOURCE_CODE', 'POP_CAT_5_CODE', 'PRIMACY_AGENCY_CODE', 'EPA_REGION', 'PWS_TYPE_CODE', 'VIOLATION_CODE', 'VIOLATION_CATEGORY_CODE', 
                                  'IS_HEALTH_BASED_IND', 'CONTAMINANT_CODE', 'COMPLIANCE_STATUS_CODE', 'VIOL_MEASURE', 'UNIT_OF_MEASURE', 'STATE_MCL', 'IS_MAJOR_VIOL_IND', 
                                  'SEVERITY_IND_CNT', 'COMPL_PER_BEGIN_DATE', 'COMPL_PER_END_DATE', 'LATEST_ENFORCEMENT_ID', 'RTC_ENFORCEMENT_ID', 'RTC_DATE', 
                                  'PUBLIC_NOTIFICATION_TIER', 'ORIGINATOR_CODE', 'SAMPLE_RESULT_ID', 'CORRECTIVE_ACTION_ID', 'RULE_CODE', 'RULE_GROUP_CODE', 'RULE_FAMILY_CODE']
    #send to lowercase
    violation_calls[i].columns.str.lower()
    
    with PostgresConnectionManager(DB_CONFIG) as conn:
        violation_calls[i].to_sql('violation', conn, schema='fsdwis', if_exists ='append', index=False)
        
        
####################################
### violation enforcement action ###

violation_enf_assoc_calls = []
count = 100000
call = 0 
while count == 100000:
    #first construct the url
    row_return_start = 1 + call*100000
    row_return_end = 100000 + call*100000
    
    #set the url
    violation_enf_assoc_url = f'https://enviro.epa.gov/enviro/efservice/{SDWIS_tables[9]}/ROWS/{row_return_start}:{row_return_end}/CSV'
    
    #get data and save as dataframe
    violation_enf_assoc_df = pd.read_csv(violation_enf_assoc_url)
    
    #save to list
    violation_enf_assoc_calls.append(violation_enf_assoc_df)
    
    #update the count to reflect number of records returned
    count = len(violation_enf_assoc_df)
    
    #update call counter
    call = call + 1
    
for i in range(len(violation_enf_assoc_calls)):
    #drop extra column
    violation_enf_assoc_calls[i].drop(columns='Unnamed: 3', inplace=True)
    #rename columns
    violation_enf_assoc_calls[i].columns = ['PWSID', 'ENFORCEMENT_ID', 'VIOLATION_ID']
    #send to lowercase
    violation_enf_assoc_calls[i].columns.str.lower()
    
    with PostgresConnectionManager(DB_CONFIG) as conn:
        violation_enf_assoc_calls[i].to_sql('violation_enf_assoc', conn, schema='fsdwis', if_exists ='append', index=False)