import pandas as pd
import os
import math
import time
import random
import sys
import numpy as np
import re
import json
import pickle
import sqlalchemy
from sqlalchemy.types import Date

from datetime import datetime, date

os.chdir('/home/gsileo/repos/drinkingwater')
from config import PATH_TO_DATA, PATH_TO_CENSUS_DATA, PATH_TO_DATA_SCRIPTS

import warnings
warnings.filterwarnings('ignore')

#########################
### Pull in Utilities ###
#########################
from db_utils import DatabaseConfiguration, PostgresConnectionManager

DB_CONFIG = DatabaseConfiguration(
    host = 'localhost',
    port = 5432,
    username = os.environ.get('POSTGRES_USERNAME'),
    password = os.environ.get('POSTGRES_PASSWORD'),
    database_name = 'water'
)

######################################################################
### Pull in Listings for Systems and Projects Plus Extra Variables ###
######################################################################

os.chdir(PATH_TO_DATA_SCRIPTS)
from ky_wris_master_sys_list_construction import *
os.chdir(PATH_TO_DATA_SCRIPTS)
from ky_wris_master_proj_list_construction import *

#################
### functions ###
#################

def convert_int(numeric_string):
    try:
        return int(numeric_string)
    except:
        return np.nan
    
def convert_salary(monetary_string):
    try:
        return int(re.sub('\$|,', '', monetary_string))
    except:
        return np.nan
    
def convert_money(monetary_string):
    try:
        return float(re.sub('\$|,', '', monetary_string))
    except:
        return np.nan
    
def approval_modified_diff(date_approved, date_modified):
    date_diff = (date_modified - date_approved).days/365
    
    if date_diff < 0:
        date_diff = -1
    
    return date_diff

##############################################
### Construct the Systems Data Master File ###
##############################################
os.chdir(f'{PATH_TO_WRIS_DATA}/systems')
sys_files = os.listdir(f'{PATH_TO_WRIS_DATA}/systems')
sys_jsons = [i for i in sys_files if 'KY' in i]

#initialize a projects dataframe to append to where the source data is the system files
system_projects_df = pd.DataFrame()

#initialize a mapping between systems and projects
system_project_pairings = pd.DataFrame()

#initialize a systems dataframe to append to
systems_df = pd.DataFrame()

#initialize a dataframe to contain selling-purchasing relationships
system_selling_pairings = pd.DataFrame()

for file in sys_jsons:
    #open the json and extract to dictionary
    with open(file) as f:
        curr_pwsid = json.load(f)
    
    #append the projects to the projects dataframe
    system_projects_df = system_projects_df.append(curr_pwsid['Projects'])
    
    #construct a projects-systems pairing from the systems data
    system_project_pairings = system_project_pairings.append([dict({'pwsid': curr_pwsid['DOW Permit ID']}, **item) for item in curr_pwsid['Projects']])
    
    #construct a systems-selling relationship dataframe -- if the relationship exists and isn't just for emergency purposes
    if curr_pwsid['Purchaser Systems']:
        system_selling_pairings = system_selling_pairings.append([{'seller': curr_pwsid['DOW Permit ID'], 'purchaser': item['Purchaser DOW Permit ID'], 'volume': convert_money(item['Ann. Vol. (MG)']), 
                                                                   'type': item['Water Type'], 'cost': np.nanmin((convert_money(item['Raw Cost (1,000 G)']), convert_money(item['Finished Cost (1,000 G)']))),
                                                                   'connections': int(item['Permenant Conn']), 'pop': convert_salary(item['Serviceable Population']),
                                                                   'households': convert_salary(item['Serviceable Households'])} 
                                                                  for item in curr_pwsid['Purchaser Systems'] if int(item['Permenant Conn']) !=0]) #omg, spelling
    
    #there are two pwsids where the county detail seems to be faulty 
    if curr_pwsid['DOW Permit ID'] in ['KY0351008', 'KY0831010']:
        #append the data for a WRIS systems dataframe
        systems_df = systems_df.append([{'pwsid': curr_pwsid['DOW Permit ID'], 'sys_name': curr_pwsid['DOW Permit Name'], 'sys_type': curr_pwsid['System Type'], 
                                     'water_source_type': curr_pwsid['Water Source Type'], 'area_development_district': curr_pwsid['ADD ID'], 
                                     'primary_county': curr_pwsid['Primary County'], 'date_established': curr_pwsid['Date Established'], 'entity_type': curr_pwsid['Entity Type'],
                                     'employees': curr_pwsid['Employees'],
                                     'directly_serviceable_pop': re.sub(',', '', curr_pwsid['Directly Serviceable Pop']), 
                                     'directly_serviceable_hh': re.sub(',', '', curr_pwsid['Directly Serviceable HH']),
                                     'indirectly_serviceable_pop': re.sub(',', '', curr_pwsid['Indirectly Serviceable Pop']), 
                                     'indirectly_serviceable_hh': re.sub(',', '', curr_pwsid['Indirectly Serviceable HH']),
                                     'purchaser_systems': len(curr_pwsid['Purchaser Systems']), 'seller_systems': len(curr_pwsid['Seller Systems']),
                                     'wholesale_customers': re.sub(',', '', curr_pwsid['Wholesale Customers']), 
                                     'residential_customers': re.sub(',', '', curr_pwsid['Residential Customers']),
                                     'commercial_customers': re.sub(',', '', curr_pwsid['Commercial Customers']), 
                                     'institutional_customers': re.sub(',', '', curr_pwsid['Institutional Customers']), 
                                     'industrial_customers': re.sub(',', '', curr_pwsid['Industrial Customers']), 
                                     'other_customers': re.sub(',', '', curr_pwsid['Other Customers']), 
                                     'total_customers': re.sub(',', '', curr_pwsid['Total Customers']), 
                                     'psc_group_id': curr_pwsid['PSC Group ID'], 
                                     'inside_city_water_rate': curr_pwsid['If municipal system, cost per 4,000 gallons of finished water inside municipality'],
                                     'outside_city_water_rate': curr_pwsid['If municipal system, cost per 4,000 gallons of finished water outside municipality'],
                                     'not_city_water_rate': curr_pwsid['If non-municipal system, cost per 4,000 gallons of finished water'],
                                     'rate_adj_date': curr_pwsid['Date of Last Rate Adjustment']}])
        
    else:      
        #append the data for a WRIS systems dataframe
        systems_df = systems_df.append([dict({'pwsid': curr_pwsid['DOW Permit ID'], 'sys_name': curr_pwsid['DOW Permit Name'], 'sys_type': curr_pwsid['System Type'], 
                                     'water_source_type': curr_pwsid['Water Source Type'], 'area_development_district': curr_pwsid['ADD ID'], 
                                     'primary_county': curr_pwsid['Primary County'], 'date_established': curr_pwsid['Date Established'], 'entity_type': curr_pwsid['Entity Type'],
                                     'employees': curr_pwsid['Employees'],
                                     'directly_serviceable_pop': re.sub(',', '', curr_pwsid['Directly Serviceable Pop']), 
                                     'directly_serviceable_hh': re.sub(',', '', curr_pwsid['Directly Serviceable HH']),
                                     'indirectly_serviceable_pop': re.sub(',', '', curr_pwsid['Indirectly Serviceable Pop']), 
                                     'indirectly_serviceable_hh': re.sub(',', '', curr_pwsid['Indirectly Serviceable HH']),
                                     'purchaser_systems': len(curr_pwsid['Purchaser Systems']), 'seller_systems': len(curr_pwsid['Seller Systems']),
                                     'wholesale_customers': re.sub(',', '', curr_pwsid['Wholesale Customers']), 
                                     'residential_customers': re.sub(',', '', curr_pwsid['Residential Customers']),
                                     'commercial_customers': re.sub(',', '', curr_pwsid['Commercial Customers']), 
                                     'institutional_customers': re.sub(',', '', curr_pwsid['Institutional Customers']), 
                                     'industrial_customers': re.sub(',', '', curr_pwsid['Industrial Customers']), 
                                     'other_customers': re.sub(',', '', curr_pwsid['Other Customers']), 
                                     'total_customers': re.sub(',', '', curr_pwsid['Total Customers']), 
                                     'psc_group_id': curr_pwsid['PSC Group ID'], 
                                     'inside_city_water_rate': curr_pwsid['If municipal system, cost per 4,000 gallons of finished water inside municipality'],
                                     'outside_city_water_rate': curr_pwsid['If municipal system, cost per 4,000 gallons of finished water outside municipality'],
                                     'not_city_water_rate': curr_pwsid['If non-municipal system, cost per 4,000 gallons of finished water'],
                                     'rate_adj_date': curr_pwsid['Date of Last Rate Adjustment'],
                                     'tot_county_conn_count': [i for i in curr_pwsid['Counties Served Detail'] if i['County Served'] == 'Totals:'][0]['Connection Count'],
                                     'tot_county_serv_pop': [i for i in curr_pwsid['Counties Served Detail'] if i['County Served'] == 'Totals:'][0]['Serviceable Population'],
                                     'tot_county_serv_hh': [i for i in curr_pwsid['Counties Served Detail'] if i['County Served'] == 'Totals:'][0]['Serviceable Households'],         
                                     'tot_county_serv_mhi': [i for i in curr_pwsid['Counties Served Detail'] if i['County Served'] == 'Totals:'][0]['Med. HH Income'],
                                     'tot_county_serv_mhi_mode': [i for i in curr_pwsid['Counties Served Detail'] if i['County Served'] == 'Totals:'][0]['MHI MOE']}, 
                                     **item) for item in curr_pwsid['Counties Served Detail'] if item['County Served'] != 'Totals:'])
        
#rename the columns in the system dataframe
systems_df.columns = ['pwsid', 'sys_name', 'sys_type', 'water_source_type', 'area_development_district', 'primary_county', 'date_established', 'entity_type', 
                      'employees', 'directly_serviceable_pop', 'directly_serviceable_hh', 'indirectly_serviceable_pop', 'indirectly_serviceable_hh', 'purchaser_systems', 
                      'seller_systems', 'wholesale_customers', 'residential_customers', 'commercial_customers', 'institutional_customers', 'industrial_customers', 
                      'other_customers', 'total_customers', 'psc_group_id', 'inside_city_rate', 'outside_city_rate', 'not_city_rate', 'rate_adj_date',
                      'tot_county_conn_count', 'tot_county_serv_pop', 'tot_county_serv_hh', 'tot_county_serv_mhi', 'tot_county_serv_mhi_moe', 'county_served', 'county_conn_count',
                      'county_serv_pop', 'county_serv_hh', 'county_serv_mhi', 'county_serv_mhi_moe']

#rename the columns in the system_projects_df
system_projects_df.columns = ['pnum', 'applicant', 'project_status', 'funding_status', 'schedule', 'project_title', 'agreed_order', 'profile_modified', 'gis_modified']

#keep only the matches between the projects and the systems from the systems data
system_project_pairings = system_project_pairings[['pwsid', 'PNUM']]
system_project_pairings.columns = ['pwsid', 'pnum']


######################################
### Systems Dataframe Construction ###
######################################

#convert date established and date rate changed into a date object
systems_df['date_established'] = pd.to_datetime(systems_df['date_established'])
systems_df['rate_adj_date'] = pd.to_datetime(systems_df['rate_adj_date'])

#convert string numbers to integers
systems_df['employees'] = systems_df['employees'].apply(lambda x: convert_int(x))
systems_df['directly_serviceable_pop'] = systems_df['directly_serviceable_pop'].apply(lambda x: convert_int(x))
systems_df['directly_serviceable_hh'] = systems_df['directly_serviceable_hh'].apply(lambda x: convert_int(x))
systems_df['indirectly_serviceable_pop'] = systems_df['indirectly_serviceable_pop'].apply(lambda x: convert_int(x))
systems_df['indirectly_serviceable_hh'] = systems_df['indirectly_serviceable_hh'].apply(lambda x: convert_int(x))
systems_df['wholesale_customers'] = systems_df['wholesale_customers'].apply(lambda x: convert_int(x))
systems_df['residential_customers'] = systems_df['residential_customers'].apply(lambda x: convert_int(x))
systems_df['commercial_customers'] = systems_df['commercial_customers'].apply(lambda x: convert_int(x))
systems_df['institutional_customers'] = systems_df['institutional_customers'].apply(lambda x: convert_int(x))
systems_df['industrial_customers'] = systems_df['industrial_customers'].apply(lambda x: convert_int(x))
systems_df['other_customers'] = systems_df['other_customers'].apply(lambda x: convert_int(x))
systems_df['total_customers'] = systems_df['total_customers'].apply(lambda x: convert_int(x))
systems_df['inside_city_rate'] = systems_df['inside_city_rate'].apply(lambda x: convert_money(x))
systems_df['outside_city_rate'] = systems_df['outside_city_rate'].apply(lambda x: convert_money(x))
systems_df['not_city_rate'] = systems_df['not_city_rate'].apply(lambda x: convert_money(x))
systems_df['tot_county_conn_count'] = systems_df['tot_county_conn_count'].apply(lambda x: convert_int(x))
systems_df['tot_county_serv_pop'] = systems_df['tot_county_serv_pop'].apply(lambda x: convert_int(x))
systems_df['tot_county_serv_hh'] = systems_df['tot_county_serv_hh'].apply(lambda x: convert_int(x))
systems_df['tot_county_serv_mhi'] = systems_df['tot_county_serv_mhi'].apply(lambda x: convert_salary(x))
systems_df['tot_county_serv_mhi_moe'] = systems_df['tot_county_serv_mhi_moe'].apply(lambda x: convert_salary(x))
systems_df['county_conn_count'] = systems_df['county_conn_count'].apply(lambda x: convert_int(x))
systems_df['county_serv_pop'] = systems_df['county_serv_pop'].apply(lambda x: convert_int(x))
systems_df['county_serv_hh'] = systems_df['county_serv_hh'].apply(lambda x: convert_int(x))
systems_df['county_serv_mhi'] = systems_df['county_serv_mhi'].apply(lambda x: convert_salary(x))
systems_df['county_serv_mhi_moe'] = systems_df['county_serv_mhi_moe'].apply(lambda x: convert_salary(x))


systems_df.reset_index(inplace=True, drop=True)

#note: there are to instances where the source water value seems incorrect
systems_df[systems_df['water_source_type'] == 'Groundwater Under The Influence Purchaser']
systems_df[systems_df['water_source_type'] == 'Groundwater Under The Influence']

### Merge in the actual county fips ###
os.chdir(PATH_TO_CENSUS_DATA)
ky_county_mapping = pd.read_pickle("ky_county_mapping.pkl")

#add the county fips columns and remove extra columns
systems_df = systems_df.merge(ky_county_mapping[['NAME20','GEOID20']], how='left', left_on = 'county_served', right_on = 'NAME20')
systems_df.drop(columns=['NAME20'], inplace=True)
systems_df.columns = ['pwsid', 'sys_name', 'sys_type', 'water_source_type', 'area_development_district', 'primary_county', 'date_established', 
                      'entity_type', 'employees', 'directly_serviceable_pop', 'directly_serviceable_hh', 'indirectly_serviceable_pop',
                      'indirectly_serviceable_hh', 'purchaser_systems', 'seller_systems', 'wholesale_customers', 'residential_customers', 
                      'commercial_customers', 'institutional_customers', 'industrial_customers', 'other_customers','total_customers', 'psc_group_id',
                      'inside_city_rate', 'outside_city_rate', 'not_city_rate', 'rate_adj_date', 'tot_county_conn_count', 'tot_county_serv_pop', 'tot_county_serv_hh',
                      'tot_county_serv_mhi', 'tot_county_serv_mhi_moe', 'county_served', 'county_conn_count', 'county_serv_pop', 'county_serv_hh', 'county_serv_mhi', 
                      'county_serv_mhi_moe', 'countyfips']


#create separate tables for the relationship files and for the pwsid info
system_county_relationship_df = systems_df[['pwsid', 'countyfips', 'county_served', 'county_conn_count', 'county_serv_pop', 'county_serv_hh', 'county_serv_mhi', 'county_serv_mhi_moe']]
unique_systems_df = systems_df.drop(columns=['countyfips', 'county_served', 'county_conn_count', 'county_serv_pop', 'county_serv_hh', 'county_serv_mhi', 'county_serv_mhi_moe'])
unique_systems_df.drop_duplicates(inplace=True, ignore_index=True)

#clean up the system-selling relationships
system_selling_pairings.drop_duplicates(inplace=True)
system_selling_pairings = system_selling_pairings[system_selling_pairings.seller != system_selling_pairings.purchaser]
system_selling_pairings.reset_index(inplace=True, drop=True)

# send out to postgres
# with PostgresConnectionManager(DB_CONFIG) as conn:
#     system_county_relationship_df.to_sql('system_county_relationship', conn, schema='wris', if_exists ='replace', index=False)
#     system_selling_pairings.to_sql('system_selling_relationship', conn, schema='wris', if_exists ='replace', index=False)
#     unique_systems_df.to_sql('systems', conn, dtype={"date_established": Date(), "rate_adj_date": Date()}, schema='wris', if_exists ='replace', index=False)
    
    
### cleaning up the relationship between the systems and the projects as sourced from the systems files ###
#convert dates into true dates
system_projects_df['gis_modified'] = pd.to_datetime(system_projects_df['gis_modified'])
system_projects_df['profile_modified'] = pd.to_datetime(system_projects_df['profile_modified'])

#drop duplicates, keeping the one with the most-recently updated GIS date
system_projects_df.sort_values(by=['pnum', 'gis_modified', 'profile_modified'], na_position='first', inplace=True) #makes sure the nans are sorted first, so not accidentally grabbed
#care more about the most up-to-date modified date more than the gis date
system_projects_df.drop_duplicates(subset = system_projects_df.columns[:-2].values, keep = 'last', inplace=True) #identify duplicates on all info but modified dates
system_projects_df.reset_index(inplace=True, drop=True)
    
#######################################
### Projects Dataframe Construction ###
#######################################

#pull in the projects data and match projects with systems as listed in the projects files, also create a list of funding sources
os.chdir(f'{PATH_TO_DATA}/wris/projects')
proj_files = os.listdir(f'{PATH_TO_DATA}/wris/projects')
proj_jsons = [i for i in proj_files if 'WX' in i]

projects_df = pd.DataFrame()
funding_sources_df = pd.DataFrame()
project_system_relationship_df = pd.DataFrame()
project_county_relationship_df = pd.DataFrame()


for p_file in proj_jsons: 
    with open(p_file) as f:
        curr_proj_dict = json.load(f)
        
    #compile together the attributes of projects
    try:    
        projects_df = projects_df.append([dict({'pnum': curr_proj_dict['Project Number'], 'project_name': curr_proj_dict['Project Title'], 'funding_status': curr_proj_dict['Funding Status'],
                                          'date_approved': curr_proj_dict['Date Approved (AWMPC)'], 'awmpc': curr_proj_dict['AWMPC'], 'psc_group_id': curr_proj_dict['PSC Group ID'], 
                                          'total_project_cost': re.sub('(\$|,)', '', curr_proj_dict['Total Project Cost']), 
                                          'total_committed_funding': curr_proj_dict['Total Committed Funding'],
                                          'month_bill_mhi_per': curr_proj_dict['System Monthly Water Bill, MHI Percent'],
                                          'water_loss_mg_2020': curr_proj_dict['Water Loss Last 12 Months MG Volume'],
                                          'water_loss_per_2020': curr_proj_dict['Water Loss Last 12 Months Percent'],                                               
                                          'total_new_households': np.nanmax([convert_int(curr_proj_dict['New or Improved Service']['Census Overlay']['To Unserved Households:']), convert_int(curr_proj_dict['New or Improved Service']['Survey Based']['To Unserved Households:'])]),
                                          'total_improved_households': np.nanmax([convert_int(curr_proj_dict['New or Improved Service']['Census Overlay']['To Underserved Households:']), convert_int(curr_proj_dict['New or Improved Service']['Survey Based']['To Underserved Households:'])]),
                                          'total_impacted_households': np.nanmax([convert_int(curr_proj_dict['New or Improved Service']['Census Overlay']['To Total Households:']), convert_int(curr_proj_dict['New or Improved Service']['Survey Based']['To Total Households:'])]),
                                          'proj_cost_per_household': convert_salary(curr_proj_dict['New or Improved Service']['Cost Per Household']),
                                          'proj_area_households': convert_int(curr_proj_dict['Demographic Impacts']['Project Area']['Households']),
                                          'proj_area_mhhi': convert_salary(re.sub('\*', '', curr_proj_dict['Demographic Impacts']['Project Area']['MHI']) if isinstance(curr_proj_dict['Demographic Impacts']['Project Area']['MHI'], str) == True else np.nan),
                                          'proj_area_nsrl': convert_int(curr_proj_dict['Demographic Impacts']['Project Area']['**NSRL']),
                                          'sys_households': convert_int(curr_proj_dict['Demographic Impacts']['Included Systems']['Households']),
                                          'sys_mhhi': convert_salary(re.sub('\*', '', curr_proj_dict['Demographic Impacts']['Included Systems']['MHI']) if isinstance(curr_proj_dict['Demographic Impacts']['Included Systems']['MHI'], str) == True else np.nan),
                                          'sys_nsrl' : convert_int(curr_proj_dict['Demographic Impacts']['Included Systems']['**NSRL']),
                                          'utilities_households': convert_int(curr_proj_dict['Demographic Impacts']['Included Utilities']['Households']),
                                          'utilities_mhhi': convert_salary(re.sub('\*', '', curr_proj_dict['Demographic Impacts']['Included Utilities']['MHI']) if isinstance(curr_proj_dict['Demographic Impacts']['Included Utilities']['MHI'], str) == True else np.nan),
                                          'utilities_nsrl' : convert_int(curr_proj_dict['Demographic Impacts']['Included Utilities']['**NSRL']),
                                          'system_count': len(curr_proj_dict['Beneficiary Systems']),
                                          'counties_impacted_count': len(curr_proj_dict['Counties Impacted by Project Area']),
                                          'estimated_construction_completion_date': curr_proj_dict['Estimated Construction Completion Date']}, **curr_proj_dict['DW Specific Impacts'])], ignore_index=True)
    except:
        #sometimes New or Improved Services and demographic Impacts aren't included
        if (('Demographic Impacts' not in curr_proj_dict.keys()) and ('New or Improved Service' not in curr_proj_dict.keys())):
            projects_df = projects_df.append([dict({'pnum': curr_proj_dict['Project Number'], 'project_name': curr_proj_dict['Project Title'], 'funding_status': curr_proj_dict['Funding Status'],
                                          'date_approved': curr_proj_dict['Date Approved (AWMPC)'], 'awmpc': curr_proj_dict['AWMPC'], 'psc_group_id': curr_proj_dict['PSC Group ID'], 
                                          'total_project_cost': re.sub('(\$|,)', '', curr_proj_dict['Total Project Cost']), 
                                          'total_committed_funding': curr_proj_dict['Total Committed Funding'],
                                          'month_bill_mhi_per': curr_proj_dict['System Monthly Water Bill, MHI Percent'],
                                          'water_loss_mg_2020': curr_proj_dict['Water Loss Last 12 Months MG Volume'],
                                          'water_loss_per_2020': curr_proj_dict['Water Loss Last 12 Months Percent'],                                                    
                                          'total_new_households': np.nan,
                                          'total_improved_households': np.nan,   
                                          'total_impacted_households': np.nan,
                                          'proj_cost_per_household': np.nan,
                                          'proj_area_households': np.nan,
                                          'proj_area_mhhi': np.nan,
                                          'proj_area_nsrl': np.nan,
                                          'sys_households': np.nan,
                                          'sys_mhhi': np.nan,
                                          'sys_nsrl' : np.nan,
                                          'utilities_households': np.nan,
                                          'utilities_mhhi': np.nan,
                                          'utilities_nsrl' : np.nan,
                                          'system_count': len(curr_proj_dict['Beneficiary Systems']),
                                          'counties_impacted_count': len(curr_proj_dict['Counties Impacted by Project Area']),
                                          'estimated_construction_completion_date': curr_proj_dict['Estimated Construction Completion Date']}, **curr_proj_dict['DW Specific Impacts'])], ignore_index=True)
            
        #sometimes only Demographic Impacts aren't included
        elif 'Demographic Impacts' not in curr_proj_dict.keys():
            projects_df = projects_df.append([dict({'pnum': curr_proj_dict['Project Number'], 'project_name': curr_proj_dict['Project Title'], 'funding_status': curr_proj_dict['Funding Status'],
                                          'date_approved': curr_proj_dict['Date Approved (AWMPC)'], 'awmpc': curr_proj_dict['AWMPC'], 'psc_group_id': curr_proj_dict['PSC Group ID'], 
                                          'total_project_cost': re.sub('(\$|,)', '', curr_proj_dict['Total Project Cost']), 
                                          'total_committed_funding': curr_proj_dict['Total Committed Funding'],
                                          'month_bill_mhi_per': curr_proj_dict['System Monthly Water Bill, MHI Percent'],
                                          'water_loss_mg_2020': curr_proj_dict['Water Loss Last 12 Months MG Volume'],
                                          'water_loss_per_2020': curr_proj_dict['Water Loss Last 12 Months Percent'],
                                          'total_new_households': np.nanmax([convert_int(curr_proj_dict['New or Improved Service']['Census Overlay']['To Unserved Households:']), convert_int(curr_proj_dict['New or Improved Service']['Survey Based']['To Unserved Households:'])]),
                                          'total_improved_households': np.nanmax([convert_int(curr_proj_dict['New or Improved Service']['Census Overlay']['To Underserved Households:']), convert_int(curr_proj_dict['New or Improved Service']['Survey Based']['To Underserved Households:'])]),
                                          'total_impacted_households': np.nanmax([convert_int(curr_proj_dict['New or Improved Service']['Census Overlay']['To Total Households:']), convert_int(curr_proj_dict['New or Improved Service']['Survey Based']['To Total Households:'])]),
                                          'proj_cost_per_household': convert_salary(curr_proj_dict['New or Improved Service']['Cost Per Household']),
                                          'proj_area_households': np.nan,
                                          'proj_area_mhhi': np.nan,
                                          'proj_area_nsrl': np.nan,
                                          'sys_households': np.nan,
                                          'sys_mhhi': np.nan,
                                          'sys_nsrl' : np.nan,
                                          'utilities_households': np.nan,
                                          'utilities_mhhi': np.nan,
                                          'utilities_nsrl' : np.nan,
                                          'system_count': len(curr_proj_dict['Beneficiary Systems']),
                                          'counties_impacted_count': len(curr_proj_dict['Counties Impacted by Project Area']),
                                          'estimated_construction_completion_date': curr_proj_dict['Estimated Construction Completion Date']}, **curr_proj_dict['DW Specific Impacts'])], ignore_index=True)
        
        #sometimes only New or Improved Services aren't included
        elif 'New or Improved Service' not in curr_proj_dict.keys():
            projects_df = projects_df.append([dict({'pnum': curr_proj_dict['Project Number'], 'project_name': curr_proj_dict['Project Title'], 'funding_status': curr_proj_dict['Funding Status'],
                                          'date_approved': curr_proj_dict['Date Approved (AWMPC)'], 'awmpc': curr_proj_dict['AWMPC'], 'psc_group_id': curr_proj_dict['PSC Group ID'], 
                                          'total_project_cost': re.sub('(\$|,)', '', curr_proj_dict['Total Project Cost']), 
                                          'total_committed_funding': curr_proj_dict['Total Committed Funding'],
                                          'month_bill_mhi_per': curr_proj_dict['System Monthly Water Bill, MHI Percent'],
                                          'water_loss_mg_2020': curr_proj_dict['Water Loss Last 12 Months MG Volume'],
                                          'water_loss_per_2020': curr_proj_dict['Water Loss Last 12 Months Percent'],
                                          'total_new_households': np.nan,
                                          'total_improved_households': np.nan,
                                          'total_impacted_households': np.nan,
                                          'proj_cost_per_household': np.nan,
                                          'proj_area_households': convert_int(curr_proj_dict['Demographic Impacts']['Project Area']['Households']),
                                          'proj_area_mhhi': convert_salary(re.sub('\*', '', curr_proj_dict['Demographic Impacts']['Project Area']['MHI']) if isinstance(curr_proj_dict['Demographic Impacts']['Project Area']['MHI'], str) == True else np.nan),
                                          'proj_area_nsrl': convert_int(curr_proj_dict['Demographic Impacts']['Project Area']['**NSRL']),
                                          'sys_households': convert_int(curr_proj_dict['Demographic Impacts']['Included Systems']['Households']),
                                          'sys_mhhi': convert_salary(re.sub('\*', '', curr_proj_dict['Demographic Impacts']['Included Systems']['MHI']) if isinstance(curr_proj_dict['Demographic Impacts']['Included Systems']['MHI'], str) == True else np.nan),
                                          'sys_nsrl' : convert_int(curr_proj_dict['Demographic Impacts']['Included Systems']['**NSRL']),
                                          'utilities_households': convert_int(curr_proj_dict['Demographic Impacts']['Included Utilities']['Households']),
                                          'utilities_mhhi': convert_salary(re.sub('\*', '', curr_proj_dict['Demographic Impacts']['Included Utilities']['MHI']) if isinstance(curr_proj_dict['Demographic Impacts']['Included Utilities']['MHI'], str) == True else np.nan),
                                          'utilities_nsrl' : convert_int(curr_proj_dict['Demographic Impacts']['Included Utilities']['**NSRL']),
                                          'system_count': len(curr_proj_dict['Beneficiary Systems']),
                                          'counties_impacted_count': len(curr_proj_dict['Counties Impacted by Project Area']),
                                          'estimated_construction_completion_date': curr_proj_dict['Estimated Construction Completion Date']}, **curr_proj_dict['DW Specific Impacts'])], ignore_index=True)
    
    #pull together funding information where available
    funding_sources_df = funding_sources_df.append([dict(item, **{'pnum': curr_proj_dict['Project Number']}) for item in curr_proj_dict['Funding Sources']])
    
    #pair projects and systems
    project_system_relationship_df = project_system_relationship_df.append([dict({'pwsid': item['PWSID']}, **{'pnum': curr_proj_dict['Project Number']}) 
                                                                            for item in curr_proj_dict['Beneficiary Systems']])
    #pair projects and counties
    project_county_relationship_df = project_county_relationship_df.append([dict({'county_impacted': item}, **{'pnum': curr_proj_dict['Project Number']}) 
                                                                            for item in curr_proj_dict['Counties Impacted by Project Area']])

#convert date approved into a date object
projects_df['date_approved'] = pd.to_datetime(projects_df['date_approved'])
projects_df['estimated_construction_completion_date'] = pd.to_datetime(projects_df['estimated_construction_completion_date'])

#convert string numbers to integers
projects_df['total_committed_funding'] = projects_df['total_committed_funding'].fillna('')
projects_df['total_committed_funding'] = projects_df['total_committed_funding'].apply(lambda x: re.sub('(\$|,)', '', x))
projects_df['total_committed_funding'] = projects_df['total_committed_funding'].apply(lambda x: convert_int(x))
projects_df['total_project_cost'] = projects_df['total_project_cost'].apply(lambda x: convert_int(x))


### NOTE THAT THIS PROJECT APPEARS TO HAVE BEEN WITHDRAWN AND THEN SUBSEQUENTLY DELETED ###
projects_df = projects_df[projects_df.pnum != 'WX21133045']
projects_df.reset_index(inplace=True, drop=True)
project_system_relationship_df = project_system_relationship_df[project_system_relationship_df.pnum != 'WX21133045']
project_system_relationship_df.reset_index(inplace=True, drop=True)
project_county_relationship_df = project_county_relationship_df[project_county_relationship_df.pnum != 'WX21133045']
project_county_relationship_df.reset_index(inplace=True, drop=True)


#pull in the primary project associations
#note that these projects don't have a primary system, or the system is still pending, i.e. being constructed -- [WX21079003, WX21133064, WX21149033, WX21157046, WX21157050, WX21191507]
os.chdir(f'{PATH_TO_DATA}/wris/sys_proj_association')
primary_association_df = pd.read_excel('primary_pwsid.xls')
project_system_relationship_df = project_system_relationship_df.merge(primary_association_df, how='left', left_on=['pnum', 'pwsid'], right_on=['pnum', 'primary_pwsid'])

#set non-primary = 0, else 1
project_system_relationship_df.primary_pwsid.fillna(0, inplace=True)
project_system_relationship_df.loc[project_system_relationship_df.primary_pwsid != 0, 'primary_pwsid'] = 1

#add in county fips in the relationship file
project_county_relationship_df = project_county_relationship_df.merge(ky_county_mapping[['NAME20','GEOID20']], how='left', left_on = 'county_impacted', right_on = 'NAME20')
project_county_relationship_df.drop(columns=['NAME20'], inplace=True)
project_county_relationship_df.columns = ['county_impacted', 'pnum', 'countyfips']
project_county_relationship_df = project_county_relationship_df[['pnum', 'countyfips', 'county_impacted']]


#### Manual Data Construction ####

#merge with systems data to obtain additional info
projects_df_merged = projects_df.merge(system_projects_df[['pnum', 'project_status', 'schedule', 'profile_modified', 'gis_modified']], how='left', on= 'pnum')

#### Edit projects merged to include missing data
missing_project_data = projects_df_merged[projects_df_merged['project_status'].isnull()].pnum.values

#the projects below have been manually updated
#these came from the extras in IUPs and some from retired systems with projects still in WRIS
manual_edits = ['WX21099033', 'WX21099025', 'WX21187320', 'WX21187207', 'WX21187216', 'WX21187002', 'WX21093041', 'WX21009039', 'WX21059013', 'WX21059039', 'WX21017005',
                'WX21007001', 'WX21093019', 'WX21017019', 'WX21059004', 'WX21059059', 'WX21093031', 'WX21059053', 'WX21059038', 'WX21059003', 'WX21001016', 'WX21059054',
                'WX21199113', 'WX21155041', 'WX21199114', 'WX21025032', 'WX21089090', 'WX21033019', 'WX21025036', 'WX21199112', 'WX21025034', 'WX21127006', 'WX21199111',
                'WX21159016', 'WX21017017', 'WX21237012', 'WX21089079', 'WX21199131', 'WX21089062', 'WX21089042', 'WX21199073', 'WX21161044', 'WX21069034', 'WX21091073',
                'WX21227074', 'WX21149041', 'WX21155023', 'WX21047021', 'WX21137052', 'WX21065006', 'WX21027043', 'WX21195740', 'WX21195034', 'WX21159007', 'WX21027008',
                'WX21003001', 'WX21199110']

#find column indexes for project status, schedule, profile modified and gis modified
proj_ind = projects_df_merged.columns.get_indexer(['project_status'])
sched_ind = projects_df_merged.columns.get_indexer(['schedule'])
prof_ind = projects_df_merged.columns.get_indexer(['profile_modified'])
gis_ind = projects_df_merged.columns.get_indexer(['gis_modified'])

#'WX21099033' -- primary water system KY0500032  
curr_row = projects_df_merged[projects_df_merged.pnum=='WX21099033'].index.item()
projects_df_merged.iloc[curr_row, proj_ind] = 'Approved' #Status
projects_df_merged.iloc[curr_row, sched_ind] = '3-5 Years' #Schedule
projects_df_merged.iloc[curr_row, prof_ind] = pd.to_datetime('2015-09-28') #profile modified
projects_df_merged.iloc[curr_row, gis_ind] = pd.to_datetime('2020-01-24') #GIS modified

#'WX21099025' -- primary water system KY0500032   
curr_row = projects_df_merged[projects_df_merged.pnum=='WX21099025'].index.item()
projects_df_merged.iloc[curr_row, proj_ind] = 'Approved' #Status
projects_df_merged.iloc[curr_row, sched_ind] = '3-5 Years' #Schedule
projects_df_merged.iloc[curr_row, prof_ind] = pd.to_datetime('2012-05-03') #profile modified
projects_df_merged.iloc[curr_row, gis_ind] = pd.to_datetime('2020-01-24') #GIS modified

#'WX21187320' -- primary water system KY0940430
curr_row = projects_df_merged[projects_df_merged.pnum=='WX21187320'].index.item()
projects_df_merged.iloc[curr_row, proj_ind] = 'Approved' #Status
projects_df_merged.iloc[curr_row, sched_ind] = '0-2 Years' #Schedule
projects_df_merged.iloc[curr_row, prof_ind] = pd.to_datetime('2012-02-15') #profile modified
projects_df_merged.iloc[curr_row, gis_ind] = pd.to_datetime('2014-11-05') #GIS modified

#'WX21187207' -- primary water system KY0940430
curr_row = projects_df_merged[projects_df_merged.pnum=='WX21187207'].index.item()
projects_df_merged.iloc[curr_row, proj_ind] = 'Constructed' #Status
projects_df_merged.iloc[curr_row, sched_ind] = '' #Schedule
projects_df_merged.iloc[curr_row, prof_ind] = pd.to_datetime('2012-06-7') #profile modified
projects_df_merged.iloc[curr_row, gis_ind] = pd.to_datetime('2011-04-13') #GIS modified

#'WX21187216' -- primary water system KY0940430
curr_row = projects_df_merged[projects_df_merged.pnum=='WX21187216'].index.item()
projects_df_merged.iloc[curr_row, proj_ind] = 'Approved' #Status
projects_df_merged.iloc[curr_row, sched_ind] = '0-2 Years' #Schedule
projects_df_merged.iloc[curr_row, prof_ind] = pd.to_datetime('2013-03-18') #profile modified
projects_df_merged.iloc[curr_row, gis_ind] = pd.to_datetime('2010-10-01') #GIS modified

#'WX21187002' -- primary water system KY0940430
curr_row = projects_df_merged[projects_df_merged.pnum=='WX21187002'].index.item()
projects_df_merged.iloc[curr_row, proj_ind] = 'Approved' #Status
projects_df_merged.iloc[curr_row, sched_ind] = '0-2 Years' #Schedule
projects_df_merged.iloc[curr_row, prof_ind] = pd.to_datetime('2013-03-19') #profile modified
projects_df_merged.iloc[curr_row, gis_ind] = pd.to_datetime('2012-03-13') #GIS modified

# 'WX21093041' -- primary water system KY0470118  
curr_row = projects_df_merged[projects_df_merged.pnum=='WX21093041'].index.item()
projects_df_merged.iloc[curr_row, proj_ind] = 'Constructed' #Status
projects_df_merged.iloc[curr_row, sched_ind] = '0-2 Years' #Schedule
projects_df_merged.iloc[curr_row, prof_ind] = pd.to_datetime('2013-03-21') #profile modified
projects_df_merged.iloc[curr_row, gis_ind] = pd.to_datetime('2010-08-06') #GIS modified

# 'WX21009039' -- primary water system KY0050490 
curr_row = projects_df_merged[projects_df_merged.pnum=='WX21009039'].index.item()
projects_df_merged.iloc[curr_row, proj_ind] = 'Constructed' #Status
projects_df_merged.iloc[curr_row, sched_ind] = '0-2 Years' #Schedule
projects_df_merged.iloc[curr_row, prof_ind] = pd.to_datetime('2015-11-06') #profile modified
projects_df_merged.iloc[curr_row, gis_ind] = pd.to_datetime('2013-02-12') #GIS modified

# 'WX21059013' -- primary water system KY0300450
curr_row = projects_df_merged[projects_df_merged.pnum=='WX21059013'].index.item()
projects_df_merged.iloc[curr_row, proj_ind] = 'Constructed' #Status
projects_df_merged.iloc[curr_row, sched_ind] = '0-2 Years' #Schedule
projects_df_merged.iloc[curr_row, prof_ind] = pd.to_datetime('2012-02-27') #profile modified
projects_df_merged.iloc[curr_row, gis_ind] = pd.to_datetime('2010-11-08') #GIS modified

# 'WX21059039'  -- primary water system KY0300450
curr_row = projects_df_merged[projects_df_merged.pnum=='WX21059039'].index.item()
projects_df_merged.iloc[curr_row, proj_ind] = 'Constructed' #Status
projects_df_merged.iloc[curr_row, sched_ind] = '3-5 Years' #Schedule
projects_df_merged.iloc[curr_row, prof_ind] = pd.to_datetime('2019-11-07') #profile modified
projects_df_merged.iloc[curr_row, gis_ind] = pd.to_datetime('2014-02-14') #GIS modified

# 'WX21017005' -- primary water system KY0090322 
curr_row = projects_df_merged[projects_df_merged.pnum=='WX21017005'].index.item()
projects_df_merged.iloc[curr_row, proj_ind] = 'Constructed' #Status
projects_df_merged.iloc[curr_row, sched_ind] = '0-2 Years' #Schedule
projects_df_merged.iloc[curr_row, prof_ind] = pd.to_datetime('2010-12-14') #profile modified
projects_df_merged.iloc[curr_row, gis_ind] = pd.to_datetime('2010-07-28') #GIS modified

# 'WX21007001' -- primary water system KY0040259
curr_row = projects_df_merged[projects_df_merged.pnum=='WX21007001'].index.item()
projects_df_merged.iloc[curr_row, proj_ind] = 'Approved' #Status
projects_df_merged.iloc[curr_row, sched_ind] = '3-5 Years' #Schedule
projects_df_merged.iloc[curr_row, prof_ind] = pd.to_datetime('2012-12-05') #profile modified
projects_df_merged.iloc[curr_row, gis_ind] = pd.to_datetime('') #GIS modified

# 'WX21093019' -- primary water system KY0470118 
curr_row = projects_df_merged[projects_df_merged.pnum=='WX21093019'].index.item()
projects_df_merged.iloc[curr_row, proj_ind] = 'Approved' #Status
projects_df_merged.iloc[curr_row, sched_ind] = '3-5 Years' #Schedule
projects_df_merged.iloc[curr_row, prof_ind] = pd.to_datetime('2015-12-08') #profile modified
projects_df_merged.iloc[curr_row, gis_ind] = pd.to_datetime('2014-10-16') #GIS modified

# 'WX21017019' -- primary water system KY0090322
curr_row = projects_df_merged[projects_df_merged.pnum=='WX21017019'].index.item()
projects_df_merged.iloc[curr_row, proj_ind] = 'Constructed' #Status
projects_df_merged.iloc[curr_row, sched_ind] = '0-2 Years' #Schedule
projects_df_merged.iloc[curr_row, prof_ind] = pd.to_datetime('2016-11-14') #profile modified
projects_df_merged.iloc[curr_row, gis_ind] = pd.to_datetime('2013-03-20') #GIS modified

# 'WX21059004' -- primary water system KY0300450
curr_row = projects_df_merged[projects_df_merged.pnum=='WX21059004'].index.item()
projects_df_merged.iloc[curr_row, proj_ind] = 'Constructed' #Status
projects_df_merged.iloc[curr_row, sched_ind] = '0-2 Years' #Schedule
projects_df_merged.iloc[curr_row, prof_ind] = pd.to_datetime('2019-11-07') #profile modified
projects_df_merged.iloc[curr_row, gis_ind] = pd.to_datetime('2013-12-19') #GIS modified

# 'WX21059059' -- primary water system KY0300450
curr_row = projects_df_merged[projects_df_merged.pnum=='WX21059059'].index.item()
projects_df_merged.iloc[curr_row, proj_ind] = 'Constructed' #Status
projects_df_merged.iloc[curr_row, sched_ind] = '0-2 Years' #Schedule
projects_df_merged.iloc[curr_row, prof_ind] = pd.to_datetime('2019-11-07') #profile modified
projects_df_merged.iloc[curr_row, gis_ind] = pd.to_datetime('2014-02-14') #GIS modified

# 'WX21093031' -- primary water system KY0470118
curr_row = projects_df_merged[projects_df_merged.pnum=='WX21093031'].index.item()
projects_df_merged.iloc[curr_row, proj_ind] = 'Constructed' #Status
projects_df_merged.iloc[curr_row, sched_ind] = '0-2 Years' #Schedule
projects_df_merged.iloc[curr_row, prof_ind] = pd.to_datetime('2013-04-24') #profile modified
projects_df_merged.iloc[curr_row, gis_ind] = pd.to_datetime('2013-02-27') #GIS modified

# 'WX21059053' -- primary water system KY0300450
curr_row = projects_df_merged[projects_df_merged.pnum=='WX21059053'].index.item()
projects_df_merged.iloc[curr_row, proj_ind] = 'Constructed' #Status
projects_df_merged.iloc[curr_row, sched_ind] = '0-2 Years' #Schedule
projects_df_merged.iloc[curr_row, prof_ind] = pd.to_datetime('2019-11-07') #profile modified
projects_df_merged.iloc[curr_row, gis_ind] = pd.to_datetime('2013-12-19') #GIS modified

# 'WX21059038' -- primary water system KY0300450
curr_row = projects_df_merged[projects_df_merged.pnum=='WX21059038'].index.item()
projects_df_merged.iloc[curr_row, proj_ind] = 'Constructed' #Status
projects_df_merged.iloc[curr_row, sched_ind] = '3-5 Years' #Schedule
projects_df_merged.iloc[curr_row, prof_ind] = pd.to_datetime('2012-11-26') #profile modified
projects_df_merged.iloc[curr_row, gis_ind] = pd.to_datetime('2011-03-08') #GIS modified

# 'WX21059003' -- primary water system KY0300450
curr_row = projects_df_merged[projects_df_merged.pnum=='WX21059003'].index.item()
projects_df_merged.iloc[curr_row, proj_ind] = 'Constructed' #Status
projects_df_merged.iloc[curr_row, sched_ind] = '0-2 Years' #Schedule
projects_df_merged.iloc[curr_row, prof_ind] = pd.to_datetime('2011-10-20') #profile modified
projects_df_merged.iloc[curr_row, gis_ind] = pd.to_datetime('2010-11-19') #GIS modified

# 'WX21001016' -- primary water system KY0010702
curr_row = projects_df_merged[projects_df_merged.pnum=='WX21001016'].index.item()
projects_df_merged.iloc[curr_row, proj_ind] = 'Constructed' #Status
projects_df_merged.iloc[curr_row, sched_ind] = '0-2 Years' #Schedule
projects_df_merged.iloc[curr_row, prof_ind] = pd.to_datetime('2012-05-25') #profile modified
projects_df_merged.iloc[curr_row, gis_ind] = pd.to_datetime('2010-10-07') #GIS modified

# 'WX21059054' -- primary water system KY0300450
curr_row = projects_df_merged[projects_df_merged.pnum=='WX21059054'].index.item()
projects_df_merged.iloc[curr_row, proj_ind] = 'Constructed' #Status
projects_df_merged.iloc[curr_row, sched_ind] = '0-2 Years' #Schedule
projects_df_merged.iloc[curr_row, prof_ind] = pd.to_datetime('2019-11-07') #profile modified
projects_df_merged.iloc[curr_row, gis_ind] = pd.to_datetime('2011-02-23') #GIS modified

# 'WX21199113' -- primary water system KY1000050
curr_row = projects_df_merged[projects_df_merged.pnum=='WX21199113'].index.item()
projects_df_merged.iloc[curr_row, proj_ind] = 'Withdrawn' #Status
projects_df_merged.iloc[curr_row, sched_ind] = '3-5 Years' #Schedule
projects_df_merged.iloc[curr_row, prof_ind] = pd.to_datetime('2019-10-01') #profile modified
projects_df_merged.iloc[curr_row, gis_ind] = pd.to_datetime('2018-04-26') #GIS modified

# 'WX21155041' -- primary water system unknown
curr_row = projects_df_merged[projects_df_merged.pnum=='WX21155041'].index.item()
projects_df_merged.iloc[curr_row, proj_ind] = 'Pending' #Status
projects_df_merged.iloc[curr_row, sched_ind] = '3-5 Years' #Schedule
projects_df_merged.iloc[curr_row, prof_ind] = pd.to_datetime('2016-12-16') #profile modified

# 'WX21199114' -- primary water system KY1000050
curr_row = projects_df_merged[projects_df_merged.pnum=='WX21199114'].index.item()
projects_df_merged.iloc[curr_row, proj_ind] = 'Withdrawn' #Status
projects_df_merged.iloc[curr_row, sched_ind] = '3-5 Years' #Schedule
projects_df_merged.iloc[curr_row, prof_ind] = pd.to_datetime('2019-10-01') #profile modified
projects_df_merged.iloc[curr_row, gis_ind] = pd.to_datetime('2017-12-07') #GIS modified

# 'WX21025032' -- primary water system KY0131012
curr_row = projects_df_merged[projects_df_merged.pnum=='WX21025032'].index.item()
projects_df_merged.iloc[curr_row, proj_ind] = 'Withdrawn' #Status
projects_df_merged.iloc[curr_row, sched_ind] = '3-5 Years' #Schedule
projects_df_merged.iloc[curr_row, prof_ind] = pd.to_datetime('2020-09-10') #profile modified
projects_df_merged.iloc[curr_row, gis_ind] = pd.to_datetime('2012-12-10') #GIS modified

# 'WX21089090' -- primary water system KY0450376
curr_row = projects_df_merged[projects_df_merged.pnum=='WX21089090'].index.item()
projects_df_merged.iloc[curr_row, proj_ind] = 'Withdrawn' #Status
projects_df_merged.iloc[curr_row, sched_ind] = '0-2 Years' #Schedule
projects_df_merged.iloc[curr_row, prof_ind] = pd.to_datetime('2016-11-14') #profile modified
projects_df_merged.iloc[curr_row, gis_ind] = pd.to_datetime('2015-12-28') #GIS modified

# 'WX21033019' -- primary water system KY0170528
curr_row = projects_df_merged[projects_df_merged.pnum=='WX21033019'].index.item()
projects_df_merged.iloc[curr_row, proj_ind] = 'Withdrawn' #Status
projects_df_merged.iloc[curr_row, sched_ind] = '3-5 Years' #Schedule
projects_df_merged.iloc[curr_row, prof_ind] = pd.to_datetime('2019-05-23') #profile modified
projects_df_merged.iloc[curr_row, gis_ind] = pd.to_datetime('2012-11-27') #GIS modified

# 'WX21025036' -- primary water system KY0131012
curr_row = projects_df_merged[projects_df_merged.pnum=='WX21025036'].index.item()
projects_df_merged.iloc[curr_row, proj_ind] = 'Withdrawn' #Status
projects_df_merged.iloc[curr_row, sched_ind] = '3-5 Years' #Schedule
projects_df_merged.iloc[curr_row, prof_ind] = pd.to_datetime('2020-09-10') #profile modified
projects_df_merged.iloc[curr_row, gis_ind] = pd.to_datetime('2012-12-10') #GIS modified

# 'WX21199112' -- primary water system KY1000050
curr_row = projects_df_merged[projects_df_merged.pnum=='WX21199112'].index.item()
projects_df_merged.iloc[curr_row, proj_ind] = 'Withdrawn' #Status
projects_df_merged.iloc[curr_row, sched_ind] = '3-5 Years' #Schedule
projects_df_merged.iloc[curr_row, prof_ind] = pd.to_datetime('2019-10-01') #profile modified
projects_df_merged.iloc[curr_row, gis_ind] = pd.to_datetime('2018-04-24') #GIS modified

# 'WX21025034' -- primary water system KY0131012
curr_row = projects_df_merged[projects_df_merged.pnum=='WX21025034'].index.item()
projects_df_merged.iloc[curr_row, proj_ind] = 'Withdrawn' #Status
projects_df_merged.iloc[curr_row, sched_ind] = '3-5 Years' #Schedule
projects_df_merged.iloc[curr_row, prof_ind] = pd.to_datetime('2020-09-10') #profile modified
projects_df_merged.iloc[curr_row, gis_ind] = pd.to_datetime('2012-12-11') #GIS modified

# 'WX21127006' -- primary water system KY0640257
curr_row = projects_df_merged[projects_df_merged.pnum=='WX21127006'].index.item()
projects_df_merged.iloc[curr_row, proj_ind] = 'Withdrawn' #Status
projects_df_merged.iloc[curr_row, sched_ind] = '3-5 Years' #Schedule
projects_df_merged.iloc[curr_row, prof_ind] = pd.to_datetime('2018-09-28') #profile modified
projects_df_merged.iloc[curr_row, gis_ind] = pd.to_datetime('2016-07-19') #GIS modified

# 'WX21199111' -- primary water system KY1000050
curr_row = projects_df_merged[projects_df_merged.pnum=='WX21199111'].index.item()
projects_df_merged.iloc[curr_row, proj_ind] = 'Withdrawn' #Status
projects_df_merged.iloc[curr_row, sched_ind] = '3-5 Years' #Schedule
projects_df_merged.iloc[curr_row, prof_ind] = pd.to_datetime('2019-10-01') #profile modified
projects_df_merged.iloc[curr_row, gis_ind] = pd.to_datetime('2018-04-26') #GIS modified

# 'WX21159016' -- primary water system KY0800273
curr_row = projects_df_merged[projects_df_merged.pnum=='WX21159016'].index.item()
projects_df_merged.iloc[curr_row, proj_ind] = 'Withdrawn' #Status
projects_df_merged.iloc[curr_row, sched_ind] = '0-2 Years' #Schedule
projects_df_merged.iloc[curr_row, prof_ind] = pd.to_datetime('2019-06-26') #profile modified
projects_df_merged.iloc[curr_row, gis_ind] = pd.to_datetime('2018-01-24') #GIS modified

# 'WX21017017' -- primary water system KY0090287
curr_row = projects_df_merged[projects_df_merged.pnum=='WX21017017'].index.item()
projects_df_merged.iloc[curr_row, proj_ind] = 'Withdrawn' #Status
projects_df_merged.iloc[curr_row, sched_ind] = '0-2 Years' #Schedule
projects_df_merged.iloc[curr_row, prof_ind] = pd.to_datetime('2016-11-15') #profile modified
projects_df_merged.iloc[curr_row, gis_ind] = pd.to_datetime('2014-01-09') #GIS modified

# 'WX21237012' -- primary water system KY1190061
curr_row = projects_df_merged[projects_df_merged.pnum=='WX21237012'].index.item()
projects_df_merged.iloc[curr_row, proj_ind] = 'Withdrawn' #Status
projects_df_merged.iloc[curr_row, sched_ind] = '3-5 Years' #Schedule
projects_df_merged.iloc[curr_row, prof_ind] = pd.to_datetime('2017-08-17') #profile modified
projects_df_merged.iloc[curr_row, gis_ind] = pd.to_datetime('2014-10-07') #GIS modified

# 'WX21089079' -- primary water system KY0450376
curr_row = projects_df_merged[projects_df_merged.pnum=='WX21089079'].index.item()
projects_df_merged.iloc[curr_row, proj_ind] = 'Withdrawn' #Status
projects_df_merged.iloc[curr_row, sched_ind] = '0-2 Years' #Schedule
projects_df_merged.iloc[curr_row, prof_ind] = pd.to_datetime('2016-11-14') #profile modified
projects_df_merged.iloc[curr_row, gis_ind] = pd.to_datetime('2016-07-27') #GIS modified

# 'WX21199131' -- primary water system KY1000403
curr_row = projects_df_merged[projects_df_merged.pnum=='WX21199131'].index.item()
projects_df_merged.iloc[curr_row, proj_ind] = 'Withdrawn' #Status
projects_df_merged.iloc[curr_row, sched_ind] = '0-2 Years' #Schedule
projects_df_merged.iloc[curr_row, prof_ind] = pd.to_datetime('2019-10-01') #profile modified
projects_df_merged.iloc[curr_row, gis_ind] = pd.to_datetime('2018-12-14') #GIS modified

# 'WX21089062' -- primary water system KY0450479
curr_row = projects_df_merged[projects_df_merged.pnum=='WX21089062'].index.item()
projects_df_merged.iloc[curr_row, proj_ind] = 'Withdrawn' #Status
projects_df_merged.iloc[curr_row, sched_ind] = '3-5 Years' #Schedule
projects_df_merged.iloc[curr_row, prof_ind] = pd.to_datetime('2019-12-09') #profile modified
projects_df_merged.iloc[curr_row, gis_ind] = pd.to_datetime('2016-05-23') #GIS modified

# 'WX21089042' -- primary water system KY0450169
curr_row = projects_df_merged[projects_df_merged.pnum=='WX21089042'].index.item()
projects_df_merged.iloc[curr_row, proj_ind] = 'Withdrawn' #Status
projects_df_merged.iloc[curr_row, sched_ind] = '6-10 Years' #Schedule
projects_df_merged.iloc[curr_row, prof_ind] = pd.to_datetime('2016-06-07') #profile modified
projects_df_merged.iloc[curr_row, gis_ind] = pd.to_datetime('2013-02-22') #GIS modified

# 'WX21199073' -- primary water system KY1000050
curr_row = projects_df_merged[projects_df_merged.pnum=='WX21199073'].index.item()
projects_df_merged.iloc[curr_row, proj_ind] = 'Withdrawn' #Status
projects_df_merged.iloc[curr_row, sched_ind] = '3-5 Years' #Schedule
projects_df_merged.iloc[curr_row, prof_ind] = pd.to_datetime('2020-07-01') #profile modified
projects_df_merged.iloc[curr_row, gis_ind] = pd.to_datetime('2018-04-24') #GIS modified

# 'WX21161044' -- primary water system KY0810275
curr_row = projects_df_merged[projects_df_merged.pnum=='WX21161044'].index.item()
projects_df_merged.iloc[curr_row, proj_ind] = 'Withdrawn' #Status
projects_df_merged.iloc[curr_row, sched_ind] = '0-2 Years' #Schedule
projects_df_merged.iloc[curr_row, prof_ind] = pd.to_datetime('2020-11-25') #profile modified
projects_df_merged.iloc[curr_row, gis_ind] = pd.to_datetime('2018-12-05') #GIS modified

# 'WX21069034' -- primary water system KY0350134
curr_row = projects_df_merged[projects_df_merged.pnum=='WX21069034'].index.item()
projects_df_merged.iloc[curr_row, proj_ind] = 'Withdrawn' #Status
projects_df_merged.iloc[curr_row, sched_ind] = '0-2 Years' #Schedule
projects_df_merged.iloc[curr_row, prof_ind] = pd.to_datetime('2020-01-19') #profile modified
projects_df_merged.iloc[curr_row, gis_ind] = pd.to_datetime('2017-02-06') #GIS modified

# 'WX21091073' -- primary water system KY0460248
curr_row = projects_df_merged[projects_df_merged.pnum=='WX21091073'].index.item()
projects_df_merged.iloc[curr_row, proj_ind] = 'Withdrawn' #Status
projects_df_merged.iloc[curr_row, sched_ind] = '3-5 Years' #Schedule
projects_df_merged.iloc[curr_row, prof_ind] = pd.to_datetime('2018-08-16') #profile modified
projects_df_merged.iloc[curr_row, gis_ind] = pd.to_datetime('2013-02-12') #GIS modified

# 'WX21227074' -- primary water system KY1140038
curr_row = projects_df_merged[projects_df_merged.pnum=='WX21227074'].index.item()
projects_df_merged.iloc[curr_row, proj_ind] = 'Withdrawn' #Status
projects_df_merged.iloc[curr_row, sched_ind] = '3-5 Years' #Schedule
projects_df_merged.iloc[curr_row, prof_ind] = pd.to_datetime('2020-05-22') #profile modified
projects_df_merged.iloc[curr_row, gis_ind] = pd.to_datetime('2013-02-12') #GIS modified

# 'WX21149041' -- primary water system KY0750907
curr_row = projects_df_merged[projects_df_merged.pnum=='WX21149041'].index.item()
projects_df_merged.iloc[curr_row, proj_ind] = 'Withdrawn' #Status
projects_df_merged.iloc[curr_row, sched_ind] = '3-5 Years' #Schedule
projects_df_merged.iloc[curr_row, prof_ind] = pd.to_datetime('2019-11-13') #profile modified
projects_df_merged.iloc[curr_row, gis_ind] = pd.to_datetime('2012-04-02') #GIS modified

# 'WX21155023' -- primary water system KY0780241
curr_row = projects_df_merged[projects_df_merged.pnum=='WX21155023'].index.item()
projects_df_merged.iloc[curr_row, proj_ind] = 'Withdrawn' #Status
projects_df_merged.iloc[curr_row, sched_ind] = '3-5 Years' #Schedule
projects_df_merged.iloc[curr_row, prof_ind] = pd.to_datetime('2019-05-02') #profile modified
projects_df_merged.iloc[curr_row, gis_ind] = pd.to_datetime('2019-01-09') #GIS modified

# 'WX21047021' -- primary water system KY0240329
curr_row = projects_df_merged[projects_df_merged.pnum=='WX21047021'].index.item()
projects_df_merged.iloc[curr_row, proj_ind] = 'Withdrawn' #Status
projects_df_merged.iloc[curr_row, sched_ind] = '0-2 Years' #Schedule
projects_df_merged.iloc[curr_row, prof_ind] = pd.to_datetime('2014-11-25') #profile modified
projects_df_merged.iloc[curr_row, gis_ind] = pd.to_datetime('2011-02-22') #GIS modified

# 'WX21137052' -- primary water system KY0690278
curr_row = projects_df_merged[projects_df_merged.pnum=='WX21137052'].index.item()
projects_df_merged.iloc[curr_row, proj_ind] = 'Withdrawn' #Status
projects_df_merged.iloc[curr_row, sched_ind] = '0-2 Years' #Schedule
projects_df_merged.iloc[curr_row, prof_ind] = pd.to_datetime('2021-04-13') #profile modified

# 'WX21065006' -- primary water system KY0330123
curr_row = projects_df_merged[projects_df_merged.pnum=='WX21065006'].index.item()
projects_df_merged.iloc[curr_row, proj_ind] = 'Withdrawn' #Status
projects_df_merged.iloc[curr_row, sched_ind] = '0-2 Years' #Schedule
projects_df_merged.iloc[curr_row, prof_ind] = pd.to_datetime('2019-10-18') #profile modified
projects_df_merged.iloc[curr_row, gis_ind] = pd.to_datetime('2017-06-05') #GIS modified

# 'WX21027043' -- primary water system KY0140966
curr_row = projects_df_merged[projects_df_merged.pnum=='WX21027043'].index.item()
projects_df_merged.iloc[curr_row, proj_ind] = 'Withdrawn' #Status
projects_df_merged.iloc[curr_row, sched_ind] = '0-2 Years' #Schedule
projects_df_merged.iloc[curr_row, prof_ind] = pd.to_datetime('2021-05-10') #profile modified

# 'WX21195740' -- primary water system KY0980575
curr_row = projects_df_merged[projects_df_merged.pnum=='WX21195740'].index.item()
projects_df_merged.iloc[curr_row, proj_ind] = 'Withdrawn' #Status
projects_df_merged.iloc[curr_row, sched_ind] = '3-5 Years' #Schedule
projects_df_merged.iloc[curr_row, prof_ind] = pd.to_datetime('2020-03-12') #profile modified
projects_df_merged.iloc[curr_row, gis_ind] = pd.to_datetime('2017-04-05') #GIS modified

# 'WX21195034' -- primary water system KY0980575
curr_row = projects_df_merged[projects_df_merged.pnum=='WX21195034'].index.item()
projects_df_merged.iloc[curr_row, proj_ind] = 'Withdrawn' #Status
projects_df_merged.iloc[curr_row, sched_ind] = '3-5 Years' #Schedule
projects_df_merged.iloc[curr_row, prof_ind] = pd.to_datetime('2018-08-27') #profile modified
projects_df_merged.iloc[curr_row, gis_ind] = pd.to_datetime('2017-12-13') #GIS modified

# 'WX21159007' -- primary water system KY0800273
curr_row = projects_df_merged[projects_df_merged.pnum=='WX21159007'].index.item()
projects_df_merged.iloc[curr_row, proj_ind] = 'Withdrawn' #Status
projects_df_merged.iloc[curr_row, sched_ind] = '0-2 Years' #Schedule
projects_df_merged.iloc[curr_row, prof_ind] = pd.to_datetime('2019-06-26') #profile modified
projects_df_merged.iloc[curr_row, gis_ind] = pd.to_datetime('2018-01-24') #GIS modified

# 'WX21027008' -- primary water system KY0140206
curr_row = projects_df_merged[projects_df_merged.pnum=='WX21027008'].index.item()
projects_df_merged.iloc[curr_row, proj_ind] = 'Withdrawn' #Status
projects_df_merged.iloc[curr_row, sched_ind] = '3-5 Years' #Schedule
projects_df_merged.iloc[curr_row, prof_ind] = pd.to_datetime('2021-05-10') #profile modified
projects_df_merged.iloc[curr_row, gis_ind] = pd.to_datetime('2010-09-21') #GIS modified

# 'WX21003001' -- primary water system KY0020386
curr_row = projects_df_merged[projects_df_merged.pnum=='WX21003001'].index.item()
projects_df_merged.iloc[curr_row, proj_ind] = 'Withdrawn' #Status
projects_df_merged.iloc[curr_row, sched_ind] = '0-2 Years' #Schedule
projects_df_merged.iloc[curr_row, prof_ind] = pd.to_datetime('2017-10-24') #profile modified
projects_df_merged.iloc[curr_row, gis_ind] = pd.to_datetime('2015-11-24') #GIS modified

# 'WX21199110' -- primary water system KY1000050
curr_row = projects_df_merged[projects_df_merged.pnum=='WX21199110'].index.item()
projects_df_merged.iloc[curr_row, proj_ind] = 'Withdrawn' #Status
projects_df_merged.iloc[curr_row, sched_ind] = '3-5 Years' #Schedule
projects_df_merged.iloc[curr_row, prof_ind] = pd.to_datetime('2019-10-01') #profile modified
projects_df_merged.iloc[curr_row, gis_ind] = pd.to_datetime('2017-12-07') #GIS modified


########################################################
# Adding in a column for 2012 project cost adjustments #
########################################################

#I don't have to update these with deflator data, because last date I observe is 05/2021

#pull in the information on deflators to adjust the project costs -- based on the date the project was approved, in millions of 2012 $
os.chdir(f'{PATH_TO_DATA}/fred')
deflator2012 = pd.read_excel('GDPDEF.xls', sheet_name='GDPDEF')
proj_deflator2012 = deflator2012[deflator2012.observation_date >= pd.Timestamp('2001-01-01')]

#add in a new column for the adjusted project cost
projects_df_merged['total_project_cost_adj'] = np.nan

for i in range(len(projects_df_merged)):
    for j in range(len(proj_deflator2012)-1):
        #first check if date
        if isinstance(projects_df_merged.date_approved.iloc[i], date):
            #then find the relevant deflator values
            if ((projects_df_merged.date_approved.iloc[i] >= proj_deflator2012.observation_date.iloc[j]) & (projects_df_merged.date_approved.iloc[i] < proj_deflator2012.observation_date.iloc[j+1])):
                projects_df_merged['total_project_cost_adj'].iloc[i] = (projects_df_merged['total_project_cost'].iloc[i]*proj_deflator2012.GDPDEF.iloc[j]/100)
                break
        #if not date, just move on
        else:
            continue

##################################################################################################
# Project Constructed Dates -- Recevied from Department of Local Government in KY -- Bill Pauley #
##################################################################################################

#Note:
#Status date reflects the current status date within project profile for each project. 
#This should reflect when the water management coordinator (WMC) set that project as constructed (the profile is locked after that action).  
#If we have a project closeout document for a project, which means we either managed a grant or loan for that project, then the last date for the closeout document is provided in the Project_Closeout_Date field.

# status_date = "Profile Last Modified" date in WRIS Audit Tab as of date email sent --> 7/26/21

#############################################################################################################################################################
## Per the description, I'm setting the "Project Completion" date as the minimum of the provided closeout date and the status date.                        ##
## I REMOVED duplicate pnums from original document using the same concept, I kept the row with the least closeout date, status dates were always the same ##

os.chdir(f'{PATH_TO_DATA}/wris/constructed_dates')
constructed_dates = pd.read_excel("WRIS_ProjectProfile_ConstructedProjects_StatusDates_Sileo.xls")
constructed_dates.columns = ['pnum', 'dlg_status_date', 'dlg_project_closeout_date']

# add the "date constructed" as the minimum of the dates as described above
constructed_dates['dlg_constructed_date'] = constructed_dates.apply(lambda x: min(x['dlg_status_date'], x['dlg_project_closeout_date']), axis=1)

#merge with the existing data to get a constructed date
projects_df_merged = projects_df_merged.merge(constructed_dates[['pnum','dlg_constructed_date']], how='left', on='pnum')

##################
# Manual Updates #
##################
# I didn't ask about project WX21035016, I must have found it later. Updating with Profile Last Modified from WRIS 02.17.2012
dlg_ind = np.where(projects_df_merged.columns == 'dlg_constructed_date')[0][0]
missing_proj_ind = projects_df_merged[projects_df_merged.pnum == 'WX21035016'].index.item()

projects_df_merged.iloc[missing_proj_ind, dlg_ind] = pd.to_datetime('2012-02-17')

#additionally some projects actually are constructed, as opposed to what it says in their WRIS status -- found in the dlg provided info (?)
# WX21195010, WX21193036, WX21207025, WX21157007, WX21053014
proj_stat_ind = np.where(projects_df_merged.columns == 'project_status')[0][0]

## WX21195010 ##
constructed_ind = projects_df_merged[projects_df_merged.pnum == 'WX21195010'].index.item()
projects_df_merged.iloc[constructed_ind, proj_stat_ind] = 'Constructed'

## WX21193036 ##
constructed_ind = projects_df_merged[projects_df_merged.pnum == 'WX21193036'].index.item()
projects_df_merged.iloc[constructed_ind, proj_stat_ind] = 'Constructed'

## WX21207025 ##
constructed_ind = projects_df_merged[projects_df_merged.pnum == 'WX21207025'].index.item()
projects_df_merged.iloc[constructed_ind, proj_stat_ind] = 'Constructed'

## WX21157007 ##
constructed_ind = projects_df_merged[projects_df_merged.pnum == 'WX21157007'].index.item()
projects_df_merged.iloc[constructed_ind, proj_stat_ind] = 'Constructed'

## WX21053014 ##
constructed_ind = projects_df_merged[projects_df_merged.pnum == 'WX21053014'].index.item()
projects_df_merged.iloc[constructed_ind, proj_stat_ind] = 'Constructed'


### rename the funding status columns -- for the ability to sort by funding status in postgres
projects_df_merged.loc[(projects_df_merged.funding_status=='Fully Funded'), 'funding_status'] = 'Funded - Fully'
projects_df_merged.loc[(projects_df_merged.funding_status=='Partially Funded'), 'funding_status'] = 'Funded - Partially'
projects_df_merged.loc[(projects_df_merged.funding_status=='Over Funded'), 'funding_status'] = 'Funded - Over'

################################
### 2009 ARRA Funds Projects ###
################################
#include an indicator if a project was included in the supplemental list, i.e. approved in 2009 and didn't end up on the 2009 IUP (which is in the 2010 IUP as KY combined them)

iup_2009_projects = ["WX21027017", "WX21027025", "WX21025008", "WX21117208", "WX21027033", "WX21145059", "WX21133050", "WX21133043", "WX21133044", "WX21133045", "WX21027031", "WX21047003",
                     "WX21047004", "WX21047027", "WX21047013", "WX21171027", "WX21217006", "WX21217011", "WX21115001", "WX21001016", "WX21001010", "WX21155028", "WX21155024", "WX21155029",
                     "WX21155023", "WX21167018", "WX21167019", "WX21167020", "WX21127003", "WX21127008", "WX21127010", "WX21183012", "WX21199086", "WX21027008", "WX21025011", "WX21001023",
                     "WX21113027", "WX21113028", "WX21133100", "WX21213029", "WX21183004", "WX21053006", "WX21053010", "WX21167006", "WX21055003", "WX21109701", "WX21083038", "WX21107025",
                     "WX21107029", "WX21233032", "WX21047028", "WX21129071", "WX21093033", "WX21203538", "WX21001020", "WX21007022", "WX21225025", "WX21007020"]

#find all the projects with approval dates between the deadline (sometime in October 2008) for regular DWSRF funding, and the supplementary list inclusion (from 2010 IUP)
projects_df_merged['arra'] = 0
projects_df_merged.loc[((projects_df_merged.date_approved>= pd.to_datetime('10-01-2008')) & (projects_df_merged.date_approved< pd.to_datetime('02-14-2009')) & (~projects_df_merged.pnum.isin(iup_2009_projects))),'arra'] = 1

################################
### Project-Systems Pairings ### 
################################

#merge the project-system pairings with the system-project pairings to make sure you have all of them, here identical to the project sourced pairings
pairing_comparison = project_system_relationship_df.merge(system_project_pairings, how = 'outer', on = ['pwsid', 'pnum'])

# send out to postgres
# with PostgresConnectionManager(DB_CONFIG) as conn:
#     project_system_relationship_df.to_sql('project_system_relationship', conn, schema='wris', if_exists ='replace', index=False)
#     project_county_relationship_df.to_sql('project_county_relationship', conn, schema='wris', if_exists ='replace', index=False)
#     projects_df_merged.to_sql('projects', conn, dtype={"date_approved": Date(), "estimated_construction_completion_date": Date(), "profile_modified": Date(),
#                                                       "gis_modified": Date(), "dlg_constructed_date": Date()}, schema='wris', if_exists ='replace', index=False)
    
##########################################
### Imputed the Constructed Years Time ###
##########################################
# 1) Take the data I have on the number of confirmed years since construction, find average time till construction completed (from approved date) by estimated time to complete
# 2) For all projects with a WRIS status which is NOT Constructed (or confirmed constructed by DLG) or Withdrawn, calculate an imputed construction date as 
#      approved date + avg time to complete by project estimated time to complete category

#note there are 8 with constructed dates and no approval date
const_projects_df = projects_df_merged[((projects_df_merged.date_approved.notnull()) & (projects_df_merged.dlg_constructed_date.notnull()))]

const_projects_df['construct_days'] = const_projects_df.apply(lambda row: (row['dlg_constructed_date'] - row['date_approved']), axis=1)
const_projects_df['construct_years_frac'] = const_projects_df.apply(lambda row: (row['dlg_constructed_date'] - row['date_approved']).days/365, axis=1)
const_projects_df['construct_years_int'] = const_projects_df.apply(lambda row: np.rint((row['dlg_constructed_date'] - row['date_approved']).days/365), axis=1)

#constuct the average days by estimated time
est_02yrs = const_projects_df[const_projects_df.schedule == '0-2 Years'].construct_days.mean()
est_35yrs = const_projects_df[const_projects_df.schedule == '3-5 Years'].construct_days.mean()
est_610yrs = const_projects_df[const_projects_df.schedule == '6-10 Years'].construct_days.mean()
est_1120yrs = const_projects_df[const_projects_df.schedule == '11-20Years'].construct_days.mean()

### NOTE: these (WX21011040, WX21173166) with a pending status & date are approved, others later withdrawn (WX21173154, WX21011039)
#use the average days to completion to fill in the missing completed dates -- anything without a status of CONSTRUCTED is imputed
# 0-2 years
projects_df_merged.loc[((~projects_df_merged.project_status.isin(['Constructed', 'Withdrawn'])) & (projects_df_merged.schedule == '0-2 Years')),'dlg_constructed_date'] = projects_df_merged[
    ((~projects_df_merged.project_status.isin(['Constructed', 'Withdrawn'])) & (projects_df_merged.schedule == '0-2 Years'))].date_approved+est_02yrs

# 3-5 years
projects_df_merged.loc[((~projects_df_merged.project_status.isin(['Constructed', 'Withdrawn'])) & (projects_df_merged.schedule == '3-5 Years')),'dlg_constructed_date'] = projects_df_merged[
    ((~projects_df_merged.project_status.isin(['Constructed', 'Withdrawn'])) & (projects_df_merged.schedule == '3-5 Years'))].date_approved+est_35yrs

# 6-10 years
projects_df_merged.loc[((~projects_df_merged.project_status.isin(['Constructed', 'Withdrawn'])) & (projects_df_merged.schedule == '6-10 Years')),'dlg_constructed_date'] = projects_df_merged[
    ((~projects_df_merged.project_status.isin(['Constructed', 'Withdrawn'])) & (projects_df_merged.schedule == '6-10 Years'))].date_approved+est_610yrs

# 11-20 years
projects_df_merged.loc[((~projects_df_merged.project_status.isin(['Constructed', 'Withdrawn'])) & (projects_df_merged.schedule == '11-20Years')),'dlg_constructed_date'] = projects_df_merged[
    ((~projects_df_merged.project_status.isin(['Constructed', 'Withdrawn'])) & (projects_df_merged.schedule == '11-20Years'))].date_approved+est_1120yrs

#round to nearest day
projects_df_merged.dlg_constructed_date = projects_df_merged.dlg_constructed_date.dt.round("d")

# send out the new projects defintions with the imputed project completion dates
# with PostgresConnectionManager(DB_CONFIG) as conn:
#     projects_df_merged.to_sql('projects_imputed', conn, dtype={"date_approved": Date(), "estimated_construction_completion_date": Date(), "profile_modified": Date(),
#                                                       "gis_modified": Date(), "dlg_constructed_date": Date()}, schema='wris', if_exists ='replace', index=False)