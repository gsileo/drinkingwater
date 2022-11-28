import pandas as pd
import os
import math
import time
import random
import sys
import numpy as np
import sqlalchemy
import geopandas as gpd
from iteration_utilities import deepflatten

from sqlalchemy.types import Date

from datetime import datetime, date

os.chdir('/home/gsileo/repos/drinkingwater')

from config import PATH_TO_CENSUS_DATA
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

##########################################
### Construct a KY County Mapping File ###
##########################################
#Note -- using 2020 for now
#sourced from: https://www.census.gov/programs-surveys/geography/technical-documentation/complete-technical-documentation/tiger-geo-line.html

os.chdir(PATH_TO_CENSUS_DATA)
ky_county_shp = gpd.read_file('ky_shapefiles/tl_2020_21_county20.shp') 

#subset to just the columns necessary
ky_county_mapping = ky_county_shp[['STATEFP20', 'COUNTYFP20', 'COUNTYNS20', 'GEOID20', 'NAME20', 'NAMELSAD20']]

#save to file
ky_county_mapping.to_pickle("ky_county_mapping.pkl")


##################################################
### Pull in the Census's Dollar-Adjusting File ###
##################################################
#Notes: 
#1) The U.S. Census Bureau uses the Bureau of Labor Statistics' (BLS) Consumer Price Index for all Urban Consumers Research Series (CPI-U-RS) for all items, not seasonally adjusted, for 1978 through 2019. For 1967 to 1977, the Census Bureau uses estimates provided by BLS from the CPI-U-X1 series. The CPI-U-X1 is an experimental series that preceded the CPI-U-RS and estimates the inflation rate in the CPI-U when applying the current rental equivalence method of measuring the cost of homeownership for years prior to 1983. The Census Bureau derived the CPI-U-RS for years before 1967 by applying the 1967 CPI-U-RS-to-CPI-U ratio to the 1947 to 1966 CPI-U.  

#2) Note: Data users can compute the percentage changes in prices between earlier years' data and 2019 data by dividing the annual average CPI-U-RS for 2019 by the annual average for the earlier year(s). For more information on the CPI-U-RS, see <www.bls.gov/cpi/research-series/home.htm>.

cpi_adjustment = pd.read_csv("CPI_U_RS.csv")

#send out to postgres
with PostgresConnectionManager(DB_CONFIG) as conn:
    cpi_adjustment.to_sql('acs_cpi_adj', conn, schema='census', if_exists ='replace', index=False)

##############################################
### Send ACS County Level Data to Postgres ###
##############################################
#data sourced from: https://data2.nhgis.org/main --> selected fields of interest for all 5 year ACS data

#keep the data of interest -- 
#(Total population by race):
#01 - Total
#02 - White alone
#03 - Black/African American Aaone
#04 - American Indian and Alaska Native alone
#05 - Asian alone
#06 - Native Hawaiian and Other Pacific Islander alone
#07 - Some other race alone
#08 - Two or more races
#09 - Two or more races: Two races including Some other race
#10 - Two or more races: Two races excluding Some other race, and three or more races

# (Educational Attainment for the Population 25 Years and Over):
#01 - Total -- (I believe this is total pop for which this info is available)
#02 - No schooling completed
#03 - Nursery school
#04 - Kindergarten
#05 - 1st grade
#06 - 2nd grade
#07 - 3rd grade
#08 - 4th grade
#09 - 5th grade
#10 - 6th grade
#11 - 7th grade
#12 - 8th grade
#13 - 9th grade
#14 - 10th grade
#15 - 11th grade
#16 - 12th grade, no diploma
#17 - Regular high school diploma
#18 - GED or alternative credential
#19 - Some college, less than 1 year
#20 - Some college, 1 or more years, no degree
#21 - Associate's degree
#22 - Bachelor's degree
#23 - Master's degree
#24 - Professional school degree
#25 - Doctorate degree

#initialize a dataframe to hold all the data
acs_county_data = pd.DataFrame()

#################
## 2005 - 2009 ##
#read in the acs county level data and subset to Kentucky data
acs_0509_county_data = pd.read_csv("nhgis0006_ds195_20095_county.csv", encoding='cp1252') #encoding needed to handle any text with n tilde
acs_0509_county_data = acs_0509_county_data[acs_0509_county_data['STATEA'] == 21]

#construct a countyfips column, drop unnecessary columns, and rename columns with the same data as other years for consistency
acs_0509_county_data['countyfips'] = acs_0509_county_data.apply(lambda row: str(row['STATEA']) + str(row['COUNTYA']).zfill(3), axis =1)
acs_0509_county_data = acs_0509_county_data[['GISJOIN', 'YEAR', 'STATE', 'COUNTY', 'countyfips', 'RLAE001', 'RLAE002', 'RLAE003', 'RLAE004', 'RLAE005', 'RLAE006', 
                                   'RLAE007', 'RLAE008', 'RLAE009', 'RLAE010', 'RNHE001', 'RP7E001']]
acs_0509_county_data.columns = ['gisjoin', 'year', 'state', 'county', 'countyfips', 'tot_pop', 'wht_pop', 'blk_pop', 'native_pop', 'asn_pop', 'isl_pop', 'other', 'two_plus', 'two_plus_some', 'two_plus_no_some', 'mhi', 'housing_units']

#2009 adjusted dollars -> adjust to 2019 dollars to match WRIS and 2012 to match with sales adjusted prices
acs_0509_county_data['mhi_2012'] = (cpi_adjustment[cpi_adjustment.Year == 2012]['CPI-U-RS'].item()/ cpi_adjustment[cpi_adjustment.Year == 2009]['CPI-U-RS'].item())*acs_0509_county_data['mhi']
acs_0509_county_data['mhi'] = (cpi_adjustment[cpi_adjustment.Year == 2019]['CPI-U-RS'].item()/ cpi_adjustment[cpi_adjustment.Year == 2009]['CPI-U-RS'].item())*acs_0509_county_data['mhi']

#append data to wider dataframe
acs_county_data = acs_county_data.append(acs_0509_county_data)


#################
## 2006 - 2010 ##
#read in the acs county level data and subset to Kentucky data
acs_0610_county_data = pd.read_csv("nhgis0006_ds176_20105_county.csv", encoding='cp1252')
acs_0610_county_data = acs_0610_county_data[acs_0610_county_data['STATEA'] == 21]

#construct a countyfips column, drop unnecessary columns, and rename columns with the same data as other years for consistency
acs_0610_county_data['countyfips'] = acs_0610_county_data.apply(lambda row: str(row['STATEA']) + str(row['COUNTYA']).zfill(3), axis =1)
acs_0610_county_data = acs_0610_county_data[['GISJOIN', 'YEAR', 'STATE', 'COUNTY', 'countyfips', 'JMBE001', 'JMBE002', 'JMBE003', 'JMBE004', 'JMBE005', 'JMBE006', 
                                   'JMBE007', 'JMBE008', 'JMBE009', 'JMBE010', 'JOIE001', 'JRIE001']]
acs_0610_county_data.columns = ['gisjoin', 'year', 'state', 'county', 'countyfips', 'tot_pop', 'wht_pop', 'blk_pop', 'native_pop', 'asn_pop', 'isl_pop', 'other', 'two_plus', 'two_plus_some', 'two_plus_no_some', 'mhi', 'housing_units']

#2010 adjusted dollars -> adjust to 2019 dollars to match WRIS and 2012 to match with sales adjusted prices
acs_0610_county_data['mhi_2012'] = (cpi_adjustment[cpi_adjustment.Year == 2012]['CPI-U-RS'].item()/ cpi_adjustment[cpi_adjustment.Year == 2010]['CPI-U-RS'].item())*acs_0610_county_data['mhi']
acs_0610_county_data['mhi'] = (cpi_adjustment[cpi_adjustment.Year == 2019]['CPI-U-RS'].item()/ cpi_adjustment[cpi_adjustment.Year == 2010]['CPI-U-RS'].item())*acs_0610_county_data['mhi']

#append data to wider dataframe
acs_county_data = acs_county_data.append(acs_0610_county_data)


#################
## 2007 - 2011 ##
#read in the acs county level data and subset to Kentucky data
acs_0711_county_data = pd.read_csv("nhgis0006_ds184_20115_county.csv", encoding='cp1252')
acs_0711_county_data = acs_0711_county_data[acs_0711_county_data['STATEA'] == 21]

#construct a countyfips column, drop unnecessary columns, and rename columns with the same data as other years for consistency
acs_0711_county_data['countyfips'] = acs_0711_county_data.apply(lambda row: str(row['STATEA']) + str(row['COUNTYA']).zfill(3), axis =1)
acs_0711_county_data = acs_0711_county_data[['GISJOIN', 'YEAR', 'STATE', 'COUNTY', 'countyfips', 'MNUE001', 'MNUE002', 'MNUE003', 'MNUE004', 'MNUE005', 'MNUE006', 
                                   'MNUE007', 'MNUE008', 'MNUE009', 'MNUE010', 'MP1E001', 'MS2E001']]
acs_0711_county_data.columns = ['gisjoin', 'year', 'state', 'county', 'countyfips', 'tot_pop', 'wht_pop', 'blk_pop', 'native_pop', 'asn_pop', 'isl_pop', 'other', 'two_plus', 'two_plus_some', 'two_plus_no_some', 'mhi', 'housing_units']

#2011 adjusted dollars -> adjust to 2019 dollars to match WRIS and 2012 to match with sales adjusted prices
acs_0711_county_data['mhi_2012'] = (cpi_adjustment[cpi_adjustment.Year == 2012]['CPI-U-RS'].item()/ cpi_adjustment[cpi_adjustment.Year == 2011]['CPI-U-RS'].item())*acs_0711_county_data['mhi']
acs_0711_county_data['mhi'] = (cpi_adjustment[cpi_adjustment.Year == 2019]['CPI-U-RS'].item()/ cpi_adjustment[cpi_adjustment.Year == 2011]['CPI-U-RS'].item())*acs_0711_county_data['mhi']

#append data to wider dataframe
acs_county_data = acs_county_data.append(acs_0711_county_data)


#################
## 2008 - 2012 ##
#read in the acs county level data and subset to Kentucky data
acs_0812_county_data = pd.read_csv("nhgis0006_ds191_20125_county.csv", encoding='cp1252')
acs_0812_county_data = acs_0812_county_data[acs_0812_county_data['STATEA'] == 21]

#construct a countyfips column, drop unnecessary columns, and rename columns with the same data as other years for consistency
acs_0812_county_data['countyfips'] = acs_0812_county_data.apply(lambda row: str(row['STATEA']) + str(row['COUNTYA']).zfill(3), axis =1)
acs_0812_county_data = acs_0812_county_data[['GISJOIN', 'YEAR', 'STATE', 'COUNTY', 'countyfips', 'QSQE001', 'QSQE002', 'QSQE003', 'QSQE004', 'QSQE005', 'QSQE006', 
                                   'QSQE007', 'QSQE008', 'QSQE009', 'QSQE010', 'QU1E001', 'QX6E001']]
acs_0812_county_data.columns = ['gisjoin', 'year', 'state', 'county', 'countyfips', 'tot_pop', 'wht_pop', 'blk_pop', 'native_pop', 'asn_pop', 'isl_pop', 'other', 'two_plus', 'two_plus_some', 'two_plus_no_some', 'mhi', 'housing_units']

#2012 adjusted dollars -> adjust to 2019 dollars to match WRIS and 2012 to match with sales adjusted prices
acs_0812_county_data['mhi_2012'] = (cpi_adjustment[cpi_adjustment.Year == 2012]['CPI-U-RS'].item()/ cpi_adjustment[cpi_adjustment.Year == 2012]['CPI-U-RS'].item())*acs_0812_county_data['mhi']
acs_0812_county_data['mhi'] = (cpi_adjustment[cpi_adjustment.Year == 2019]['CPI-U-RS'].item()/ cpi_adjustment[cpi_adjustment.Year == 2012]['CPI-U-RS'].item())*acs_0812_county_data['mhi']

#append data to wider dataframe
acs_county_data = acs_county_data.append(acs_0812_county_data)


#################
## 2009 - 2013 ##
#read in the acs county level data and subset to Kentucky data
acs_0913_county_data = pd.read_csv("nhgis0006_ds201_20135_county.csv", encoding='cp1252')
acs_0913_county_data = acs_0913_county_data[acs_0913_county_data['STATEA'] == 21]

#construct a countyfips column, drop unnecessary columns, and rename columns with the same data as other years for consistency
acs_0913_county_data['countyfips'] = acs_0913_county_data.apply(lambda row: str(row['STATEA']) + str(row['COUNTYA']).zfill(3), axis =1)
acs_0913_county_data = acs_0913_county_data[['GISJOIN', 'YEAR', 'STATE', 'COUNTY', 'countyfips', 'UEQE001', 'UEQE002', 'UEQE003', 'UEQE004', 'UEQE005', 'UEQE006', 
                                   'UEQE007', 'UEQE008', 'UEQE009', 'UEQE010', 'UHDE001', 'UKME001']]
acs_0913_county_data.columns = ['gisjoin', 'year', 'state', 'county', 'countyfips', 'tot_pop', 'wht_pop', 'blk_pop', 'native_pop', 'asn_pop', 'isl_pop', 'other', 'two_plus', 'two_plus_some', 'two_plus_no_some', 'mhi', 'housing_units']

#2013 adjusted dollars -> adjust to 2019 dollars to match WRIS and 2012 to match with sales adjusted prices
acs_0913_county_data['mhi_2012'] = (cpi_adjustment[cpi_adjustment.Year == 2012]['CPI-U-RS'].item()/ cpi_adjustment[cpi_adjustment.Year == 2013]['CPI-U-RS'].item())*acs_0913_county_data['mhi']
acs_0913_county_data['mhi'] = (cpi_adjustment[cpi_adjustment.Year == 2019]['CPI-U-RS'].item()/ cpi_adjustment[cpi_adjustment.Year == 2013]['CPI-U-RS'].item())*acs_0913_county_data['mhi']

#append data to wider dataframe
acs_county_data = acs_county_data.append(acs_0913_county_data)


#################
## 2010 - 2014 ##
#read in the acs county level data and subset to Kentucky data
acs_1014_county_data = pd.read_csv("nhgis0006_ds206_20145_county.csv", encoding='cp1252')
acs_1014_county_data = acs_1014_county_data[acs_1014_county_data['STATEA'] == 21]

#construct a countyfips column, drop unnecessary columns, and rename columns with the same data as other years for consistency
acs_1014_county_data['countyfips'] = acs_1014_county_data.apply(lambda row: str(row['STATEA']) + str(row['COUNTYA']).zfill(3), axis =1)
acs_1014_county_data = acs_1014_county_data[['GISJOIN', 'YEAR', 'STATE', 'COUNTY', 'countyfips', 'ABA2E001', 'ABA2E002', 'ABA2E003', 'ABA2E004', 'ABA2E005', 'ABA2E006', 
                                   'ABA2E007', 'ABA2E008', 'ABA2E009', 'ABA2E010', 'ABDPE001', 'ABGVE001']]
acs_1014_county_data.columns = ['gisjoin', 'year', 'state', 'county', 'countyfips', 'tot_pop', 'wht_pop', 'blk_pop', 'native_pop', 'asn_pop', 'isl_pop', 'other', 'two_plus', 'two_plus_some', 'two_plus_no_some', 'mhi', 'housing_units']

#2014 adjusted dollars -> adjust to 2019 dollars to match WRIS and 2012 to match with sales adjusted prices
acs_1014_county_data['mhi_2012'] = (cpi_adjustment[cpi_adjustment.Year == 2012]['CPI-U-RS'].item()/ cpi_adjustment[cpi_adjustment.Year == 2014]['CPI-U-RS'].item())*acs_1014_county_data['mhi']
acs_1014_county_data['mhi'] = (cpi_adjustment[cpi_adjustment.Year == 2019]['CPI-U-RS'].item()/ cpi_adjustment[cpi_adjustment.Year == 2014]['CPI-U-RS'].item())*acs_1014_county_data['mhi']

#append data to wider dataframe
acs_county_data = acs_county_data.append(acs_1014_county_data)


#################
## 2011 - 2015 ##
#read in the acs county level data and subset to Kentucky data
acs_1115_county_data = pd.read_csv("nhgis0006_ds215_20155_county.csv", encoding='cp1252')
acs_1115_county_data = acs_1115_county_data[acs_1115_county_data['STATEA'] == 21]

#construct a countyfips column, drop unnecessary columns, and rename columns with the same data as other years for consistency
acs_1115_county_data['countyfips'] = acs_1115_county_data.apply(lambda row: str(row['STATEA']) + str(row['COUNTYA']).zfill(3), axis =1)
acs_1115_county_data = acs_1115_county_data[['GISJOIN', 'YEAR', 'STATE', 'COUNTY', 'countyfips', 'ADKXE001', 'ADKXE002', 'ADKXE003', 'ADKXE004', 'ADKXE005', 'ADKXE006', 
                                   'ADKXE007', 'ADKXE008', 'ADKXE009', 'ADKXE010', 'ADNKE001', 'ADPYE001']]
acs_1115_county_data.columns = ['gisjoin', 'year', 'state', 'county', 'countyfips', 'tot_pop', 'wht_pop', 'blk_pop', 'native_pop', 'asn_pop', 'isl_pop', 'other', 'two_plus', 'two_plus_some', 'two_plus_no_some', 'mhi', 'housing_units']

#2015 adjusted dollars -> adjust to 2019 dollars to match WRIS and 2012 to match with sales adjusted prices
acs_1115_county_data['mhi_2012'] = (cpi_adjustment[cpi_adjustment.Year == 2012]['CPI-U-RS'].item()/ cpi_adjustment[cpi_adjustment.Year == 2015]['CPI-U-RS'].item())*acs_1115_county_data['mhi']
acs_1115_county_data['mhi'] = (cpi_adjustment[cpi_adjustment.Year == 2019]['CPI-U-RS'].item()/ cpi_adjustment[cpi_adjustment.Year == 2015]['CPI-U-RS'].item())*acs_1115_county_data['mhi']

#append data to wider dataframe
acs_county_data = acs_county_data.append(acs_1115_county_data)


#################
## 2012 - 2016 ##
#read in the acs county level data and subset to Kentucky data
acs_1216_county_data = pd.read_csv("nhgis0006_ds225_20165_county.csv", encoding='cp1252')
acs_1216_county_data = acs_1216_county_data[acs_1216_county_data['STATEA'] == 21]

#construct a countyfips column, drop unnecessary columns, and rename columns with the same data as other years for consistency
acs_1216_county_data['countyfips'] = acs_1216_county_data.apply(lambda row: str(row['STATEA']) + str(row['COUNTYA']).zfill(3), axis =1)
acs_1216_county_data = acs_1216_county_data[['GISJOIN', 'YEAR', 'STATE', 'COUNTY', 'countyfips', 'AF2ME001', 'AF2ME002', 'AF2ME003', 'AF2ME004', 'AF2ME005', 'AF2ME006', 
                                   'AF2ME007', 'AF2ME008', 'AF2ME009', 'AF2ME010', 'AF49E001', 'AF7NE001']]
acs_1216_county_data.columns = ['gisjoin', 'year', 'state', 'county', 'countyfips', 'tot_pop', 'wht_pop', 'blk_pop', 'native_pop', 'asn_pop', 'isl_pop', 'other', 'two_plus', 'two_plus_some', 'two_plus_no_some', 'mhi', 'housing_units']

#2016 adjusted dollars -> adjust to 2019 dollars to match WRIS and 2012 to match with sales adjusted prices
acs_1216_county_data['mhi_2012'] = (cpi_adjustment[cpi_adjustment.Year == 2012]['CPI-U-RS'].item()/ cpi_adjustment[cpi_adjustment.Year == 2016]['CPI-U-RS'].item())*acs_1216_county_data['mhi']
acs_1216_county_data['mhi'] = (cpi_adjustment[cpi_adjustment.Year == 2019]['CPI-U-RS'].item()/ cpi_adjustment[cpi_adjustment.Year == 2016]['CPI-U-RS'].item())*acs_1216_county_data['mhi']

#append data to wider dataframe
acs_county_data = acs_county_data.append(acs_1216_county_data)


#################
## 2013 - 2017 ##
#read in the acs county level data and subset to Kentucky data
acs_1317_county_data = pd.read_csv("nhgis0006_ds233_20175_county.csv", encoding='cp1252')
acs_1317_county_data = acs_1317_county_data[acs_1317_county_data['STATEA'] == 21]

#construct a countyfips column, drop unnecessary columns, and rename columns with the same data as other years for consistency
acs_1317_county_data['countyfips'] = acs_1317_county_data.apply(lambda row: str(row['STATEA']) + str(row['COUNTYA']).zfill(3), axis =1)
acs_1317_county_data = acs_1317_county_data[['GISJOIN', 'YEAR', 'STATE', 'COUNTY', 'countyfips', 'AHY2E001', 'AHY2E002', 'AHY2E003', 'AHY2E004', 'AHY2E005', 'AHY2E006', 
                                   'AHY2E007', 'AHY2E008', 'AHY2E009', 'AHY2E010', 'AH1PE001', 'AH35E001']]
acs_1317_county_data.columns = ['gisjoin', 'year', 'state', 'county', 'countyfips', 'tot_pop', 'wht_pop', 'blk_pop', 'native_pop', 'asn_pop', 'isl_pop', 'other', 'two_plus', 'two_plus_some', 'two_plus_no_some', 'mhi', 'housing_units']

#2017 adjusted dollars -> adjust to 2019 dollars to match WRIS and 2012 to match with sales adjusted prices
acs_1317_county_data['mhi_2012'] = (cpi_adjustment[cpi_adjustment.Year == 2012]['CPI-U-RS'].item()/ cpi_adjustment[cpi_adjustment.Year == 2017]['CPI-U-RS'].item())*acs_1317_county_data['mhi']
acs_1317_county_data['mhi'] = (cpi_adjustment[cpi_adjustment.Year == 2019]['CPI-U-RS'].item()/ cpi_adjustment[cpi_adjustment.Year == 2017]['CPI-U-RS'].item())*acs_1317_county_data['mhi']

#append data to wider dataframe
acs_county_data = acs_county_data.append(acs_1317_county_data)


#################
## 2014 - 2018 ##
#read in the acs county level data and subset to Kentucky data
acs_1418_county_data = pd.read_csv("nhgis0006_ds239_20185_county.csv", encoding='cp1252')
acs_1418_county_data = acs_1418_county_data[acs_1418_county_data['STATEA'] == 21]

#construct a countyfips column, drop unnecessary columns, and rename columns with the same data as other years for consistency
acs_1418_county_data['countyfips'] = acs_1418_county_data.apply(lambda row: str(row['STATEA']) + str(row['COUNTYA']).zfill(3), axis =1)
acs_1418_county_data = acs_1418_county_data[['GISJOIN', 'YEAR', 'STATE', 'COUNTY', 'countyfips', 'AJWNE001', 'AJWNE002', 'AJWNE003', 'AJWNE004', 'AJWNE005', 'AJWNE006', 
                                   'AJWNE007', 'AJWNE008', 'AJWNE009', 'AJWNE010', 'AJZAE001', 'AJ1SE001']]
acs_1418_county_data.columns = ['gisjoin', 'year', 'state', 'county', 'countyfips', 'tot_pop', 'wht_pop', 'blk_pop', 'native_pop', 'asn_pop', 'isl_pop', 'other', 'two_plus', 'two_plus_some', 'two_plus_no_some', 'mhi', 'housing_units']

#2018 adjusted dollars -> adjust to 2019 dollars to match WRIS and 2012 to match with sales adjusted prices
acs_1418_county_data['mhi_2012'] = (cpi_adjustment[cpi_adjustment.Year == 2012]['CPI-U-RS'].item()/ cpi_adjustment[cpi_adjustment.Year == 2018]['CPI-U-RS'].item())*acs_1418_county_data['mhi']
acs_1418_county_data['mhi'] = (cpi_adjustment[cpi_adjustment.Year == 2019]['CPI-U-RS'].item()/ cpi_adjustment[cpi_adjustment.Year == 2018]['CPI-U-RS'].item())*acs_1418_county_data['mhi']

#append data to wider dataframe
acs_county_data = acs_county_data.append(acs_1418_county_data)


#################
## 2015 - 2019 ##
#read in the acs county level data and subset to Kentucky data
acs_1519_county_data = pd.read_csv("nhgis0006_ds244_20195_county.csv", encoding='cp1252')
acs_1519_county_data = acs_1519_county_data[acs_1519_county_data['STATEA'] == 21]

#construct a countyfips column, drop unnecessary columns, and rename columns with the same data as other years for consistency
acs_1519_county_data['countyfips'] = acs_1519_county_data.apply(lambda row: str(row['STATEA']) + str(row['COUNTYA']).zfill(3), axis =1)
acs_1519_county_data = acs_1519_county_data[['GISJOIN', 'YEAR', 'STATE', 'COUNTY', 'countyfips', 'ALUCE001', 'ALUCE002', 'ALUCE003', 'ALUCE004', 'ALUCE005', 'ALUCE006', 
                                   'ALUCE007', 'ALUCE008', 'ALUCE009', 'ALUCE010', 'ALW1E001', 'ALZJE001']]
acs_1519_county_data.columns = ['gisjoin', 'year', 'state', 'county', 'countyfips', 'tot_pop', 'wht_pop', 'blk_pop', 'native_pop', 'asn_pop', 'isl_pop', 'other', 'two_plus', 'two_plus_some', 'two_plus_no_some', 'mhi', 'housing_units']

#2019 adjusted dollars -> adjust to 2012 to match with sales adjusted prices
acs_1519_county_data['mhi_2012'] = (cpi_adjustment[cpi_adjustment.Year == 2012]['CPI-U-RS'].item()/ cpi_adjustment[cpi_adjustment.Year == 2019]['CPI-U-RS'].item())*acs_1519_county_data['mhi']

#append data to wider dataframe
acs_county_data = acs_county_data.append(acs_1519_county_data)
acs_county_data.reset_index(inplace=True, drop=True)

#send out to postgres
with PostgresConnectionManager(DB_CONFIG) as conn:
    acs_county_data.to_sql('acs_county', conn, schema='census', if_exists ='replace', index=False)
    

################################################################################################
### Construct county MHI, Race Demographics, and Tot Pop by Year, averaging ACS observations ### 
################################################################################################

#construct a dataframe containing the actual years in each acs 5 year grouping, paired with the counties in KY
years_acs_years_county = pd.DataFrame({'years': np.tile(list(deepflatten([[i for i in range(2005+j,2010+j)] for j in range(11)])), acs_county_data.countyfips.nunique()),
                                       'acs_years': np.tile(np.repeat(acs_county_data.year.unique(),5), acs_county_data.countyfips.nunique()),
                                       'countyfips': np.repeat(acs_county_data.countyfips.unique(), acs_county_data.year.nunique()*5)})

#merge with the acs data so there is an entry for each year covered by the data
years_acs_years_county_aug = years_acs_years_county.merge(acs_county_data, how='left', left_on=['acs_years', 'countyfips'], right_on=['year','countyfips'])
    
#aggregate to the individual year level by taking the average of the available observations
acs_yearly_county_data = years_acs_years_county_aug.groupby(['years', 'countyfips', 'state', 'county'], as_index=False).agg({'tot_pop': 'mean', 'wht_pop': 'mean', 'blk_pop': 'mean',
                                                                                                                             'native_pop': 'mean', 'asn_pop': 'mean', 'isl_pop': 'mean', 
                                                                                                                             'other': 'mean', 'two_plus': 'mean', 'two_plus_some': 'mean',
                                                                                                                             'two_plus_no_some': 'mean', 'mhi': 'mean', 'mhi_2012': 'mean',
                                                                                                                             'housing_units': 'mean'})
#rename columns
acs_yearly_county_data.rename(columns={'years': 'year'}, inplace = True)


########################################################################################################################
### Add in the Size of the County from the Shapefiles -- only collected at 2000 and 2020, so for 2010 used 2020 data ### 
########################################################################################################################
#Census only tracks area every 10 years, for 2010 used 2020's 2010 area mass https://www.census.gov/quickfacts/fact/note/US/LND110210

os.chdir(PATH_TO_CENSUS_DATA)
ky_county_shp_10 = gpd.read_file('ky_shapefiles/tl_2020_21_county10.shp') 
ky_county_shp_00 = gpd.read_file('ky_shapefiles/tl_2010_21_county00.shp') 

#merge with the land area (in sq meters) and assign based on the year
acs_yearly_county_data = acs_yearly_county_data.merge(ky_county_shp_00[['CNTYIDFP00', 'ALAND00']], how='left', left_on='countyfips', right_on='CNTYIDFP00')
acs_yearly_county_data = acs_yearly_county_data.merge(ky_county_shp_10[['GEOID10', 'ALAND10']], how='left', left_on='countyfips', right_on='GEOID10')
acs_yearly_county_data['land_area'] = acs_yearly_county_data.apply(lambda row: row.ALAND00 if row.year<2010 else row.ALAND10, axis=1)
acs_yearly_county_data['land_area'] = acs_yearly_county_data['land_area'].values*3.86102e-7 #conversion from sq meters to sq miles
acs_yearly_county_data.drop(columns=['CNTYIDFP00', 'ALAND00', 'GEOID10', 'ALAND10'], inplace=True)


#send out to postgres
with PostgresConnectionManager(DB_CONFIG) as conn:
    acs_yearly_county_data.to_sql('acs_county_yearly', conn, schema='census', if_exists ='replace', index=False)    
    