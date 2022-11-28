import pandas as pd
import os
import math
import time
import random
import sys
import numpy as np
from IPython.display import clear_output

import geopandas as gpd
from shapely.geometry import Point, Polygon
import pickle

os.chdir('/home/gsileo/repos/drinkingwater')

###############################################
# https://www1.ncdc.noaa.gov/pub/data/cdo/documentation/GHCND_documentation.pdf

from config import PATH_TO_NOAA_DATA, PATH_TO_SALES_DATA, PATH_TO_CENSUS_DATA
import warnings
warnings.filterwarnings('ignore')


### Functions ###
def map_to_weekend(date, weekends):
    '''
    Goal: to return the end of the week in which the inputted date belongs
    
    Inputs:
    date: a datetime object which needs to be categorized into a week
    weekends: a list or array of dates which mark the end of a week
    
    Outputs:
    end_of_week: the date for the end of the week that the inputted date belongs
    
    '''
    #check each end of week to determine where the date falls
    for i in range(len(weekends)):
        #if arriving at a weekend and find that the date is prior to that week-end, then the week it falls into is the current week (due to ascending order of weekends)
        if ((date <= weekends[i]) == True):
            end_of_week = weekends[i]
            break
            
    return end_of_week

##############################
### Pull in Saved Out Data ###
##############################

# os.chdir(PATH_TO_NOAA_DATA)
# ky_noaa_data = pickle.load(open("ky_noaa_data_df.pkl", "rb"))
# unique_panel_df = pickle.load(open("noaa_panel_df.pkl", "rb"))

#######################################################
### Pull in the saved out files from NOAA Downloads ###
#######################################################
#data sourced from: https://www.ncdc.noaa.gov/cdo-web/search; downloaded a subset at a time as an excel spreadsheet -- Daily Summaries; Stations; Kentucky
# https://www.ncdc.noaa.gov/cdo-web/orders?email=gs907@georgetown.edu&id=2665098, picked by station subsets

os.chdir(f'{PATH_TO_NOAA_DATA}/raw')
noaa_files = os.listdir(f'{PATH_TO_NOAA_DATA}/raw')

#initalize a dataframe to contain all the information
ky_noaa_data = pd.DataFrame()

for f in noaa_files:
    #read in each file and append to the larger dataframe
    curr_df = pd.read_csv(f)
    ky_noaa_data = ky_noaa_data.append(curr_df)
    
#drop unnecessary columns
ky_noaa_data.drop(columns = ['DAPR', 'DAPR_ATTRIBUTES', 'DASF', 'DASF_ATTRIBUTES', 'MDPR', 'MDPR_ATTRIBUTES', 'MDSF', 'MDSF_ATTRIBUTES', 'SNWD', 'SNWD_ATTRIBUTES', 'TOBS',
       'TOBS_ATTRIBUTES', 'DWPR', 'DWPR_ATTRIBUTES'], inplace=True)

#change names to lowercase
ky_noaa_data.columns = [x.lower() for x in ky_noaa_data.columns]
#changing the date data to datetime
ky_noaa_data.date = ky_noaa_data.date.apply(lambda x: pd.to_datetime(x))
ky_noaa_data.reset_index(inplace=True, drop=True)

################################################
### Create a "week-end" for date aggregation ###
################################################

#pull in a dataframe with only the unique week-ends and county combinations for which there is sales data
os.chdir(f'{PATH_TO_SALES_DATA}/out')
unique_panel_df = pd.read_pickle("ky_sales_unique_id_only_df.pkl")
unique_panel_df.sort_values(by=['week_end', 'fips_county_code'], inplace=True)
unique_panel_df.columns = ['week_end', 'county']
unique_panel_df.reset_index(inplace=True, drop=True)

#extract the week ends and sort
unique_week_ends = ky_sales_data_df.week_end.unique()
unique_week_ends.sort()

########################################
### Pulling in the county shapefiles ###
########################################

#Note -- using 2020 for now, not sure 2010 vs 2020 are different
#sourced from: https://www.census.gov/programs-surveys/geography/technical-documentation/complete-technical-documentation/tiger-geo-line.html
os.chdir(PATH_TO_CENSUS_DATA)
ky_county_shp = gpd.read_file('ky_shapefiles/tl_2020_21_county20.shp') 

#obtain the entirety of Kentucky's geometry by "dissolving" the polygons of the counties into a state
ky_state_shp = ky_county_shp.dissolve(by='STATEFP20')

#########################################################
### Determine which county each of the stations is in ###
#########################################################

ky_unique_stations = ky_noaa_data[['station', 'latitude', 'longitude']]
ky_unique_stations.drop_duplicates(inplace=True, ignore_index=True)

#create a geographic point for the station
ky_unique_stations['location'] = ky_unique_stations.apply(lambda row: Point(row.longitude, row.latitude), axis=1)

ky_unique_stations['county'] = ''

#run through each of the stations and determine which county it is in
for i in range(len(ky_unique_stations)):
    #set station location
    curr_station = ky_unique_stations.location.iloc[i]
    
    #run through the counties and determine which county it is in
    for j in range(len(ky_county_shp)):
        if curr_station.within(ky_county_shp.geometry.iloc[j]) == True:
            ky_unique_stations.county.iloc[i] = ky_county_shp.GEOID20.iloc[j]
            
#now there are some stations which are actually technically outside of Kentucky. Using the distance metric, determine which is the closest county 
#and populate the county column with that value
for i in range(len(ky_unique_stations)):
    if ky_unique_stations.county.iloc[i] == '':
        #use the station point
        curr_point = ky_unique_stations.location.iloc[i]
        
        #find which county has the shortest distance to that point
        closest_index = ky_county_shp.distance(curr_point).argmin()
        
        #update the county to the one closest to the station inside KY
        ky_unique_stations.county.iloc[i] = ky_county_shp.GEOID20.iloc[closest_index]
        
####################################
### Construct Panel Weather Data ###
####################################

#merge with the location/county data for the unique stations
ky_noaa_data = ky_noaa_data.merge(ky_unique_stations, how='left', on=['station', 'latitude', 'longitude'])

#add in the column indicating which week-end the data belongs to
ky_noaa_data = ky_noaa_data[ky_noaa_data.date <= pd.to_datetime('2019-12-28')] #drop the dates past the last week that I have sales data for
ky_noaa_data.reset_index(inplace=True, drop=True)
ky_noaa_data['week_end'] = ky_noaa_data.date.apply(lambda x: map_to_weekend(x, unique_week_ends))
ky_noaa_data['year'] = ky_noaa_data['date'].apply(lambda x: x.year)

#save the data out to file -- takes a long time to construct the "week-end" column
os.chdir(PATH_TO_NOAA_DATA)
ky_noaa_data.to_pickle("ky_noaa_data_df.pkl")

###################################################################################
### Aggregating the data for each week-end/county combination in the sales data ###
###################################################################################

#construct aggregated data for each week-county combination, with nans to be filled in by using surrounding county data
ky_data_agg = ky_noaa_data.groupby(by=['week_end', 'county'], as_index=False).agg({'tmax': 'mean', 'tmin': 'mean', 'tavg': 'mean', 'prcp': 'mean'})

#merge with the panel to determine which county/weeks need to be re-constructed
unique_panel_df = unique_panel_df.merge(ky_data_agg, how='left', on=['week_end', 'county'])

#save the unaltered version of the unique panel df out to file
os.chdir(PATH_TO_NOAA_DATA)
unique_panel_df.to_pickle("noaa_panel_df.pkl")

#how many need to be imputed? ~40% for tmax and tmin, 90% for tavg, 17% prcp
print(f'{len(unique_panel_df[unique_panel_df.tmax.isnull()])}, tmax, which is {len(unique_panel_df[unique_panel_df.tmax.isnull()])/len(unique_panel_df)}')
print(f'{len(unique_panel_df[unique_panel_df.tmin.isnull()])}, tmin, which is {len(unique_panel_df[unique_panel_df.tmin.isnull()])/len(unique_panel_df)}')
print(f'{len(unique_panel_df[unique_panel_df.tavg.isnull()])}, tavg, which is {len(unique_panel_df[unique_panel_df.tavg.isnull()])/len(unique_panel_df)}')
print(f'{len(unique_panel_df[unique_panel_df.prcp.isnull()])}, prcp, which is {len(unique_panel_df[unique_panel_df.prcp.isnull()])/len(unique_panel_df)}')

#how many have NO data in a week? -- None
weekly_test = unique_panel_df.groupby(by=['week_end'], as_index = False).agg({'tmax': 'mean', 'tmin': 'mean', 'tavg': 'mean', 'prcp': 'mean'})
print(len(weekly_test[weekly_test.tmax.isnull()]), ' tmax')
print(len(weekly_test[weekly_test.tmin.isnull()]), ' tmin')
print(len(weekly_test[weekly_test.tavg.isnull()]), ' tavg')
print(len(weekly_test[weekly_test.prcp.isnull()]), ' prcp')

########################################################################################################################################################
### Imputing the missing data for a county by taking the data for the station that is closest to the county, even if the station isn't in the county ###
########################################################################################################################################################

#split the noaa data into dataframes where only rows with valid entries for each type of data exist and aggregate to the station week-end average
ky_noaa_tmax = ky_noaa_data[ky_noaa_data.tmax.notnull()]
ky_noaa_tmax_agg = ky_noaa_tmax.groupby(by=['station', 'week_end'], as_index=False).agg({'tmax': 'mean'})
ky_noaa_tmax_agg = ky_noaa_tmax_agg.merge(ky_unique_stations, how='left', on='station')

ky_noaa_tmin = ky_noaa_data[ky_noaa_data.tmin.notnull()]
ky_noaa_tmin_agg = ky_noaa_tmin.groupby(by=['station', 'week_end'], as_index=False).agg({'tmin': 'mean'})
ky_noaa_tmin_agg = ky_noaa_tmin_agg.merge(ky_unique_stations, how='left', on='station')

ky_noaa_tavg = ky_noaa_data[ky_noaa_data.tavg.notnull()]
ky_noaa_tavg_agg = ky_noaa_tavg.groupby(by=['station', 'week_end'], as_index=False).agg({'tavg': 'mean'})
ky_noaa_tavg_agg = ky_noaa_tavg_agg.merge(ky_unique_stations, how='left', on='station')

ky_noaa_prcp = ky_noaa_data[ky_noaa_data.prcp.notnull()]
ky_noaa_prcp_agg = ky_noaa_prcp.groupby(by=['station', 'week_end'], as_index=False).agg({'prcp': 'mean'})
ky_noaa_prcp_agg = ky_noaa_prcp_agg.merge(ky_unique_stations, how='left', on='station')

#run through every row and fill in the missing data
for w in range(len(unique_panel_df)):
    #if tmax is missing
    if np.isnan(unique_panel_df.tmax.iloc[w]) == True:
        #find the geometry of the current county of interest
        curr_county_geom = ky_county_shp[ky_county_shp.GEOID20 == unique_panel_df.county.iloc[w]].geometry
        
        #subset the tmax data to those with valid data in this week
        curr_tmax_data = ky_noaa_tmax_agg[ky_noaa_tmax_agg.week_end == unique_panel_df.week_end.iloc[w]]
        
        #find the distances between the county and the stations
        tmax_dist_list = [curr_county_geom.distance(curr_tmax_data.location.iloc[i]).item() for i in range(len(curr_tmax_data))]
        tmax_dist_index = tmax_dist_list.index(min(tmax_dist_list))
        
        #set the tmax value to the value in the subsetted data for the location with the minumum distance
        unique_panel_df.tmax.iloc[w] = curr_tmax_data.tmax.iloc[tmax_dist_index]
        
    #if tmin is missing
    if np.isnan(unique_panel_df.tmin.iloc[w]) == True:
        #find the geometry of the current county of interest
        curr_county_geom = ky_county_shp[ky_county_shp.GEOID20 == unique_panel_df.county.iloc[w]].geometry
        
        #subset the tmin data to those with valid data in this week
        curr_tmin_data = ky_noaa_tmin_agg[ky_noaa_tmin_agg.week_end == unique_panel_df.week_end.iloc[w]]
        
        #find the distances between the county and the stations
        tmin_dist_list = [curr_county_geom.distance(curr_tmin_data.location.iloc[i]).item() for i in range(len(curr_tmin_data))]
        tmin_dist_index = tmin_dist_list.index(min(tmin_dist_list))
        
        #set the tmin value to the value in the subsetted data for the location with the minumum distance
        unique_panel_df.tmin.iloc[w] = curr_tmin_data.tmin.iloc[tmin_dist_index]
        
    #if tavg is missing
    if np.isnan(unique_panel_df.tavg.iloc[w]) == True:
        #find the geometry of the current county of interest
        curr_county_geom = ky_county_shp[ky_county_shp.GEOID20 == unique_panel_df.county.iloc[w]].geometry
        
        #subset the tavg data to those with valid data in this week
        curr_tavg_data = ky_noaa_tavg_agg[ky_noaa_tavg_agg.week_end == unique_panel_df.week_end.iloc[w]]
        
        #find the distances between the county and the stations
        tavg_dist_list = [curr_county_geom.distance(curr_tavg_data.location.iloc[i]).item() for i in range(len(curr_tavg_data))]
        tavg_dist_index = tavg_dist_list.index(min(tavg_dist_list))
        
        #set the tmin value to the value in the subsetted data for the location with the minumum distance
        unique_panel_df.tavg.iloc[w] = curr_tavg_data.tavg.iloc[tavg_dist_index]
        
    #if prcp is missing
    if np.isnan(unique_panel_df.prcp.iloc[w]) == True:
        #find the geometry of the current county of interest
        curr_county_geom = ky_county_shp[ky_county_shp.GEOID20 == unique_panel_df.county.iloc[w]].geometry
        
        #subset the prcp data to those with valid data in this week
        curr_prcp_data = ky_noaa_prcp_agg[ky_noaa_prcp_agg.week_end == unique_panel_df.week_end.iloc[w]]
        
        #find the distances between the county and the stations
        prcp_dist_list = [curr_county_geom.distance(curr_prcp_data.location.iloc[i]).item() for i in range(len(curr_prcp_data))]
        prcp_dist_index = prcp_dist_list.index(min(prcp_dist_list))
        
        #set the prcp value to the value in the subsetted data for the location with the minumum distance
        unique_panel_df.prcp.iloc[w] = curr_prcp_data.prcp.iloc[prcp_dist_index]
        
    if w%1000 == 0:
        print(f'{w} iteration')

#save out the file -- the for-loop takes a VERY long time to run        
os.chdir(PATH_TO_NOAA_DATA)
unique_panel_df.to_pickle("noaa_panel_full_df.pkl")


#################################################################################
### Also construct a similar dataframe for the same values on an annual basis ###
#################################################################################
#aggregate the observation data to the year level
ky_yrly_weather_data = pd.DataFrame({'year': np.repeat(np.sort(ky_noaa_data.year.unique()), ky_county_shp.GEOID20.nunique()), 'county': np.tile(np.sort(ky_county_shp.GEOID20.unique()), ky_noaa_data.year.nunique())})

ky_yrly_weather_data = ky_yrly_weather_data.merge(ky_noaa_data.groupby(by=['year', 'county'], as_index=False).agg({'tmax': np.nanmean, 'tmin': np.nanmean, 'tavg': np.nanmean, 'prcp': np.nanmean}), how='left', on=['year','county'])


#split the noaa data into dataframes where only rows with valid entries for each type of data exist and aggregate to the station week-end average
ky_noaa_tmax = ky_noaa_data[ky_noaa_data.tmax.notnull()]
ky_noaa_tmax_agg_yrly = ky_noaa_tmax.groupby(by=['station', 'year'], as_index=False).agg({'tmax': 'mean'})
ky_noaa_tmax_agg_yrly = ky_noaa_tmax_agg_yrly.merge(ky_unique_stations, how='left', on='station')

ky_noaa_tmin = ky_noaa_data[ky_noaa_data.tmin.notnull()]
ky_noaa_tmin_agg_yrly = ky_noaa_tmin.groupby(by=['station', 'year'], as_index=False).agg({'tmin': 'mean'})
ky_noaa_tmin_agg_yrly = ky_noaa_tmin_agg_yrly.merge(ky_unique_stations, how='left', on='station')

ky_noaa_tavg = ky_noaa_data[ky_noaa_data.tavg.notnull()]
ky_noaa_tavg_agg_yrly = ky_noaa_tavg.groupby(by=['station', 'year'], as_index=False).agg({'tavg': 'mean'})
ky_noaa_tavg_agg_yrly = ky_noaa_tavg_agg_yrly.merge(ky_unique_stations, how='left', on='station')

ky_noaa_prcp = ky_noaa_data[ky_noaa_data.prcp.notnull()]
ky_noaa_prcp_agg_yrly = ky_noaa_prcp.groupby(by=['station', 'year'], as_index=False).agg({'prcp': 'mean'})
ky_noaa_prcp_agg_yrly = ky_noaa_prcp_agg_yrly.merge(ky_unique_stations, how='left', on='station')

#run through every row and fill in the missing data
for y in range(len(ky_yrly_weather_data)):
    #if tmax is missing
    if np.isnan(ky_yrly_weather_data.tmax.iloc[y]) == True:
        #find the geometry of the current county of interest
        curr_county_geom = ky_county_shp[ky_county_shp.GEOID20 == ky_yrly_weather_data.county.iloc[y]].geometry
        
        #subset the tmax data to those with valid data in this week
        curr_tmax_data = ky_noaa_tmax_agg_yrly[ky_noaa_tmax_agg_yrly.year == ky_yrly_weather_data.year.iloc[y]]
        
        #find the distances between the county and the stations
        tmax_dist_list = [curr_county_geom.distance(curr_tmax_data.location.iloc[i]).item() for i in range(len(curr_tmax_data))]
        tmax_dist_index = tmax_dist_list.index(min(tmax_dist_list))
        
        #set the tmax value to the value in the subsetted data for the location with the minumum distance
        ky_yrly_weather_data.tmax.iloc[y] = curr_tmax_data.tmax.iloc[tmax_dist_index]
        
    #if tmin is missing
    if np.isnan(ky_yrly_weather_data.tmin.iloc[y]) == True:
        #find the geometry of the current county of interest
        curr_county_geom = ky_county_shp[ky_county_shp.GEOID20 == ky_yrly_weather_data.county.iloc[y]].geometry
        
        #subset the tmin data to those with valid data in this week
        curr_tmin_data = ky_noaa_tmin_agg_yrly[ky_noaa_tmin_agg_yrly.year == ky_yrly_weather_data.year.iloc[y]]
        
        #find the distances between the county and the stations
        tmin_dist_list = [curr_county_geom.distance(curr_tmin_data.location.iloc[i]).item() for i in range(len(curr_tmin_data))]
        tmin_dist_index = tmin_dist_list.index(min(tmin_dist_list))
        
        #set the tmin value to the value in the subsetted data for the location with the minumum distance
        ky_yrly_weather_data.tmin.iloc[y] = curr_tmin_data.tmin.iloc[tmin_dist_index]
        
    #if tavg is missing
    if np.isnan(ky_yrly_weather_data.tavg.iloc[y]) == True:
        #find the geometry of the current county of interest
        curr_county_geom = ky_county_shp[ky_county_shp.GEOID20 == ky_yrly_weather_data.county.iloc[y]].geometry
        
        #subset the tavg data to those with valid data in this week
        curr_tavg_data = ky_noaa_tavg_agg_yrly[ky_noaa_tavg_agg_yrly.year == ky_yrly_weather_data.year.iloc[y]]
        
        #find the distances between the county and the stations
        tavg_dist_list = [curr_county_geom.distance(curr_tavg_data.location.iloc[i]).item() for i in range(len(curr_tavg_data))]
        tavg_dist_index = tavg_dist_list.index(min(tavg_dist_list))
        
        #set the tmin value to the value in the subsetted data for the location with the minumum distance
        ky_yrly_weather_data.tavg.iloc[y] = curr_tavg_data.tavg.iloc[tavg_dist_index]
        
    #if prcp is missing
    if np.isnan(ky_yrly_weather_data.prcp.iloc[y]) == True:
        #find the geometry of the current county of interest
        curr_county_geom = ky_county_shp[ky_county_shp.GEOID20 == ky_yrly_weather_data.county.iloc[y]].geometry
        
        #subset the prcp data to those with valid data in this week
        curr_prcp_data = ky_noaa_prcp_agg_yrly[ky_noaa_prcp_agg_yrly.year == ky_yrly_weather_data.year.iloc[y]]
        
        #find the distances between the county and the stations
        prcp_dist_list = [curr_county_geom.distance(curr_prcp_data.location.iloc[i]).item() for i in range(len(curr_prcp_data))]
        prcp_dist_index = prcp_dist_list.index(min(prcp_dist_list))
        
        #set the prcp value to the value in the subsetted data for the location with the minumum distance
        ky_yrly_weather_data.prcp.iloc[y] = curr_prcp_data.prcp.iloc[prcp_dist_index]
        
    if y%100 == 0:
        print(f'{y} iteration')

#save out the file -- the for-loop takes a VERY long time to run        
os.chdir(PATH_TO_NOAA_DATA)
ky_yrly_weather_data.to_pickle("noaa_yearly_panel_full_df.pkl")