import pandas as pd
import os
import math
import time
import sys
import numpy as np
import sqlalchemy

import pickle

os.chdir('/home/gsileo/repos/drinkingwater')

from config import PATH_TO_FSDWIS_DATA 

from db_utils import DatabaseConfiguration, PostgresConnectionManager

DB_CONFIG = DatabaseConfiguration(
    host = 'localhost',
    port = 5432,
    username = os.environ.get('POSTGRES_USERNAME'),
    password = os.environ.get('POSTGRES_PASSWORD'),
    database_name = 'water'
)

#######################################
### Select all the KY Water Systems ###
#######################################

with PostgresConnectionManager(DB_CONFIG) as conn:
    #select the KY water systems
    ky_ws_sql = "select * from fsdwis.water_system where primacy_agency_code = 'KY';"

    #read the query and pull into pandas dataframe
    ky_sys_fsdwis = pd.read_sql_query(ky_ws_sql, conn)
    
#save to dataframe
os.chdir(PATH_TO_FSDWIS_DATA)
ky_sys_fsdwis.to_pickle('ky_sys_fsdwis.pkl') 


##############################################
### Select all the KY Violations 2000-2019 ###
##############################################

with PostgresConnectionManager(DB_CONFIG) as conn:
    #select the KY water systems
    ky_viol_sql = "select * from fsdwis.ky_viol where compl_per_begin_date > '2000-01-01';"

    #read the query and pull into pandas dataframe
    ky_viol_fsdwis = pd.read_sql_query(ky_viol_sql, conn)
    
#save to dataframe
os.chdir(PATH_TO_FSDWIS_DATA)
ky_viol_fsdwis.to_pickle('ky_viol_fsdwis.pkl') 
