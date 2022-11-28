import pandas as pd
import os
import numpy as np
import json
import pickle
import re

os.chdir('/home/gsileo/repos/drinkingwater')

from config import PATH_TO_WRIS_DATA, PATH_TO_KY_IUP_DATA

##################################################### Projects #####################################################

#from the saved system files
os.chdir(f'{PATH_TO_WRIS_DATA}/systems')
sys_files = os.listdir(f'{PATH_TO_WRIS_DATA}/systems')
sys_jsons = [i for i in sys_files if 'KY' in i]

#initialize a projects dataframe to append to
projects_df = pd.DataFrame()

for file in sys_jsons:
    #open the json and extract to dictionary
    with open(file) as f:
        curr_pwsid = json.load(f)
        
        #append the projects to the projects dataframe
        projects_df = projects_df.append(curr_pwsid['Projects'])
        
#rename the columns
projects_df.columns = ['pnum', 'applicant', 'project_status', 'funding_status', 'schedule', 'project_title', 'agreed_order', 'profile_modified', 'gis_modified']

#extract the project numbers and indicate the source of the data, also drop duplicates
projects_master = pd.DataFrame({'pnum': projects_df['pnum'].values, 'source': 'system_listings'})
projects_master.drop_duplicates(inplace=True, ignore_index=True)

#from the IUP data -- which I should clean up at some point
os.chdir(PATH_TO_KY_IUP_DATA)
iup_df = pickle.load(open("iup_data_clean_df.pkl", "rb"))

iup_projects = []

for i in range(len(iup_df)):
    #check that it's a string, and is in project format
    if isinstance(iup_df.pnum.iloc[i], str):
        #now that it is a string, check if it follows project format
        if ('WX' in iup_df.pnum.iloc[i]):
            #append the projects in the cell
            iup_projects.extend(re.split(',',iup_df.pnum.iloc[i]))

#extras from the IUP           
proj_not_in_wris = pd.DataFrame({'pnum': ['WX21133044', 'WX21217011', 'WX21167018', 'WX21167019', 'WX21167020', 'WX21127010',
                                         'WX21025011', 'WX21183004', 'WX21083038', 'WX21001020', 'WX21025099', 'WX21149046',
                                         'WX21049004', 'WX21075013', 'WX21001020', 'WX21149017', 'WX21177015', 'WX21111856',
                                         'WX21113031', 'WX21035029', 'WX21125454', 'WX21111857', 'WX21111858', 'WX21105008',
                                         'WX21041705', 'WX21113031', 'WX21089003', 'WX21025006', 'WX21159021', 'WX21113031'],
                      'reason_omitted': ['Not in WRIS', 'Not in WRIS', 'Not in WRIS', 'Not in WRIS', 'Not in WRIS', 'Not in WRIS',
                                         'Not in WRIS', 'Not in WRIS', 'Not in WRIS', 'Not in WRIS', 'Not in WRIS', 'Not in WRIS',
                                         'Not in WRIS', 'Not in WRIS', 'Not in WRIS', 'Not in WRIS', 'Not in WRIS', 'Not in WRIS',
                                         'Not in WRIS', 'Not in WRIS', 'Not in WRIS', 'Not in WRIS', 'Not in WRIS', 'Not in WRIS',
                                         'Not in WRIS', 'Not in WRIS', 'Not in WRIS', 'Not in WRIS', 'Not in WRIS', 'Not in WRIS']})

#drop duplicates from iup listing
iup_projects_clean = list(set(iup_projects))

#find the iup projects that are not in wris, and not already found in the system files
new_iup_projects = [i for i in iup_projects_clean if ((i not in list(proj_not_in_wris.pnum)) and (i not in list(projects_master.pnum)))]

#append to the projects_master
projects_master = projects_master.append([{'pnum': new_iup_projects[i], 'source': 'ky_iups'} for i in range(len(new_iup_projects))], ignore_index=True)

#lastly, those projects which have pwsids no longer in wris
proj_sys_not_active = ['WX21099033', 'WX21059013', 'WX21059039', 'WX21017005', 'WX21007001', 'WX21099025', 'WX21187320', 'WX21093019', 'WX21187207',
                       'WX21059004', 'WX21059059', 'WX21187216', 'WX21187002', 'WX21093031', 'WX21059053', 'WX21059038', 'WX21059003', 'WX21059054']

#append to the projects_master
projects_master = projects_master.append([{'pnum': proj_sys_not_active[i], 'source': 'deactivated_sys'} for i in range(len(proj_sys_not_active))], ignore_index=True)

# #save to file
# os.chdir(PATH_TO_WRIS_DATA)
# with open("projects_master.pkl", 'wb') as f: 
#     pickle.dump(projects_master, f)
