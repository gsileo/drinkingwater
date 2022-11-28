import pandas as pd
import os
import numpy as np
import json
import pickle
import re

os.chdir('/home/gsileo/repos/drinkingwater')

from config import PATH_TO_KY_DATA_COLLECTION, PATH_TO_FSDWIS_DATA, PATH_TO_WRIS_DATA, PATH_TO_KY_IUP_DATA

##################################################### Systems #####################################################

####################################################
# pull in the existing data from different sources #
####################################################
os.chdir(PATH_TO_KY_DATA_COLLECTION)

#from the 2015 Water Management Plan
watermanagement2015 = pd.read_excel('2015WaterManagementPlan.xls')

#from the Drinking Water Watch (state SDWIS)
statesdwis = pd.read_excel('DWWSystems.xls')

#pull in the Federal SDWIS systems
os.chdir(PATH_TO_FSDWIS_DATA)
fsdwis_systems = pd.read_pickle("ky_sys_fsdwis.pkl")

#change datatype to date
fsdwis_systems['pws_deactivation_date'] = pd.to_datetime(fsdwis_systems['pws_deactivation_date'])

#subset the fsdwis systems to only those which are grant eligible because those are the only ones that should be in the WRIS system as WRIS is primarily used to ask for funds
fsdwis_systems_eligible = fsdwis_systems[fsdwis_systems['is_grant_eligible_ind'] == 'Y']

#looked up each of these PWSIDs individually and confirmed not in WRIS, listed reason for ommitting -- this was before I knew funding eligibility was the real filter
sys_not_in_wris = pd.DataFrame({'pwsid': ['KY0533545', 'KY0183715', 'KY0720553', 'KY0843034', 'KY0642666', 'KY0390608',
                                      'KY0180309', 'KY0040259', 'KY0010702', 'KY0100004', 'KY1180999', 'KY1180962',
                                      'KY0483458', 'KY0070325', 'KY0050490', 'KY0370607', 'KY0090322', 'KY0593423',
                                      'KY0593743', 'KY0602174', 'KY0602175', 'KY0602280', 'KY0603622', 'KY1112214',
                                      'KY1190894', 'KY0603267', 'KY0603287', 'KY0603404', 'KY0602752', 'KY0603198',
                                      'KY0922899', 'KY0210420', 'KY0332189', 'KY0532233', 'KY0533195', 'KY0560609',
                                      'KY0560639', 'KY0562410', 'KY0722446', 'KY0730190', 'KY0720583', 'KY0570495',
                                      'KY0483389', 'KY0190597', 'KY0183648', 'KY1021014', 'KY0423489', 'KY0533471',
                                      'KY0940430', 'KY0110664', 'KY0082012', 'KY0082248', 'KY0082344', 'KY0082393',
                                      'KY0083124', 'KY0010082', 'KY0021006', 'KY0132267', 'KY0180103', 'KY0152891',
                                      'KY0470118', 'KY0672553', 'KY0673052', 'KY0673238', 'KY0672823', 'KY0792883',
                                      'KY0790946', 'KY0052054', 'KY0043371', 'KY0070640', 'KY0152087', 'KY0462024',
                                      'KY0650412', 'KY1033464', 'KY0183500', 'KY0573746', 'KY0253535', 'KY0980898',
                                      'KY0983725', 'KY0983726', 'KY0982196', 'KY0183007', 'KY0300450', 'KY0760464',
                                      'KY0190562', 'KY0500032', 'KY0080910', 'KY0080928', 'KY0203742','KY0980327',
                                      'KY0182822', 'KY0920631', 'KY0180308', 'KY0423170', 'KY0513013', 'KY0082242',
                                      'KY0182057', 'KY0360384', 'KY0483135', 'KY0822462', 'KY0082081', 'KY0072714',
                                      'KY1193703', 'KY0760441', 'KY0183334', 'KY0222251', 'KY0970922', 'KY0183519',
                                      'KY0833498', 'KY0762637', 'KY0072713', 'KY0082411', 'KY0670652', 'KY0202008',
                                      'KY1112211', 'KY0730522', 'KY1112364', 'KY0420971', 'KY0482399', 'KY0082592',
                                      'KY0462386', 'KY0722918', 'KY0182841', 'KY0132249', 'KY0512406', 'KY0082082',
                                      'KY0462387', 'KY0732457', 'KY0183132', 'KY0043507', 'KY0672292', 'KY0132994',
                                      'KY0720557', 'KY0792758', 'KY0183626', 'KY0512440', 'KY0080976', 'KY0182842',
                                      'KY0420540', 'KY0420541', 'KY0483472', 'KY0673641', 'KY0480621', 'KY0400586',
                                      'KY0410605', 'KY0940337', 'KY0930235', 'KY1100559', 'KY1050416', 'KY0920373',
                                      'KY0052235', 'KY0820643', 'KY0213072', 'KY1182706', 'KY0672690', 'KY0430245',
                                      'KY0482970', 'KY0810409', 'KY0312130', 'KY0722877', 'KY0083393', 'KY1103399',
                                      'KY0672290', 'KY0040015', 'KY0673056', 'KY0422022', 'KY0212769', 'KY0822466',
                                      'KY0972313', 'KY1112221', 'KY0953193', 'KY0983370', 'KY0972509', 'KY0602422',
                                      'KY1112365', 'KY1112212', 'KY1112486', 'KY1112210', 'KY1113460', 'KY1112213',
                                      'KY1112837', 'KY0082894', 'KY0183544'], 
                            'reason_omitted': 
                            ['No projects', 'No projects', 'Not in WRIS', 'Not in WRIS', 'Not in WRIS', 'Not in WRIS',
                            'Eliminated by project WX21035017, merged with KY0180306', 'No longer active', 'No longer active', 'No longer active', 'No longer active', 'No longer active',
                            'No longer active', 'No longer active', 'No longer active', 'No longer active', 'No longer active', 'No longer active',
                            'Not in WRIS', 'No longer active', 'Not in WRIS', 'Not in WRIS', 'Not in WRIS', 'Not in WRIS',
                            'Not in WRIS', 'No longer active', 'No longer active', 'Not in WRIS', 'Not in WRIS', 'Not in WRIS',
                            'Not in WRIS', 'Not in WRIS', 'Not in WRIS', 'No longer active', 'No longer active', 'Not in WRIS',
                            'No longer active', 'Not in WRIS', 'Not in WRIS', 'Not in WRIS', 'Not in WRIS', 'No longer active',
                            'Not in WRIS', 'Not in WRIS', 'No longer active', 'No longer active', 'Not in WRIS', 'Not in WRIS',
                            'No longer active', 'Not in WRIS', 'Not in WRIS', 'No longer active', 'Not in WRIS', 'No longer active',
                            'Not in WRIS', 'Not in WRIS', 'Not in WRIS', 'Not in WRIS', 'Not in WRIS', 'Not in WRIS',
                            'No longer active', 'No longer active', 'No longer active', 'No longer active', 'Not in WRIS', 'No longer active',
                            'Not in WRIS', 'No longer active', 'Not in WRIS', 'Not in WRIS', 'Not in WRIS', 'No longer active',
                            'No longer active', 'Not in WRIS', 'Not in WRIS', 'Not in WRIS', 'Not in WRIS', 'No longer active',
                            'Not in WRIS', 'Not in WRIS', 'Not in WRIS', 'No projects', 'No longer active', 'Not in WRIS',
                            'Not in WRIS', 'No longer active', 'No projects', 'No projects', 'No projects', 'Not in WRIS',
                            'No projects', 'No projects', 'No longer active as of 6/1/2020', 'No projects', 'No projects', 'No projects',
                            'No projects', 'Not in WRIS', 'No projects', 'No projects', 'No projects', 'No projects',
                            'No projects', 'Not in WRIS', 'No projects', 'No projects', 'Not in WRIS', 'No projects',
                            'No projects', 'No projects', 'No projects', 'No projects', 'Not in WRIS', 'No projects',
                            'No projects', 'No projects, no owner', 'No projects', 'No projects', 'No projects', 'No projects',
                            'No projects', 'No projects', 'No projects', 'Not in WRIS', 'No projects', 'No projects',
                            'No projects', 'No projects', 'No projects', 'No projects', 'No projects', 'Not in WRIS',
                            'Not in WRIS', 'No projects', 'No projects', 'No projects', 'No projects', 'No projects',
                            'Not in WRIS', 'Not in WRIS', 'Not in WRIS', 'Not in WRIS', 'Not in WRIS', 'Not in WRIS',
                            'Not in WRIS', 'Not in WRIS', 'Not in WRIS', 'Not in WRIS', 'Not in WRIS', 'Not in WRIS',
                            'Not in WRIS', 'Not in WRIS', 'Not in WRIS', 'Not in WRIS', 'Not in WRIS', 'Not in WRIS',
                            'Not in WRIS', 'Not in WRIS', 'Not in WRIS', 'Not in WRIS', 'Not in WRIS', 'Not in WRIS',
                            'Not in WRIS', 'Not in WRIS', 'Not in WRIS', 'Not in WRIS', 'Not in WRIS', 'Not in WRIS',
                            'Not in WRIS', 'Not in WRIS', 'Not in WRIS', 'Not in WRIS', 'Not in WRIS', 'No projects',
                            'No projects', 'No projects', 'No projects', 'No projects', 'No projects', 'No projects',
                            'No projects', 'No projects', 'No projects'] })

#others that I manually looked up -- most systems have GIS data, but some do not, the below systems do not have GIS data
sys_no_gis = ['KY0183106', 'KY0210419', 'KY0241001', 'KY0340965', 'KY0560258', 'KY0722980', 'KY0753505']

########################
# Merging the datasets #
########################

#merging the 2015 watermanagement plan with the state SDWIS
merge1 = statesdwis[['PWSID', 'Water System Name']].merge(watermanagement2015['PWSID'], how= 'outer', on = 'PWSID', indicator=True)
merge1.columns = ['pwsid', 'state_pws_name', 'source_ind']
merge1['source_ind'] = merge1.source_ind.apply(lambda x: 'state_sdwis' if x=='left_only' else ('wmp_state_sdwis' if x=='both' else 'wmp2015'))

#merging with the federal SDWIS
pws_master = merge1.merge(fsdwis_systems_eligible[['pwsid', 'pws_name']], how='outer', on = 'pwsid', indicator=True)
pws_master['source'] = pws_master.apply(lambda row: row.source_ind if row._merge=='left_only' else ('fed_sdwis' if row._merge== 'right_only' 
                                                    else ('both_sdwis' if row.source_ind == 'state_sdwis' else('wmp_fed_sdwis' if row.source_ind == 'wmp2015' else 'all') )), axis=1)
pws_master['pws_name_check'] = pws_master.apply(lambda row: True if row.state_pws_name == row.pws_name else False, axis=1)

#if the pwsid is not in the "not in WRIS" list, then it should be in WRIS
pws_master['WRIS'] = pws_master.pwsid.apply(lambda x: 0 if x in list(sys_not_in_wris.pwsid) else 1)

#renaming the columns to use for a master and filling in one missing PWSID
pws_master = pws_master[pws_master['WRIS'] == 1]
pws_master = pws_master[['pwsid', 'state_pws_name', 'source']]
pws_master.rename(columns={'state_pws_name': 'dow_permit_name'}, inplace=True)
pws_master.reset_index(inplace=True, drop=True)

# #save to file
# os.chdir(PATH_TO_WRIS_DATA)
# pws_master.to_pickle("systems_master.pkl")


################################################################################################################################################################################################
###############################
## Extra System Notes

#assume all from all sources to be in WRIS
# KY0730533
# KY0040259
# KY0050490
# KY0090322
# KY0940430
# KY0462024
# KY0650412

##### NOTES ######
# state sdwis only
# KY0533545 -- in system no projects 
# KY0183715 -- in system no projects 
# KY0720553 -- not in WRIS
# KY0843034 -- not in WRIS
# KY0642666 -- not in WRIS
# KY0390608 -- not in WRIS

# wmp2015 only
# KY0730533 -- active projects

# wmp_fed_sdwis
# KY0180309 -- was eliminated by project WX21035017 and merged with KY0180306
# KY0040259 -- active projects
# KY0010702 -- in system no projects
# KY0100004 -- in system no projects 
# KY1180999 -- in system no projects 
# KY1180962 -- in system no projects 
# KY0483458 -- in system no projects 
# KY0070325 -- in system no projects 
# KY0050490 -- completed projects
# KY0370607 -- in system no projects 
# KY0090322 -- completed projects

#from Fed SDWIS only -- deactivated in WRIS
# KY0593423 -- in system no projects 
# KY0593743 -- not in WRIS
# KY0602174 -- in system no projects
# KY0602175 -- not in WRIS 
# KY0602280 -- not in WRIS 
# KY0603622 -- not in WRIS 
# KY1112214 -- not in WRIS 
# KY1190894 -- not in WRIS 
# KY0603267 -- in system no projects
# KY0603287 -- in system no projects
# KY0603404 -- not in WRIS
# KY0602752 -- not in WRIS
# KY0603198 -- not in WRIS
# KY0922899 -- not in WRIS
# KY0210420 -- not in WRIS
# KY0332189 -- not in WRIS
# KY0532233 -- in system no projects
# KY0533195 -- in system no projects
# KY0560609 -- not in WRIS
# KY0560639 -- in system no projects
# KY0562410 -- not in WRIS
# KY0722446 -- not in WRIS
# KY0730190 -- not in WRIS
# KY0720583 -- not in WRIS
# KY0570495 -- in system no projects
# KY0483389 -- not in WRIS
# KY0190597 -- not in WRIS
# KY0183648 -- in system no projects
# KY1021014 -- in system no projects (bottled water plant.. interestingly)
# KY0423489 -- not in WRIS
# KY0533471 -- not in WRIS
# KY0940430 -- active projects
# KY0110664 -- not in WRIS
# KY0082012 -- not in WRIS
# KY0082248 -- in system no projects
# KY0082344 -- not in WRIS
# KY0082393 -- in system no projects
# KY0083124 -- not in WRIS
# KY0010082 -- not in WRIS
# KY0021006 -- not in WRIS
# KY0132267 -- not in WRIS
# KY0180103 -- not in WRIS
# KY0152891 -- not in WRIS
# KY0470118 -- in system no projects
# KY0672553 -- in system no projects
# KY0673052 -- in system no projects
# KY0673238 -- in system no projects
# KY0672823 -- not in WRIS
# KY0792883 -- in system no projects
# KY0790946 -- not in WRIS
# KY0052054 -- in system no projects
# KY0043371 -- not in WRIS
# KY0070640 -- not in WRIS
# KY0152087 -- not in WRIS
# KY0462024 -- completed projects
# KY0650412 -- completed projects
# KY1033464 -- not in WRIS
# KY0183500 -- not in WRIS
# KY0573746 -- not in WRIS
# KY0253535 -- not in WRIS
# KY0980898 -- in system no projects
# KY0983725 -- not in WRIS
# KY0983726 -- not in WRIS
# KY0982196 -- not in WRIS
