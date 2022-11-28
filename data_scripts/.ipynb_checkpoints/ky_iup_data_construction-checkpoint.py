### work in progress ###
import pandas as pd
import os
import math
import time
import random
import sys
import numpy as np
import PyPDF2 as ppdf
import re
import json
import pickle

from datetime import datetime, date

os.chdir('/home/gsileo/repos/drinkingwater')

from config import PATH_TO_KY_IUP_DATA
import warnings
warnings.filterwarnings('ignore')

###########################################
### Cleaning up the IUP Data since 2010 ###
###########################################
os.chdir(PATH_TO_KY_IUP_DATA)
iup_df = pd.read_excel("Consolidated_IUP_data.xls")

#replacing whitespace in the WRIS column, and any carriage returns with spaces in the applying entites and project titles
iup_df.WRIS = iup_df.WRIS.apply(lambda x: re.sub('\s', '', x) if isinstance(x, str) else x)
iup_df['Applying Entity'] = iup_df['Applying Entity'].apply(lambda x: re.sub('\r', ' ', x) if isinstance(x, str) else x)
iup_df['Project Title']= iup_df['Project Title'].apply(lambda x: re.sub('\r', ' ', x) if isinstance(x, str) else x)

#subset the data to just the columns of interest
iup_augmented = iup_df[['Fiscal Year', 'Project Ranking', 'Project Score', 'DWSRF',
       'WRIS', 'PWSID', 'Applying Entity', 'Project Title', 'Total Project Cost',
       'Total Requested Amount', 'Invited Amount', 'Status', 'MHI',
       'Principal Forgiveness', 'Population']]

iup_augmented.columns = ['fiscal_year', 'ranking', 'score', 'loan_num',
       'pnum', 'pwsid', 'entity', 'title', 'proj_cost', 'requested_amount', 'invited_amount', 'status', 'mhi',
       'iup_principal_forgiveness', 'population']

################################################
### Augmenting Status from DW Annual Reports ###
################################################

### FY2011 ###
#accepted
accepted_2011 = [1, 4, 7, 10, 11, 16, 17, 22, 25, 26, 29, 32] #assumed approved means accepted
for a in accepted_2011:
    curr_index = iup_augmented[((iup_augmented.fiscal_year == 2011) & (iup_augmented.ranking == a))].index.item()
    iup_augmented.status.iloc[curr_index] = 'Accepted'

#bypassed
bypassed_2011 = [24]
for b in bypassed_2011:
    curr_index = iup_augmented[((iup_augmented.fiscal_year == 2011) & (iup_augmented.ranking == b))].index.item()
    iup_augmented.status.iloc[curr_index] = 'Bypassed'    
    
#declined
declined_2011 = [5, 12, 23, 28, 35] 
for d in declined_2011:
    curr_index = iup_augmented[((iup_augmented.fiscal_year == 2011) & (iup_augmented.ranking == d))].index.item()
    iup_augmented.status.iloc[curr_index] = 'Declined'    

#expired
expired_2011 = [3, 8, 13, 14, 15, 18, 27, 30, 31, 33, 34, 36, 37, 38] #I assume "did not submit" falls into the declined category
for e in expired_2011:
    curr_index = iup_augmented[((iup_augmented.fiscal_year == 2011) & (iup_augmented.ranking == e))].index.item()
    iup_augmented.status.iloc[curr_index] = 'Expired'    
    
#not_invited
notinvited_2011 = [i for i in range(39,110)]
for n in notinvited_2011:
    curr_index = iup_augmented[((iup_augmented.fiscal_year == 2011) & (iup_augmented.ranking == n))].index.item()
    iup_augmented.status.iloc[curr_index] = 'Not Invited' 
    
#withdrawn
withdrawn_2011 = [2, 6, 9, 19, 20, 21]
for w in withdrawn_2011:
    curr_index = iup_augmented[((iup_augmented.fiscal_year == 2011) & (iup_augmented.ranking == w))].index.item()
    iup_augmented.status.iloc[curr_index] = 'Withdrawn' 
    

### FY2012 ###
#accepted
accepted_2012 = [1, 2, 3, 5, 6, 7, 10, 16, 18, 19, 20, 21, 22, 23, 24, 26, 29] #assumed approved means accepted
for a in accepted_2012:
    curr_index = iup_augmented[((iup_augmented.fiscal_year == 2012) & (iup_augmented.ranking == a))].index.item()
    iup_augmented.status.iloc[curr_index] = 'Accepted'
    
#declined
declined_2012 = [4, 8, 12, 17, 27, 30, 32, 33, 35, 36] 
for d in declined_2012:
    curr_index = iup_augmented[((iup_augmented.fiscal_year == 2012) & (iup_augmented.ranking == d))].index.item()
    iup_augmented.status.iloc[curr_index] = 'Declined'    
    
#not_invited
notinvited_2012 = [11, 15, 25, 28, 31] + [i for i in range(37,82)] #includes previously funded
for n in notinvited_2012:
    curr_index = iup_augmented[((iup_augmented.fiscal_year == 2012) & (iup_augmented.ranking == n))].index.item()
    iup_augmented.status.iloc[curr_index] = 'Not Invited' 
    
#withdrawn
withdrawn_2012 = [9, 13, 14, 34] #includes funded with other sources
for w in withdrawn_2012:
    curr_index = iup_augmented[((iup_augmented.fiscal_year == 2012) & (iup_augmented.ranking == w))].index.item()
    iup_augmented.status.iloc[curr_index] = 'Withdrawn' 

    
### FY2013 ###
#accepted
accepted_2013 = [2, 4, 7, 11, 12, 13, 20, 21, 22, 23, 26, 32, 34, 39] #assumed approved means accepted
for a in accepted_2013:
    curr_index = iup_augmented[((iup_augmented.fiscal_year == 2013) & (iup_augmented.ranking == a))].index.item()
    iup_augmented.status.iloc[curr_index] = 'Accepted'
    
#declined
declined_2013 = [1, 5, 6, 8, 10, 17, 18, 24, 25, 28, 29, 30, 31, 33, 36, 37, 38] 
for d in declined_2013:
    curr_index = iup_augmented[((iup_augmented.fiscal_year == 2013) & (iup_augmented.ranking == d))].index.item()
    iup_augmented.status.iloc[curr_index] = 'Declined'    
    
#not_invited
notinvited_2013 = [3, 9, 14, 15, 19, 27] + [i for i in range(40,63)] #includes previously funded
for n in notinvited_2013:
    curr_index = iup_augmented[((iup_augmented.fiscal_year == 2013) & (iup_augmented.ranking == n))].index.item()
    iup_augmented.status.iloc[curr_index] = 'Not Invited' 
    
#withdrawn
withdrawn_2013 = [16, 35] #includes funded with other sources
for w in withdrawn_2013:
    curr_index = iup_augmented[((iup_augmented.fiscal_year == 2013) & (iup_augmented.ranking == w))].index.item()
    iup_augmented.status.iloc[curr_index] = 'Withdrawn' 

    
### FY2014 ###
#accepted
accepted_2014 = [5, 8, 10, 13, 14, 15, 17, 44, 52] #assumed approved means accepted
for a in accepted_2014:
    curr_index = iup_augmented[((iup_augmented.fiscal_year == 2014) & (iup_augmented.ranking == a))].index.item()
    iup_augmented.status.iloc[curr_index] = 'Accepted'

#bypassed -- this one was ineligible
bypassed_2014 = [18]
for b in bypassed_2014:
    curr_index = iup_augmented[((iup_augmented.fiscal_year == 2014) & (iup_augmented.ranking == b))].index.item()
    iup_augmented.status.iloc[curr_index] = 'Bypassed'    
    
#declined
declined_2014 = [1, 2, 6, 7, 16, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 43, 45, 46, 47, 48, 49, 50, 51, 53] 
for d in declined_2014:
    curr_index = iup_augmented[((iup_augmented.fiscal_year == 2014) & (iup_augmented.ranking == d))].index.item()
    iup_augmented.status.iloc[curr_index] = 'Declined'    
    
#not_invited
notinvited_2014 = [3, 4, 9, 11, 12, 42] #includes previously funded
for n in notinvited_2014:
    curr_index = iup_augmented[((iup_augmented.fiscal_year == 2014) & (iup_augmented.ranking == n))].index.item()
    iup_augmented.status.iloc[curr_index] = 'Not Invited' 
    

### FY2015 -- MISSING ###


### FY2016 ###
#accepted
accepted_2016 = [1, 2, 3, 6, 8, 10, 11, 21, 27, 32, 33, 36, 38, 41, 42, 45, 47, 49]
for a in accepted_2016:
    curr_index = iup_augmented[((iup_augmented.fiscal_year == 2016) & (iup_augmented.ranking == a))].index.item()
    iup_augmented.status.iloc[curr_index] = 'Accepted'
    
#declined
declined_2016 = [4, 5, 7, 9, 12, 13, 14, 15, 16, 17, 18, 19, 20, 22, 23, 24, 25, 26, 28, 29, 30, 31, 34, 35, 37, 39, 40, 43, 44, 46, 48] 
for d in declined_2016:
    curr_index = iup_augmented[((iup_augmented.fiscal_year == 2016) & (iup_augmented.ranking == d))].index.item()
    iup_augmented.status.iloc[curr_index] = 'Declined'    
    
#not_invited
notinvited_2016 = [i for i in range(50,72)] #note, ranking 59-71 not in DWSRF Annual Report
for n in notinvited_2016:
    curr_index = iup_augmented[((iup_augmented.fiscal_year == 2016) & (iup_augmented.ranking == n))].index.item()
    iup_augmented.status.iloc[curr_index] = 'Not Invited' 
    
    
    
### FY2017 ###
#accepted
accepted_2017 = [1, 3, 5, 6, 7, 12, 14]
for a in accepted_2017:
    curr_index = iup_augmented[((iup_augmented.fiscal_year == 2017) & (iup_augmented.ranking == a))].index.item()
    iup_augmented.status.iloc[curr_index] = 'Accepted'

#bypassed
bypassed_2017 = [2, 4, 16]
for b in bypassed_2017:
    curr_index = iup_augmented[((iup_augmented.fiscal_year == 2017) & (iup_augmented.ranking == b))].index.item()
    iup_augmented.status.iloc[curr_index] = 'Bypassed'    
    
#declined
declined_2017 = [8, 9, 10, 11, 13, 15, 17, 18, 19, 20, 21, 22, 23, 24]
for d in declined_2017:
    curr_index = iup_augmented[((iup_augmented.fiscal_year == 2017) & (iup_augmented.ranking == d))].index.item()
    iup_augmented.status.iloc[curr_index] = 'Declined'    
    
#not_invited
notinvited_2017 = [i for i in range(25,70)] #note, ranking 59-69 not in DWSRF Annual Report
for n in notinvited_2017:
    curr_index = iup_augmented[((iup_augmented.fiscal_year == 2017) & (iup_augmented.ranking == n))].index.item()
    iup_augmented.status.iloc[curr_index] = 'Not Invited'    

    
### FY2018 ###
#accepted
accepted_2018 = [2, 3, 4, 5, 6, 16, 17, 18, 19]
for a in accepted_2018:
    curr_index = iup_augmented[((iup_augmented.fiscal_year == 2018) & (iup_augmented.ranking == a))].index.item()
    iup_augmented.status.iloc[curr_index] = 'Accepted'

#bypassed
bypassed_2018 = ['*', 1, 10, 11]
for b in bypassed_2018:
    curr_index = iup_augmented[((iup_augmented.fiscal_year == 2018) & (iup_augmented.ranking == b))].index.item()
    iup_augmented.status.iloc[curr_index] = 'Bypassed'    
    
#declined
declined_2018 = [7, 8, 9, 14, 15, 20, 21]
for d in declined_2018:
    curr_index = iup_augmented[((iup_augmented.fiscal_year == 2018) & (iup_augmented.ranking == d))].index.item()
    iup_augmented.status.iloc[curr_index] = 'Declined'    
    
#expired
expired_2018 = [12]
for e in expired_2018:
    curr_index = iup_augmented[((iup_augmented.fiscal_year == 2018) & (iup_augmented.ranking == e))].index.item()
    iup_augmented.status.iloc[curr_index] = 'Expired'    

#withdrawn
withdrawn_2018 = [13]
for w in withdrawn_2018:
    curr_index = iup_augmented[((iup_augmented.fiscal_year == 2018) & (iup_augmented.ranking == w))].index.item()
    iup_augmented.status.iloc[curr_index] = 'Withdrawn'    
    
#not_invited
notinvited_2018 = [i for i in range(22,62)] #note, ranking 58-61 not in DWSRF Annual Report
for n in notinvited_2018:
    curr_index = iup_augmented[((iup_augmented.fiscal_year == 2018) & (iup_augmented.ranking == n))].index.item()
    iup_augmented.status.iloc[curr_index] = 'Not Invited'    
    
    
### FY2019 ###
#accepted
accepted_2019 = [2, 4, 5, 9, 18, 19, 20, 35, 41, 42, 44, 59]
for a in accepted_2019:
    curr_index = iup_augmented[((iup_augmented.fiscal_year == 2019) & (iup_augmented.ranking == a))].index.item()
    iup_augmented.status.iloc[curr_index] = 'Accepted'

#bypassed
bypassed_2019 = [1, 6, 10, 13, 22, 27, 31, 36, 37, 38, 43, 45, 47, 48, 50, 53, 55, 58, 63, 67, 71, 74]
for b in bypassed_2019:
    curr_index = iup_augmented[((iup_augmented.fiscal_year == 2019) & (iup_augmented.ranking == b))].index.item()
    iup_augmented.status.iloc[curr_index] = 'Bypassed'    
    
#declined
declined_2019 = [7, 8, 11, 14, 16, 17, 23, 26, 30, 33, 39, 40, 52, 60, 62, 69, 70, 73]
for d in declined_2019:
    curr_index = iup_augmented[((iup_augmented.fiscal_year == 2019) & (iup_augmented.ranking == d))].index.item()
    iup_augmented.status.iloc[curr_index] = 'Declined'    

#expired
expired_2019 = [49, 65, 66, 68, 72, 75]
for e in expired_2019:
    curr_index = iup_augmented[((iup_augmented.fiscal_year == 2019) & (iup_augmented.ranking == e))].index.item()
    iup_augmented.status.iloc[curr_index] = 'Expired'    

#withdrawn
withdrawn_2019 = [3, 12, 15, 21, 24, 25, 28, 29, 32, 34, 46, 51, 54, 56, 57, 61, 64]
for w in withdrawn_2019:
    curr_index = iup_augmented[((iup_augmented.fiscal_year == 2019) & (iup_augmented.ranking == w))].index.item()
    iup_augmented.status.iloc[curr_index] = 'Withdrawn' 
    

### FY2020 ###
#handling the "0" ranks -- all but moorehead and green-taylor were bypassed, as they are in the df
curr_index = iup_augmented[((iup_augmented.fiscal_year == 2020) & (iup_augmented.loan_num == "F19-002"))].index.item()
iup_augmented.status.iloc[curr_index] = 'Accepted'

curr_index = iup_augmented[((iup_augmented.fiscal_year == 2020) & (iup_augmented.loan_num == "F19-018"))].index.item()
iup_augmented.status.iloc[curr_index] = 'Withdrawn'


#accepted
accepted_2020 = [2, 4, 6, 9, 13, 14, 16, 17, 19, 21, 26, 34, 39, 41, 44, 47, 48]
for a in accepted_2020:
    curr_index = iup_augmented[((iup_augmented.fiscal_year == 2020) & (iup_augmented.ranking == a))].index.item()
    iup_augmented.status.iloc[curr_index] = 'Accepted'

#bypassed
bypassed_2020 = [3, 25, 28, 42, 43, 45, 46]
for b in bypassed_2020:
    curr_index = iup_augmented[((iup_augmented.fiscal_year == 2020) & (iup_augmented.ranking == b))].index.item()
    iup_augmented.status.iloc[curr_index] = 'Bypassed'    
    
#declined
declined_2020 = [5, 11, 12, 15, 23, 24, 27, 30, 35, 36, 37, 40, 49]
for d in declined_2020:
    curr_index = iup_augmented[((iup_augmented.fiscal_year == 2020) & (iup_augmented.ranking == d))].index.item()
    iup_augmented.status.iloc[curr_index] = 'Declined'    

#withdrawn
withdrawn_2020 = [1, 7, 8, 10, 18, 20, 22, 29, 31, 32, 33, 38]
for w in withdrawn_2020:
    curr_index = iup_augmented[((iup_augmented.fiscal_year == 2020) & (iup_augmented.ranking == w))].index.item()
    iup_augmented.status.iloc[curr_index] = 'Withdrawn'      
    
#not_invited
notinvited_2020 = [i for i in range(50,72)]
for n in notinvited_2020:
    curr_index = iup_augmented[((iup_augmented.fiscal_year == 2020) & (iup_augmented.ranking == n))].index.item()
    iup_augmented.status.iloc[curr_index] = 'Not Invited'    


# os.chdir(PATH_TO_KY_IUP_DATA)
# pickle.dump(iup_augmented, open("iup_data_clean_df.pkl", "wb"))