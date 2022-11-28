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

os.chdir('/home/gsileo/repos/drinkingwater')
from config import PATH_TO_KY_DATA_COLLECTION, PATH_TO_WRIS_DATA

import warnings
warnings.filterwarnings('ignore')

###########################################################################
# Pull in the project data cleaning functions and the data initialization #
###########################################################################

#change the "save_manual_data" flag to true if you want to overwrite those
os.chdir(f'{PATH_TO_KY_DATA_COLLECTION}/functions')
from proj_data_initialization import *

os.chdir(f'{PATH_TO_KY_DATA_COLLECTION}/functions')
import proj_data_functions as pdf

#########################################
# Pull in some functions for conversion #
#########################################

def convert_int(numeric_string):
    try:
        return int(numeric_string)
    except:
        return np.nan
    
def convert_money(monetary_string):
    try:
        return int(re.sub('\$|,', '', monetary_string))
    except:
        return np.nan

########################################################
# Pull in the data master -- i.e. list of all projects #
########################################################

os.chdir(PATH_TO_WRIS_DATA)
projects_master = pd.read_pickle("projects_master.pkl")

################################################
# Creating json files for the bulk of the data #
################################################

#create lists for the specific issues
eng_problems = []
contact_problems = []
budget_problems = []
overall_problems = []
demo_problems = []
date_problems = []
new_cust_problems = []

#create a list of unique, non-manual projects
unique_projects = [i for i in projects_master.pnum if i not in manual_projects]

for p, proj in enumerate(unique_projects):    
    
    print(f'processing project {proj}, which is {p} out of {len(unique_projects)}')
    
    #open and read the pdf file
    os.chdir(f'{PATH_TO_KY_DATA_COLLECTION}/WRIS/project_reports')
    ky_proj = open(f'{proj}_ProfileReport.pdf', 'rb')
    
    proj_clean_text = pdf.clean_proj_text(ky_proj, proj)
    
    ###########################################
    ### Use regex to extract below elements ###
    ###########################################
    p_list = []
    for i in range(len(project_keys)):   
        #set endpoints
        left = project_endpoints[i][0]
        right = project_endpoints[i][1]
        
        #extract the only date modified data
        if project_keys[i] == 'Date Legal Applicant Data Modified':
            try:
                date_mod = str.strip(re.search('(?<=Date Last Modified:)\s?\d{2}\.\d{2}\.\d{4}', proj_clean_text).group())
                p_list.append(date_mod)
            except:
                p_list.append('')
                date_problems.append(proj)

        # project engineer info
        elif project_keys[i] == 'Project Engineer Information':
            #extract the engineering info, then parse it into the components, where possible
            try: 
                full_prj_eng = re.search(f'(?:{left}).*?(?={right})', proj_clean_text).group()
                engineer_dict = pdf.engineering_data(full_prj_eng)

            except:
                #is there an engineering section in the document at all?
                try:
                    engineering_info = re.search('Project Engineer \(PE\) Information:', proj_clean_text).group()
                    #append to errors list
                    eng_problems.append(proj)
                    engineer_dict = {}
                    
                #if not, just append an empty dictionary and don't raise flag
                except:
                    engineer_dict = {}
                
            p_list.append(engineer_dict)

        # project admin/ applicant contact info
        elif project_keys[i] == 'Project Contact Information':
            #extract the contact info for the project
            try: 
                prj_contact = re.search(f'(?<={left})((?!{left}).)*?(?={right})', proj_clean_text).group()
                contact_dict = pdf.admin_data(prj_contact)

            except:
                print('errors with project contact info')
                contact_dict = {}
                #append to errors list
                contact_problems.append(proj)

            p_list.append(contact_dict)

        elif project_keys[i] == 'Estimated Budget':
            #extract the contact info for the project
            try: 
                budget_string = re.search(f'(?<={left})((?!{left}).)*?(?={right})', proj_clean_text).group()
                budget_dict = pdf.budget_data(budget_string)

            except:
                print('errors with budget data')
                budget_dict = {}
                #append to errors list
                budget_problems.append(proj)

            p_list.append(budget_dict)

        elif project_keys[i] == 'Beneficiary Systems':
            #extract the beneficiary systems
            beneficiaries = str.strip(re.search(f'(?<={left})((?!{right}).)*?(?={right})', proj_clean_text).group())

            beneficiary_list = []

            #split beneficiaries on pwsid and append to list
            split_beneficiaries = re.split('(KY\d{7})', beneficiaries)
            for i in range(1,len(split_beneficiaries),2):
                beneficiary_list.append({'PWSID': str.strip(split_beneficiaries[i]), 'System Name': str.strip(split_beneficiaries[i+1])})

            p_list.append(beneficiary_list)

        elif (project_keys[i] == 'Counties Impacted by Project Area') | (project_keys[i] == 'Counties Served by Impacted Systems'):
            #turn counties into a list
            impacted_counties = str.strip(re.search(f'(?<={left})((?!{right}).)*?(?={right})', proj_clean_text).group())
            impacted_counties_list = re.split(' ', impacted_counties)

            p_list.append(impacted_counties_list)

        elif (project_keys[i] == 'Project Legislative Districts') | (project_keys[i] == 'System Legislative Districts'):
            try:
                leg_districts = str.strip(re.search(f'(?<={left})((?!{left}).)*?(?={right})', proj_clean_text).group())
                leg_districts_list = re.findall('(?:(?:House|Senate|Congressional).*?(?=House|Senate|Congressional|$))', leg_districts)
                leg_districts_list = [str.strip(i) for i in leg_districts_list]

                leg_dists_list = [{'District': re.split('(?<=\d)\s(?=[A-Z])', lgd)[0], 'Legislator': re.split('(?<=\d)\s(?=[A-Z])', lgd)[1]} for lgd in leg_districts_list]

                p_list.append(leg_dists_list)
            except:
                p_list.append([])
                #append to errors list
                demo_problems.append(proj)

        elif project_keys[i] == 'Groundwater Sensitivity Zones':
            try:
                groundwater_zones = str.strip(re.search(f'(?<={left})((?!{left}).)*?(?={right})', proj_clean_text).group())
                gz_list = re.findall('(?:\d{10}).*?(?=\d{10}|$)', groundwater_zones)
                groundwater_zones_list = [{'HUC Code': re.search('\d{10}', gz).group(), 'Watershed Name': str.strip(re.search('(?<=\d{10}).*', gz).group())} for gz in gz_list]
                p_list.append(groundwater_zones_list)
            except:
                p_list.append([])
                #append to errors list
                demo_problems.append(proj)
                
        #to be cleaned up eventually
        elif project_keys[i] == 'Project Notes':
            try:
                notes_info = re.search(f'(?<={left}).*?(?={right})', proj_clean_text).group()            
                p_list.append(str.strip(notes_info))
            except:
                p_list.append('')

        else:
            try:
                p_list.append(str.strip(re.search(f'(?<={left})((?!{left}).)*?(?={right})', proj_clean_text).group()))
            except:
                p_list.append('')
                #append to errors list
                overall_problems.append((project_keys[i], proj))
                
    #construct the dictionary with the relevant keys
    curr_dict = dict(zip(project_keys, p_list))
    
    ### Later additions ###
    #add in the AWMPC
    curr_dict['AWMPC'] = str.strip(re.search('(?<=Submitted By:)((?!Funding Status:).)*?(?=Funding Status:)', proj_clean_text).group())
    
    #add in the Funding Source Notes -- check to see if there's no funding data, and therefore wrong capturing groups used
    funding_notes_info = str.strip(re.findall('(?:Total Committed(?:\s\$[0-9\,]*)?)((?:(?!Total Committed).)*?)(?:Funding Source Notes)', proj_clean_text)[0])
    
    if 'This project will be requesting SRF funding for fiscal year 2023' in funding_notes_info:
        funding_notes_info = str.strip(re.search('(?<=Applicable Date)(.*?)(?=Funding Source Notes)', proj_clean_text).group())
        
    curr_dict['Funding Source Notes'] = funding_notes_info

    ######################################
    ### Extract tables from html files ###
    ######################################
    
    #now extract the tables and add to the dictionary        
    curr_tables = pd.read_html(f'{PATH_TO_KY_DATA_COLLECTION}/WRIS/project_reports/project_reports_html/{proj}_ProfileReport.html')    

    #extract the tables needed
    for t,tb in enumerate(curr_tables):
        curr_tb = tb.iloc[1:,:]
        curr_tb.columns = tb.iloc[0,:]

        #funding sources
        if 'Funding Source' in curr_tb.columns:
            #extract the table
            full_funding_table = curr_tb
            funding_source_table = full_funding_table.iloc[:-1,:]
            funding_source_list = [funding_source_table.iloc[i,:].to_dict() for i in range(len(funding_source_table))]

            #check if funding needs to be handled manually because the table gets split over two pages
            if (len(funding_source_table) != 0 and 'Total Committed' not in full_funding_table.iloc[-1,0]):
                #pull in the next half of the table from the misread data
                funding_table2 = curr_tables[t+1]
                funding_table2 = funding_table2.iloc[:,0:6] #just in case there's a random extra column in there
                funding_table2.columns = funding_source_table.columns
                full_funding_table = full_funding_table.append(funding_table2)

                funding_source_table = full_funding_table.iloc[:-1,:]
                funding_source_list = [funding_source_table.iloc[i,:].to_dict() for i in range(len(funding_source_table))]

            #append to the dictionary for the project
            curr_dict['Funding Sources'] = funding_source_list
            if funding_source_list:
                curr_dict['Total Committed Funding'] = curr_tb.iloc[-1,3]
            else:
                curr_dict['Total Committed Funding'] = ''

        #Economic Impacts
        elif 'Economic Impacts' in curr_tb.columns:
            #check to make sure it's long enough, if not, fetch data from next table
            if len(curr_tb) == 10:
                econ_imp_table = curr_tb.iloc[4:,:-1]

            else:
                econ_imp_table1 = curr_tables[t].append(curr_tables[t+1])
                #set relevant columns and subset to just the desired data
                rel_cols = ['Population:', 'Households:', 'MHI:', 'MHI MOE', 'MOE as Pct:', '**NSRL:']
                econ_imp_table = econ_imp_table1[econ_imp_table1[0].isin(rel_cols)].iloc[:,:4]

            #construct a dictionary
            econ_imp_dict = {}
            econ_imp_dict['Project Area'] = dict(zip(['Population', 'Households', 'MHI', 'MHI MOE', 'MOE as %', '**NSRL'], econ_imp_table.iloc[:,1].values))
            econ_imp_dict['Included Systems'] = dict(zip(['Population', 'Households', 'MHI', 'MHI MOE', 'MOE as %', '**NSRL'], econ_imp_table.iloc[:,2].values))
            econ_imp_dict['Included Utilities'] = dict(zip(['Population', 'Households', 'MHI', 'MHI MOE', 'MOE as %', '**NSRL'], econ_imp_table.iloc[:,3].values))
            econ_imp_dict['Notes'] = 'Population and household counts are based on 2010 census block values from the SF1 (100%) dataset. MHI Source is from the American Community Survey 2015-2019 5Yr Estimates (Table B19013) *(for the primary system operated by the above listed beneficiary utilities). MHI MOE = Med HH Income Margin of Error. ** NSRL (Non-Standard Rate Levels): 0 = Income above Kentucky MHI (KMHI). 1 = Income between 80% KMHI and KMHI. 2 = Income less than or equal to 80% KMHI. - KMHI = $50,589 - 80% KHMI = $40,471'

            #append to the dictionary for the project
            curr_dict['Demographic Impacts'] = econ_imp_dict
            
        #new customers 
        elif 'New Customers' in curr_tb.columns:
            try:
                #check to make sure it's long enough, if not, fetch data from next table
                if len(curr_tb) == 4:
                    new_cust_table = curr_tb

                else:
                    new_cust_table1 = curr_tables[t].append(curr_tables[t+1])
                    #set relevant columns and subset to just the desired data
                    rel_cols_cust = ['New Residential Customers:', 'New Commercial Customers:', 'New Institutional Customers:', 'New Industrial Customers:']
                    new_cust_table = new_cust_table1[new_cust_table1[0].isin(rel_cols_cust)]

                #append values to dict
                curr_dict['New Residential Customers'] = new_cust_table.iloc[0,1]
                curr_dict['New Commercial Customers'] = new_cust_table.iloc[1,1]
                curr_dict['New Institutional Customers'] = new_cust_table.iloc[2,1]
                curr_dict['New Industrial Customers'] = new_cust_table.iloc[3,1]
            except:
                curr_dict['New Residential Customers'] = ''
                curr_dict['New Commercial Customers'] = ''
                curr_dict['New Institutional Customers'] = ''
                curr_dict['New Industrial Customers'] = ''
                new_cust_problems.append(proj)

        #new or improved service
        elif 'New or Improved Service' in curr_tb.columns:
            service_dict = {}
            service_dict['Survey Based'] = dict(zip(curr_tb.iloc[1:4,0].values, curr_tb.iloc[1:4,1].values))
            service_dict['Census Overlay'] = dict(zip(curr_tb.iloc[1:4,0].values, curr_tb.iloc[1:4,2].values))
            service_dict['Cost Per Household'] = curr_tb.iloc[4,1]
            service_dict['Notes'] = '* GIS Census block overlay figures are estimates of population and households potentially served by systems and projects based on a proximity analysis of relevant service lines to census block boundaries. ** Cost per household is based on surveyed household counts, not GIS overlay values.'

            #append to the dictionary for the project
            curr_dict['New or Improved Service'] = service_dict

        #Project Inventory
        elif 'Mapped Point Features' in curr_tb.columns:
            #extract the dataframe info from the read in html
            mapped_point_table = curr_tb.iloc[1:,:]
            mapped_point_table.columns = curr_tb.iloc[0,:].values
            mapped_point_table.reset_index(inplace=True, drop=True)

            #check to see if the next table is probably extra data
            try:
                mapped_point_test = re.search('KY\d{7}', curr_tables[t+1].iloc[0,0]).group()
                mapped_point_table2 = curr_tables[t+1]
                mapped_point_table2.columns = mapped_point_table.columns
                mapped_point_table = mapped_point_table.append(mapped_point_table2)

                mapped_point_tables_list = [mapped_point_table.iloc[i,:].to_dict() for i in range(len(mapped_point_table))]
            except:
                mapped_point_tables_list = [mapped_point_table.iloc[i,:].to_dict() for i in range(len(mapped_point_table))]

            #append to the dictionary for the project
            curr_dict['Mapped Point Features'] = mapped_point_tables_list

        elif 'Mapped Line Features' in curr_tb.columns:
            #extract the dataframe info from the read in html
            mapped_line_table = curr_tb.iloc[1:-1,:]
            mapped_line_table.columns = curr_tb.iloc[0,:].values
            mapped_line_table.reset_index(inplace=True, drop=True)

            #check to see if the next table is probably extra data
            try:
                mapped_line_test = re.search('KY\d{7}', curr_tables[t+1].iloc[0,0]).group()
                mapped_line_table2 = curr_tables[t+1]
                mapped_line_table2.columns = mapped_line_table.columns
                mapped_line_table = mapped_line_table.append(mapped_line_table2)

                mapped_line_tables_list = [mapped_line_table.iloc[i,:].to_dict() for i in range(len(mapped_line_table))] 
            except:
                mapped_line_tables_list = [mapped_line_table.iloc[i,:].to_dict() for i in range(len(mapped_line_table))] 

            #append to the dictionary for the project
            curr_dict['Mapped Line Features'] = mapped_line_tables_list   
            
    ### For Some Reason the NSRL for the Projects Doesn't Get Extracted, Updating ###
    # 0 = Greater than or equal to Kentucky MHI (KMHI).
    # 1 = Between 80% KMHI and KMHI (exclusive).
    # 2 = Less than or equal to 80% KMHI.
    # - KMHI = $50,589
    # - 80% KMHI = $40,471

    if ((isinstance(curr_dict['Demographic Impacts']['Project Area']['Population'], str)) and (np.isnan(convert_int(curr_dict['Demographic Impacts']['Project Area']['**NSRL'])))):
        
        # 0 = Greater than or equal to Kentucky MHI (KMHI)
        if convert_money(curr_dict['Demographic Impacts']['Project Area']['MHI']) >= 50589:
            curr_dict['Demographic Impacts']['Project Area']['**NSRL'] = '0'
        
        # 1 = Between 80% KMHI and KMHI (exclusive)
        elif ((convert_money(curr_dict['Demographic Impacts']['Project Area']['MHI']) < 50589) and (convert_money(curr_dict['Demographic Impacts']['Project Area']['MHI']) > 40471)):
            curr_dict['Demographic Impacts']['Project Area']['**NSRL'] = '1'
            
        # 2 = Less than or equal to 80% KMHI    
        elif (convert_money(curr_dict['Demographic Impacts']['Project Area']['MHI']) <= 40471):
            curr_dict['Demographic Impacts']['Project Area']['**NSRL'] = '2'  

    #save to file
    os.chdir(f'{PATH_TO_WRIS_DATA}/projects')
    with open(f'{proj}.json', 'w') as fp:
        json.dump(curr_dict, fp)
               
#######################################################################
### Adding in the information on DW Impacts for non-manual projects ###
#######################################################################

#pulling in the project impacts info
os.chdir(f'{PATH_TO_WRIS_DATA}/project_checklist')
checklist_df = pd.read_csv('dw_impacts_checklist.csv')

checklist_df['pnum'] = checklist_df['file_name'].apply(lambda x: x[0:10])
checklist_df = checklist_df[['pnum', 'public_health', 'achieve_compliance', 'future_requirements', 'not_compliance_related', 'court_order', 'primary_system', 'court_ord_no']]

#pulling in each of the project jsons and updating
os.chdir(f'{PATH_TO_WRIS_DATA}/projects')
proj_files = os.listdir(f'{PATH_TO_WRIS_DATA}/projects')
proj_jsons = [i for i in proj_files if 'WX' in i]

#run through each of the projects and add the relevant checklist info
for p_file in proj_jsons:
    #extract the current project number
    curr_proj = p_file[0:10]
    
    #pull in the json and update the files
    with open(p_file) as f:
        curr_proj_dict = json.load(f)

        #subset the checklist info to the current pwsid
        checklist_curr = checklist_df[checklist_df.pnum == curr_proj]

        #initialize current attributes and add to the list to be appended to the dictionary
        curr_attr_dict = dict({'health_emergency': 0, 'non_compl_get_compl': 0, 'future_requirements': 0, 'not_compl_related': 0, 'court_order': 0, 'no_viol': 0})
        curr_attr_list = []

        if checklist_curr.public_health.item() > 0:
            curr_attr_list.append('This project relates to a public health emergency.')
            curr_attr_dict['health_emergency'] = 1
        if checklist_curr.achieve_compliance.item() > 0:
            curr_attr_list.append('This project will assist a non-compliant system to achieve compliance.')
            curr_attr_dict['non_compl_get_compl'] = 1
        if checklist_curr.future_requirements.item() > 0:
            curr_attr_list.append('This project will assist a compliant system to meet future requirements.')
            curr_attr_dict['future_requirements'] = 1
        if checklist_curr.not_compliance_related.item() > 0:
            curr_attr_list.append('This project will provide assistance not compliance related.')
            curr_attr_dict['not_compl_related'] = 1
        if checklist_curr.court_order.item() > 0:
            curr_attr_list.append(f'This project is necessary to achieve full or partial compliance with a court order, agreed order, or a judicial or administrative consent decree. Order number {checklist_curr.court_ord_no.item()}')
            curr_attr_dict['court_order'] = 1
        if checklist_curr.primary_system.item() > 0:
            curr_attr_list.append('Primary system has not received any SDWA Notices of Violation within the previous state fiscal year-July through June, i.e. July 2014 – June 2015).')
            curr_attr_dict['no_viol'] = 1

        curr_proj_dict['DW Specific Impacts'] = curr_attr_dict
        curr_proj_dict['DW Specific Impacts List'] = curr_attr_list
        
        #save to file
        os.chdir(f'{PATH_TO_WRIS_DATA}/projects')
        with open(f"{curr_proj_dict['Project Number']}.json", "w") as fp:
            json.dump(curr_proj_dict, fp)
        
######################
# Problems and Notes #
######################
print(len(eng_problems))
print(len(contact_problems))
print(len(budget_problems))
print(len(overall_problems))
print(len(demo_problems))
print(len(date_problems))
print(len(new_cust_problems))

### Contact Problems ###
#Project WX21119225 has no alternate contact information

### Overall Problems ###
# all confirmed to just not contain links to the most recent fee schedule

# overall_problems_projects: ['WX21059068', 'WX21079003', 'WX21103057', 'WX21133064', 'WX21149033', 'WX21149042', 'WX21157046', 'WX21157050', 'WX21187400', 'WX21191507', 'WX21197010', 'WX21197011', 'WX21219019', 'WX21219020', 'WX21219021', 'WX21219023', 'WX21219031', 'WX21219032', 'WX21219035', 'WX21219036', 'WX21219040', 'WX21219041', 'WX21219042', 'WX21219043', 'WX21219044']

### Demographic Problems ###
#for all of those with demographic problems, redo all of the demographic info from tables, needed to be done by hand, might be able to change this with the html files
#new issues with WX21185025, WX21139026

### New Customer Problems ###
#WX21029056, WX21029176, WX21029193, WX21025052, WX21165011, WX21029281 -- no problems when looked on WRIS, fixed by hand

### Beneficiary Systems ###
#WX21111160 -- name of system extracted incorrectly, to be fixed

### Problem with Description not loading properly ###
#WX21043048 
# os.chdir(f'{PATH_TO_WRIS_DATA}/projects')
# with open('WX21043048.json') as f:
#     curr_proj_dict = json.load(f)
#     curr_proj_dict['Project Description'] = 'Install sludge lagoons at the WTP to replace the existing mechanical sludge handling system.'
    
#     os.chdir(f'{PATH_TO_WRIS_DATA}/projects')
#     with open(f"{curr_proj_dict['Project Number']}.json", "w") as fp:
#         json.dump(curr_proj_dict, fp)
    