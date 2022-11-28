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

##############################################
# Pull in the system data cleaning functions #
##############################################

os.chdir(f'{PATH_TO_KY_DATA_COLLECTION}/functions')
import sys_data_functions as sdf

#########################################
# Page 0 pre-function construction data #
#########################################

#construct the first page keys
first_page_keys = ['DOW Permit ID', 'DOW Permit Type', 'DOW Permit Name', 'WRIS System Name', 'System Type', 'Water Source Type', 'ADD WMC Contact', 'ADD ID', 'Primary County',
                   'Dow Field Office', 'Permit Issued', 'Permit Expired', 'Permit Inactivated', 'Facility Name', 'Facility Contact', 'Facility Phone', 'Facility Addr 1', 
                   'Facility Addr 2', 'Facility City, State Zip', 'Date Ops and Management Last Modified', 'Physical Inf. Operations Contact', 'Business/Financials Contact', 'Manager', 'Date Contact Last Modified',
                   'Entity Name', 'Office Phone', 'Office Fax', 'Office Address 1', 'Office Address 2', 'Office City, State Zip', 'Entity Type', 'PSC Group ID', 'Owner Entity Name', 
                   'Owner Web URL', 'Owner Office EMail', 'Owner Office Phone', 'Owner Toll Free', 'Owner Fax', 'Owner Mail Address Line 1', 'Owner Phys Address Line 1', 
                   'Owner Mail Address Line 2', 'Owner Phys Address Line 2', 'Owner Mail City, State Zip', 'Owner Phys City, State Zip', 'Contact', 'Financial Contact', 
                   'Auth Official', 'Contact Title', 'Financial Contact Title', 'Auth Official Title', 'Contact EMail', 'Financial Contact EMail', 'Auth Official EMail', 
                   'Contact Phone', 'Financial Contact Phone', 'Auth Official Phone', 'Data Source', 'Date Owner Entity Last Modified']

#construct endpoints
first_page_endpoints = [['DOW Permit ID:', 'Link: DOW SDWIS Report'], ['DOW Permit Type:', 'DOW Permit Name:'], ['DOW Permit Name:', 'WRIS System Name:'], 
                        ['WRIS System Name:', 'System Type:'], ['System Type:', 'Water Source Type:'], ['Water Source Type:', 'ADD WMC Contact:'], ['ADD WMC Contact:', 'ADD ID:'],
                        ['ADD ID:', 'Primary County:'], ['Primary County:', 'Dow Field Office:'], ['Dow Field Office:', 'Permit Dates: Issued:'], ['Permit Dates: Issued:', 'Expired:'],
                        ['Expired:', 'Inactivated:'], ['Inactivated:', 'OPERATIONS AND MANAGEMENT INFORMATION'], ['Facility Name:', 'Facility Contact:'], 
                        ['Facility Contact:', 'Facility Phone:'], ['Facility Phone:', 'Facility Addr 1:'], ['Facility Addr 1:', 'Facility Addr 2:'], 
                        ['Facility Addr 2:', 'City, State Zip:'], ['City, State Zip:', 'Date Last Modified: '], ['Date Last Modified: ', 'System Management Contact Information:'],
                        ['1 Operations Contact:', '2 Business Contact:'], ['2 Business Contact:', 'Manager:'], ['Manager:', 'Date Last Modified: '], 
                        ['Date Last Modified: ', 'System Management Entity Information:'], ['Entity Name:', 'Office Phone:'], ['Office Phone:', ' Fax: '], 
                        [' Fax: ', 'Office Address 1:'], ['Office Address 1:', 'Office Address 2:'], ['Office Address 2:', 'City, State Zip:'], 
                        ['City, State Zip:', 'OWNER ENTITY INFORMATION'], ['Entity Type:', 'PSC Group ID:'], ['PSC Group ID:', 'Entity Name:'], ['Entity Name:', 'Web URL:'],
                        ['Web URL:', 'Office EMail:'], ['Office EMail:', 'Office Phone:'], ['Office Phone:', 'Toll Free:'], ['Toll Free:', 'Fax:'], ['Fax:', 'Mail Address Line 1:'],
                        ['Mail Address Line 1:', 'Phys Address Line 1:'], ['Phys Address Line 1:', 'Mail Address Line 2:'], ['Mail Address Line 2:', 'Phys Address Line 2:'],
                        ['Phys Address Line 2:', 'Mail City, State Zip:'], ['Mail City, State Zip:', 'Phys City, State Zip:'], ['Phys City, State Zip:', 'Contact:'],
                        ['Contact:', 'Financial Contact:'], ['Financial Contact:', 'Auth Official:'], ['Auth Official:', 'Contact Title:'], 
                        ['Contact Title:', 'Financial Contact Title:'], ['Financial Contact Title:', 'Auth Official Title:'], ['Auth Official Title:', 'Contact EMail:'], 
                        ['Contact EMail:', 'Financial Contact EMail:'], ['Financial Contact EMail:', 'Auth Official EMail:'], ['Auth Official EMail:', 'Contact Phone:'], 
                        ['Contact Phone:', 'Financial Contact Phone:'], ['Financial Contact Phone:', 'Auth Official Phone:'], ['Auth Official Phone:', 'Data Source:'],
                        ['Data Source:', 'Date Last Modified: '], ['Date Last Modified: ', 'System Respondent']]


#########################################
# Page 1 pre-function construction data #
#########################################

#construct the keys for counties served detail table
counties_table_keys = ['County Served', 'Connection Count', 'Serviceable Population', 'Serviceable Households', 'Med. HH Income', 'MHI MOE']

#construct purchaser system keys -- note: water types are F-finished(treated), R-raw(untreated), B-both
purchaser_table_keys = ['Purchaser DOW Permit ID', 'Purchaser Name', 'Water Type', 'Ann. Vol. (MG)', 'Raw Cost (1,000 G)', 'Finished Cost (1,000 G)', 'Permanent Conn', 
                     'Seasonal Conn', 'Emergency Conn', 'Serviceable Population', 'Serviceable Households']

#construct seller system keys -- note: water types are F-finished(treated), R-raw(untreated), B-both
seller_table_keys = ['Seller DOW Permit ID', 'Seller Name', 'Water Type', 'Ann. Vol. (MG)', 'Raw Cost (1,000 G)', 'Finished Cost (1,000 G)', 'Permanent Conn', 
                     'Seasonal Conn', 'Emergency Conn']

#fiscal attributes keys
fiscal_keys = ['Date Established', 'Employees', 'Cost per 4,000 gallons of finished water inside municipality', 
               'If municipal system, cost per 4,000 gallons of finished water outside municipality', 
               'System Produce Water?', 'System have wholesale customers?', 'System purchase water?',  
               'If non-municipal system, cost per 4,000 gallons of finished water', 'Date of Last Rate Adjustment', 'Comments', 'Date Fiscal Attributes Last Modified']

#########################################
# Page 2 pre-function construction data #
#########################################

#construct the keys for counties served detail table
watertreatment_table_keys = ['Facility Name', 'Design Capacity (MGD)', 'Ave. Daily Prod. (MGD)', 'High. Daily Prod. (MGD)']

#construct operational statistics keys 
operational_stats_keys = ['Total Annual Vol. Produced (MG)', 'Total Annual Vol. Purchased (MG)', 'Total Annual Vol. Provided (MG)', 'Estimated Annual Water Loss',
                          'Wholesale Usage (MG)', 'Residential Usage (MG)', 'Commercial Usage (MG)', 'Institutional Usage (MG)', 'Industrial Usage (MG)', 
                          'Other Cust. Usage (MG)', 'Flushing, Maintenance and Fire Protection Usage (MG)', 'Total Annual Water Usage (MG)']
#construct endpoints
operational_endpoints = [['Total Annual Vol. Produced \(MG\):', 'Total Annual Vol. Purchased \(MG\)'], ['Total Annual Vol. Purchased \(MG\):', 'Total Annual Vol. Provided \(MG\)'], 
                         ['Total Annual Vol. Provided \(MG\):', 'Estimated Annual Water Loss'], ['Estimated Annual Water Loss:', 'WRIS SDWIS MOR'], 
                         ['Wholesale Usage \(MG\):', 'Residential Customers'], ['Residential Usage \(MG\):', 'Commercial Customers'], 
                         ['Commercial Usage \(MG\):', 'Institutional Customers'], ['Institutional Usage \(MG\):', 'Industrial Customers'], 
                         ['Industrial Usage \(MG\):', 'Other Customers'], ['Other Cust. Usage \(MG\):', 'Total Customers'], 
                         ['Flushing, Maintenance and Fire Protection Usage \(MG\):', 'Total Annual Water Usage \(MG\)'], 
                         ['Total Annual Water Usage \(MG\):', 'Water supply inadequacies']]
#remaining third page keys
third_page_keys = ['Wholesale Customers', 'Residential Customers', 'Commercial Customers', 'Institutional Customers', 'Industrial Customers', 'Other Customers', 'Total Customers',
                  'Water supply inadequacies during normal operating conditions', 'Water supply inadequacies during drought operating conditions', 'Operational Stats Comments', 
                   'Date Op Stats Last Modified', 'WMP Site Visit Date', 'WMP Survey Administrator', 'WMP Principal Respondent', 'WMP Other Respondent(s)', 'WMP Comments', 
                   'Date WMP Visit Last Modified']

#remaining third page endpoints
third_page_endpoints = [['Wholesale Customers:', 'Wholesale Usage'], ['Residential Customers:', 'Residential Usage'], ['Commercial Customers:', 'Commercial Usage'], 
                        ['Institutional Customers:', 'Institutional Usage'], ['Industrial Customers:', 'Industrial Usage'], ['Other Customers:', 'Other Cust. Usage'],
                        ['Total Customers:', 'Flushing, Maintenance and'], ['during normal operating conditions:', 'Water supply inadequacies during drought'],
                        ['drought operating conditions:', 'Comments'], ['Comments:', 'Date Last Modified'], ['Date Last Modified:', 'WMP Site Visit'], 
                        ['Survey Date:', 'Survey Administrator'], ['Survey Administrator:', 'Principal Respondent'], ['Principal Respondent:', 'Other Respondent'], 
                        ['Other Respondent\(s\):', 'Comments'], ['Comments:', 'Date Last Modified:  \d{2}\.\d{2}\.\d{4}   Kentucky'], 
                        ['Date Last Modified:', 'Kentucky Infrastructure']]

###########################################
# Page 3/4 pre-function construction data #
###########################################

#projects keys
project_table_keys = ['PNUM', 'Applicant', 'Project Status', 'Funding Status', 'Schedule', 'Project Title', 'Agreed Order', 'Profile Modified', 'GIS Modified']

###########################
# Pull in the data master #
###########################

#note, this has already excluded any pwsids that at the time I knew to not be in WRIS
os.chdir(PATH_TO_WRIS_DATA)
systems_master = pd.read_pickle("systems_master.pkl")

##############
# Data Notes #
##############

#construct a list of pwsids that fall outside of the normal expectation for pdf reading and need to be handled separately
problem_pwsids = ['KY0160052', 'KY0050929', 'KY0340250',  'KY0920332', 'KY1140487', 'KY0480498', 'KY1050157', 'KY0440168']

#These PWSIDs have no associated projects, confirmed
# KY0920025 -- 5 pages, page 5 blank
# KY0070729 -- 4 pages, no projects
# KY0081013 -- 4 pages, no projects
# KY0483727 -- 4 pages, no projects
# KY0180509 -- 4 pages, no projects
# KY0180502 -- 4 pages, no projects
# KY0073691 -- 4 pages, no projects
# KY0090287 -- 4 pages, no projects
# KY0720552 -- 5 pages, page 5 blank
# KY0310940 -- 4 pages, no projects
# KY0070282 -- 5 pages, page 5 blank
# KY1183728 -- 4 pages, no projects
# KY1000973 -- 5 pages, page 5 blank

#############################################################
# Running through the main data construction for most files #
#############################################################

for s in range(len(systems_master)):
    #set the current system
    curr_pws = systems_master['pwsid'].iloc[s]
    
    if curr_pws not in problem_pwsids:
        #open the pdf file and read the pdf
        os.chdir(f'{PATH_TO_KY_DATA_COLLECTION}/WRIS/system_reports')
        ky_file = open(f'WRIS_DwSystemDataReport_{curr_pws}.pdf', 'rb')
        ky_pdf = ppdf.PdfFileReader(ky_file)

        #extract the number of pages
        page_num = ky_pdf.getNumPages()

        print('working on #: ', curr_pws, f' which has {page_num} pages')

        #initialize the dict to be saved for each pws
        full_pwsid_dict = {}

        ####################################################################
        # Extract data from the first four pages (all pws have at least 4) #
        ####################################################################

        # Page 0 #
        page0 = ky_pdf.getPage(0)
        page0_text = page0.extractText()
        page0_dict = sdf.wris_system_page0(page0_text, first_page_keys, first_page_endpoints)
        full_pwsid_dict.update(page0_dict)

        # Page 1 #
        page1 = ky_pdf.getPage(1)
        page1_text = page1.extractText()
        page1_dict = sdf.wris_system_page1(page1_text, counties_table_keys, purchaser_table_keys, seller_table_keys)
        full_pwsid_dict.update(page1_dict)

        # Page 2 #
        page2 = ky_pdf.getPage(2)
        page2_text = page2.extractText()
        page2_dict = sdf.wris_system_page2(page2_text, watertreatment_table_keys, operational_stats_keys, operational_endpoints, third_page_keys, third_page_endpoints)
        full_pwsid_dict.update(page2_dict)

        # Page 3 -- ONLY FOR PROJECTS #
        page3 = ky_pdf.getPage(3)
        page3_text = page3.extractText()
        projects_dict = sdf.wris_system_page3(page3_text, project_table_keys)

        #handle cases where there are more than 4 pages
        if page_num > 4:
            #cycle through the remaining pages to extract all data
            for p in range(4,page_num):
                print('page: ', p+1)
                curr_page = ky_pdf.getPage(p)
                curr_page_text = curr_page.extractText()

                projects_dict = sdf.wris_system_project_page(curr_page_text, project_table_keys, projects_dict)

        #now that the projects have been extracted, update the pws dictionary
        full_pwsid_dict.update(projects_dict)

        #save to file
        os.chdir(f'{PATH_TO_WRIS_DATA}/systems')
        with open(f'{curr_pws}.json', 'w') as fp:
            json.dump(full_pwsid_dict, fp)
    
    else:
        print(f'{curr_pws} in problem pwsids, to be handled separately')
        
####################################
# Handling the Exceptions Manually #
####################################

## These ones have the same issues -- need to run page 1 twice ##
for prob in problem_pwsids[0:5]:
    os.chdir(f'{PATH_TO_KY_DATA_COLLECTION}/WRIS/system_reports')
    ky_file = open(f'WRIS_DwSystemDataReport_{prob}.pdf', 'rb')
    ky_pdf = ppdf.PdfFileReader(ky_file)

    #extract the number of pages
    page_num = ky_pdf.getNumPages()

    print('working on #: ', prob, f' which has {page_num} pages')

    #initialize the dict to be saved for each pws
    full_pwsid_dict = {}

    ####################################################################
    # Extract data from the first four pages (all pws have at least 4) #
    ####################################################################

    # Page 0 #
    page0 = ky_pdf.getPage(0)
    page0_text = page0.extractText()
    page0_dict = sdf.wris_system_page0(page0_text, first_page_keys, first_page_endpoints)
    full_pwsid_dict.update(page0_dict)
    
    ### Altered ##

    # Page 1 #
    page1 = ky_pdf.getPage(1)
    page1_text = page1.extractText()
    page1_dict = sdf.wris_system_page1_alt(page1_text, counties_table_keys, purchaser_table_keys, seller_table_keys)
    full_pwsid_dict.update(page1_dict)

    # Page 2 #
    page2 = ky_pdf.getPage(2)
    page2_text = page2.extractText()
    page2_dict = sdf.wris_system_page1_alt(page2_text, counties_table_keys, purchaser_table_keys, seller_table_keys)
    full_pwsid_dict.update(page2_dict)
    
    # Page 3 #
    page3 = ky_pdf.getPage(3)
    page3_text = page3.extractText()
    page3_dict = sdf.wris_system_page2(page3_text, watertreatment_table_keys, operational_stats_keys, operational_endpoints, third_page_keys, third_page_endpoints)
    full_pwsid_dict.update(page3_dict)

    # Page 4 -- ONLY FOR PROJECTS #
    page4 = ky_pdf.getPage(4)
    page4_text = page4.extractText()
    projects_dict = sdf.wris_system_page3(page4_text, project_table_keys)

    #cycle through the remaining pages to extract all data
    for p in range(5, page_num):
        print('page: ', p+1)
        curr_page = ky_pdf.getPage(p)
        curr_page_text = curr_page.extractText()

        projects_dict = sdf.wris_system_project_page(curr_page_text, project_table_keys, projects_dict)

    #now that the projects have been extracted, update the pws dictionary
    full_pwsid_dict.update(projects_dict)

    #save to file
    os.chdir(f'{PATH_TO_WRIS_DATA}/systems')
    with open(f'{prob}.json', 'w') as fp:
        json.dump(full_pwsid_dict, fp)
        
############################## KY0480498 ###################################
# KY0480498 -- has page 1 & 2 problems -- some of 2 is on page 1

os.chdir(f'{PATH_TO_KY_DATA_COLLECTION}/WRIS/system_reports')
curr_pwsid = 'KY0480498'
ky_file = open(f'WRIS_DwSystemDataReport_{curr_pwsid}.pdf', 'rb')
ky_pdf = ppdf.PdfFileReader(ky_file)

#initialize the dict to be saved for each pws
full_pwsid_dict = {}

########
# Page 0
page0 = ky_pdf.getPage(0)
page0_text = page0.extractText()

page0_dict = sdf.wris_system_page0(page0_text, first_page_keys, first_page_endpoints)

#manual extraction from WRIS
page0_dict['DOW Permit ID'] = 'KY0480498'
page0_dict['DOW Permit Type'] = 'Drinking Water (PWSID)'
page0_dict['DOW Permit Name'] = 'Black Mtn Utility/Louellen'
page0_dict['WRIS System Name'] = 'BMUD - Louellen'
page0_dict['System Type'] = 'Community'
page0_dict['Water Source Type'] = 'Groundwater Under The Influence Purchaser'
page0_dict['ADD WMC Contact'] = 'Daniel Mullins'
page0_dict['ADD ID'] = 'CVADD'
page0_dict['Primary County'] = 'Harlan'
page0_dict['Dow Field Office'] = 'London'
page0_dict['Permit Issued'] = '09-01-1984'

#update master dict
full_pwsid_dict.update(page0_dict)

#manually add in demographic info
dem_dict = {'Counties Served Detail': [{'County Served': 'Harlan',
   'Connection Count': '206',
   'Serviceable Population': '785',
   'Serviceable Households': '380',
   'Med. HH Income': '$32,784',
   'MHI MOE': '$14,085'}],
 'MHI Source': 'American Community Survey 2015-2019 5Yr Estimates (Table  B19013) .  MHI MOE = Med HH Income Margin of Error.',
 'Counties Directly Served': '1',
 'Directly Serviceable Pop': '785',
 'Directly Serviceable HH': '380',
 'Indirectly Serviceable Pop': '',
 'Indirectly Serviceable HH': ''}

#update master dict
full_pwsid_dict.update(dem_dict)

########
# Page 1
page1 = ky_pdf.getPage(1)
page1_text = page1.extractText()
page1_dict = sdf.wris_system_page1_alt(page1_text, counties_table_keys, purchaser_table_keys, seller_table_keys)

#update master dict
full_pwsid_dict.update(page1_dict)

########
# Page 2
page2 = ky_pdf.getPage(2)
page2_text = page2.extractText()

page2_dict = sdf.wris_system_page2(page2_text, watertreatment_table_keys, operational_stats_keys, operational_endpoints, third_page_keys, third_page_endpoints)

#update master dict
full_pwsid_dict.update(page2_dict)

########
# Page 3
page3 = ky_pdf.getPage(3)
page3_text = page3.extractText()

page3_dict = sdf.wris_system_page3(page3_text, project_table_keys)

#update master dict
full_pwsid_dict.update(page3_dict)

########
# Page 4
page4 = ky_pdf.getPage(4)
page4_text = page4.extractText()

projects_dict = sdf.wris_system_project_page(page4_text, project_table_keys, page3_dict)

#update master dict
full_pwsid_dict.update(projects_dict)

#save to file
os.chdir(f'{PATH_TO_WRIS_DATA}/systems')
with open(f'{curr_pwsid}.json', 'w') as fp:
    json.dump(full_pwsid_dict, fp)
    
    
############################## KY1050157 ###################################
# KY1050157 -- some of page 3 is on page 2

os.chdir(f'{PATH_TO_KY_DATA_COLLECTION}/WRIS/system_reports')
curr_pwsid = 'KY1050157'
ky_file = open(f'WRIS_DwSystemDataReport_{curr_pwsid}.pdf', 'rb')
ky_pdf = ppdf.PdfFileReader(ky_file)

#initialize the dict to be saved for each pws
full_pwsid_dict = {}

########
# Page 0
page0 = ky_pdf.getPage(0)
page0_text = page0.extractText()

page0_dict = sdf.wris_system_page0(page0_text, first_page_keys, first_page_endpoints)

#update master dict
full_pwsid_dict.update(page0_dict)

########
# Page 1
page1 = ky_pdf.getPage(1)
page1_text = page1.extractText()
page1_dict = sdf.wris_system_page1(page1_text, counties_table_keys, purchaser_table_keys, seller_table_keys)

#update master dict
full_pwsid_dict.update(page1_dict)

#manually add in water treatment plant info
wtp_dict = {'Water Treatment Plants': [{'Facility Name': 'ROYAL SPRING WTP',
   'Design Capacity (MGD)': '4.000',
   'Ave. Daily Prod. (MGD)': '2.962',
   'High. Daily Prod. (MGD)': '3.851'}]}

#update master dict
full_pwsid_dict.update(wtp_dict)

########
# Page 2
page2 = ky_pdf.getPage(2)
page2_text = page2.extractText()

page2_dict = sdf.wris_system_page2_alt(page2_text, watertreatment_table_keys, operational_stats_keys, operational_endpoints, third_page_keys, third_page_endpoints)

#update master dict
full_pwsid_dict.update(page2_dict)

########
# Page 3
page3 = ky_pdf.getPage(3)
page3_text = page3.extractText()

page3_dict = sdf.wris_system_page3(page3_text, project_table_keys)

#update master dict
full_pwsid_dict.update(page3_dict)

########
# Page 4
page4 = ky_pdf.getPage(4)
page4_text = page4.extractText()

projects_dict = sdf.wris_system_project_page(page4_text, project_table_keys, page3_dict)

#update master dict
full_pwsid_dict.update(projects_dict)

#save to file
os.chdir(f'{PATH_TO_WRIS_DATA}/systems')
with open(f'{curr_pwsid}.json', 'w') as fp:
    json.dump(full_pwsid_dict, fp)

    
############################## KY0440168 ###################################
# KY0440168 -- problem with extracting the date established

os.chdir(f'{PATH_TO_KY_DATA_COLLECTION}/WRIS/system_reports')
curr_pwsid = 'KY0440168'
ky_file = open(f'WRIS_DwSystemDataReport_{curr_pwsid}.pdf', 'rb')
ky_pdf = ppdf.PdfFileReader(ky_file)

#initialize the dict to be saved for each pws
full_pwsid_dict = {}

# Page 0 #
page0 = ky_pdf.getPage(0)
page0_text = page0.extractText()
page0_dict = sdf.wris_system_page0(page0_text, first_page_keys, first_page_endpoints)
full_pwsid_dict.update(page0_dict)

# Page 1 #
page1 = ky_pdf.getPage(1)
page1_text = page1.extractText()
page1_dict = sdf.wris_system_page1(page1_text, counties_table_keys, purchaser_table_keys, seller_table_keys)
full_pwsid_dict.update(page1_dict)

# Page 2 #
page2 = ky_pdf.getPage(2)
page2_text = page2.extractText()
page2_dict = sdf.wris_system_page2(page2_text, watertreatment_table_keys, operational_stats_keys, operational_endpoints, third_page_keys, third_page_endpoints)
full_pwsid_dict.update(page2_dict)

# Page 3 -- ONLY FOR PROJECTS #
page3 = ky_pdf.getPage(3)
page3_text = page3.extractText()
projects_dict = sdf.wris_system_page3(page3_text, project_table_keys)

#handle cases where there are more than 4 pages
if page_num > 4:
    #cycle through the remaining pages to extract all data
    for p in range(4,page_num):
        print('page: ', p+1)
        curr_page = ky_pdf.getPage(p)
        curr_page_text = curr_page.extractText()

        projects_dict = sdf.wris_system_project_page(curr_page_text, project_table_keys, projects_dict)

#now that the projects have been extracted, update the pws dictionary
full_pwsid_dict.update(projects_dict)

#manually update the date established
full_pwsid_dict['Date Established'] = '01.01.1964'

#save to file
os.chdir(f'{PATH_TO_WRIS_DATA}/systems')
with open(f'{curr_pws}.json', 'w') as fp:
    json.dump(full_pwsid_dict, fp)
    
    
#################################################################################################################################################################################################
###############################################################################################################
# In order to do the following, you need to re-download Adobe Acrobat Pro, and export all system pdfs to html #
###############################################################################################################

for pwsid in systems_master['pwsid']:
    #open the json and extract to dictionary
    os.chdir(f'{PATH_TO_WRIS_DATA}/systems')
    with open(f'{pwsid}.json') as f:
        curr_pwsid = json.load(f)
    
    #extract the html tables
    curr_tables = pd.read_html(f'{PATH_TO_KY_DATA_COLLECTION}/WRIS/system_reports/system_reports_html/WRIS_DwSystemDataReport_{pwsid}.html', header=0)
    
    #initialize empty df to hold the project dataframe(s)
    project_temp_df = pd.DataFrame()
    
    #initialize empty df to hold the project dataframe(s)
    counties_temp_df = pd.DataFrame()
        
    for t,tb in enumerate(curr_tables):
        #check to see if table contains projects
        if 'PNUM' in tb.columns:
            #append the table with the project info to the project dataframe
            project_temp_df = project_temp_df.append(tb)
        
        #look for counties served data
        if 'County Served' in tb.columns:
            #append the table with the project info to the project dataframe
            counties_temp_df = counties_temp_df.append(tb)
            
#             #remove any row containing totals
#             counties_temp_df = counties_temp_df[counties_temp_df['County Served'] != 'Totals:']
             
    #create a list of dictionaries with the data for all the projects and one for all the counties
    project_list = [dict(zip(project_temp_df.columns, project_temp_df.iloc[i])) for i in range(len(project_temp_df))]
    counties_list = [dict(zip(counties_temp_df.columns, [str(x) for x in counties_temp_df.iloc[i]])) for i in range(len(counties_temp_df))]

    #update the project and counties served lists
    curr_pwsid['Projects'] = project_list
    curr_pwsid['Counties Served Detail'] = counties_list
    
    #overwrite the old jsons with the bad data
    os.chdir(f'{PATH_TO_WRIS_DATA}/systems')
    with open(f'{pwsid}.json', 'w') as fp:
        json.dump(curr_pwsid, fp)