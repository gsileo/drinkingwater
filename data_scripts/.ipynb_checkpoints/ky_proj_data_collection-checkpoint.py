import pandas as pd
import os
import math
import time
import random
import sys
import numpy as np
from selenium import webdriver
from selenium.webdriver import FirefoxOptions
from selenium.webdriver.common.by import By

os.chdir('/home/gsileo/repos/drinkingwater')

from config import PATH_TO_WRIS_DATA, PATH_TO_KY_DATA_COLLECTION
import warnings
warnings.filterwarnings('ignore')

#####################################################
# Pull in the data master with the list of projects #
#####################################################

os.chdir(PATH_TO_WRIS_DATA)
projects_master = pd.read_pickle("projects_master.pkl")

##########################################
# Set-up to pull down the WRIS pdf Files #
##########################################

#setting options to run the browser-fetching "headlessly"
opts = FirefoxOptions()
opts.add_argument("--headless")

#setting options for saving the files that get downloaded via "clicking"
profile = webdriver.FirefoxProfile()
profile.set_preference("browser.download.folderList", 2)
profile.set_preference("browser.download.manager.showWhenStarting", False)
profile.set_preference("browser.helperApps.neverAsk.saveToDisk", "application/pdf")
profile.set_preference("pdfjs.disabled", True)

#set profile options and create driver
profile.set_preference("browser.download.dir",  f'{PATH_TO_KY_DATA_COLLECTION}/WRIS/project_reports')
driver = webdriver.Firefox(firefox_profile=profile, firefox_options=opts)

#####################################################################
# Run through each of the projects and download the project reports #
#####################################################################
for i in range(len(projects_master)):
    #set the current pwsid and url
    curr_proj = projects_master.pnum.iloc[i]
    curr_url = f'https://wris.ky.gov/Portal/DWPrjData/{curr_proj}'
    
    print(f'currently working on {i+1} out of {len(projects_master)}')
    
    #fetch the webpage and click to download
    driver.get(curr_url)

    #save the project profile
    try:
        driver.find_element_by_xpath('//input[@value="Download Profile (PDF)"]').click()
    except:
        print(f'Failed to download project profile for {curr_proj}')
    
    #set the system to sleep
    time.sleep(random.randint(30,60))
    
#close the selenium driver    
driver.close()
