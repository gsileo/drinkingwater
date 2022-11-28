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

###################################################
# Pull in the data master with the list of pwsids #
###################################################

#note, this has already excluded any pwsids that at the time I knew to not be in WRIS
os.chdir(PATH_TO_WRIS_DATA)
systems_master = pd.read_pickle("systems_master.pkl")

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
profile.set_preference("browser.download.dir", f'{PATH_TO_KY_DATA_COLLECTION}/WRIS/system_reports')
driver = webdriver.Firefox(firefox_profile=profile, firefox_options=opts)

####################################################################################################
# Run through each of the pwsids and download both the system reports and asset management reports #
####################################################################################################
for i in range(len(systems_master)):
    #set the current pwsid and url
    curr_pwsid = systems_master.pwsid.iloc[i]
    curr_url = f'https://wris.ky.gov/portal/DwSysData/{curr_pwsid}'
    
    print(f'currently working on {i+1} out of{len(systems_master)}')
    
    #fetch the webpage and click to download
    driver.get(curr_url)

    #save the asset management report -- needs to be manually moved to the asset management folder
    try:
        driver.find_element_by_xpath('//input[@value="Download Asset Management Report"]').click()
    except:
        print(f'{curr_pwsid} has no Asset Management Report')
    
    #save the system report
    try:
        driver.find_element_by_xpath('//input[@value="Download WRIS System Report"]').click()
    except:
        print(f'{curr_pwsid} has no WRIS System Report')
    
    #set the system to sleep
    time.sleep(random.randint(30,60))

#close the selenium driver    
driver.close()


