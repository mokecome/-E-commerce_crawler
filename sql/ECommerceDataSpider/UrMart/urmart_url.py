# -*- coding: utf-8 -*-
"""
Created on Tue Mar  7 10:58:38 2023

@author: mokecome
"""
import pandas as pd
from selenium import webdriver
import parsel
from selenium.webdriver.chrome.options import Options
import time
from selenium.webdriver.chrome.service import Service
import re
import math
import sys
import os
sys.path.append(sys.path.append(os.path.abspath(os.path.join(os.getcwd(), "../.."))))
from config_spider import year,month,day,raw_url_format


base_url='https://urmart.com/'
chrome_options = Options()
chrome_options.add_argument('--headless')
driver = webdriver.Chrome(service=Service(r'../chromedriver-win64/chromedriver.exe'), options=chrome_options) 

df1=pd.DataFrame([])
df = pd.read_csv('{}{}{}/Raw_data/urmart_cate.csv'.format(year,month,day))[[raw_url_format[2]]]
headers = {'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.3'}
num=-1
for tag3_url in df[raw_url_format[2]].to_list():
    time.sleep(3)
    driver.get(tag3_url)
    driver.maximize_window()
    driver.implicitly_wait(10)   
    for i in range(1, 11, 2):
        time.sleep(0.5)
        j = i / 9 
        js = 'document.documentElement.scrollTop = document.documentElement.scrollHeight * %f' % j
        driver.execute_script(js)  
    pageSource = driver.page_source
    selector= parsel.Selector(pageSource)
    if  selector.css('div.sc-fUyOpj::text')!=[]:   
        num=selector.css('div.sc-fUyOpj::text').get().strip()
    if selector.css('div.sc-fHSIpi::text')!=[]:
        num=selector.css('div.sc-fHSIpi::text').get().strip() 
    if selector.css('div.sc-bEetH::text')!=[]:
        num=selector.css('div.sc-bEetH::text').get().strip()  
    if num==-1:
        num=0
        print('沒抓到項數')
    print(num)
    df2=pd.DataFrame({raw_url_format[2]:[tag3_url], 
                      raw_url_format[4]:[num]}) 
    df1=pd.concat([df1,df2],axis =0)   
df1=df1.drop_duplicates()#要先去重       
df1.reset_index(drop=True,inplace=True)

df1[raw_url_format[4]]=df1[raw_url_format[4]].apply(lambda x:"".join(re.findall(r'共(.+?)項',str(x))))
df1[raw_url_format[5]]=df1[raw_url_format[4]].apply(lambda x:0 if x=='' else  x).astype(int)
df1[raw_url_format[5]]=df1[raw_url_format[5]].apply(lambda x:math.ceil(x/30))


df1.to_csv('{}{}{}/temp/urmart_url_page.csv'.format(year,month,day), index = False)
 