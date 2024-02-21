# -*- coding: utf-8 -*-
"""
Created on Tue Jul  4 17:35:33 2023

@author: mokecome
"""
import parsel
import pandas as pd
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
import time
from selenium.webdriver.chrome.service import Service
import sys
import os
sys.path.append(sys.path.append(os.path.abspath(os.path.join(os.getcwd(), "../.."))))
from config_spider import year,month,day,raw_cate_format

if not os.path.isdir('{}{}{}/temp'.format(year,month,day)):
   os.makedirs('{}{}{}/temp'.format(year,month,day))
if not os.path.isdir('{}{}{}/Raw_data'.format(year,month,day)):
   os.makedirs('{}{}{}/Raw_data'.format(year,month,day))
if not os.path.isdir('{}{}{}/Combine_data'.format(year,month,day)):
   os.makedirs('{}{}{}/Combine_data'.format(year,month,day))

df1=pd.DataFrame([])
chrome_options = Options()
chrome_options.add_argument('--headless')
driver = webdriver.Chrome(service=Service(r'../chromedriver-win64/chromedriver.exe'), options=chrome_options) 
first=['women','men','kids','baby']
for tag1 in first:
    tag1_url='https://www.uniqlo.com/tw/zh_TW/{}_L2.html'.format(tag1)
    driver.get(tag1_url)
    driver.maximize_window()
    for i in range(1, 11, 2):
        time.sleep(0.5)
        j = i / 9 
        js = 'document.documentElement.scrollTop = document.documentElement.scrollHeight * %f' % j
        driver.execute_script(js) 
    pageSource = driver.page_source
    selector= parsel.Selector(pageSource)
    
    category = selector.css('li.bd_categories_item')
    for c in category:
        tag2_url=c.css('a::attr(href)').get().strip()
        tag2=c.css('a div.bd_categories_txt::text').get().strip()
        print(tag2_url,tag2)
        
        if '/' in tag2:
            tag2=tag2.replace('\n','').replace(' ','')
        df2=pd.DataFrame({raw_cate_format[1]:['Uniqlo'],
                          raw_cate_format[2]:[tag1.replace('\n','')],
                          raw_cate_format[3]:[tag2.replace('\n','')],
                          'tag2_url':[tag2_url]
                          })
        df1=pd.concat([df1,df2],axis =0)
df1.drop_duplicates().reset_index(drop=True).to_excel('{}{}{}/temp/uniqlo_class1.xlsx'.format(year,month,day), index = False)

df2=pd.DataFrame([])
for tag2_url in df1['tag2_url']:
    driver.get(tag2_url)
    driver.maximize_window()
    for i in range(1, 11, 2): 
        time.sleep(0.5)
        j = i / 9 
        js = 'document.documentElement.scrollTop = document.documentElement.scrollHeight * %f' % j
        driver.execute_script(js) 
    pageSource = driver.page_source
    selector= parsel.Selector(pageSource)
    lis = selector.css('ul.bd-lineup li')
    for li in lis:
        tag3_url=li.css('a::attr(href)').get().strip()
        tag3=li.css('a h1::text').get().strip()
        print(tag3_url,tag3)
        if '/' in tag3:
            tag3=tag3.replace('\n','').replace(' ','')
        df3=pd.DataFrame({
                          'tag2_url':[tag2_url],
                          raw_cate_format[-2]:[tag3_url],
                          raw_cate_format[4]:[tag3.replace('\n','')]
                          })
        df2=pd.concat([df2,df3],axis =0)   
df2.drop_duplicates().reset_index(drop=True).to_excel('{}{}{}/temp/uniqlo_class2.xlsx'.format(year,month,day), index = False)

df=pd.merge(left=df1,right=df2,how="left",on=['tag2_url'])
for idx ,row in df.iterrows():
    tag2_url=row['tag2_url']
    tag2=row[raw_cate_format[3]]
    if pd.isna(row[raw_cate_format[-2]]):
        df[raw_cate_format[-2]][idx]=tag2_url
        
for c in list(set(raw_cate_format).difference(set(df.columns.tolist()))):
    df[c]=''
df[raw_cate_format[0]]=year+month+day
df[raw_cate_format].to_csv('{}{}{}/Raw_data/uniqlo_cate.csv'.format(year,month,day),index=False)
