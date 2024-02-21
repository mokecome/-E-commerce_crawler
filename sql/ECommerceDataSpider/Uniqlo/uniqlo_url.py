# -*- coding: utf-8 -*-
"""
Created on Fri Jul  7 16:40:53 2023

@author: mokecome
"""
import parsel
import pandas as pd
from selenium import webdriver
from selenium.webdriver.chrome.options import Options  # 导入浏览器的可选项功能
import time
from selenium.webdriver.chrome.service import Service
import sys
import os
sys.path.append(sys.path.append(os.path.abspath(os.path.join(os.getcwd(), "../.."))))
from config_spider import year,month,day,raw_url_format
   
df1=pd.DataFrame([]) 
chrome_options = Options()
chrome_options.add_argument('--headless')
driver = webdriver.Chrome(service=Service(r'../chromedriver-win64/chromedriver.exe'), options=chrome_options)    
 
df=pd.read_csv('{}{}{}/Raw_data/uniqlo_cate.csv'.format(year,month,day))
for tag3_url in df[raw_url_format[2]]:
    driver.get(tag3_url)
    driver.maximize_window()
    for i in range(1, 11, 2):
        time.sleep(0.5)
        j = i / 9 
        js = 'document.documentElement.scrollTop = document.documentElement.scrollHeight * %f' % j
        driver.execute_script(js) 
    pageSource = driver.page_source
    selector= parsel.Selector(pageSource)

    category1 = selector.css('div.h-product-group')        
    for c1 in category1.css('li.product-li a'):
        Detail_url='https://www.uniqlo.com/tw'+c1.css('::attr(href)').get().strip()
        print(Detail_url)
        try:
               
            df2=pd.DataFrame({
                                  raw_url_format[2]:[tag3_url],
                                  raw_url_format[-1]:[Detail_url]
                                  })
            df1=pd.concat([df1,df2],axis =0)
        except:
            print('解析错误')
df1=df1.drop_duplicates().reset_index(drop=True)
for c in list(set(raw_url_format).difference(set(df1.columns.tolist()))):
    df1[c]=''
df1[raw_url_format[0]]=year+month+day
df1[raw_url_format].drop_duplicates(subset=[raw_url_format[2],raw_url_format[-1]],keep='last').to_csv('{}{}{}/Raw_data/uniqlo_scrapy_url.csv'.format(year,month,day),index=False)

df1.drop_duplicates().to_csv('{}{}{}/raw_url.csv'.format(year,month,day),index=False)