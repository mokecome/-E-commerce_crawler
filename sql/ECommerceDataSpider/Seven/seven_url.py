# -*- coding: utf-8 -*-
"""
Created on Tue Jun  7 04:07:49 2022

@author: Bill
"""

import requests
import parsel
import csv
import faker
import pandas as pd #用to_csv  要df1=df1.append(df)
import sys
import os
sys.path.append(sys.path.append(os.path.abspath(os.path.join(os.getcwd(), "../.."))))
from config_spider import year,month,day,raw_url_format

df = pd.read_csv('{}{}{}/Raw_data/seven_cate.csv'.format(year,month,day))
#開啟csv檔導入url     重新儲存
csv_description = open('eBusiness/seven_scrapy_url.csv', mode='w', encoding='utf-8', newline='')
csv_writer = csv.writer(csv_description)#url格式
csv_writer.writerow(raw_url_format)
headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36'}
get_time=year+month+day
tag_id=''
num=''
page=''
for tag4_url in df[raw_url_format[2]].to_list() : #TAG_URL
    number=0
    while True:
        if tag4_url != tag4_url:
            break
        tag4_add_url=str(tag4_url)+'&pageNo={}'.format(number)     
        fake = faker.Faker() 
        headers = {'User-Agent':fake.user_agent()}
        try:
            tag4_add_url_response = requests.get(url=tag4_add_url, headers=headers, timeout=5)
            html_data = tag4_add_url_response.text
            selector = parsel.Selector(html_data)
            Details = selector.css('div.Class_Product_List p.Title a')
            if Details==[] :
                break
            if Details:
                for Detail in Details:
                    Detail_url= Detail.css('::attr(href)').get().strip()         
                    print(tag4_url,Detail_url)
                    csv_writer.writerow((get_time,tag_id,tag4_url,tag4_add_url,num,page,Detail_url))

        except requests.exceptions.RequestException as e:
                    print(e)
                    continue
        number=number+1
csv_description.close()  
df=pd.read_csv('eBusiness/seven_scrapy_url.csv')
df.drop_duplicates().to_csv('eBusiness/seven_scrapy_url.csv',index=False)
df.drop_duplicates().to_csv('{}{}{}/Raw_data/seven_scrapy_url.csv'.format(year,month,day),index=False)
df.drop_duplicates().to_csv('{}{}{}/raw_url.csv'.format(year,month,day),index=False)



