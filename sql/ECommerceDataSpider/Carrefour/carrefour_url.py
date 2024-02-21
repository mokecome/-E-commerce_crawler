# -*- coding: utf-8 -*-
"""
Created on Mon Jul 18 10:49:06 2022

@author: mokecome
"""

#擴充tag3_url
import requests
import parsel
import csv
import pandas as pd 
import faker
import sys
import os
sys.path.append(sys.path.append(os.path.abspath(os.path.join(os.getcwd(), "../.."))))
from config_spider import year,month,day,raw_url_format

csv_qne = open('carrefour_spider/carrefour_scrapy_url.csv', mode='w', encoding='utf-8', newline='')
csv_writer = csv.writer(csv_qne)
csv_writer.writerow(raw_url_format)
get_time=year+month+day
tag_id=''
num=''
page=''
df_tag3_url = pd.read_csv('{}{}{}/Raw_data/carrefour_cate.csv'.format(year,month,day)).astype(str)
for tag3_url in df_tag3_url[raw_url_format[2]].to_list():  
    print(tag3_url)
    number=0
    while True:
        tag3_add_url=tag3_url+'?start={}'.format(number)
       
        fake = faker.Faker() 
        headers = {'User-Agent':fake.user_agent()}
        tag3_add_url_response = requests.get(url=tag3_add_url, headers=headers)
        html_data = tag3_add_url_response.text
        selector = parsel.Selector(html_data)
        Details=selector.css('div.hot-recommend-item')       
        if Details==[]:
            break
        
        if Details:
            for Detail in Details:
                Detail_url = Detail.css('div.commodity-desc div a::attr(href)').get().strip()
                Detail_url='https://online.carrefour.com.tw'+Detail_url
                print(tag3_url,tag3_add_url,Detail_url)
                csv_writer.writerow((get_time,tag_id,tag3_url,tag3_add_url,num,page,Detail_url)) #詳情頁url     
        number=number+24        
csv_qne.close()


df=pd.read_csv('carrefour_spider/carrefour_scrapy_url.csv')
df.drop_duplicates().to_csv('carrefour_spider/carrefour_scrapy_url.csv',index=False)
for c in list(set(raw_url_format).difference(set(df.columns.tolist()))):
    df[c]=''
df.drop_duplicates().to_csv('{}{}{}/Raw_data/carrefour_scrapy_url.csv'.format(year,month,day),index=False)
df.drop_duplicates().to_csv('{}{}{}/raw_url.csv'.format(year,month,day),index=False)