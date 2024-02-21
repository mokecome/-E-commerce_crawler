# -*- coding: utf-8 -*-
"""
Created on Wed Jul  5 11:58:32 2023

@author: mokecome
"""

import requests
import parsel
import csv
import pandas as pd 
import sys
import os
sys.path.append(sys.path.append(os.path.abspath(os.path.join(os.getcwd(), "../.."))))
from config_spider import year,month,day,raw_url_format,raw_product_format

df = pd.read_csv('{}{}{}/Raw_data/watsons_cate.csv'.format(year,month,day))
#開啟csv檔導入url     重新儲存
csv_description = open('{}{}{}/Raw_data/watsons_scrapy_url.csv'.format(year,month,day), mode='w', encoding='utf-8', newline='')
csv_writer = csv.writer(csv_description)
csv_writer.writerow([raw_url_format[2],raw_url_format[3],raw_product_format[1],raw_product_format[2],raw_product_format[3]])#raw_url_format[2:4]+raw_product_format[1:4]
headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36'}
for tag3_url in df[raw_url_format[2]].to_list() : 
    number=0
    while True:
        tag3_add_url=tag3_url+'?currentPage={}'.format(number)
        tag3_add_url_response = requests.get(url=tag3_add_url, headers=headers)
        html_data = tag3_add_url_response.text
        selector = parsel.Selector(html_data)
        if selector.css('div.productPrice div.productOriginalPrice::text')!=[]:
            price=selector.css('div.productPrice div.productOriginalPrice::text').get().strip().replace('$','')
        elif selector.css('div.productPrice div.formatted-value::text')!=[]:
            price=selector.css('div.productPrice div.formatted-value::text').get().strip().replace('$','')
        else:
            print(tag3_add_url)
        Details = selector.css('e2-product-tile div.productInfo h2 a')
        if Details==[]:
            break
        if Details:
            for Detail in Details: 
                Detail_name= Detail.css('::text').get().strip()   
                Detail_url= Detail.css('::attr(href)').get().strip()   
                Detail_url='https://www.watsons.com.tw'+Detail_url
                csv_writer.writerow((tag3_url,tag3_add_url,Detail_url,Detail_name,price))
                #print(tag3_url,Detail_url,Detail_name)
        number=number+1
csv_description.close()  

df1=pd.read_csv('{}{}{}/Raw_data/watsons_scrapy_url.csv'.format(year,month,day))
for c in list(set(raw_url_format).difference(set(df1.columns.tolist()))):
    df1[c]=''
df1[raw_url_format[0]]=year+month+day
df1.to_csv('{}{}{}/raw_url.csv'.format(year,month,day),index=False)