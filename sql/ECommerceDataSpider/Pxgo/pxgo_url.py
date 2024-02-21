# -*- coding: utf-8 -*-
"""
Created on Thu Jun 16 11:56:25 2022

@author: mokecome
"""
import requests
import pandas as pd
import json
import sys
import os
sys.path.append(sys.path.append(os.path.abspath(os.path.join(os.getcwd(), "../.."))))
from config_spider import year,month,day,raw_url_format,raw_product_format

headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36'}
get_product_url='https://api-pxbox.es.pxmart.com.tw/app/1.0/spu/get_products?CategoryId={}&SortType=0&PageIndex={}&PageSize=20' 

df1=pd.DataFrame([])
df=pd.read_csv('{}{}{}/Raw_data/pxgo_cate.csv'.format(year,month,day))
for tag_id in df[raw_url_format[1]]:
    PageIndex=1
    while True:
        r1 = requests.get(url=get_product_url.format(tag_id,PageIndex),headers = headers)
        data=json.loads(r1.text).get('data')
        for p in data['products']:
            print(get_product_url.format(tag_id,PageIndex),p['product_id'])
            df2=pd.DataFrame({
                              raw_url_format[1]:[tag_id],
                              raw_url_format[-1]:['https://pxbox.es.pxmart.com.tw/product/{}'.format(p['product_id'])],
                              raw_product_format[2]:[p['product_name']],  
                              raw_product_format[3]:[p['market_price']],
                              raw_product_format[4]:[p['sale_price']],
                                },index=[0]) 
            df1=pd.concat([df1, df2],axis =0)
        PageIndex=PageIndex+1
        if data['page_count']<PageIndex:
            break
        
df1.reset_index(inplace=True,drop=True)
for c in list(set(raw_url_format).difference(set(df1.columns.tolist()))):
    df1[c]=''
df1[raw_url_format[0]]=year+month+day
df1.to_csv('{}{}{}/Raw_data/pxgo_scrapy_url.csv'.format(year,month,day), index = False)
df1.to_csv('{}{}{}/raw_url.csv'.format(year,month,day),index=False)



   

