# -*- coding: utf-8 -*-
"""
Created on Tue Nov 21 10:09:16 2023

@author: mokecome
"""
import requests
import pandas as pd
import json
import sys
import os
sys.path.append(sys.path.append(os.path.abspath(os.path.join(os.getcwd(), "../.."))))
from config_spider import year,month,day,raw_url_format,raw_product_format

base_url='https://m.etmall.com.tw/Product/Get?store={}&page={}'    
headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36'}
df1=pd.DataFrame([])
df=pd.read_csv('{}{}{}/Raw_data/Etmall_cate.csv'.format(year,month,day))
page=0
for tag1_id,tag3_url in zip(df[raw_url_format[1]],df[raw_url_format[2]]):
    tag3_id=tag3_url.split('/')[-1]
    r = requests.post(url=base_url.format(tag1_id,page),headers = headers,data={'cateID':tag3_id,'filterType':'','sortType':'2','moneyMinimum':'', 'moneyMaximum':'','pageSize':'10'})
    num=json.loads(r.text).get('totalProducts')
    r1 = requests.post(url=base_url.format(tag1_id,page),headers = headers,data={'cateID':tag3_id,'filterType':'','sortType':'2','moneyMinimum':'', 'moneyMaximum':'','pageSize':num})
    
    data=json.loads(r1.text).get('products')
    for p in data:
        print(p['title'])
        df2=pd.DataFrame({
                          raw_url_format[1]:[tag1_id],
                          raw_url_format[2]:[tag3_url],
                          raw_product_format[1]:['https://m.etmall.com.tw'+p['pageLink']],
                          raw_product_format[3]:[p['marketingPrice']],
                          raw_product_format[2]:[p['title']],
                            },index=[0]) 
        df1=pd.concat([df1, df2],axis =0)

df1.reset_index(inplace=True,drop=True)
for c in list(set(raw_url_format).difference(set(df1.columns.tolist()))):
    df1[c]=''
df1[raw_url_format[0]]=year+month+day
df1.to_csv('{}{}{}/Raw_data/etmall_scrapy_url.csv'.format(year,month,day), index = False)
df1.to_csv('{}{}{}/raw_url.csv'.format(year,month,day),index=False)