# -*- coding: utf-8 -*-
"""
Created on Wed Jul  5 14:18:39 2023

@author: mokecome
"""

import requests
import pandas as pd
import csv
import parsel
import re
import time
import sys
import os
sys.path.append(sys.path.append(os.path.abspath(os.path.join(os.getcwd(), "../.."))))
from config_spider import year,month,day,re_crawl_url,raw_url_format,raw_product_format,combine_cud_format

def remove_punctuation(s):
        rule = "[^a-zA-Z0-9\u4e00-\u9fa5']"
        rule = re.compile(rule)
        s = s.replace("’","'")
        s = rule.sub(' ',s)
        s = ' '.join(s.split())
        return s
headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36'}
df_U=pd.read_csv('{}{}{}/Raw_data/watsons_scrapy_url.csv'.format(year,month,day)).astype(str)
df_P=pd.DataFrame({raw_url_format[-1]:[]})

csv_qne = open('{}{}{}/Raw_data/watsons_raw_product.csv'.format(year,month,day), mode='w', encoding='utf-8', newline='')
csv_writer = csv.writer(csv_qne)
csv_writer.writerow([raw_product_format[1],raw_product_format[5],raw_product_format[6]])
for Detail_url in re_crawl_url(df_U,df_P)[1]:
    time.sleep(1)
    response = requests.get(url=Detail_url, headers=headers)
    html_data = response.text
    selector = parsel.Selector(html_data)
    description=''
    Desc = selector.css('div.longDesc p')
    for D in Desc: 
        if D.css('p::text')!=[]:
            if remove_punctuation(D.css('p::text').get())!='':
                description=description+D.css('p::text').get()  
    spec={}
    Spec = selector.css('table.ecTable tr') #反應過慢
    for s in Spec:
        spec[s.css('td.td1::text').get()]=s.css('td.td2::text').get()
    print(spec)
    csv_writer.writerow((Detail_url,description,spec))
csv_qne.close()
df_P=pd.read_csv('{}{}{}/Raw_data/watsons_raw_product.csv'.format(year,month,day)).astype(str)
df_UP=pd.merge(df_U,df_P, how='inner')#交集  
for c in list(set(raw_url_format+raw_product_format).difference(set(df_UP.columns.tolist()))):
    df_UP[c]=''
df_UP[raw_product_format[0]]=year+month+day
df_UP[raw_url_format].drop_duplicates(subset=[raw_url_format[2],raw_url_format[-1]],keep='last').to_csv('{}{}{}/Raw_data/watsons_scrapy_url.csv'.format(year,month,day),index=False)
df_UP[raw_product_format].drop_duplicates(subset=[raw_url_format[-1]],keep='last').to_csv('{}{}{}/Raw_data/watsons_raw_product.csv'.format(year,month,day),index=False)

df_C=pd.read_csv('{}{}{}/Raw_data/watsons_cate.csv'.format(year,month,day)).astype(str)
df_U=pd.read_csv('{}{}{}/Raw_data/watsons_scrapy_url.csv'.format(year,month,day)).astype(str)
df_D=pd.read_csv('{}{}{}/Raw_data/watsons_raw_product.csv'.format(year,month,day)).astype(str)
df_CUD=pd.merge(pd.merge(df_C,df_U,how='inner'),df_D, how='inner')#交集   

df_CUD[combine_cud_format[-1]]=df_CUD[raw_product_format[1]].apply(lambda x:str(x.split('/')[-1].split('BP_')[1]))
df_CUD=df_CUD[combine_cud_format].drop_duplicates()


df_CUD[combine_cud_format].to_csv('{}{}{}/Combine_data/watsons_cud.csv'.format(year,month,day),index=False)


