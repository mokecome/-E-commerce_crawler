# -*- coding: utf-8 -*-
"""
Created on Tue Jul 25 14:34:49 2023

@author: mokecome
"""
import csv
import re
import requests
import pandas as pd
import parsel
import sys
import os
sys.path.append(sys.path.append(os.path.abspath(os.path.join(os.getcwd(), "../.."))))
from config_spider import year,month,day,re_crawl_url,raw_url_format,raw_product_format,combine_cud_format


headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36'}


df_U=pd.read_csv('{}{}{}/raw_url.csv'.format(year,month,day))

if  os.path.exists('{}{}{}/Raw_data//pxgo_raw_product.csv'.format(year,month,day)):
    df_P=pd.read_csv('{}{}{}/Raw_data/pxgo_raw_product.csv'.format(year,month,day))
else:
    df_P=pd.DataFrame({raw_url_format[-1]:[]})
csv_qne = open('{}{}{}/Raw_data/pxgo_raw_product.csv'.format(year,month,day), mode='a', encoding='utf-8', newline='')
csv_writer = csv.writer(csv_qne)
csv_writer.writerow([raw_product_format[1],raw_product_format[5],raw_product_format[6]])

for Detail_url in re_crawl_url(df_U,df_P)[1]:
    response = requests.get(url=Detail_url, headers=headers)
    selector= parsel.Selector(response.text)  
    desc_spec= selector.css('body').get()
    if re.findall('features:"(.*?)",specifications', desc_spec, re.S)!=[]:
        desc_raw= re.findall('features:"(.*?)",specifications', desc_spec, re.S)[0]
    elif re.findall('.*?property="og:description" content="(.*?)"', desc_spec, re.S)!=[]:
        desc_raw=re.findall('.*?property="og:description" content="(.*?)"', desc_spec, re.S)[0]
    else:
        desc_raw=''
    if re.findall('specifications:"(.*?)",sales_pitch', desc_spec, re.S)!=[]:
        spec= re.findall('specifications:"(.*?)",sales_pitch', desc_spec, re.S)[0]
    else:
        spec=''
    print(desc_raw,spec)
    csv_writer.writerow((Detail_url,desc_raw,spec))

csv_qne.close()
df_P=pd.read_csv('{}{}{}/Raw_data/pxgo_raw_product.csv'.format(year,month,day))
df_P[raw_product_format[5]]=df_P[raw_product_format[5]].str.replace('\n','')
df_P[raw_product_format[6]]=df_P[raw_product_format[6]].str.replace('\n','')

df_UP=pd.merge(df_U,df_P, how='inner')
for c in list(set(raw_url_format+raw_product_format).difference(set(df_UP.columns.tolist()))):
    df_UP[c]=''
df_UP[raw_url_format[0]]=year+month+day
df_UP[raw_url_format].drop_duplicates(subset=[raw_url_format[1],raw_url_format[-1]],keep='last').to_csv('{}{}{}/Raw_data/pxgo_scrapy_url.csv'.format(year,month,day), index = False)
df_UP[raw_product_format].drop_duplicates(subset=[raw_url_format[-1]],keep='last').to_csv('{}{}{}/Raw_data/pxgo_raw_product.csv'.format(year,month,day), index = False)




df_C=pd.read_csv('{}{}{}/Raw_data/pxgo_cate.csv'.format(year,month,day)).astype(str)
df_U=pd.read_csv('{}{}{}/Raw_data/pxgo_scrapy_url.csv'.format(year,month,day)).astype(str)
df_P=pd.read_csv('{}{}{}/Raw_data/pxgo_raw_product.csv'.format(year,month,day)).astype(str)

df_CUD=pd.merge(pd.merge(df_C,df_U, how='inner'),df_P, how='inner')#交集
df_CUD[combine_cud_format[-1]]=df_CUD[raw_product_format[1]].apply(lambda x:str(x.split('/')[-1]))#NO
df_CUD=df_CUD[combine_cud_format].drop_duplicates()

df_CUD[combine_cud_format].to_csv('{}{}{}/Combine_data/pxgo_cud.csv'.format(year,month,day),index=False)


