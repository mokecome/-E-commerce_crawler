# -*- coding: utf-8 -*-
"""
Created on Wed Mar  8 15:16:42 2023

@author: mokecome
"""

import requests
import pandas as pd
import numpy as np
import json
import re
import sys
import os
sys.path.append(sys.path.append(os.path.abspath(os.path.join(os.getcwd(), "../.."))))
from config_spider import year,month,day,raw_url_format,raw_product_format,combine_cud_format
'''
for c in list(set(raw_url_format).difference(set(df1.columns.tolist()))):
    df1[c]=''
df1[raw_url_format[0]]=year+month+day
'''
headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.3'}
base_url='https://urmart.com/salespage/'
df_page=pd.read_csv('{}{}{}/temp/urmart_url_page.csv'.format(year,month,day)).astype(str)
df_UP=pd.DataFrame([])
for idx, row in df_page.iterrows(): 
    tag3_url = row[raw_url_format[2]] 
    tag3_id = tag3_url.split('/')[-1].split('?')[0]
    page = row[raw_url_format[-2]]
    url='https://api.lucas.urmart.com/api/v1/member/sales/pages/custom-cached/?limit={}&offset=0&tag_id={}'.format(int(page)*30,tag3_id)
    response_t = requests.get(url=url, headers=headers)
    print(url)
    if 'tag' in tag3_url:
       
        data_json = json.loads(response_t.text,strict=False) 
        for i in data_json['result']['results']:
            spec={}
            if i['spec']!=None and i['spec']!='':
                for j in eval(i['spec']):
                    for jj in j['options']:
                        spec[jj['label']]=jj['value']+' '+jj['unit']
            if   i['plans']!=[]:
                  price= i['plans'][0]['price']                                   
            df2=pd.DataFrame({raw_url_format[2]:[tag3_url], 
                              raw_product_format[1]:[base_url+str(i['id'])],
                              raw_product_format[3]:[price],
                              raw_product_format[2]:[i['name']],
                              raw_product_format[5]:[i['short_description']],
                              raw_product_format[6]:[spec]}) 
            df_UP=pd.concat([df_UP,df2],axis =0) 
    elif 'category' in tag3_url:
        url='https://api.lucas.urmart.com/api/v1/member/sales/pages/custom-cached/?limit={}&offset=0&category_id={}'.format(int(page)*30,tag3_id)
        response_c = requests.get(url=url, headers=headers)  
        data_json = json.loads(response_c.text, strict=False) 
        for i in data_json['result']['results']:
            spec={}
            if i['spec']!=None and i['spec']!='':
                spec_origin=[]
                for j in eval(i['spec']):
                    for jj in j['options']:
                        spec_origin.append(jj)
                        spec[jj['label']]=jj['value']+' '+jj['unit']
            if  i['plans']!=[]:
                    price= i['plans'][0]['price']    
            df2=pd.DataFrame({raw_url_format[2]:[tag3_url], 
                              raw_product_format[1]:[base_url+str(i['id'])],
                              raw_product_format[3]:[price],
                              raw_product_format[2]:[i['name']],
                              raw_product_format[5]:[i['short_description']],
                              raw_product_format[6]:[spec]}) 
            df_UP=pd.concat([df_UP,df2],axis =0)  
df_UP=df_UP.reset_index(drop=True)



for c in list(set(raw_url_format+raw_product_format).difference(set(df_UP.columns.tolist()))):
    df_UP[c]=''
df_UP['GET_TIME']=year+month+day
df_UP[raw_url_format].drop_duplicates(subset=[raw_url_format[2],raw_url_format[-1]],keep='last').to_csv('{}{}{}/Raw_data/urmart_scrapy_url.csv'.format(year,month,day), index = False)
df_UP[raw_product_format].drop_duplicates(subset=[raw_url_format[-1]],keep='last').to_csv('{}{}{}/Raw_data/urmart_raw_product.csv'.format(year,month,day), index = False)

#--------------------------------------------------------

def remove_punctuations(s):  #去除標點符號和英數字
        rule = "[^\u4e00-\u9fa5']"
        rule = re.compile(rule)
        s = s.replace("’","'")
        s = rule.sub(' ',s)
        s = ' '.join(s.split())
        return s


url='https://api.lucas.urmart.com/api/v1/member/sales/pages/{}/?event_id=1701765453899&fbp=fb.1.1694139306114.1914884353&action_source=web'
headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.3'}
df_clean = pd.read_csv('{}{}{}/Raw_data/urmart_raw_product.csv'.format(year,month,day))
for idx,row in df_clean.iterrows():
    detail_id=row[raw_product_format[1]].split('/')[-1]
    response_t = requests.get(url= url.format(detail_id), headers=headers)
    data_json = json.loads(response_t.text) 
    if data_json['result']!=[]:
        data_json['result']['tag_types_attributes'].keys()
        spec={}
        for j in data_json['result']['tag_types_attributes'].keys():
            spec[j]=data_json['result']['tag_types_attributes'][j][0]['name']
       
        if row[raw_product_format[6]]=='{}' or row[raw_product_format[6]]=={}:
            df_clean.loc[idx,raw_product_format[6]]=str(spec)
df_clean[raw_product_format[5]]=df_clean[raw_product_format[5]].astype(str).apply(remove_punctuations)
df_clean[raw_product_format[5]]=df_clean[raw_product_format[5]].map(lambda x: '' if x is np.nan  else x)  
df_clean[raw_product_format[5]]=df_clean[raw_product_format[5]].apply(lambda x: ''  if x.replace(" ", "").isdigit()  else  x)



df_C = pd.read_csv('{}{}{}/Raw_data/urmart_cate.csv'.format(year,month,day)).astype(str)
df_U=pd.read_csv('{}{}{}/Raw_data/urmart_scrapy_url.csv'.format(year,month,day)).astype(str)
df_D=df_clean.astype(str)
df_CUD=pd.merge(pd.merge(df_C,df_U, how='inner'),df_D, how='inner')#交集
df_CUD[combine_cud_format[-1]]=df_CUD[raw_product_format[1]].apply(lambda x:str(x.split('/')[-1]))
df_CUD=df_CUD[combine_cud_format].drop_duplicates()
df_CUD[combine_cud_format].to_csv('{}{}{}/Combine_data/urmart_cud.csv'.format(year,month,day),index=False)



