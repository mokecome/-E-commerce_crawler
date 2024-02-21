# -*- coding: utf-8 -*-
"""
Created on Tue Nov 21 11:43:40 2023

@author: mokecome
"""
import csv
import requests
import pandas as pd
import re
import sys
import os
sys.path.append(sys.path.append(os.path.abspath(os.path.join(os.getcwd(), "../.."))))
from config_spider import year,month,day,re_crawl_url,raw_url_format,raw_product_format,combine_cud_format,get_gzip_compressed
    
# def clear_spec(s):
#         rule = "[^、()\u4e00-\u9fa5']"
#         rule = re.compile(rule)
#         s = s.replace("’","'")
#         s = rule.sub(' ',s)
#         s = ' '.join(s.split())
#         return s   

def remove_upprintable_chars(s):
    s=s.replace('\u200b','')
    s=s.replace('\ufeff','')
    s=s.replace('/p','')
    s=s.replace('\\n',' ')
    s=s.replace('\\u003cp','')
    s=s.replace('\\u003cbr','')
    s=s.replace('\\u003c','')
    s=s.replace('\\u003e','')
    return s

headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36'}
df_U=pd.read_csv('{}{}{}/Raw_data/etmall_scrapy_url.csv'.format(year,month,day)).astype(str)
if  os.path.exists('{}{}{}/Raw_data//etmall_raw_product.csv'.format(year,month,day)):
    df_P=pd.read_csv('{}{}{}/Raw_data/etmall_raw_product.csv'.format(year,month,day))
else:
    df_P=pd.DataFrame({raw_url_format[-1]:[]})
i=0
csv_description = open('{}{}{}/Raw_data/etmall_raw_product.csv'.format(year,month,day), mode='a', encoding='utf-8', newline='')
csv_writer = csv.writer(csv_description)
csv_writer.writerow([raw_product_format[1],raw_product_format[5],'PAGE_SOURCE'])
while (re_crawl_url(df_U,df_P)[0]) and (i<2):
    for idx,detail_url in enumerate(re_crawl_url(df_U,df_P)[1]):
        response = requests.get(url=detail_url,headers = headers)
        html_data = response.text 
        Description = re.findall('<meta name="description" content="(.*?)" />', html_data, re.S)
        if Description!=[]:
            Description = remove_upprintable_chars(Description[0])
            if Description.isalnum():
                Description='' 
        else:
            Description =''
        
        # if len(html_data.split('營養分析：'))>1:
        #     spec['營養分析']=remove_upprintable_chars(html_data.split('營養分析：')[1].split('※')[0])
        # else:
        #     spec['營養分析']=''
        # if len(html_data.split('主成分'))>1:    
        #     spec['主成分']=clear_spec(html_data.split('主成分')[1].split('成分說明')[0])
        # else:
        #     spec['主成分']=''
        # if len(html_data.split('成分說明'))>1:  
        #     spec['成分說明']=clear_spec(html_data.split('成分說明')[1].split('：')[0])
        # else:
        #     spec['成分說明']=''
        print(Description) 
        csv_writer.writerow((detail_url,Description,get_gzip_compressed(html_data)))
    df_P=pd.read_csv('{}{}{}/Raw_data/etmall_raw_product.csv'.format(year,month,day))
    i=i+1
#clear
df_P.reset_index(inplace=True,drop=True)
df_P.to_csv('{}{}{}/etmall_de.csv'.format(year,month,day),index=False)
df_UP=pd.merge(df_U,df_P, how='inner')#交集  
for c in list(set(raw_url_format+raw_product_format).difference(set(df_UP.columns.tolist()))):
    df_UP[c]=''
df_UP[raw_product_format[0]]=year+month+day
df_UP[raw_url_format].drop_duplicates(subset=[raw_url_format[2],raw_url_format[-1]],keep='last').to_csv('{}{}{}/Raw_data/etmall_scrapy_url.csv'.format(year,month,day),index=False)
df_UP[raw_product_format].drop_duplicates(subset=[raw_url_format[-1]],keep='last').to_csv('{}{}{}/Raw_data/etmall_raw_product.csv'.format(year,month,day),index=False)

df_C=pd.read_csv('{}{}{}/Raw_data/etmall_cate.csv'.format(year,month,day)).astype(str)
df_U=pd.read_csv('{}{}{}/Raw_data/etmall_scrapy_url.csv'.format(year,month,day)).astype(str)
df_D=pd.read_csv('{}{}{}/Raw_data/etmall_raw_product.csv'.format(year,month,day)).astype(str)
df_CUD=pd.merge(pd.merge(df_C,df_U,how='inner'),df_D, how='inner')#交集   
df_CUD[combine_cud_format[-1]]=df_CUD[raw_product_format[1]].apply(lambda x:str(x.split('/')[-1].split('?')[0]))
df_CUD=df_CUD[combine_cud_format].drop_duplicates()

df_CUD[combine_cud_format].to_csv('{}{}{}/Combine_data/etmall_cud.csv'.format(year,month,day),index=False)
