# -*- coding: utf-8 -*-
"""
Created on Wed Jun  1 18:53:50 2022

@author: Bill
"""
import requests
import pandas as pd
import json
import sys
import os
sys.path.append(sys.path.append(os.path.abspath(os.path.join(os.getcwd(), "../.."))))
from config_spider import year,month,day,raw_cate_format

if not os.path.isdir('./{}{}{}/temp'.format(year,month,day)):#判斷資料夾
    os.makedirs('./{}{}{}/temp'.format(year,month,day))
if not os.path.exists('{}{}{}/Raw_data'.format(year,month,day)):#判斷資料夾
    os.makedirs('{}{}{}/Raw_data'.format(year,month,day))
if not os.path.exists('{}{}{}/Combine_data'.format(year,month,day)):
    os.makedirs('{}{}{}/Combine_data'.format(year,month,day))



url = 'https://api.lucas.urmart.com/api/v1/member/sales/category-contents/'
headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.3'}
response = requests.get(url=url, headers=headers)
data_json = json.loads(response.text) 

for level  in data_json['result']:
    df_news=pd.DataFrame([])
    df_new=pd.DataFrame([])
    for l in data_json['result'][level]:
        print(l.get('parent'),l.get('name'),l.get('id'),l.get('link'))
        
        df2=pd.DataFrame({
                          '{}_parent'.format(level):[l.get('parent')],   
                          '{}_name'.format(level):[l.get('name')],
                          '{}_id'.format(level):[l.get('id')],
                          '{}_link'.format(level):[l.get('link')],
                          }) 
        df_new=pd.concat([df_new, df2],axis =0)
        
    df_news=pd.concat([df_news,df_new],axis =0)
    df_news.reset_index(drop=True,inplace=True)
    df_news.to_excel('./{}{}{}/temp/urmart_class_{}.xlsx'.format(year,month,day,level), index = False) 
    
df1=pd.read_excel('./{}{}{}/temp/urmart_class_level_1.xlsx'.format(year,month,day))
df2=pd.read_excel('./{}{}{}/temp/urmart_class_level_2.xlsx'.format(year,month,day))
df3=pd.read_excel('./{}{}{}/temp/urmart_class_level_3.xlsx'.format(year,month,day))

df2.rename({'level_2_parent':'level_1_id'},axis='columns',inplace=True)
df3.rename({'level_3_parent':'level_2_id'},axis='columns',inplace=True)
df12=pd.merge(df1, df2, how='outer',on='level_1_id')
df123=pd.merge(df12, df3, how='outer',on='level_2_id')
df123['level_3_link']=df123['level_3_link']
for idx, row in df123.iterrows():
    if pd.isna(row['level_3_link']):
       if 'page=1' not in row['level_2_link']:
           df123.loc[idx,'level_3_link'] =row['level_1_link']
       else:
           df123.loc[idx,'level_3_link'] =row['level_2_link']

df123[raw_cate_format[0]]=year+month+day    
df123[raw_cate_format[1]]='UrMart'
df123=df123.rename({'level_1_name': raw_cate_format[2], 'level_2_name': raw_cate_format[3], 'level_3_name':raw_cate_format[4],'level_3_link':raw_cate_format[-2]},axis='columns')



for c in list(set(raw_cate_format).difference(set(df123.columns.tolist()))):
    df123[c]=''
df123[raw_cate_format].to_csv('{}{}{}/Raw_data/urmart_cate.csv'.format(year,month,day),index=False)

 