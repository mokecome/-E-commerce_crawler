# -*- coding: utf-8 -*-
"""
Created on Wed Jun 15 17:36:35 2022

@author: mokecome
"""
import json
import requests
import pandas as pd
import sys
import os
sys.path.append(sys.path.append(os.path.abspath(os.path.join(os.getcwd(), "../.."))))
from config_spider import year,month,day,raw_cate_format
  
if not os.path.isdir('{}{}{}/Raw_data'.format(year,month,day)):
   os.makedirs('{}{}{}/Raw_data'.format(year,month,day))
if not os.path.isdir('{}{}{}/Combine_data'.format(year,month,day)):
   os.makedirs('{}{}{}/Combine_data'.format(year,month,day))
   
url='https://api-pxbox.es.pxmart.com.tw/app/1.0/spu/get_categories'#隔日達otherday  
headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36'}
r1 = requests.get(url=url,headers = headers)
df1=pd.DataFrame([])

get_time=year+month+day
content=json.loads(r1.text).get('data')
for c in content:
    if c['children']==[]:
        print(c['Name'],c['Id'])
    else:
        for cc in c['children']:
            if cc['children']==[]:
                df2=pd.DataFrame({raw_cate_format[1]:'Pxgo',
                                  raw_cate_format[2]:[c['category_name']],
                                  'TAG1_ID':[c['category_id']],  
                                  raw_cate_format[3]:[cc['category_name']],
                                  'TAG2_ID':[cc['category_id']],
                                  raw_cate_format[4]:[''],
                                  'TAG3_ID':[0], #沒有填0
                                    },index=[0])
                df1=pd.concat([df1, df2],axis =0)
                print(c['category_name'],c['category_id'],cc['category_name'],cc['category_id'])
            else:
                for ccc in cc['children']:
                    df2=pd.DataFrame({raw_cate_format[1]:'Pxgo',
                                      raw_cate_format[2]:[c['category_name']],
                                      'TAG1_ID':[c['category_id']],  
                                      raw_cate_format[3]:[cc['category_name']],
                                      'TAG2_ID':[cc['category_id']],
                                      raw_cate_format[4]:[ccc['category_name']],
                                      'TAG3_ID':[ccc['category_id']],
                                        },index=[0])
                    df1=pd.concat([df1, df2],axis =0)
                    print(c['category_name'],c['category_id'],cc['category_name'],cc['category_id'],ccc['category_name'],ccc['category_id'])
                    
# https://pxbox.es.pxmart.com.tw/category/{}/{}/{}
df1.reset_index(inplace=True)
df1[raw_cate_format[-1]]=0
for idx,row in df1.iterrows(): 
    tag2_id=row['TAG2_ID']
    tag3_id=row['TAG3_ID']
    if tag3_id==0:
        df1.loc[idx,raw_cate_format[-1]]=tag2_id
    else:
        df1.loc[idx,raw_cate_format[-1]]=tag3_id

for c in list(set(raw_cate_format).difference(set(df1.columns.tolist()))):
    df1[c]=''
df1[raw_cate_format[0]]=get_time
df1[raw_cate_format].to_csv('{}{}{}/Raw_data/pxgo_cate.csv'.format(year,month,day), index = False)
    






