import pandas as pd
import requests
import json
import parsel
import sys
import os
sys.path.append(sys.path.append(os.path.abspath(os.path.join(os.getcwd(), "../.."))))
from config_spider import year,month,day,raw_cate_format
 
if not os.path.exists('{}{}{}/Raw_data'.format(year,month,day)):#判斷資料夾
    os.makedirs('{}{}{}/Raw_data'.format(year,month,day))
if not os.path.exists('{}{}{}/Combine_data'.format(year,month,day)):
    os.makedirs('{}{}{}/Combine_data'.format(year,month,day))
   
url='https://m.etmall.com.tw/Category/GetStores'
cate_url='https://m.etmall.com.tw/category/{}'
headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36'}
response = requests.post(url, headers=headers)
html_data = response.text
df1=pd.DataFrame([])
for c in json.loads(html_data):
    print(c['cateID'],c['cateName'])
    url=cate_url.format(c['cateID'])
    response = requests.get(url, headers=headers)
    html_data = response.text    
    selector = parsel.Selector(html_data)
    lis = selector.css('script::text').getall()                
    for cc in json.loads([l for l in lis if 'var model = {' in l][0].split(',"HotTopics":')[0].split('"Collections":')[1]):
        if cc['Childs']!=None:
            for ccc in cc['Childs']:
                print(ccc['CateName'],'https://m.etmall.com.tw'+ccc['PageLink'])
                df2=pd.DataFrame({raw_cate_format[1]:['Etmall'],
                                  raw_cate_format[2]:[c['cateName']],
                                  raw_cate_format[-1]:[c['cateID']],
                                  raw_cate_format[3]:[cc['CateName']],
                                  raw_cate_format[4]:[ccc['CateName']],
                                  raw_cate_format[-2]:['https://m.etmall.com.tw'+ccc['PageLink']]
                                  },index=[0])
                df1=pd.concat([df1,df2],axis =0)
df1.reset_index(drop=True,inplace=True)   


for c in list(set(raw_cate_format).difference(set(df1.columns.tolist()))):
    df1[c]=''
df1[raw_cate_format[0]]=year+month+day    
df1=df1[df1[raw_cate_format[2]]=='寵物']
df1[raw_cate_format].to_csv('{}{}{}/Raw_data/etmall_cate.csv'.format(year,month,day), index = False)
