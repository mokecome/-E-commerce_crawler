# -*- coding: utf-8 -*-
"""
Created on Mon Jul 10 16:46:25 2023

@author: mokecome
"""
import json
import requests
import pandas as pd
import re
from sklearn.feature_extraction.text import CountVectorizer
import sys
import os
sys.path.append(sys.path.append(os.path.abspath(os.path.join(os.getcwd(), "../.."))))
from config_spider import year,month,day,re_crawl_url,raw_product_format,raw_url_format,combine_cud_format


def remove_punctuations(s):  #去除標點符號和數字
        rule = "[^\u4e00-\u9fa5']"
        rule = re.compile(rule)
        s = s.replace("’","")
        s = rule.sub(' ',s)
        s = ' '.join(s.split())
        return s
    

headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36'}
df_U=pd.read_csv('{}{}{}/Raw_data/uniqlo_scrapy_url.csv'.format(year,month,day)).astype(str)

if  os.path.exists('{}{}{}/Raw_data/uniqlo_raw_product.csv'.format(year,month,day)):
    df_P=pd.read_csv('{}{}{}/Raw_data/uniqlo_raw_product.csv'.format(year,month,day))
else:
    df_P=pd.DataFrame({raw_url_format[-1]:[]})


for detail_url in re_crawl_url(df_U,df_P)[1]:
    url='https://www.uniqlo.com/tw/data/products/spu/zh_TW/{}.json'.format(detail_url.split('productCode=')[-1])
    r1 = requests.get(url=url,headers = headers)
    product=json.loads(r1.text) #含商品分類 共39個
    price=product.get('summary')['originPrice']
    Detail_name=product.get('summary')['name']
    desc=product.get('desc')['instruction']
    # spec={}
    # try:
    #     if '商品材質' in desc:
    #         spec={d.split(':')[0]:d.split(':')[1] for d in desc.split('商品材質</strong><br />\n')[1].split('<br />')[0].split('/')}
    #         desc=desc.replace(desc.split('商品材質</strong><br />\n')[1].split('<br />')[0],'')
    # except:
    #     pass
    print(desc)

    df2=pd.DataFrame({    
            raw_product_format[1]:[detail_url],
            raw_product_format[2]:[Detail_name],
            raw_product_format[3]:[price],
            raw_product_format[5]:[desc],
            #raw_product_format[6]:[spec]
                  })
    df_P=pd.concat([df_P,df2],axis =0)
df_P=df_P.reset_index(drop=True)


for c in list(set(raw_product_format).difference(set(df_P.columns.tolist()))):
    df_P[c]=''
df_P[raw_product_format[0]]=year+month+day
df_P[raw_product_format].drop_duplicates(subset=[raw_url_format[-1]],keep='last').to_csv('{}{}{}/Raw_data/uniqlo_raw_product.csv'.format(year,month,day), index = False)


def get_strlen_count(corpus, n=1,n_end=1,freq=20 ):
    vec = CountVectorizer(ngram_range=(n, n_end)).fit(corpus)
    bag_of_words = vec.transform(corpus)
    sum_words = bag_of_words.sum(axis=0)
    words_freq=[]
    for word, idx in vec.vocabulary_.items():# 
        if sum_words[0, idx]>freq:
            words_freq.append((word, sum_words[0, idx]))       
    words_freq =sorted(words_freq, key = lambda x: (len(x[0]),x[1]), reverse=True)  #頻次>200 的 len(x[0])  #多級排序
    return words_freq
#處理重複的句子
for st  in  [raw_product_format[5]]:
    common_words = get_strlen_count(df_P[st],8,25,20)   #先清4-15字  頻次取30
    for i,_ in common_words:
        df_P[st]=df_P[st].str.replace(i,'')

df_P[raw_product_format[5]]=df_P[raw_product_format[5]].apply(lambda x:x.split('洗滌方式')[0].split('注意事項')[0])
df_P[raw_product_format[5]]=df_P[raw_product_format[5]].apply(remove_punctuations)
df_P.to_csv('{}{}{}/uniqlo_de.csv'.format(year,month,day), index = False)
#合併
df_C=pd.read_csv('{}{}{}/Raw_data/uniqlo_cate.csv'.format(year,month,day)).astype(str)
df_U=pd.read_csv('{}{}{}/Raw_data/uniqlo_scrapy_url.csv'.format(year,month,day)).astype(str)
df_D=pd.read_csv('{}{}{}/uniqlo_de.csv'.format(year,month,day)).astype(str)
df_CUD=pd.merge(pd.merge(df_C,df_U,how='inner'),df_D, how='inner')#交集
df_CUD[combine_cud_format[-1]]=df_CUD[raw_product_format[1]].apply(lambda x:str(x.split('productCode=')[-1]))
df_CUD=df_CUD[combine_cud_format].drop_duplicates()

df_CUD[combine_cud_format].to_csv('{}{}{}/Combine_data/uniqlo_cud.csv'.format(year,month,day),index=False)


