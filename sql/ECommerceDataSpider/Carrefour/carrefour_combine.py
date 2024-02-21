# -*- coding: utf-8 -*-
"""
Created on Wed Jun  8 10:24:05 2022

@author: mokecome
"""
import requests
import faker
import parsel
import pandas as pd
import numpy as np
import re
from sklearn.feature_extraction.text import CountVectorizer
import sys
import os
sys.path.append(sys.path.append(os.path.abspath(os.path.join(os.getcwd(), "../.."))))
from config_spider import year,month,day,re_crawl_url,raw_url_format,raw_product_format,combine_cud_format


df_U = pd.read_csv('carrefour_spider/carrefour_scrapy_url.csv')
df_P = pd.read_csv('carrefour_spider/carrefour_product.csv',encoding='utf-8',engine='python')
i=0
while (re_crawl_url(df_U,df_P)[0]) and (i<2):#重抓
    for Detail_url in re_crawl_url(df_U,df_P)[1]:
        fake = faker.Faker() 
        headers = {'User-Agent':fake.user_agent()}
        try:
            Detail_response = requests.get(url=Detail_url, headers=headers)
            selector_Detail= parsel.Selector(Detail_response.text)     
        except Exception as e:
            print(e)
        #上半部
        
        if selector_Detail.css('div.goods-info  div.title p')!=[]:
            Detail_name = selector_Detail.css('div.goods-info  div.title p::text').get().strip()
        elif selector_Detail.css('div.goods-info  div.title h1')!=[]:
            Detail_name = selector_Detail.css('div.goods-info  div.title h1::text').get().strip()
            
        if selector_Detail.css('div.title div.hot span:nth-child(1)')!=[]:
            num_sales=selector_Detail.css('div.title div.hot span:nth-child(1)::text').get().split('：')[1].strip()
        else:
            num_sales=''
        
        if selector_Detail.css('div.title div.hot span:nth-child(1) a')!=[]:
            brand=selector_Detail.css('div.title div.hot span:nth-child(1) a::text').get().strip()
        else:
            brand=None 
        
        if selector_Detail.css('div.current-money span.original-p')!=[]:
            Oldprice= selector_Detail.css('div.current-money span.original-p::text').get().strip().replace('$','')
            Promprice= selector_Detail.css('div.current-money span.money::text').get().strip().replace('$','')
        elif selector_Detail.css('div.goods-info span.current-money span.money')!=[]:
            Oldprice= selector_Detail.css('div.goods-info span.current-money span.money::text').get().strip().replace('$','')
            Promprice= None 
        else:
            print('no_price')


        #下半部 
        Description=selector_Detail.css('div.tab-wrapper div.info  p::text').getall()   
        spec=selector_Detail.css('div.spec-table tbody tr td::text').getall()
        if spec==[]:
            spec=[num_sales]
        df2=pd.DataFrame({raw_product_format[1]:[Detail_url],  
                          raw_product_format[2]:[Detail_name],
                          raw_product_format[3]:[Oldprice],
                          raw_product_format[4]:[Promprice],
                          raw_product_format[5]:[Description],
                          raw_product_format[6]:[spec],
                          raw_product_format[9]:[brand]
                          }) 
        df_P =pd.concat([df_P,df2],axis =0)
        print(brand,Oldprice)
    i=i+1  
df_P=df_P.reset_index(drop=True)
#-----------------------------------------------------------------------------
df_UP=pd.merge(df_U,df_P,how='inner')
for c in list(set(raw_url_format+raw_product_format).difference(set(df_UP.columns.tolist()))):
    df_UP[c]=''
df_UP[raw_product_format[0]]=year+month+day
df_UP[raw_url_format].drop_duplicates(subset=[raw_url_format[2],raw_url_format[-1]],keep='last').to_csv('{}{}{}/Raw_data/carrefour_scrapy_url.csv'.format(year,month,day),index=False) #index=False
df_UP[raw_product_format].drop_duplicates(subset=[raw_url_format[-1]],keep='last').to_csv('{}{}{}/Raw_data/carrefour_raw_product.csv'.format(year,month,day),index=False)
def remove_punctuations_word(s):  #去除標點符號和數字
        rule = "[^\u4e00-\u9fa5']"
        rule = re.compile(rule)
        s = s.replace("’","'")
        s = rule.sub(' ',s)
        s = s.replace("包","").replace("入","").replace("袋","").replace("克","").replace("組","")
        s = ' '.join(s.split())
        return s
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

df_UP[raw_product_format[5]]=df_UP[raw_product_format[5]].apply(remove_punctuations_word)  
#格式整理
'''
df1['容量']=df1['SPEC1'].map(lambda x: None if x == None else x.get('容量'))
df1['商品來源國家']=df1['SPEC1'].map(lambda x: None if x == None else x.get('商品來源國家'))
df1['保存天數']=df1['SPEC1'].map(lambda x: None if x == None else x.get('保存天數'))
df1['保存溫層']=df1['SPEC1'].map(lambda x: None if x == None else x.get('保存溫層'))
df1['應免稅']=df1['SPEC1'].map(lambda x: None if x == None else x.get('應免稅'))
'''

#處理重複的句子
for st  in  [raw_product_format[5]]:
    common_words = get_strlen_count(df_UP[st],6,15,30) 
    for i,_ in common_words:
          df_UP[st]=df_UP[st].str.replace(i,'')
df_UP[raw_product_format[5]]=df_UP[raw_product_format[5]].apply(lambda x:'' if '訂購者是不會收到此商品' in x else x)
#------------------------------------------------------------------------------------------------------
df_D=df_UP[raw_product_format].drop_duplicates(subset=[raw_url_format[-1]],keep='last').replace('nan','').replace(np.nan,'').astype(str)
df_D.to_csv('{}{}{}/carrefour_de.csv'.format(year,month,day),index=False)

df_C=pd.read_csv('{}{}{}/Raw_data/carrefour_cate.csv'.format(year,month,day)).replace('nan','').replace(np.nan,'').astype(str)
df_U=pd.read_csv('{}{}{}/raw_url.csv'.format(year,month,day)).replace('nan','').replace(np.nan,'').astype(str)

df_CUD=pd.merge(pd.merge(df_C,df_U,how='inner'),df_D, how='inner')#交集
df_CUD[combine_cud_format[-1]]=df_CUD[raw_product_format[1]].apply(lambda x:str(x.split('/')[-1])).str.replace('.html','')
df_CUD=df_CUD[combine_cud_format].drop_duplicates()

df_CUD[combine_cud_format].to_csv('{}{}{}/Combine_data/carrefour_cud.csv'.format(year,month,day),index=False)











