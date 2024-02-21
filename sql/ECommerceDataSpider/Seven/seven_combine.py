# -*- coding: utf-8 -*-
"""
Created on Wed Jun  8 10:24:05 2022

@author: mokecome
"""
import requests
import parsel
import pandas as pd
import numpy as np
import json
from sklearn.feature_extraction.text import CountVectorizer
import sys
import os
sys.path.append(sys.path.append(os.path.abspath(os.path.join(os.getcwd(), "../.."))))
from config_spider import year,month,day,re_crawl_url,raw_url_format,raw_product_format,combine_cud_format


df_U =pd.read_csv('eBusiness/seven_scrapy_url.csv',encoding='utf-8',engine='python')
df_P =pd.read_csv('eBusiness/seven_raw_product.csv',encoding='utf-8',engine='python') 
headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36'}
i=0
#伊必朗折扣 無相關商品資料(有網址卻沒東西:貨到通知)  漏10條以內
while (re_crawl_url(df_U,df_P)[0]) and (i<2):
    for Detail_url in re_crawl_url(df_U,df_P)[1]:
        Detail_response = requests.get(url=Detail_url, headers=headers,stream=True)
        selector_Detail= parsel.Selector(Detail_response.text)
        if selector_Detail.css('div.prodintroright h1 strong::text').get()==np.nan:
            Detail_name = selector_Detail.css('div.prodintroright h1 strong::text').get().strip()
            desc_short= selector_Detail.css('div.prodintroright div.prodinfo p::text').getall()
            if selector_Detail.css('span#mPrice_right')!=[]:
                Oldprice= selector_Detail.css('span#mPrice_right::text').get().strip().replace('$','')
                Promprice=selector_Detail.css('span#cPrice_right::text').get().strip().replace('$','')
            elif selector_Detail.css('span#cPrice_right')!=[]:
                Oldprice= selector_Detail.css('span#cPrice_right::text').get().strip().replace('$','')
                Promprice=None
            else:
                print(Detail_url,'---no_price')
        #下半部 
            if selector_Detail.css('div.TabbedPanelsContentGroup p font')!=[]:
                features= selector_Detail.css('div.TabbedPanelsContentGroup p font::text').getall()
            
            desc_raw =selector_Detail.css('div.TabbedPanelsContentGroup font::text').getall()
    
            df2=pd.DataFrame({raw_product_format[1]:[Detail_url],  
                              raw_product_format[2]:[Detail_name],
                              raw_product_format[3]:[Oldprice],
                              raw_product_format[4]:[Promprice],
                              raw_product_format[5]:[desc_raw],
                              raw_product_format[7]:[desc_short],
                              raw_product_format[8]:[features],
                              }) 
            df_P=pd.concat([df_P, df2],axis =0)
    i=i+1
      
df_P=df_P.reset_index(drop=True)
df_P[raw_product_format[5]]=df_P[raw_product_format[5]].str.replace('、' ,'')
df_P[raw_product_format[7]]=df_P[raw_product_format[7]].str.replace('、' ,'')
df_P[raw_product_format[8]]=df_P[raw_product_format[8]].str.replace('、' ,'')
df_P.to_csv('{}{}{}/Raw_data/seven_raw_product.csv'.format(year,month,day), index = False)
#-------------------------------------------------------
def convert(text):
    try:
        lines=eval(text)
    except:
        lines=text
    if text=='':
        lines=[]
        
    results=[]
    for line in lines:
        if line.startswith("【") :
            results.append(line)
        else:
            try:
                results[-1]=results[-1]+line 
            except:
                pass          
    k=[]#下標
    for i,line in enumerate(results):
        try:   
            line0=line.split('】')[0]+'】'
            line1=line.split('】')[1]
            #處理line1為''  標記發生的下標     
            if line1=='':
                k.append(i) 
        except:
            pass   
    results_new=[]
    results_move=[]
    new_=-1
    for kk in k:
        if new_<=kk:
            s = "".join(results[kk:kk+2])
            #去掉  再加上
            results_move.append(results[kk])
            try:
                results_move.append(results[kk+1])
            except:
                pass
            results_new.append(s)
            new_=kk+2
    
    #取差集  
    results_diff = list(set(results).difference(set(results_move)))     
    results_new.extend(results_diff)         
    results1=[]   
    for line in results_new:
        try:
            line0=line.split('】')[0]+'】'
            line1=line.split('】')[1]

            results1.append(line0)
            results1.append(line1)
        except:
            pass                                       
    list1=results1[::2] 
    list2=results1[1::2] 
                                        
    results_dict=dict(zip(list1,list2))                           
    results_json = json.dumps(results_dict, ensure_ascii=False)  #存成json 
   #其他格式   
    if results_json=='[]' or results_json=='{}':
       results_json=''    
    return results_json
def specification(text):
    lines=[
 '【包裝尺寸】',
 '【產品規格】',
 '【重 量】',
 '【商品規格】',
 '【商品內容重量】',
 '【材質 規格】',
 '【商品尺寸】',
 '【產品規格/容量】',
 '【機殼規格】',
 '【規格】',
 '【/規格/包裝內容】',
 '【商品名稱規格】',
 '【包裝內容/成分/規格】',
 '【其他規格】',
 '【額定容量】',
 '【/規格/包裝內容】',
 '【尺寸約】',
 '【材質/規格】',
 '【材質 規格 包裝內容】',
 '【產品重量】',
 '【包裝規格】',
 '【規格.尺寸】',
 '【成分／規格／包裝內容】',
 '【尺寸】',
 '【規格包裝內容】',
 '【產品尺寸】',
 '【規格說明】',
 '【商品重量】',
 '【尺寸規格】',
 '【商品容量】',
 '【規格/保存期限】',
 '【重量】',
 '【淨含量】',
 '【規格/尺寸】',
 '【款式】',
 '【 商品規格】',
 '【規格/包裝內容】',
 '【商品規格 】',
 '【商品包裝】',
 '【規格/】',
 '【規格 】',
 '【材質 規格尺寸】',
 '【規格容量】',
 '【系統規格】',
 '【商品重量含包裝】']
    # eval(text) 字典
    s=''
    try:
        for i in  lines:
            s=s+eval(text).get(i,'')
            if eval(text).get(i,'')!='':   #共存就加空白
                s=s+' '
        return s
    except:
        return ''

def brand(text):
    if text=='':
        return text
    lines=['【產地、品名】','【商品品牌】','【品牌名稱】','【品牌精神】','【品牌故事】','【品牌介紹】','【品排故事】','【品牌、產品】','【品牌/品名】','【品牌 】','【品名】','【品名/保存資訊】']
    s=''
    for i in  lines:
        s=s+eval(text).get(i,'')
        if eval(text).get(i,'')!='':
            s=s+' '
    return s
def contents(text):
    lines=['【成份／重量】','【包裝內容物】','【商品成分／食品添加物】','【包裝內容/成份/規格】','【商品成分2】','【商品成分/商品規格】',
 '【主要材質】','【添  加  物】','【規格/成分/營養標示】','【材質】',
 '【食品添加物】',
 '【全成份】',
 '【包裝內容 】',
 '【/成分/規格】',
 '【商品成份】',
 '【材質/規格/包裝內容】',
 '【材質/成分】',
 '【組合內容】',
 '【成份/材質】',
 '【商品成分／食品添加物／營養標示】',
 '【材質/成分/規格】',
 '【規格/成分】',
 '【商品組合內容】',
 '【食品添加物名稱】',
 '【成分】',
 '【商品成分】',
 '【包裝內容】',
 '【成份】',
 '【材料】',
 '【成分／規格／營養標示／廠商資料】',
 '【商品成份/商品規格】',
 '【產品成分】',
 '【成份/規格/廠商資訊】',
 '【材質(成份)】',
 '【材質/成分/規格/包裝內容】',
 '【成分/規格/包裝內容】',
 '【原物料及保存標示】',
 '【內容】',
 '【食品添加物名稱/商品成分】',
 '【主要成份】',
 '【成分／規格】',
 '【其他成分】',
 '【商品成分4】',
 '【成份、其它】',
 '【商品成分3】',
 '【重量.內容】',
 '【成分/食品添加物】',
 '【商品規格/包裝內容】',
 '【主成分】',
 '【產品內容】',
 '【成分/營養標示/廠商資料】',
 '【內容物】',
 '【商品成分-2】',
 '【營養成分】',
 '【材質 成分 規格 包裝內容】',
 '【贈品/配件】',
 '【主要成分】',
 '【成份/規格】','【材質/成份/規格/包裝內容】','【膠囊成分】']
    # eval(text) 字典
    s=''
    for i in  lines:
        s=s+eval(text).get(i,'')
        if eval(text).get(i,'')!='':   #共存就加空白
            s=s+' '
    return s
# 得到描述中n-gram特征中的TopK个
def get_top_n_words(corpus, n=1,n_end=1, k=None):
    vec = CountVectorizer(ngram_range=(n, n_end), stop_words='english').fit(corpus)
    bag_of_words = vec.transform(corpus)
    sum_words = bag_of_words.sum(axis=0)
    words_freq = [(word, sum_words[0, idx]) for word, idx in vec.vocabulary_.items()]
    words_freq =sorted(words_freq, key = lambda x: x[1], reverse=True) 
    return words_freq[:k]

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

df_P=pd.read_csv('{}{}{}/Raw_data/seven_raw_product.csv'.format(year,month,day)).replace('nan','').replace(np.nan,'').astype(str)

df_P['EX_DATA']=df_P[raw_product_format[5]].apply(convert)
df_P['BRAND']=df_P['EX_DATA'].apply(brand)
#df1['PRODUCT_CONTENT']=df1[raw_product_format[5]].map(contents,na_action='ignore')
df_U=pd.read_csv('{}{}{}/Raw_data/seven_scrapy_url.csv'.format(year,month,day)).replace('nan','').replace(np.nan,'').astype(str)
df_UP=pd.merge(df_U,df_P, how='inner')#交集  

for c in list(set(raw_url_format+raw_product_format).difference(set(df_UP.columns.tolist()))):
    df_UP[c]=''
df_UP['GET_TIME']=year+month+day
df_UP[raw_url_format].drop_duplicates(subset=[raw_url_format[2],raw_url_format[-1]],keep='last').to_csv('{}{}{}/Raw_data/seven_scrapy_url.csv'.format(year,month,day),index=False)
df_UP[raw_product_format].drop_duplicates(subset=[raw_url_format[-1]],keep='last').to_csv('{}{}{}/Raw_data/seven_raw_product.csv'.format(year,month,day),index=False)

df_UP[raw_product_format[7]]=df_UP[raw_product_format[7]].apply(lambda x:''.join(eval(x)))
df_UP[raw_product_format[8]]=df_UP[raw_product_format[8]].apply(lambda x:''.join(eval(x)))
df_UP[raw_product_format[5]]=df_UP[raw_product_format[7]]+' '+df_P[raw_product_format[8]]

#處理重複的句子
'''
for st  in  [raw_product_format[8]]: 
    common_words = get_strlen_count(df_UP[st],10,20,80)   #先清10-20
    for i,_ in common_words:
        df_UP[st]=df_UP[st].str.replace(i,'')   
    common_words_1 = get_strlen_count(df_UP[st],4,9,80)   #清4-9字
    for i,_ in common_words_1:
        df_UP[st]=df_UP[st].str.replace(i,'')
'''
#剩下數字和空格就清空
def spec_clean(x):
    if (':' not in x) and (x!=''):
        return '{'+'規格:'+x.strip('')+'}'
    elif '■' in x:
        return '{'+','.join(x.split('■')).strip('')+'}'
    else:
        return np.nan     
df_UP[raw_product_format[6]]=df_UP['EX_DATA'].map(specification,na_action='ignore')
#df_UP[raw_product_format[6]]=df_UP[raw_product_format[6]].apply(spec_clean)
df_D=df_UP[raw_product_format].drop_duplicates(subset=[raw_url_format[-1]],keep='last').astype(str)
df_D.to_csv('{}{}{}/seven_de.csv'.format(year,month,day), index = False)

df_C=pd.read_csv('{}{}{}/Raw_data/seven_cate.csv'.format(year,month,day)).replace('nan','').replace(np.nan,'').astype(str)
df_U=pd.read_csv('{}{}{}/Raw_data/seven_scrapy_url.csv'.format(year,month,day)).replace('nan','').replace(np.nan,'').astype(str)

df_CUD=pd.merge(pd.merge(df_C,df_U,how='inner'),df_D, how='inner')#交集   
df_CUD[combine_cud_format[-1]]=df_CUD[raw_product_format[1]].apply(lambda x:x.split('&catid')[0].split('=')[1])
df_CUD=df_CUD[combine_cud_format].drop_duplicates()


df_CUD[combine_cud_format].to_csv('{}{}{}/Combine_data/seven_cud.csv'.format(year,month,day),index=False)


