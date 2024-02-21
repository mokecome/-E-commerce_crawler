# -*- coding: utf-8 -*-
"""
Created on Mon Jun 13 09:02:31 2022

@author: mokecome
"""

import requests
import parsel
import csv
import sys
import os
sys.path.append(sys.path.append(os.path.abspath(os.path.join(os.getcwd(), "../.."))))
from config_spider import year,month,day,raw_cate_format

if not os.path.exists('{}{}{}/Raw_data'.format(year,month,day)):#判斷資料夾
    os.makedirs('{}{}{}/Raw_data'.format(year,month,day))
if not os.path.exists('{}{}{}/Combine_data'.format(year,month,day)):
    os.makedirs('{}{}{}/Combine_data'.format(year,month,day))

url = 'https://online.carrefour.com.tw/'
base_url='https://online.carrefour.com.tw'
csv_qne = open('{}{}{}/Raw_data/carrefour_cate.csv'.format(year,month,day), mode='w', encoding='utf-8', newline='')
csv_writer = csv.writer(csv_qne)
csv_writer.writerow(raw_cate_format)
headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36'}
response = requests.get(url=url, headers=headers)
html_data = response.text
selector = parsel.Selector(html_data)
get_time=year+month+day  
source='Carrefour'
tag1_list=[]
tag2_list=[]
tag3_list=[]
tag3_url_list=[]
lis = selector.css('div.detailed-content ul.first-level li.first-level-item') #所有分類
for li in lis:
    print('第一級-------')
    tag1 = li.css('a::text').get()
    tag1_list.append(tag1)
    
lis1 = selector.css('div.category-panel-item-wrapper')
for li1 in lis1:
    tag2 = li1.css('a.category-panel-label::text').get()
    tag2_list.append(tag2)    
print(tag2_list)

lis2 = selector.css('div.category-panel-item-list')
for li2 in lis2:
    tag3 = li2.css('a.category-panel-item::text').getall()
    if li2.css('a.category-panel-item::text')!=[]:
        tag3_url = li2.css('a.category-panel-item::attr(href)').getall()
    else:
        tag3_url=''
    tag3_url=list(map(lambda x:base_url+x, tag3_url))
    print(tag3,tag3_url)  
    tag3_list.append(tag3)
    tag3_url_list.append(tag3_url)

i=0
j=0
for tag2 in tag2_list:
    if tag2 ==None:
        i=i+1
    else:
        for tag3,tag3_url in zip(tag3_list[j],tag3_url_list[j]):
            csv_writer.writerow((get_time,source,tag1_list[i].replace('\n',''),tag2.replace('\n',''),tag3.replace('\n',''),'',tag3_url,''))
        j=j+1
csv_qne.close()  