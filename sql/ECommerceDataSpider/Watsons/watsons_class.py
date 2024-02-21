# -*- coding: utf-8 -*-
"""
Created on Tue Jul  4 17:57:26 2023

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
   
url = 'https://www.watsons.com.tw'
api_url='https://api.watsons.com.tw/api/v2/wtctw/categoryTree/{}?fields=FULL&level=2&lang=zh_TW&curr=TWD'

csv_qne = open('{}{}{}/Raw_data/watsons_cate.csv'.format(year,month,day), mode='w', encoding='utf-8', newline='')
csv_writer = csv.writer(csv_qne)
csv_writer.writerow(raw_cate_format)
headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36'}
response = requests.get(url=url, headers=headers)
html_data = response.text
selector = parsel.Selector(html_data)

get_time=year+month+day
tag4=''
tag_id=''
lis = selector.css('div.swiper-wrapper wtc-navigation-tab a') #所有分類
for li in lis:
    print('第一級-------')
    tag1 = li.css('a::text').get().strip()
    tag1_url = li.css('a::attr(href)').get().strip()
    tag1_url=url+tag1_url
    print(tag1,tag1_url)        
   
    response1 = requests.get(url=api_url.format(tag1_url.split('/')[-1]), headers=headers)
    json_data = response1.json()
    for leve in json_data['secondLevelLinks']:
        print('第二級-----')
        tag2=leve['linkName']
        tag2_url=url+leve['url']
        print(tag2,tag2_url)
        if 'childList' in leve.keys():
            for l in leve['childList']:
                tag3=l['linkName']
                tag3_url=url+l['url']
                print(l['linkName'],l['url'])
                csv_writer.writerow((get_time,'Watsons',tag1,tag2,tag3,tag4,tag3_url,tag_id))
csv_qne.close()         
