# -*- coding: utf-8 -*-
"""
Created on Wed Jun  1 18:53:50 2022

@author: Bill
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
   
url = 'https://shop.7-11.com.tw/shop/rui001.faces'
headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36'}
response_cookie = requests.get(url=url, headers=headers)
cookies = dict(response_cookie.cookies) #cookies= requests.utils.dict_from_cookiejar(response_cookie.cookies)
response = requests.get(url=url, headers=headers, cookies=cookies)
html_data = response.text
selector = parsel.Selector(html_data)


#類別
lis = selector.css('div#navbar ul li')[:9]
csv_qne = open('{}{}{}/Raw_data/seven_cate.csv'.format(year,month,day), mode='w', encoding='utf-8', newline='')
csv_writer = csv.writer(csv_qne)
csv_writer.writerow(raw_cate_format)
base_url='https://shop.7-11.com.tw/shop/'

get_time=year+month+day
source='Seven'
tag_id=''
for li in lis:
    print('第一級-------')
    tag_name1 = li.css('h2::text').get().strip()  
    print(tag_name1)
    print('第二級--------------')
    for li2 in  li.css('div.ClassItem'):
        tag_name2 = li2.css('h2 a::text').get().strip()
        #url  
        li_href2 = li2.css('h2 a::attr(href)').get().strip()
        li_url2=base_url+li_href2 
        print(tag_name2,li_url2)
        if tag_name2=='用品/醫材/18禁情趣商品':
            for t in li2.css('dl dt'):
                tag_name3=t.css('h3 a::text').get().strip()
                tag_url3=t.css('h3 a::attr(href)').get().strip()
                csv_writer.writerow(('7Eleven',tag_name1,tag_name2,tag_name3,'',tag_url3))
        print('第三級---------------')
        response2 = requests.get(url=li_url2, headers=headers)
        html_data2 = response2.text
        selector2= parsel.Selector(html_data2)
        lis3 = selector2.css('ul.class2 >li')
        for li3 in lis3:
            
            tag_name3 = li3.css('h3 a::text').get().strip() 
            print('第四級---------------------------------')
            for i,li33 in enumerate(li3.css('ul.class3 >li >h3')):
                tag_name4=li33.css('a::text').get().replace('\n','')
                tag_url4=li33.css('a::attr(href)').get() #詳情頁
                csv_writer.writerow((get_time,source,tag_name1.replace('\n',''),tag_name2.replace('\n',''),tag_name3.replace('\n',''),tag_name4.replace('\n',''),tag_url4,tag_id))
csv_qne.close()



  