# -*- coding: utf-8 -*-
"""
Created on Mon Jan 15 13:04:00 2024

@author: mokecome
"""
import os
import subprocess
import time
import concurrent.futures
import pandas as pd
from config_spider import config,year,month,day,get_last_month,Tag_Check,Source_list

def cmd(command,work_cwd):
    subp = subprocess.Popen(command, shell=True,cwd=work_cwd)
    print(command)
    subp.wait()
    print('命令結束')
    
class Spider:
    def __init__(self,num=0):
        print('當前路徑:',os.getcwd().replace('\\','/'))
        self.num=num
    def Crawl_data(self,Source):
        for c in  config[Source]: 
            if 'scrapy' in c: 
                temp=c.split(';')[0]
                cc=c.split(';')[1]
                cmd(cc,f'./ECommerceDataSpider/{Source}/{temp}')
            else:
                print(f'./ECommerceDataSpider/{Source}')
                cmd(c,f'./ECommerceDataSpider/{Source}')
    def Many_Crawl(self,S_list):
        with concurrent.futures.ProcessPoolExecutor(max_workers=6) as executor:
            for Source in self.S_list:
                executor.submit(self.Crawl_data, Source)

            
if __name__ == '__main__':
    spider=Spider()
    Source = input("請輸入要抓取的電商:")
    while Source not in Source_list: 
        Source = input("請輸入要抓取的電商:")
    time_1 = time.time()
    spider.Crawl_data(Source)
    time_2 = time.time()    
    use_time = int(time_2) - int(time_1)
    print(Source,use_time)
    #欄位檢查 
    if os.path.isfile(f'./ECommerceDataSpider/{Source}/{get_last_month()}/Combine_data/{Source.lower()}_cud.csv'):
        df_now=pd.read_csv(f'./ECommerceDataSpider/{Source}/{year}{month}{day}/Combine_data/{Source.lower()}_cud.csv')
        df_pre=pd.read_csv(f'./ECommerceDataSpider/{Source}/{get_last_month()}/Combine_data/{Source.lower()}_cud.csv')
        print(Tag_Check(df_pre,df_now,'TAG1',0.3,100)) #PRICE,PRICE_SALE..
      
    #spider.Many_Crawl(['Seven','Carrefour','Trplus','UrMart','Organic','Uniqlo','Watsons','Healthyliving','Decathlon','Etmall','Petpetgo','Pxgo','Ikea'])
    #spider.Many_Crawl(['momo','Eslite'])
    #spider.Many_Crawl(['Net','Poya','Medfirst'])