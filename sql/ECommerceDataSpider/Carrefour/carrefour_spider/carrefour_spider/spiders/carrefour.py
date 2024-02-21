import scrapy
from ..items import CarrefourSpiderItem
import sys
sys.path.append("C:/Users/mokecome/Desktop/Sql") 
from config_spider import raw_url_format
class CarrefourSpider(scrapy.Spider):
    name = 'carrefour'
    #allowed_domains = ['carrefour']
    #start_urls = ['http://carrefour/']

    def start_requests(self): 
        """重写 start_urls 规则""" 
        import pandas as pd
        df = pd.read_csv('carrefour_scrapy_url.csv',encoding='utf-8',engine='python')
        A= df[raw_url_format[-1]].to_list()
        
        for detail_url in  A:
            yield scrapy.Request(url=detail_url,callback=self.parse,meta={'detail_url':detail_url})     
        
    def parse(self, response):
        
        Detail_url = response.request.meta.get('detail_url')#當前請求url
        if response.css('div.goods-info  div.title h1'):
            Detail_name = response.css('div.goods-info  div.title h1::text').get().strip()
        elif response.css('div.goods-info  div.title p'):
            Detail_name = response.css('div.goods-info  div.title p::text').get().strip()
            
        num_sales=response.css('div.title div.hot span:nth-child(1)::text').get().split('：')[1].strip()
        
        
        if response.css('div.title div.hot span:nth-child(1) a'):
            brand=response.css('div.title div.hot span:nth-child(1) a::text').get().strip()
        else:
            brand=None
        if response.css('div.current-money span.original-p'):
            Oldprice= response.css('div.current-money span.original-p::text').get().strip().replace('$','')
            Promprice= response.css('div.current-money span.money::text').get().strip().replace('$','')
        else:
            Oldprice= response.css('div.goods-info span.current-money span.money::text').get().strip().replace('$','')
            Promprice= None 

        #下半部 
        Description=response.css('div.tab-wrapper div.info  p::text').getall()
         
        spec=response.css('div.spec-table tbody tr td::text').getall()
        if spec==[]:
            spec=[num_sales]
 
        item = CarrefourSpiderItem(PROD_URL=Detail_url,PROD_NM=Detail_name,SPEC=spec,BRAND=brand,
                                 PRICE=Oldprice,PRICE_SALE=Promprice,DESC_RAW=Description)
   

        yield item


