import scrapy
from ..items import EbusinessItem
import sys
sys.path.append("C:/Users/mokecome/Desktop/Sql")
from config_spider  import raw_url_format


class A711DetailSpider(scrapy.Spider):
    name = 'seven_detail'
    #allowed_domains = ['']
    def __init__(self,name=None):
        super().__init__()
        self.name=name
    def start_requests(self): 
        import pandas as pd
        df = pd.read_csv(self.name,encoding='utf-8',engine='python')
        df=df.drop_duplicates(subset=raw_url_format[-1])
        A= df[raw_url_format[-1]].to_list()

        for detail_url in  A:
            yield scrapy.Request(url=detail_url,callback=self.parse,meta={'detail_url':detail_url})     
        
    def parse(self, response):
        
        Detail_url = response.request.meta.get('detail_url')#當前請求url
        Detail_name = response.css('div.prodintroright h1 strong::text').get().strip()
        Desc = response.css('div.prodintroright div.prodinfo p::text').getall()
         
        if response.css('span#mPrice_right'):
            Oldprice = response.css('span#mPrice_right::text').get().strip().replace('$','')
            Promprice=response.css('span#cPrice_right::text').get().strip().replace('$','')
        else:
            Oldprice= response.css('span#cPrice_right::text').get().strip().replace('$','')
            Promprice=None
       
        #下半部 
        if response.css('div.TabbedPanelsContentGroup p font')!=[]:
            features = response.css('div.TabbedPanelsContentGroup p font::text').getall()
       
        Description = response.css('div.TabbedPanelsContentGroup font::text').getall()

 
        item = EbusinessItem(PROD_URL=Detail_url,PROD_NM=Detail_name,
                      DESC_SHORT=Desc,PRICE=Oldprice,PRICE_SALE=Promprice,DESC_FULL=features,DESC_RAW=Description)
   
        if not item:  self.logger.warning("沒有爬取到{}".format(Detail_url))

        yield item
