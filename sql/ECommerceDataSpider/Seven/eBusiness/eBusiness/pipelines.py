# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html

import os
# useful for handling different item types with a single interface
import csv
from itemadapter import ItemAdapter

from .items import EbusinessItem

#import logging
class EbusinessPipeline:
    def process_item(self, item, spider):
        return item
filter_list = []
class CSVpipeline:
    def __init__(self):
        self.f = open('seven_raw_product.csv', mode='w', encoding='utf-8', newline='')
        self.csv_write = csv.DictWriter(self.f, fieldnames=['PROD_URL','PROD_NM','DESC_SHORT','PRICE','PRICE_SALE','DESC_FULL','DESC_RAW'])
        self.csv_write.writeheader()

    def process_item(self, item, spider):
        #if not item: logging.warning("無資料匯出!")

        if item not in filter_list:
            # 当前这个item是第一次返回的结果
            filter_list.append(item)
            d = dict(item)
      
            self.csv_write.writerow(d)
      
        return item

    def close_spider(self, spider):
        self.f.close()