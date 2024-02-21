# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy


class EbusinessItem(scrapy.Item):
    # define the fields for your item here like:
    # name = scrapy.Field()
    PROD_URL= scrapy.Field()
    PROD_NM = scrapy.Field()
    DESC_SHORT=scrapy.Field()
    PRICE=scrapy.Field()
    PRICE_SALE=scrapy.Field()
    DESC_FULL=scrapy.Field()
    DESC_RAW=scrapy.Field()



