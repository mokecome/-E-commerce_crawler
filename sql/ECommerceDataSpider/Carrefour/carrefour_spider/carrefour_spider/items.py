# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy


class CarrefourSpiderItem(scrapy.Item):
    # define the fields for your item here like:
    # name = scrapy.Field()
    PROD_URL=scrapy.Field()
    PROD_NM=scrapy.Field()
    SPEC =scrapy.Field()
    BRAND=scrapy.Field()
    PRICE=scrapy.Field()
    PRICE_SALE=scrapy.Field()
    DESC_RAW=scrapy.Field()


