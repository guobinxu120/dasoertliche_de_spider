# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://doc.scrapy.org/en/latest/topics/item-pipeline.html
from scrapy import signals
import os, csv
from collections import OrderedDict


class DasoertlicheDeSpiderPipeline(object):
    @classmethod
    def from_crawler(cls, crawler):
        pipeline = cls()
        crawler.signals.connect(pipeline.spider_opened, signals.spider_opened)
        crawler.signals.connect(pipeline.spider_closed, signals.spider_closed)
        return pipeline


    def spider_opened(self, spider):
        pass

    def spider_closed(self, spider):
        filename = spider.city.replace(' ', '') + '_'+ spider.keyword.replace(' ', '') + '.csv'
        f1 = open(filename,"w")
        writer = csv.writer(f1, delimiter=',',quoting=csv.QUOTE_ALL)
        writer.writerow(spider.models[0].keys())

        for data in spider.models:
            writer.writerow(data.values())
        f1.close()

    def process_item(self, item, spider):

        return item
