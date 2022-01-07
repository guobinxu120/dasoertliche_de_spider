# -*- coding: utf-8 -*-

from scrapy import Spider, Request
import re, os, requests
from collections import OrderedDict
import json, csv
from scrapy.crawler import CrawlerProcess


_keyword = 'Ingenieurbüro'
_city = 'Düsseldorf'

total_list = []
def writeCsv(items):
    f1 = open("{}_{}.csv".format(_keyword.replace(' ', ''), _city.replace(' ', '')),"w", newline='')
    writer = csv.writer(f1, delimiter=',',quoting=csv.QUOTE_ALL)
    writer.writerow(items[0].keys())
    for item in items:
        writer.writerow(item.values())
    f1.close()


class dasoertlicheSpider(Spider):
    name = "dasoertliche"

    keyword = _keyword
    city = _city
    count = 0

    def __init__(self, city=None, keyword=None, *args, **kwargs):
        super(dasoertlicheSpider, self).__init__(*args, **kwargs)
        self.start_url = 'https://www.dasoertliche.de/Controller?district=&kgs=05111000&zvo_ok=0&la=&choose=true&page=0&context=0&action=43&buc=184&topKw=0&form_name=search_nat&zvo_ok=0&zbuab=&buc=184&form_name=search_nat&plz=&buab=&kgs=&quarter=&district=&ciid=&kw={}&ci={}'.format(self.keyword.replace(' ', '+'), self.city.replace(' ', '+'))

    def start_requests(self):
        yield Request(self.start_url, callback= self.parse, dont_filter=True)
    def parse(self, response):
        urls = response.xpath('//div[contains(@class,"hit clearfix")]//a[@class="name "]/@href').extract()
        for cat_url in urls:
            yield Request(response.urljoin(cat_url), self.parse_urls)

        # next_url = response.xpath('//span/a[@title="zur nächsten Seite"]/@href').extract_first()
        next_url = response.xpath('//div[@class="paging"]/span/a[@class]/@href').extract_first()
        if next_url:
            yield Request(next_url, self.parse)

    def parse_urls(self, response):
        item = OrderedDict()
        item['company name'] = response.xpath('//div[@class="name"]/h1/text()').extract_first()
        item['Name of owner'] = ''
        item['Address'] = ''.join(response.xpath('//div[@class="det_address"]/text()').extract()).strip().replace('\n', ' ')
        item['Tel.'] = response.xpath('//span[@itemprop="telephone"]/text()').extract_first()
        item['homepage'] = response.xpath('//span[@itemprop="url"]/text()').extract_first()
        item['Email'] = None
        item['page_url'] = response.url
        email = response.xpath('//a[@class="mail"]/text()').extract_first()
        if email:
            if not 'senden' in email:
                item['Email'] = email

        total_list.append(item)
        yield item


def runspider():
    process = CrawlerProcess({
        'USER_AGENT': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0.3396.99 Safari/537.36',
        'CONCURRENT_REQUESTS': 2,
        'DOWNLOAD_DELAY':1,
        'ROBOTSTXT_OBEY': False
    })

    process.crawl(dasoertlicheSpider)
    process.start() #
    writeCsv(total_list)
    return total_list

dd = runspider()