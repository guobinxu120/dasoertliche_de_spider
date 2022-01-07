# -*- coding: utf-8 -*-

from scrapy import Spider, Request
import re, os, requests
from collections import OrderedDict
import json, csv
from scrapy.crawler import CrawlerProcess


_keyword = 'Pflegeheim'
_city = 'Calw'

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
        # self.start_url = 'https://www.dasoertliche.de/Controller?district=&kgs=05111000&zvo_ok=0&la=&choose=true&page=0&context=0&action=43&buc=184&topKw=0&form_name=search_nat&zvo_ok=0&zbuab=&buc=184&form_name=search_nat&plz=&buab=&kgs=&quarter=&district=&ciid=&kw={}&ci={}'.format(self.keyword.replace(' ', '+'), self.city.replace(' ', '+'))
        self.start_url = 'https://www.dasoertliche.de/Controller?choose=true&page=0&context=0&action=43&topKw=0&form_name=search_nat&zvo_ok=0&kw={}&ci={}'.format(self.keyword.replace(' ', '+'), self.city.replace(' ', '+'))

    def start_requests(self):
        yield Request(self.start_url, callback= self.parse, dont_filter=True)
    def parse(self, response):
        urls = response.xpath('//div[contains(@class,"hit clearfix")]//a[@class="name "]/@href').extract()
        for cat_url in urls:
            yield Request(response.urljoin(cat_url), self.parse_urls)
            #for test
            # yield Request('https://www.dasoertliche.de/?id=2239127845228415230488&recuid=6T62SNJ3G64ZXEE5CEDFAR3IWUNTAWZHKH5EOBDYJY744AQ&action=58&pagePos=2&dar=1&kw=Pflegeheim&form_name=detail&lastFormName=search_nat&ci=Berlin&recFrom=1&hitno=24&kgs=11000000&zvo_ok=0&radius=5&orderby=name&ttforderby=rel&buc=2239&verlNr=1126&page=78&context=11', self.parse_urls)


        next_url = response.xpath('//div[@class="paging"]/span/a[@class]/@href').extract_first()
        if next_url:
            yield Request(next_url, self.parse)

    def parse_urls(self, response):

        item = OrderedDict()
        item['company name'] = response.xpath('//div[@class="name"]/h1/text()').extract_first()
        item['Name of owner'] = ''
        address = response.xpath('//div[@class="det_address"]/text()').extract()
        if len(address) > 1:
            item['Street'] = address[0].strip()
            item['City'] = address[1].strip().split(' ')[1]
            item['Zipcode'] = address[1].strip().split(' ')[0]
            if '-' in address[0]:
                item['District'] = address[1].strip().split('-')[-1].strip()
            else:
                item['District'] = ''
        elif len(address) == 1:
            item['Street'] = ''
            item['City'] = address[0].strip().split(' ')[1]
            item['Zipcode'] = address[0].strip().split(' ')[0]
            if '-' in address[0]:
                item['District'] = address[0].strip().split('-')[-1].strip()
            else:
                item['District'] = ''

        item['Tel.'] = response.xpath('//span[@itemprop="telephone"]/text()').extract_first()
        item['homepage'] = response.xpath('//span[@itemprop="url"]/text()').extract_first()
        item['Email'] = None
        item['Fax'] = None
        item['page_url'] = response.url
        email = response.xpath('//a[@class="mail"]/@href').extract_first()
        if email:
            if not 'senden' in email:
                item['Email'] = email.replace('mailto:', '')

        det_numbers = response.xpath('//table[@class="det_numbers"]/tr')
        for det in det_numbers:
            name = det.xpath('./td[@class="first"]/text()').extract_first()
            if not name:
                continue
            if 'Telefax' in name:
                fax_val = det.xpath('./td/span/text()').extract_first()
                item['Fax'] = fax_val
            elif 'Telefon' in name:
                tel_val = det.xpath('./td/span/text()').extract_first()
                item['Tel.'] = tel_val

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