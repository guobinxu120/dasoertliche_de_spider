# -*- coding: utf-8 -*-
#!/usr/bin/python
from multiprocessing import Pool
import os, sys

_city = 'Düsseldorf'
_keyward = 'Ingenieurbüro'

def _crawl(spider_name_params=None):
	# if spider_name_params:
		# print (spider_name_params)
	print (">>>>> Starting {} spider".format(spider_name_params))
		# file_name = 'output/{}_result.csv'.format(spider_name_params)

		# os.remove(file_name)
	if len(spider_name_params) > 1:
		city = spider_name_params[0]
		keyward = spider_name_params[1]
	else:
		city = _city
		keyward = _keyward

	filepath = city.replace(' ', '') + '_'+ keyward.replace(' ', '') + '.csv'
	if os.path.isfile(filepath):
		os.remove(filepath)
	command = 'scrapy crawl dasoertliche -o {} -a city="{}" -a keyword="{}"'.format(filepath, city, keyward)

	os.system(command)
	print ("finished.")
	# return None

def run_crawler(spider_names):

	# for spider_name in spider_names:
	pool = Pool(processes=5)
	pool.map(_crawl, [spider_names])

if __name__ == '__main__':
	spider_names = []
	if len(sys.argv) == 1:
		spider_names = []
	elif len(sys.argv) > 2:
		spider_names = [sys.argv[1], sys.argv[2]]
	run_crawler(spider_names)
