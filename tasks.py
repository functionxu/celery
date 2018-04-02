#!/usr/bin/python
# coding=utf-8

from celery import Celery
import httplib
import demjson
from urlparse import urlparse
from pyquery import PyQuery
import Queue
import threading
import json
import sys
import StringIO
import gzip

app = Celery('tasks',
             broker='redis://192.168.2.102',
             backend='redis://192.168.2.102'
)

@app.task
def add(x, y):
	return x + y


@app.task
def crawler_wallstreet():
	list = crawler_list()
	return list
#	for item in list:
#		app.send_task('tasks.crawler_detail', [item])
 
def crawler_list():
	domain = 'api.wallstreetcn.com'
	port = 443

	global pageSize
	path = '/v2/pcarticles?page=0&category=most-recent&limit=20' 

	http = httplib.HTTPSConnection(domain, port, strict=False, timeout=10);
	http.request('GET', path) 
	resp = http.getresponse()

#	print resp.status
	if (resp.status == 200):
		list_json = resp.read();
		list_json = list_json.decode();
		list = demjson.decode(list_json, 'utf-8');

	return parse_list(list)
	
@app.task
def crawler_detail(item):
	url_info = urlparse(item['details_url'])
#	print url_info
#	print type(url_info);

#	if (url_info.scheme == 'http'):
#		http = httplib.HTTPConnection(url_info.netloc);		
#	else:
	http = httplib.HTTPSConnection(url_info.netloc, 443, strict=False, timeout=10);	

	headers = {
		'user-agent' : 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/59.0.3071.104 Safari/537.36'
	}
	http.request('GET', url_info.path.replace('node', 'articles'), '', headers) 
	resp = http.getresponse()
	
	print resp.status
	print url_info
	
	if (resp.status == 200):
		html = resp.read()
		print resp.getheader('content-encoding')
		if (resp.getheader('content-encoding') == 'gzip'):
			gzip_stream = StringIO.StringIO(html)
			gzipper = gzip.GzipFile(fileobj=gzip_stream)
			html = gzipper.read()

		#print html
		#sys.exit()
		item['details'] = parse_details(html)
		if (not item['details']):
			item = False;
	else:
		item = False;
	return item

def parse_details(html):
	pq = PyQuery(html)
	print pq('.node-article-content').html()

	return pq('.node-article-content').html()

def parse_list(list):
	data = []	
	for row in list['posts']:
		if (row['type'] != 'article'):
			continue
		
		item = {}
		item['title'] = row['resource']['title']
		item['time'] = row['resource']['createdAt']
		item['details_url'] = row['resource']['url']
		item['summary'] = row['resource']['summary']
		
		# 过滤掉外链
		if ('wallstreet' not in item['details_url']):
			continue
		item['list_img'] = row['resource']['imageUrl']
		print json.dumps(item, encoding='UTF-8', ensure_ascii=False)

		data.append(item)
	return data
