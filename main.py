#!/usr/bin/python
# coding=utf-8
from tasks import add
from celery import group
from tasks import crawler_wallstreet
from tasks import crawler_detail

result = crawler_wallstreet.delay()
list = result.get()

data = group((crawler_detail.s(item) for item in list))().get()
#print group((crawler_detail.delay(item) for item in list))

#print group(add.s(i, i) for i in xrange(10))().get()
