# -*- coding: utf-8 -*-

import logging

from scrapy import signals


class LianjiaSpiderMiddleware(object):

    @classmethod
    def from_crawler(cls, crawler):
        # This method is used by Scrapy to create your spiders.
        s = cls()
        crawler.signals.connect(s.spider_opened, signal=signals.spider_opened)
        crawler.signals.connect(s.spider_closed, signal=signals.spider_closed)
        crawler.signals.connect(s.engine_stopped, signal=signals.engine_stopped)
        return s

    def spider_opened(self):
        pass

    def spider_closed(self, spider, reason):
        logging.info("spider_closed------------------------------------")

    def engine_stopped(self):
        logging.info("engine_stopped-------------------------------------")
