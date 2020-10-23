# -*- coding: utf-8 -*-

DB_CONFIG = {
    'host': '10.36.166.37',
    'port': 5002,
    'user': 'test_seagull',
    'password': 'test_seagull',
    'database': 'lianjia'
}

BOT_NAME = 'lianjia'

SPIDER_MODULES = ['spiders']
NEWSPIDER_MODULE = 'spiders'

LOG_LEVEL = "INFO"

ROBOTSTXT_OBEY = False

SPIDER_MIDDLEWARES = {
    'lianjia.middlewares.LianjiaSpiderMiddleware': 1,
}

ITEM_PIPELINES = {
    'lianjia.pipelines.LianjiaPipeline': 1,
}
