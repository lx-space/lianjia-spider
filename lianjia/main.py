import os
import sys
from scrapy.cmdline import execute

# 添加当前项目的绝对地址
sys.path.append(os.path.dirname(os.path.abspath(__file__)))


def community():
    # 执行爬取小区
    execute(['scrapy', 'crawl', 'community'])


def selling_house():
    execute(['scrapy', 'crawl', 'selling_house'])


def sold_house():
    execute(['scrapy', 'crawl', 'sold_house'])


if __name__ == '__main__':
    sold_house()
