# -*- coding: utf-8 -*-

import logging
import pymysql

from scrapy import signals
from pydispatch import dispatcher

from lianjia import settings
from lianjia.items import SoldHouseItem, CommunityItem, SellingHouseItem
from spiders.community_spider import CommunitySpider
from spiders.sold_house_spider import SoldHouseSpider
from spiders.selling_house_spider import SellingHouseSpider


class LianjiaPipeline(object):
    db = None
    cur = None
    amount = 0
    list = []

    def __init__(self) -> None:
        dispatcher.connect(self.spider_closed, signal=signals.spider_closed)
        self.db = pymysql.connect(**settings.DB_CONFIG)
        self.cur = self.db.cursor(cursor=pymysql.cursors.DictCursor)

    # 爬虫执行完成后执行保存动作
    def spider_closed(self, spider):
        logging.info('数据采集完毕---------------------------------------------------执行保存操作')
        if spider.name == CommunitySpider.name:
            item = CommunityItem()
            item['finish'] = True
            self.process_item(item, spider)
        elif spider.name == SellingHouseSpider.name:
            item = SellingHouseItem()
            item['finish'] = True
            self.process_item(item, spider)
        elif spider.name == SoldHouseSpider.name:
            item = SoldHouseItem()
            item['finish'] = True
            self.process_item(item, spider)
        self.db.close()
        self.cur.close()

    def process_item(self, item, spider):
        if spider.name == CommunitySpider.name:
            return self.save_community(item)
        elif spider.name == SellingHouseSpider.name:
            return self.save_selling_house(item)
        elif spider.name == SoldHouseSpider.name:
            return self.save_sold_house(item)

    # 存储小区信息
    def save_community(self, item):
        # 查询该小区信息是否已经保存
        sql = 'select id, code, name from community where code = {}'.format(item['code'])
        self.cur.execute(sql)
        row = self.cur.fetchone()
        if row:
            logging.info("小区已保存 小区名称:{}".format(item['name']))
            return item

        logging.info("保存小区数据 小区名称:{} 已保存数量:{}".format(item['name'], self.amount))
        sql = 'insert into  community(code, name, district, version) ' \
              'values("{}", "{}", "{}", "{}")'.format(item['code'], item['name'], item['district'], item['version'])
        self.cur.execute(sql)
        self.db.commit()
        self.amount += 1
        return item

    #  存储售卖中的房源信息
    def save_selling_house(self, item):
        sql = 'select id, code from selling_house where code = {}'.format(item['code'])
        self.cur.execute(sql)
        row = self.cur.fetchone()
        if row:
            logging.info("在售房源已保存 房源名称:{}".format(item['title']))
            return item

        logging.info("保存在售房源数据 房源名称:{} 已保存数量:{}".format(item['title'], self.amount))
        sql = 'insert into selling_house(code, community_code, title, price, price_per, ' \
              'price_unit, type, size, on_sale_date, deleted) ' \
              'values("{}", "{}", "{}", "{}", "{}",' \
              '"{}", "{}", "{}", "{}", "{}")' \
            .format(item['code'], item['community_code'], item['title'], item['price'], item['price_per'],
                    item['price_unit'], item['type'], item['size'], item['on_sale_date'], item['deleted'])
        self.cur.execute(sql)
        self.db.commit()
        self.amount += 1
        return item

    #  存储售出的房源信息
    def save_sold_house(self, item):
        sql = 'select id, code from sold_house where code = {}'.format(item['code'])
        self.cur.execute(sql)
        row = self.cur.fetchone()
        if row:
            logging.info("已售房源已保存 房源名称:{}".format(item['title']))
            return item

        logging.info("保存已售房源数据 房源名称:{} 已保存数量:{}".format(item['title'], self.amount))
        sql = 'insert into sold_house(code, community_code, title, selling_price, sold_price, ' \
              'sold_price_per, price_unit, type, size, on_sale_date, sold_date) ' \
              'values("{}", "{}", "{}", "{}", "{}", ' \
              '"{}", "{}", "{}", "{}", {}, "{}")' \
            .format(item['code'], item['community_code'], item['title'], item['selling_price'], item['sold_price'],
                    item['sold_price_per'], item['price_unit'], item['type'], item['size'], item['on_sale_date'],
                    item['sold_date'])
        self.cur.execute(sql)
        self.db.commit()
        self.amount += 1
        return item
