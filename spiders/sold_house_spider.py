import json
import scrapy
import logging
import pymysql

from lianjia import settings
from scrapy.utils.project import get_project_settings
from lianjia.items import CommunityItem, SoldHouseItem

"""
爬取房屋信息
"""


class SoldHouseSpider(scrapy.Spider):
    name = 'sold_house'
    base_url = 'https://cd.lianjia.com/chengjiao/'

    def __init__(self, name=None, db_password=None, **kwargs):
        if db_password is not None and db_password != '':
            settings_copy = get_project_settings()
            db_conf = settings_copy.get('DB_CONFIG')
            db_conf['password'] = db_password
            settings_copy.set('DB_CONFIG', db_conf)
        super().__init__(name, **kwargs)

    def start_requests(self):
        sql = 'select * from community where sold_house_status = 0'
        db = pymysql.connect(**settings.DB_CONFIG)
        cur = db.cursor(cursor=pymysql.cursors.DictCursor)
        cur.execute(sql)
        rows = cur.fetchall()
        db.close()
        cur.close()
        for row in rows:
            url = self.base_url + 'c' + row['code']
            yield scrapy.Request(url=url, meta=row, callback=self.parse_index)

    # 解析列表首页，获取列表页数
    def parse_index(self, response):
        # 如果该小区没有售出房源，解析首页的时候会直接跳转到成交首页，需要特殊判断
        if len(response.xpath('//span[@class="checkbox checked"]').extract()) == 0:
            return
        page_data = response.xpath('//div[@class="page-box house-lst-page-box"]/@page-data').extract()
        if not page_data:
            total_page = 0
        else:
            page_data = page_data[0]
            dict_page_data = json.loads(page_data)
            total_page = dict_page_data['totalPage']
        for page in range(total_page):
            code = response.meta['code']
            name = response.meta['name']
            logging.info('正在解析小区:{}, 第 {} 页, 共 {} 页'.format(name, page + 1, total_page))
            url = self.base_url + '/pg' + str(page + 1) + 'c' + code
            yield scrapy.Request(url=url, callback=self.parse_house_list)

    # 解析房源列表
    def parse_house_list(self, response):
        house_urls = response.xpath('//div[@class="title"]/a/@href').extract()
        deal_dates = response.xpath('//div[@class="dealDate"]/text()').extract()
        for i in range(len(house_urls)):
            yield scrapy.Request(house_urls[i], meta={'deal_date': deal_dates[i]}, callback=self.parse_house_detail)

    # 解析房源详情
    def parse_house_detail(self, response):
        item = SoldHouseItem()
        # 房源code
        item['code'] = response.xpath(
            '//div[@class="house-title LOGVIEWDATA LOGVIEW"]/@data-lj_action_resblock_id').extract()[0]
        # 小区code
        item['community_code'] = response.xpath(
            '//div[@class="house-title LOGVIEWDATA LOGVIEW"]/@data-lj_action_housedel_id').extract()[0]
        # 标题
        item['title'] = response.xpath('//div[@class="wrapper"]/text()').extract()[0]
        item['title'] = item['title'].replace('{', '')
        item['title'] = item['title'].replace('}', '')
        item['title'] = item['title'].replace('"', ' ')
        # 挂牌价格
        item['selling_price'] = response.xpath('//div[@class="msg"]/span/label/text()').extract()[0]
        # 售出价格
        item['sold_price'] = response.xpath('//span[@class="dealTotalPrice"]/i/text()').extract()[0]
        # 价格单位（万）
        item['price_unit'] = response.xpath('//span[@class="dealTotalPrice"]/text()').extract()[0]
        # 两室一厅
        type = response.xpath('//div[@class="content"]/ul/li/text()').extract()[0]
        item['type'] = str(type).strip()
        # 大小
        item['size'] = response.xpath('//div[@class="content"]/ul/li/text()').extract()[4]
        # 上架时间
        item['on_sale_date'] = response.xpath('//div[@class="transaction"]/div/ul/li/text()').extract()[2]
        # 售出单价
        item['sold_price_per'] = response.xpath('//div[@class="price"]/b/text()').extract()[0]
        # 售出日期
        deal_date = response.meta['deal_date']
        item['sold_date'] = deal_date.replace('.', '-')
        item['finish'] = False
        item['on_sale_date'] = item['on_sale_date'].replace(' ', '')
        item['selling_price'] = str(item['selling_price']).strip()
        if item['selling_price'] == '暂无数据':
            item['selling_price'] = 0
        if item['on_sale_date'] == '暂无数据':
            item['on_sale_date'] = 'null'
        yield item
