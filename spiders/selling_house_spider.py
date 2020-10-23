import json
import scrapy
import pymysql
import logging

from lianjia import settings
from lianjia.items import SellingHouseItem
from scrapy.utils.project import get_project_settings

"""爬取房屋信息
"""


def process_title(title):
    title = title.replace('{', '')
    title = title.replace('}', '')
    title = title.replace('"', ' ')
    return title


def parse_enable_house(response):
    item = SellingHouseItem()

    code = response.xpath(
        '//div[@class="btnContainer  LOGVIEWDATA LOGVIEW"]/@data-lj_action_resblock_id').extract()
    if not code:
        return None
    # 房源code
    item['code'] = code[0]

    # 小区code
    community_code = response.xpath(
        '//div[@class="btnContainer  LOGVIEWDATA LOGVIEW"]/@data-lj_action_housedel_id').extract()[0]
    item['community_code'] = community_code

    # 标题
    title = response.xpath('//h1[@class="main"]/text()').extract()[0]
    item['title'] = process_title(title)

    # 价格
    price = response.xpath(
        '//div[@class="price "]/span[@class="total"]/text()').extract()[0]
    item['price'] = price

    # 价格单位（万）
    price_unit = response.xpath('//div[@class="price "]/span[@class="unit"]/span/text()').extract()[0]
    item['price_unit'] = price_unit

    # 单价
    price_per = response.xpath('//span[@class="unitPriceValue"]/text()').extract()[0]
    item['price_per'] = price_per

    # 两室一厅
    type = response.xpath('//div[@class="room"]/div[@class="mainInfo"]/text()').extract()[0]
    item['type'] = type

    # 大小
    size = response.xpath('//div[@class="area"]/div[@class="mainInfo"]/text()').extract()[0]
    item['size'] = size

    # 上架时间
    on_sale_date = response.xpath('//div[@class="transaction"]/div[@class="content"]/ul/li/span/text()').extract()[1]
    item['on_sale_date'] = on_sale_date

    # 是否下架
    item['deleted'] = False

    item['finish'] = False
    return item


def parse_disable_house(response):
    item = SellingHouseItem()
    # 房源code
    code = response.xpath(
        '//div[@class="btnContainer disable LOGVIEWDATA LOGVIEW"]/@data-lj_action_resblock_id').extract()
    if not code:
        return None
    item['code'] = code[0]

    # 小区code
    community_code = response.xpath(
        '//div[@class="btnContainer disable LOGVIEWDATA LOGVIEW"]/@data-lj_action_housedel_id').extract()[0]
    item['community_code'] = community_code

    # 标题
    title = response.xpath('//h1[@class="main"]/text()').extract()[0]
    item['title'] = process_title(title)

    # 价格
    price = response.xpath('//div[@class="price isRemove"]/span[@class="total"]/text()').extract()[0]
    item['price'] = price

    # 价格单位（万）
    price_unit = response.xpath('//div[@class="price isRemove"]/span[@class="unit"]/span/text()').extract()[0]
    item['price_unit'] = price_unit

    # 单价
    price_per = response.xpath('//span[@class="unitPriceValue"]/text()').extract()[0]
    item['price_per'] = price_per

    # 两室一厅
    type = response.xpath('//div[@class="room"]/div[@class="mainInfo"]/text()').extract()[0]
    item['type'] = type

    # 大小
    size = response.xpath('//div[@class="area"]/div[@class="mainInfo"]/text()').extract()[0]
    item['size'] = size

    # 上架时间
    on_sale_date = response.xpath('//div[@class="transaction"]/div[@class="content"]/ul/li/span/text()').extract()[1]
    item['on_sale_date'] = on_sale_date

    # 是否下架
    item['deleted'] = True

    item['finish'] = False
    return item


class SellingHouseSpider(scrapy.Spider):
    name = 'selling_house'
    base_url = 'https://cd.lianjia.com/ershoufang/'

    def __init__(self, name=None, db_password=None, **kwargs):
        if db_password is not None and db_password != '':
            settings_copy = get_project_settings()
            db_conf = settings_copy.get('DB_CONFIG')
            db_conf['password'] = db_password
            settings_copy.set('DB_CONFIG', db_conf)
        super().__init__(name, **kwargs)

    def start_requests(self):
        sql = 'select * from community where selling_house_status = 0'
        db = pymysql.connect(**settings.DB_CONFIG)
        cur = db.cursor(cursor=pymysql.cursors.DictCursor)
        cur.execute(sql)
        rows = cur.fetchall()
        db.close()
        cur.close()
        for row in rows:
            code = row['code']
            name = row['name']
            url = self.base_url + 'c' + code
            meta = {'code': code, 'name': name}
            yield scrapy.Request(url=url, meta=meta, callback=self.parse_index)

    # 解析列表首页，获取列表页数
    def parse_index(self, response):
        page_data = response.xpath('//div[@class="page-box house-lst-page-box"]/@page-data').extract()
        code = response.meta['code']
        name = response.meta['name']
        if not page_data:
            total_page = 0
        else:
            page_data = page_data[0]
            dict_page_data = json.loads(page_data)
            total_page = dict_page_data['totalPage']
        if total_page == 100:
            total_page = 0
        for page in range(total_page):
            logging.info('正在解析小区:{}, 第 {} 页, 共 {} 页'.format(name, page + 1, total_page))
            url = self.base_url + '/pg' + str(page + 1) + 'c' + code
            yield scrapy.Request(url=url, callback=self.parse_house_list)



    # 解析房源列表
    def parse_house_list(self, response):
        house_codes = response.xpath('//div[@class="title"]/a/@data-housecode').extract()
        if not house_codes:
            house_codes = response.xpath('//div[@class="title"]/a/@data-lj_action_housedel_id').extract()
        for code in house_codes:
            url = self.base_url + str(code) + '.html'
            yield scrapy.Request(url=url, callback=self.parse_house_detail)

    # 解析房源详情
    def parse_house_detail(self, response):
        item = parse_enable_house(response)
        if not item:
            item = parse_disable_house(response)
        yield item
