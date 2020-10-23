import json
import scrapy
import logging

from lianjia.items import CommunityItem

"""
爬取所有小区
先获取成都市所有区县列表，再遍历区县列表获取小区
"""


class CommunitySpider(scrapy.Spider):
    # 当前版本号
    version = 1
    name = 'community'
    allowed_domains = ['cd.lianjia.com']
    start_urls = ['https://cd.lianjia.com/xiaoqu/']

    def __init__(self, **kwargs):
        super().__init__(self.name, **kwargs)

    # 解析行政区
    def parse(self, response):
        # 获取成都市所有区县列表
        district_urls = response.xpath('//div[@data-role="ershoufang"]/div/a/@href').extract()
        district_names = response.xpath('//div[@data-role="ershoufang"]/div/a/text()').extract()
        for i in range(len(district_urls)):
            logging.info('正在解析' + district_names[i] + '，url=' + district_urls[i])
            district_full_url = 'https://cd.lianjia.com' + district_urls[i]
            yield scrapy.Request(url=district_full_url,
                                 meta={'base_url': district_full_url},
                                 callback=self.parse_community_index)

    #  解析指定行政区下的小区信息
    def parse_community_index(self, response):
        # 解析小区页数
        page_data = response.xpath('//div[@class="page-box house-lst-page-box"]/@page-data').extract()[0]
        dict_page_data = json.loads(page_data)
        base_url = response.meta['base_url']
        total_page = dict_page_data['totalPage']
        # 返回的页数最多30页 兜底设置为100
        if total_page == 30:
            total_page = 100
        for page in range(1, total_page + 1):
            logging.info('正在解析第' + str(page) + '页，共' + str(total_page) + '页')
            yield scrapy.Request(url=base_url + '/pg' + str(page), callback=self.parse_community)

    #  解析列表页小的小区列表
    def parse_community(self, response):
        # 小区ID
        codes = response.xpath('//li[@class="clear xiaoquListItem"]/@data-id').extract()
        # 小区名字
        names = response.xpath('//div[@class="title"]/a/text()').extract()
        # 小区位置
        district_list = response.xpath('//div[@class="positionInfo"]/a[@class="district"]/text()').extract()
        for i in range(len(codes)):
            item = CommunityItem()
            item['code'] = codes[i]
            item['name'] = names[i]
            item['district'] = district_list[i]
            item['version'] = self.version
            item['finish'] = False
            yield item
