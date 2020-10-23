# -*- coding: utf-8 -*-

import scrapy


# 小区信息
class CommunityItem(scrapy.Item):
    # 小区代码
    code = scrapy.Field()
    # 小区名称
    name = scrapy.Field()
    # 小区位置
    district = scrapy.Field()
    # 在售二手房数量
    selling_house_amount = scrapy.Field()
    # 售卖中的房屋单价
    selling_avg_price = scrapy.Field()
    # 已成交房屋数量
    sold_house_amount = scrapy.Field()
    # 已售出的房屋单价
    sold_avg_price = scrapy.Field()
    # 版本号
    version = scrapy.Field()
    # 表示数据已爬取完毕，执行保存操作
    finish = scrapy.Field()


# 售卖中的房源信息
class SellingHouseItem(scrapy.Item):
    # 房源code
    code = scrapy.Field()
    # 小区code
    community_code = scrapy.Field()
    # 房源title
    title = scrapy.Field()
    # 挂牌价格
    price = scrapy.Field()
    # 挂牌单价
    price_per = scrapy.Field()
    # 价格单位（万）
    price_unit = scrapy.Field()
    # 类型（两室一厅）
    type = scrapy.Field()
    # 大小（59.5平米）
    size = scrapy.Field()
    # 上架时间
    on_sale_date = scrapy.Field()
    # 是否下架
    deleted = scrapy.Field()
    # 采集时间
    gmt_create = scrapy.Field()
    # 更新时间（最新一次采集时间）
    gmt_update = scrapy.Field()
    # 表示数据已爬取完毕，执行保存操作
    finish = scrapy.Field()


# 售出的房源信息
class SoldHouseItem(scrapy.Item):
    # 房源code
    code = scrapy.Field()
    # 小区code
    community_code = scrapy.Field()
    # 房源title
    title = scrapy.Field()
    # 挂牌价格
    selling_price = scrapy.Field()
    # 成交价格
    sold_price = scrapy.Field()
    # 成交单价
    sold_price_per = scrapy.Field()
    # 价格单位（万）
    price_unit = scrapy.Field()
    # 类型（两室一厅）
    type = scrapy.Field()
    # 大小（59.5平米）
    size = scrapy.Field()
    # 上架时间
    on_sale_date = scrapy.Field()
    # 是否下架
    deleted = scrapy.Field()
    # 售出时间
    sold_date = scrapy.Field()
    # 采集时间
    gmt_create = scrapy.Field()
    # 表示数据已爬取完毕，执行保存操作
    finish = scrapy.Field()
