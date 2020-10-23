from location import db_util
import requests
import threading
from queue import Queue

#  {
#             "name":"国家税务总局成都市武侯区税务局",
#             "location":{
#                 "lat":30.638013,
#                 "lng":104.041324
#             },
#             "address":"四川省成都市武侯区二环路南四段39号",
#             "province":"四川省",
#             "city":"成都市",
#             "area":"武侯区",
#             "street_id":"411b56f63f70bcf5b1077d02",
#             "telephone":"(028)85094508",
#             "detail":1,
#             "uid":"411b56f63f70bcf5b1077d02"
#         }

#         {
#             "name":"199FTC",
#             "location":{
#                 "lat":30.566015,
#                 "lng":104.063718
#             },
#             "address":"天府一街535号",
#             "province":"四川省",
#             "city":"成都市",
#             "area":"武侯区",
#             "street_id":"cc7802f584f73316c72ba113",
#             "detail":1,
#             "uid":"cc7802f584f73316c72ba113"
#         }

def query_community(last_id):
    sql = 'select id, code, name ' \
          'from community ' \
          'where id > {} ' \
          'order by id ' \
          'limit 100'.format(last_id)
    return db_util.query(sql)


def get_community_location(community_code):
    sql = 'select id, code, name ' \
          'from community_location ' \
          'where code = {}'.format(community_code)
    return db_util.get(sql)


def parse_community_location(community_name):
    location_lat = 'NULL'
    location_lng = 'NULL'
    uid = 'NULL'

    url = 'http://api.map.baidu.com/place/v2/search?' \
          'query={}&' \
          'city_limit=true&' \
          'region=成都&' \
          'output=json&' \
          'ak=y326DeWUG3G05cVFV0cz6jzqyef0VeLU'.format(community_name)
    response = requests.get(url)
    response_json = response.json()
    result_list = response_json['results']
    if result_list:
        result = result_list[0]
        if 'location' in result:
            location = result['location']
            location_lat = location['lat']
            location_lng = location['lng']
        if 'uid' in result:
            uid = result['uid']
    return location_lat, location_lng, uid


def save_community_location(item, location):
    sql = 'insert into community_location ' \
          '(code, name, location_lat, location_lng, uid) ' \
          'values ( "{}", "{}", "{}", "{}", "{}")' \
        .format(item[1], item[2], location[0], location[1], location[2])
    db_util.execute(sql)


def consumer(community_list, consumer_name):
    for item in community_list:
        community_location = get_community_location(item[1])
        if community_location:
            print(consumer_name, community_location[2])
            continue
        location = parse_community_location(item[2])
        print(consumer_name, item[2])
        save_community_location(item, location)


# 生产者类
class Producer(threading.Thread):
    def __init__(self, name, queue):
        threading.Thread.__init__(self, name=name)
        self.data = queue

    def run(self):
        community_list = query_community(0)
        while community_list:
            self.data.put(community_list)
            last_id = community_list[-1][0]
            print("{} put {} finished!".format(last_id, self.getName()))
            community_list = query_community(last_id)


# 消费者类
class Consumer(threading.Thread):
    def __init__(self, name, queue):
        threading.Thread.__init__(self, name=name)
        self.data = queue

    def run(self):
        community_list = self.data.get()
        while community_list:
            consumer(community_list, self.getName())
            community_list = self.data.get()


if __name__ == '__main__':
    queue = Queue(10)
    Producer('Producer', queue).start()

    for i in range(10):
        Consumer('Consumer' + str(i), queue).start()
