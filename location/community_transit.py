import requests
import threading
from queue import Queue

from location import db_util


def query_community(last_id):
    sql = 'select id, name, location_lat, location_lng, uid ' \
          'from community_location ' \
          'where id > {} ' \
          'and transit_duration_to_swj = 0 ' \
          'order by id ' \
          'limit 100'.format(last_id)
    return db_util.query(sql)


def parse_community_transit(item):
    distance = 0
    duration = 0
    location_lat = item[2]
    location_lng = item[3]
    uid = item[4]
    origin = str(location_lat) + ',' + str(location_lng)
    if location_lng == 'NULL':
        return 0, 0
    if uid != 'NULL':
        url = 'http://api.map.baidu.com/direction/v2/transit?' \
              'origin={}&' \
              'origin_uid={}&' \
              'destination=30.638013,104.041324&' \
              'destination_uid=411b56f63f70bcf5b1077d02&' \
              'page_size=1&' \
              'departure_time=08:00&' \
              'ak=y326DeWUG3G05cVFV0cz6jzqyef0VeLU'.format(origin, uid)
    else:
        url = 'http://api.map.baidu.com/direction/v2/transit?' \
              'origin={}&' \
              'destination=30.638013,104.041324&' \
              'destination_uid=411b56f63f70bcf5b1077d02&' \
              'page_size=1&' \
              'departure_time=08:00&' \
              'ak=y326DeWUG3G05cVFV0cz6jzqyef0VeLU'.format(origin)
    response = requests.get(url)
    content_info = response.json()
    result = content_info['result']
    if result and result['routes']:
        route = result['routes'][0]
        distance = route['distance']
        duration = route['duration']
    return distance, duration


def save_community_transit(item, transit):
    sql = 'update community_location ' \
          'set transit_distance_to_swj = {} ,' \
          'transit_duration_to_swj = {} ' \
          'where id = {}'.format(transit[0], transit[1], item[0])
    db_util.execute(sql)


def consumer(community_list, consumer_name):
    for item in community_list:
        print(consumer_name + item[1])
        transit = parse_community_transit(item)
        save_community_transit(item, transit)


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


def main():
    queue = Queue(10)
    producer = Producer('Producer', queue)
    producer.start()

    for i in range(10):
        consumer = Consumer('Consumer' + str(i), queue)
        consumer.start()


if __name__ == '__main__':
    main()
