import pymysql

db_host = '10.36.166.37'
db_port = 5002
db_user = 'test_seagull'
db_pwd = 'test_seagull'
db_database = 'lianjia'


def query(sql):
    db = pymysql.connect(host=db_host, port=db_port, user=db_user, password=db_pwd, database=db_database)
    cursor = db.cursor()
    cursor.execute(sql)
    results = cursor.fetchall()
    db.close()
    return results


def get(sql):
    db = pymysql.connect(host=db_host, port=db_port, user=db_user, password=db_pwd, database=db_database)
    cursor = db.cursor()
    cursor.execute(sql)
    result = cursor.fetchone()
    db.close()
    return result


def execute(sql):
    db = pymysql.connect(host=db_host, port=db_port, user=db_user, password=db_pwd, database=db_database)
    cursor = db.cursor()
    cursor.execute(sql)
    db.commit()
    db.close()
