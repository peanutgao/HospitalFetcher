
import requests
import pymysql


# class DataType(Enum):
#     Province = 0
#     Data = 1


def request_data(url, args):
    if len(url) == 0:
        return

    r = requests.post(url, args)
    json = r.json()

    if json['code'] != 0:
        print('请求出错')

    data = json['data']
    # print("抓取的数据是：", data)

    return data


def request_hospital(city_id):
    url = 'https://ad.dabai.7lk.com/hospital/list'
    d = {
        'cityId': city_id,
        'doctorId': 368982,
        'token': '2e627b90f1380c4ac5ee'
    }

    return request_data(url, d)


def request_city(pid):
    url = "https://ad.dabai.7lk.com/city/list"
    d = {
        'pid': pid,
        'doctorId': 368982,
        'token': '2e627b90f1380c4ac5ee'
    }

    return request_data(url, d)


def request_provinces():
    return request_city(1)


def start_fetcher():
    print('开始连接数据库')
    db = pymysql.connect('localhost', 'root', 'gys605976', "hospitals")
    cursor = db.cursor()

    sql1 = """create table if not exists t_cities(
    id int(32) primary key not null auto_increment,
    mid int(32),
    name varchar(32),
    type int(32),
    parentId int(32)
    )    
    """

    sql2 = """create table if not exists t_hospitals(
    id int primary key not null auto_increment,
    mid int(32),
    name varchar(32),
    cityId int(32)
    )
    """

    cursor.execute(sql1)
    cursor.execute(sql2)

    # 请求数据
    provinces = request_provinces()

    for p in provinces:
        save_cities_2_db(cursor, p, db)
        
        cities = request_city(p['id'])
        
        for c in cities:
            print("--> 正在抓取：%s" % c)

            # 保存城市数据
            save_cities_2_db(cursor, c, db)

            hospitals = request_hospital(c['id'])

            for h in hospitals:
                print("--> 正在抓取：%s" % h)
                save_hospitals_2_db(cursor, h, db)

    print('爬取并插入MySQL完成')

    db.commit()
    cursor.close()


def save_cities_2_db(cursor, c, db):
    c_sql = "Insert into t_cities ( mid, name, type, parentId) values (%d, \"%s\", %d, %d)" % (
    c["id"], c["name"], c["type"], c["parentId"])

    print(c_sql)
    try:
        cursor.execute(c_sql)
    except ValueError:
        print(ValueError)
        db.rollback()


def save_hospitals_2_db(cursor, h, db):
    h_sql = "insert into t_hospitals(mid, name, cityId) values (%d, \"%s\", %d)" % (h["id"], h["name"], h["cityId"])

    try:
        cursor.execute(h_sql)
    except ValueError:
        print(ValueError)
        db.rollback()
    # {'id': 2717, 'name': '汕头大学医学院第二附属医院', 'cityId': 801}


def main():
    start_fetcher()


if __name__ == "__main__":
    main()
