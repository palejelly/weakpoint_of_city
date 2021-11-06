# -*- coding: utf-8 -*-

# 지하철 : http://data.seoul.go.kr/dataList/datasetView.do?infId=OA-12765&srvType=A&serviceKind=1&currentPageNo=1 활용
# 버스 : http://ws.bus.go.kr/api/rest/stationinfo/getStationByPos 활용
import urllib.request
import json
import pymysql
import math
from pyproj import Proj  # 좌표계 변환을 위해 패키지 설치했습니다.
from pyproj import transform
from xml.etree.ElementTree import parse

# mysql 연결
conn = pymysql.connect(host='localhost', user='root', password='0812', db='sentience', charset='utf8')
curs = conn.cursor()

# 혹시 모르니 사용하는 table들을 먼저 지우고 시작
delete_sql = """DROP TABLE IF EXISTS neartrans"""
curs.execute(delete_sql)
delete_sql = """DROP TABLE IF EXISTS scoreboard"""
curs.execute(delete_sql)

# neartrans 라는 이름의 table 생성합니다.
# requested_id는 여러개의 좌표가 주어졌을 때 그중 순서입니다.
# sta_type 는 지하철역인지 버스정류장인지 sta_name 은 역/정류장이름, sta_x,y는 역/정류장의 wgs84 좌표값,
# distance 는 requested_x,y 에서 sts_x,y까지 거리(단위는 미터)
# requested_x,y는 요청했던 wgs84 좌표값을 저장합니다.
sql_neartrans = """
CREATE TABLE IF NOT EXISTS neartrans (
        id INT UNSIGNED NOT NULL AUTO_INCREMENT,
        requested_id INT(5),
        sta_type VARCHAR(20) NOT NULL,
        sta_name VARCHAR(30) NOT NULL, 
        subwayNm VARCHAR(11),
        sta_x FLOAT(10,6) NOT NULL,
        sta_y FLOAT(10,6) NOT NULL,
        distance FLOAT(10) NOT NULL,
        requested_x FLOAT(10,6) NOT NULL,
        requested_y FLOAT(10,6) NOT NULL,
        PRIMARY KEY(id)
);
"""

curs.execute(sql_neartrans)
conn.commit()

# scoreboard 테이블은 neartrans 에서 정보를 추출해서 점수를 기록하는 테이블입니다.
# requested_x,y는 조회하고싶었던 좌표입니다.
# score는 판단 기준이 되는 점수입니다.
# sublinecount는 주변을 지나는 지하철 호선의 개수
# subcount, buscount는 주변의 지하철역, 버스정류장 개수


sql_scoreboard = """
CREATE TABLE IF NOT EXISTS scoreboard(
    id INT NOT NULL AUTO_INCREMENT,
    requested_x FLOAT(10,6) ,
    requested_y FLOAT(10,6) ,
    score INT(5),
    sublinecount INT(3) ,
    subcount INT(5) ,
    buscount INT(5) ,
    PRIMARY KEY(id)
);
"""

curs.execute(sql_scoreboard)
conn.commit()

WGS84 = {'proj': 'latlong', 'datum': 'WGS84', 'ellps': 'WGS84', }

TM127 = {'proj': 'tmerc', 'lat_0': '38N', 'lon_0': '127.0028902777777777776E',
         'ellps': 'bessel', 'x_0': '200000', 'y_0': '500000', 'k': '1.0',
         'towgs84': '-146.43,507.89,681.46'}


def wgs84_to_tm127(longitude, latitude):
    return transform(Proj(**WGS84), Proj(**TM127), longitude, latitude)


def tm127_to_wgs84(x, y):
    return transform(Proj(**TM127), Proj(**WGS84), x / 2.5, y / 2.5)


# 경도, 위도를 입력해주면 2km 이내의 지하철역의 위치, 호선 정보를 알려주는 함수
def get_near_subway(input_long, input_lat, requested_id):
    # 요청인자
    # KEY 고유키
    # TYPE 타입,저는 json으로 요청했습니다.
    # SERVICE 제가 사용한 것은 nearBy라는 근처 지하철역을 찾는 API 입니다.
    # START_INDEX,END_INDEX 시작, 끝을 정합니다. 저는 10개 찾도록 했습니다.
    # gpsX,gpsY 찾을 기준 위치입니다. 6자리에 200000,500000 근처에 값이 있으니 TM127 좌표인 것 같습니다.

    url = "http://swopenAPI.seoul.go.kr/api/subway/"
    key = "52436b516770616c36307045556b4e/"
    call_type = "json/"
    service = "nearBy/"
    start_index = "0/"
    end_index = "10/"
    gps_x, gps_y = list(map(str, wgs84_to_tm127(input_long, input_lat)))  # 좌표계를 변환합니다.

    # url 을 조합해서 만듭니다.
    api_url = url + key + call_type + service + start_index + end_index + gps_x + "/" + gps_y

    # url 에서 데이터를 받아옵니다.
    data = urllib.request.urlopen(api_url).read().decode('utf8')
    data_json = json.loads(data)

    # json data에서 stationList 아래에 원하는 정보들이 있습니다.
    parsed_json = data_json['stationList']

    # 원하는 정보를 이제 파싱하여 mysql에 삽입합니다.
    pre_sql = """
        INSERT INTO neartrans (sta_type, requested_id, sta_name, subwayNm, sta_x, sta_y, distance, requested_x, requested_y) VALUES('subway',"""
    pre_sql += str(requested_id) + ","
    tail_sql = str(input_long) + ", " + str(input_lat) + ");"

    passing_data = {}
    passing_data_dist = {}
    for one_parsed in parsed_json:

        # 두 지점의 거리를 계산하는 부분입니다.
        def dist(x1, y1, x2, y2):
            return int(math.sqrt(pow(float(x1) - float(x2), 2) + pow(float(y1) - float(y2), 2)))
            # float형 소숫점 뒤 값이 너무 길어서 어차피 제곱해서 소수값은 큰 의미가 없기 때문에 편의를 위해 int로 변환했습니다.

        # 요청한 지점부터 지하철역까지 거리를 dis 에 저장합니다.
        dis = dist(one_parsed['subwayXcnts'], one_parsed['subwayYcnts'], gps_x, gps_y)

        if dis <= 2000:  # 2km 이내의 지하철역을 조회합니다.
            passing_data_dist[one_parsed['rowNum']] = dis
            passing_data[one_parsed['rowNum']] = one_parsed['statnNm'] # statnNm은 역명입니다.

            # mysql에 정보를 삽입합니다. subwayXcnts,Ycnts 는 x,y tm127 좌표, subwayNm 는 호선이름입니다.
            sta_x, sta_y = tm127_to_wgs84(float(one_parsed['subwayXcnts']), float(one_parsed['subwayYcnts']))
            insert_sql = pre_sql + "'" + one_parsed['statnNm'] + "'" + ", '" + one_parsed['subwayNm'] + "'," + str(
                sta_x) + ", " + \
                         str(sta_y) + "," + str(dis) + ","

            insert_sql += tail_sql


            curs.execute(insert_sql)
            conn.commit()


def get_near_bus(input_long, input_lat, requested_id):
    # 정류소정보조회서비스 API 입니다. 기준위치 x,y 검색반지름을 입력하면 정류소 좌표, 이름 id, 거리 등을 알려줍니다.

    pre_url = "http://ws.bus.go.kr/api/rest/stationinfo/getStationByPos?ServiceKey="
    serviceKey = "MUJfguVrJ%2F3HJJ9lUVgIhcMSTg4xxyIhpQte1RMt28q8kcqmIUFqu%2BxGJ3%2FkPG%2BG1qUm1iuRvOGGQOaPQIr%2BGg%3D%3D"
    tmX = str(input_long)  # tmX는 wgs84좌표계로 입력받음.. (위도, 경도를 이용하는 좌표계)
    tmY = str(input_lat)
    radius = str(1000)  # 버스는 지하철과 다르게 입력값에 반지름이 있었습니다. 버스는 1km로 설정했습니다.
    # 서울시 API 접속해서 값을 보내고 그 결과(주변 버스정류장) 정보를 받습니다.
    api_url = pre_url + serviceKey + "&tmX=" + tmX + "&tmY=" + tmY + "&radius=" + radius
    data = urllib.request.urlopen(api_url).read().decode('utf8')

    # 여기서 문제가 위에 get_ner_subway 는 json 형식이었는데 이 API 는 xml 밖에 지원하지 않습니다.
    # json 공부도 하루걸렸는데 xml도 하루 썼습니다 ㅜ
    # 그래서 xml의 Elementtree 를 import 했습니다.

    xmlFile = "test.xml"
    f = open(xmlFile, "w")
    f.write(data)
    f.seek(0)  # 앞으로 돌려서 다시 보기위해.
    root = parse("test.xml").getroot()

    passing_data = root.find("msgBody")

    pre_sql = """INSERT INTO neartrans(sta_type, requested_id, sta_name, sta_x, sta_y, distance, requested_x, requested_y) VALUES('bus',"""
    pre_sql += str(requested_id) + ","
    tail_sql = str(input_long) + ", " + str(input_lat) + ");"

    for child in passing_data:  # xml파일에서 역 이름, 역 gps 좌표 등을 가져옵니다.
        insert_sql = pre_sql + "'" + str(child.findtext("stationNm")) + "' ," + str(child.findtext("gpsX")) + "," + str(
            child.findtext("gpsY")) + "," + str(child.findtext("dist")) + "," + tail_sql

        curs.execute(insert_sql)
        conn.commit()


def basic_scoring(arr_cords):
    # requested_id 는 여러 개의 좌표를 받았을 때 그 순서입니다.(1부터 순서대로)
    requested_id = 1
    for cord in arr_cords:
        long, lat = cord[0], cord[1]


        get_near_subway(long, lat, requested_id)
        get_near_bus(long, lat, requested_id)

        # count_distinct 는 mysql에서 neartrans table의 값들을 분석합니다.
        # subwayNm 칼럼의 서로다른 값 개수(호선 개수) , 중복을 제거한 지하철역, 버스정류장 개수를 리턴합니다.
        def count_distinct(type): #type 는 subwayNm, subway, bus 중에 하나
            sql =""
            if type == "subwayNm":
                sql = "SELECT COUNT(DISTINCT subwayNm) FROM neartrans WHERE requested_id = " + str(
                    requested_id) + ";"

            else:
                sql = "SELECT COUNT(DISTINCT sta_name) FROM neartrans WHERE requested_id = {0} AND sta_type = {1};".format(str(
                requested_id), "'" + type + "'")


            curs.execute(sql)
            conn.commit()
            result = curs.fetchone()
            return result[0]

        sublinecount = count_distinct("subwayNm")
        subcount = count_distinct("subway")
        buscount = count_distinct("bus")

        insert_sql = """INSERT INTO scoreboard (sublinecount, subcount, buscount) VALUES({0},{1},{2});""".format(sublinecount, subcount, buscount)
        curs.execute(insert_sql)
        conn.commit()

        update_sql = """UPDATE scoreboard SET requested_x = {0} , requested_y = {1} WHERE id = {2} ;""".format(long, lat, requested_id)
        curs.execute(update_sql)
        conn.commit()

        requested_id += 1


def score(arr_cords,coef):
    basic_scoring(arr_cords)
    requested_id = 1
    bestscore=0
    for cord in arr_cords:
        sql = """SELECT DISTINCT subwayNm FROM neartrans WHERE sta_type = 'subway' AND requested_id = %s ; """
        curs.execute(sql,str(requested_id))
        conn.commit()
        lineresult = curs.fetchall()
        linescore=0

        # 가장 중요하다고생각합니다. 호선별로 가장 가까운 정류장까지의 거리를 구하고 점수를 구합니다.
        for idx,line in enumerate(lineresult):
            sql = """SELECT MIN(distance) FROM neartrans WHERE subwayNm = {0} AND requested_id=""".format("'" + line[0] + "'")
            sql += str(requested_id) + ";"
            curs.execute(sql)
            conn.commit()
            result=curs.fetchone()
            linescore += 2000/result[0]


        # 지하철에 의한 점수를 구합니다. 2000/지하철역 까지의 거리를 모두 더합니다. 가까울수록 좋습니다.
        mult_sql="""SELECT SUM(2000.0/distance) AS subwayscore FROM neartrans WHERE requested_id = %s AND sta_type = 'subway';"""
        curs.execute(mult_sql,requested_id)
        conn.commit()
        subresult = curs.fetchone()
        subwayscore = subresult[0]
        # 결과는 subwayscore에 저장됩니다.

        # 버스에의한 점수를 구합니다. 1000/버스 정류장까지의 거리를 모두 더합니다. 가까울수록 더 좋습니다.
        mult_sql = """SELECT SUM(1000.0/distance) AS busscore FROM neartrans WHERE requested_id = %s AND sta_type = 'bus';"""
        curs.execute(mult_sql, requested_id)
        conn.commit()
        busresult = curs.fetchone()
        busscore = busresult[0]
        # 결과는 busscore에 저장됩니다.

        scoring_sql="""UPDATE scoreboard SET score = %s WHERE id = """ + str(requested_id)+ ";"

        # 점수를 메기는 핵심 부분입니다. subwayscore는 linescore와 겹치는 경향이 있고, 버스는 흔하니까 이 subwayscore,busscore 비중은 낮췄습니다.
        score=int((linescore+subwayscore/4)*coef+busscore*(1-coef)/20)
        if score>=bestscore:
            bestscore = score
        curs.execute(scoring_sql,score)
        conn.commit()
        requested_id += 1
    # 1등을 추출해서 출력하는 부분입니다.
    selecting_sql="""SELECT *FROM scoreboard WHERE score= %s """
    curs.execute(selecting_sql,bestscore)
    conn.commit()
    result=curs.fetchone()

    print("좌표 : ({0}, {1})가 근처에 지하철 {2}개 호선, 지하철역 {3} 개, 버스정류장 {4} 개로 교통이 제일 좋습니다.".format(result[1],result[2],result[4],result[5],result[6]))




a = [(126.929518, 37.569931), (126.937566, 37.471598),(127.037207, 37.495373)]

score(a, 0.8)

conn.close()
