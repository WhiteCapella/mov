import requests
import os
import pandas as pd

def echo(yaho):
    return yaho

def save2df(load_dt):
    df = list2df(load_dt)
    # df에 load_dt column 추가 (조회 일자 : YYYYMMDD)
    # 아래 파일 저장시 load_dt 기준으로 파티셔닝
    df['load_dt']=load_dt
    print(df.head(5))
    df.to_parquet('~/tmp/test_parquet', partition_cols=['load_dt'])
    return df


def list2df(load_dt) -> list:
    l = req2list(load_dt)
    df = pd.DataFrame(l)
    return df

def req2list(load_dt) -> list:
    _, data = req(load_dt)
    l = data['boxOfficeResult']['dailyBoxOfficeList']
    return l

def get_key():
    key = os.getenv('MOVIE_API_KEY')
    return key

def req(load_dt):
    #url = gen_url('20240720')
    url = gen_url(load_dt)
    r = requests.get(url)
    code = r.status_code
    data = r.json()
    print(data)
    return code, data


def gen_url(dt):
    base_url = "http://www.kobis.or.kr/kobisopenapi/webservice/rest/boxoffice/searchDailyBoxOfficeList.json"
    key = get_key()
    url = f"{base_url}?key={key}&targetDt={dt}"

    return url
