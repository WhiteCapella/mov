import requests
import os
import pandas as pd

def echo(yaho):
    return yaho

def apply_type2df(load_dt, path):
    #TODO
    #저장한 parquet 파일들을 변형시키기
    df = pd.read_parquet(f'{path}/load_dt={load_dt}')
    num_cols = ['rnum', 'rank', 'rankInten', 'salesAmt', 'audiCnt', 'audiAcc', 'scrnCnt', 'showCnt', 'salesShare', 'salesInten', 'salesChange', 'audiInten', 'audiChange']
    
    for c in num_cols:
        df[c] = pd.to_numeric(df[c])

    return df

def save2df(load_dt, url_param={}):
    df = list2df(load_dt, url_param)
    # df에 load_dt column 추가 (조회 일자 : YYYYMMDD)
    # 아래 파일 저장시 load_dt 기준으로 파티셔닝
    df['load_dt']=load_dt
    return df


def list2df(load_dt, url_param={}):
    l = req2list(load_dt, url_param)
    df = pd.DataFrame(l)
    return df

def req2list(load_dt, url_param={}):
    _, data = req(load_dt, url_param)
    l = data['boxOfficeResult']['dailyBoxOfficeList']
    return l

def get_key():
    key = os.getenv('MOVIE_API_KEY')
    return key

def req(load_dt, url_param={}):
    #url = gen_url('20240720')
    url = gen_url(load_dt, url_param)
    r = requests.get(url)
    code = r.status_code
    data = r.json()
    return code, data




def gen_url(dt):
    base_url = "http://www.kobis.or.kr/kobisopenapi/webservice/rest/boxoffice/searchDailyBoxOfficeList.json"
    key = get_key()
    url = f"{base_url}?key={key}&targetDt={dt}"
    for k, v in url_param.items():
        url = url + f"&{k}={v}"

    return url
