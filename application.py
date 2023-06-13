import requests
from bs4 import BeautifulSoup
import pandas as pd
import re
import openai
import time
from datetime import datetime, timedelta
from flask import Flask, render_template, request, jsonify

# mongoDB와 연결
from pymongo import MongoClient
client = MongoClient("////////")
db = client.news_data

# 네이버뉴스로 조회 가능한 뉴스 기사의 정보 스크래핑하는 함수
def scrap_news(keyword): # start_date, end_date
    # 지정된 키워드 -> 몽고DB에 저장 업데이트 실행

    keyword = keyword.upper()

    start_date = datetime.now().strftime("%Y%m%d") # current date #'20230525'

    end_date = datetime.now().strftime("%Y%m%d") # current date

    # 실행 시간 (마지막 업데이트 시간)
    last_updated = datetime.now().strftime("%Y%m%d %H:%M") # current date

    # 이미 가져온 기사와 겹치지 않게
    db = client.news_data

    # DB에 저장된 가장 최근 뉴스 가져옴
    if keyword == 'MTS':
        #latest = list(db.mts.find().sort('_id', -1).limit(1))
        latest = list(db.mts.find().sort([("date", -1), ("time", -1)]).limit(1))[0]
    elif keyword == '마이데이터':
        #latest = list(db.mydata.find().sort('_id', -1).limit(1))
        latest = list(db.mydata.find().sort([("date", -1), ("time", -1)]).limit(1))[0]
    else: # 토큰증권/증권형 토큰
        #latest = list(db.sto.find().sort('_id', -1).limit(1))
        latest = list(db.sto.find().sort([("date", -1), ("time", -1)]).limit(1))[0]

    latest_date = ''.join(latest['date'].split('.'))
    latest_time = latest['time']

    start_date = max(start_date, latest_date)

    news_scrap = []
    naver_urls = []
    news_info_list = []
    page_num = 1
    cnt = 0
    i = 1
    is_dup = 0 # DB와 중복 여부

    print(keyword, '에 대한 뉴스 검색 결과')

    while True:
        try:
            # 네이버 뉴스에 키워드 검색
            url = f'https://search.naver.com/search.naver?where=news&query={keyword}&sort=1&nso=so%3Add%2Cp%3Afrom{start_date}to{end_date}&start={i}'
            headers = {'User-Agent': 'Mozilla/5.0'}
            response = requests.get(url, headers=headers)
            soup = BeautifulSoup(response.content, 'html.parser')
            article_list = soup.select('#main_pack > section > div > div.group_news > ul > li')
            
            # 검색된 뉴스 리스트가 있는 경우
            if article_list:
                #print('페이지', page_num)
                cnt += len(article_list)
                for article in article_list:

                    try:
                        title = article.select_one(".news_tit").text
                        desc = article.select_one("a.api_txt_lines").text
                        press = article.select_one("a.info.press").text
                        doc = {
                            "title": title,
                            "description": desc,
                            "press": press
                        }

                        # 스크래핑하는 기준 1. 제목에 keyword 포함 2. 이미 DB에 있는 뉴스가 아님 - 제목, 언론사로 구분
                        # DB에 있는 뉴스인지 판단
                        if (title == latest['title']) & (press == latest['press']):
                            is_dup = 1
                            break
                        else:
                            # 제목에 키워드 포함하는 기사인지 판단
                            if keyword in title:
                                info_group = article.select('div.info_group > a')
                                # 네이버뉴스 링크있는 경우에만 스크래핑
                                if len(info_group) >= 2: 
                                    naver_url = info_group[1]['href']
                                    naver_urls.append(naver_url)
                                    news_info_list.append(doc)
                                else:
                                    pass
                            else: # 키워드와 매칭되지 않는 뉴스의 경우
                                pass
                    except:
                        pass
                # DB와 중복된 뉴스 찾은 경우(이미 저장된 뉴스 기사와 겹쳐 더 가져올 기사 없는 경우 스크래핑 중단)
                if is_dup == 1:
                    break
            # 검색된 뉴스 리스트가 없는 경우
            else:
                break

            i += 10 # 다음 페이지로 이동
            page_num += 1

        except:
            # 네이버 뉴스 키워드 검색 오류시
            print('Error - 뉴스 기사 목록 오류')
            break

    try:
        for url in naver_urls:
            response = requests.get(url, headers=headers)
            soup = BeautifulSoup(response.content, 'html.parser')
            date_element = soup.select('div.media_end_head_info_datestamp > div') # 수정된 일자가 있는 경우 해당 일자 기준으로 가져옴
            if len(date_element) > 1:
                date_txt = date_element[1].text
            else:
                date_txt = date_element[0].text
            main_article = soup.select_one('#dic_area').text
            date = date_txt.split()[0][2:-1] 
            if date_txt.split()[1] == '오전':
                time = date_txt.split()[2]
            else:
                hour = int(date_txt.split()[2].split(':')[0])
                minute = date_txt.split()[2].split(':')[1]
                hour += 12
                time = str(hour) + ':' + minute

            #time = ' '.join(date_txt.split()[1:3])
            a_dict = {
                'date': date,
                'time': time,
                'main_article': main_article,
                'url': url
            }

            news_scrap.append(a_dict)
    except:
        print('Error - 네이버 기사 스크래핑 오류')

    # 추가 스크래핑된 뉴스가 있는 경우
    if len(news_scrap) > 0:

        # 리프레시 버튼으로 가져오는 경우 두 리스트 길이가 다를 수 있음 고려
        df = pd.concat([pd.DataFrame(news_info_list[:len(news_scrap)]), pd.DataFrame(news_scrap)], axis=1)
        df['keyword'] = [keyword] * df.shape[0]
        # 기본 전처리
        df = df.sort_values(['date', 'time']) # 시간 순서대로 정렬해 저장
        df['press'] = df['press'].apply(lambda x: x.replace('선정', '').strip()) #언론사 이름에서 '선정' 제외        
        df['main_article'] = df['main_article'].apply(lambda x: re.sub("[`]", "'", x))
        df['title'] = df['title'].apply(lambda x: re.sub("[`]", "'", x))

        print('스크래핑된 뉴스 기사 수:', df.shape[0])
        print('검색된 뉴스 기사 수:', cnt)

        #print('뉴스 요약 시작')
        df['summary'] = get_summary(df)

        # DB에 저장
        if keyword == 'MTS':
            # while True:
            #     if db.mts.find_one({'title': df.loc[0, 'title'], 'press': df.loc[0, 'press']}):
            #         df = df.iloc[1:, :]
            #         if df.shape[0] == 0:
            #             print('업데이트 이미 완료됨')
            #             break
                # else:
                #     # 중복되는 기사 없으면 요약 추가
            db.mts.insert_many(df.to_dict('records'))
                    
        elif keyword == '마이데이터':
            # while True:
            #     if db.mydata.find_one({'title': df.loc[0, 'title'], 'press': df.loc[0, 'press']}):
            #         df = df.iloc[1:, :]
            #         if df.shape[0] == 0:
            #             print('업데이트 이미 완료됨')
            #             break
            db.mydata.insert_many(df.to_dict('records'))
        else: # '토큰증권', '증권형 토큰'
            db.sto.insert_many(df.to_dict('records'))
            #db.sto.find({}).sort()

        print(keyword + 'DB 업데이트 완료')
        result = 'updated'

    # 추가된 뉴스 기사 없는 경우
    else:
        print('업데이트 이미 완료됨')
        result = 'no_update'

    #return last_updated, cnt, df.shape[0], result # 마지막 실행시간, 검색된 기사 수, 스크래핑된 기사 수, 실행결과
def get_summary(df):

    # 발급받은 API 키 설정
    OPENAI_API_KEY = "////"

    openai.api_key = OPENAI_API_KEY

    prompt = [
        {
            "role": "system",
            "content": "You are an assistant who is good at summarizing financial and business articles"
        },
        {
            "role": "assistant",
            "content": "You fully understand how to summarize a news article efficiently that is describe in the link https://medium.com/@articlesumm27/how-to-summarize-a-newspaper-article-efficiently-top-tips-and-guideline-regarding-on-this-topic-88b1fd36c026"
        },
        {
            "role": "user", 
            "content": "Condense up to 3 important sentences, each with less than 15 words in Korean, and in bullet list"
        }
    ]

    summary = []
    
    i = 0
    
    for idx in range(df.shape[0]):

        messages = prompt.copy()

        query = f"뉴스 기사의 제목과 텍스트를 입력받아 증권사 신사업 부서의 입장에서 요약해줘. 각 문장이 '다'로 끝나지 않게 요약해줘\
            제목은 다음과 같고: {df.loc[idx, 'title']}\n뉴스 기사는 아래와 같아\n{df.loc[idx, 'main_article']}"

        # 사용자 메시지 추가
        messages.append(
            {
                "role": "user", 
                "content": query
            }
        )

        response = openai.ChatCompletion.create(
                model="gpt-3.5-turbo",
                messages=messages
            )

        if i%3 == 0:
           time.sleep(60)
        
        answer = response['choices'][0]['message']['content']
        summary.append(answer)

        i += 1

        print(i, '번째 뉴스 요약 완료')
    
    return summary

def search_news(keyword): # start_date, end_date

    client = MongoClient('')
    db = client.news_data

    # 지정된 키워드와 동일한 경우
    if keyword == 'MTS':
        return pd.DataFrame(db.mts.find().sort([("date", -1), ("time", -1)]).limit(50)).drop('_id', axis=1)
        #return pd.DataFrame(db.mts.find({}).sort('_id', -1).limit(50)).drop('_id', 1)
    elif keyword == '마이데이터':
        return pd.DataFrame(db.mydata.find().sort([("date", -1), ("time", -1)]).limit(50)).drop('_id', axis=1)
    elif keyword in ['토큰증권', '증권형 토큰']:
        return pd.DataFrame(db.sto.find().sort([("date", -1), ("time", -1)]).limit(50)).drop('_id', axis=1)
    
    # 키워드 입력해서 검색하는 경우 - 현재 기준 3일치 뉴스 기사 가져옴
    else:
        current_date = datetime.now()
        previous_date = current_date - timedelta(days=3)

        start_date = previous_date.strftime("%Y%m%d")
        end_date = datetime.now().strftime("%Y%m%d") # current date

        news_scrap = []
        naver_urls = []
        news_info_list = []
        page_num = 1
        cnt = 0
        i = 1

        print(keyword, '에 대한 뉴스 검색 결과')

        while True:
            try:
                url = f'https://search.naver.com/search.naver?where=news&query={keyword}&sort=1&nso=so%3Add%2Cp%3Afrom{start_date}to{end_date}&start={i}'
                headers = {'User-Agent': 'Mozilla/5.0'}
                response = requests.get(url, headers=headers)
                soup = BeautifulSoup(response.content, 'html.parser')
                article_list = soup.select('#main_pack > section > div > div.group_news > ul > li')
                # article_list 안에 빈 요소 들어오는 경우 예외처리

                if article_list:
                    #print('페이지', page_num)
                    cnt += len(article_list)
                    for article in article_list:
                        try:
                            title = article.select_one(".news_tit").text
                            desc = article.select_one("a.api_txt_lines").text
                            press = article.select_one("a.info.press").text
                            doc = {
                                "title": title,
                                "description": desc,
                                "press": press
                            }

                            if keyword in title:
                                info_group = article.select('div.info_group > a')
                                # 네이버뉴스 링크있는 경우
                                if len(info_group) >= 2: 
                                    naver_url = info_group[1]['href']
                                    naver_urls.append(naver_url)
                                    news_info_list.append(doc)
                                else:
                                    pass
                            else:
                                pass
                        except:
                            pass
                else:
                    break
                i += 10
                page_num += 1
            except:
                print('Error - 뉴스 기사 목록 오류')
                break

        for url in naver_urls:
            try:
                response = requests.get(url, headers=headers)
                soup = BeautifulSoup(response.content, 'html.parser')
                date_txt = soup.select_one('div.media_end_head_info_datestamp > div').text # 수정된 일자가 있는 경우 최초 일자 기준으로 가져옴
                # if len(date_element) > 1:
                #     date_txt = date_element[1].text
                # else:
                #     date_txt = date_element[0].text
                main_article = soup.select_one('#dic_area').text
                date = date_txt.split()[0][2:-1] 
                if date_txt.split()[1] == '오전':
                    time = date_txt.split()[2]
                else:
                    hour = int(date_txt.split()[2].split(':')[0])
                    minute = date_txt.split()[2].split(':')[1]
                    hour += 12
                    time = str(hour) + ':' + minute

                #time = ' '.join(date_txt.split()[1:3])
                a_dict = {
                    'date': date,
                    'time': time,
                    'main_article': main_article,
                    'url': url
                }
                news_scrap.append(a_dict)
                
            except:
                print('Error - 네이버 기사 스크래핑 오류')

        if len(news_scrap) > 0:

            # 리프레시 버튼으로 가져오는 경우 두 리스트 길이가 다를 수 있음 고려
            df = pd.concat([pd.DataFrame(news_info_list[:len(news_scrap)]), pd.DataFrame(news_scrap)], axis=1)
            df['keyword'] = [keyword] * df.shape[0]
            # 기본 전처리
            
            #df = df.sort_values(['date', 'time']) # 시간 순서대로 정렬해 저장
            #df['press'] = df['press'].apply() 언론사 이름에서 '선정' 제외

            df['main_article'] = df['main_article'].apply(lambda x: re.sub("[`]", "'", x))
            df['title'] = df['title'].apply(lambda x: re.sub("[`]", "'", x))

            print('스크래핑된 뉴스 기사 수:', df.shape[0])
            print('검색된 뉴스 기사 수:', cnt)

            return df
        
        # 검색결과 없는 경우
        else:
            print('0건 검색됨')
            return pd.DataFrame() # 빈 데이터프레임 반환 

##################################################

# flask 선언
application = app = Flask(__name__)

@app.route('/')
def main():
   return render_template('index.html')

@app.route('/home', methods=['GET'])
def home_page():
    data = {}

    data['mts'] = list(db.mts.find({}, {'_id': 0}))[::-1]
    data['mydata'] = list(db.mydata.find({}, {'_id': 0}))[::-1]
    data['sto'] = list(db.sto.find({}, {'_id': 0}))[::-1]
    return render_template('home.html', data=data)

import pandas as pd

@app.route('/search', methods=['GET', 'POST'])
def search_page():
    if request.method == 'POST':
        keyword = request.form['keyword']
        results = search_news(keyword)  
        results_json = results.to_json(orient='records')
        return results_json
    else:
        return render_template('search.html')

@app.route('/refresh', methods=['POST'])
def refresh_data():
    tab = request.json.get('tab')  
    normTab = tab.upper()

    if normTab == 'MTS':
        scrap_news('MTS')
        db.refresh_date.update_one(
            {"keyword":normTab},
            {'$set': {'refreshed_at': datetime.now()}},
            upsert=True
            )
        return 'Refresh successful'  
    elif normTab == '마이데이터':
        scrap_news('마이데이터')
        db.refresh_date.update_one(
            {"keyword":normTab},
            {'$set': {'refreshed_at': datetime.now()}},
            upsert=True
            )
        return 'Refresh successful'
    elif normTab == '토큰증권':
        scrap_news('토큰증권')
        db.refresh_date.update_one(
            {"keyword":normTab},
            {'$set': {'refreshed_at': datetime.now()}},
            upsert=True
            )
        return 'Refresh successful'
    elif normTab == 'STO':
        scrap_news('STO')
        return 'Refresh successful'
    else:
        return '이미 업데이트된 데이터입니다.'  
    
    
@app.route('/last_refreshed')
def get_last_refreshed():
    last_refreshed = {}
    for tab in ['MTS', '마이데이터', 'STO']:
        result = db.refresh_date.find_one({'keyword': tab}, {'_id': 0, 'refreshed_at': 1}, sort=[('refreshed_at', -1)])
        if result:
            last_refreshed[tab] = result['refreshed_at']
        else:
            last_refreshed[tab] = None

    return jsonify(last_refreshed)



if __name__ == '__main__':  
   app.run()
