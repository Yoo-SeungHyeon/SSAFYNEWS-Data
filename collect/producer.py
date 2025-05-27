import json
import time
import feedparser
from collections import deque
from kafka import KafkaProducer

histoty = deque(maxlen=1000)

def check_duplicate(url):
    if url in histoty:
        print("⚠️ 중복")
        return True
    else:
        histoty.append(url)
        return False


def rss_n_kafka_producer(URL):
    # URL = "https://www.khan.co.kr/rss/rssdata/total_news.xml"
    response = feedparser.parse(URL)

    # 피드 갯수 확인
    print(f"불러온 피드 갯수: {len(response['entries'])}")

    # Kafka 프로듀서 설정
    producer = KafkaProducer(
            bootstrap_servers='broker:9092',
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
    
    # 반복문으로 피드들에서 데이터 추출
    for article in response['entries']:
        # 중복 체크
        if check_duplicate(article['link']): break
        
        author = article['author']
        link = article['link']
        summary = article['summary']
        title = article['title']
        updated = article['updated']

        message = {
            "author": author,
            "link": link,
            "summary": summary,
            "title": title,
            "updated": updated
        }
        
        producer.send("article-topic", value=message)
        print(f"메시지 전송 완료: {message['title']}")

    # 혹시 남은 메시지가 남아있다면 전송
    producer.flush()

    # 프로듀서 종료
    producer.close()


RSS_FEED_URL_LIST = ["https://www.khan.co.kr/rss/rssdata/total_news.xml",       # 경향신문 / 전체 뉴스
                    "https://www.khan.co.kr/rss/rssdata/cartoon_news.xml",      # 경향신문 / 만평
                    "https://www.khan.co.kr/rss/rssdata/opinion_news.xml",      # 경향신문 / 오피니언
                    "https://www.khan.co.kr/rss/rssdata/politic_news.xml",      # 경향신문 / 정치
                    "https://www.khan.co.kr/rss/rssdata/economy_news.xml",      # 경향신문 / 경제
                    "https://www.khan.co.kr/rss/rssdata/society_news.xml",      # 경향신문 / 사회
                    "https://www.khan.co.kr/rss/rssdata/local_news.xml",        # 경향신문 / 지역
                    "https://www.khan.co.kr/rss/rssdata/kh_world.xml",          # 경향신문 / 국제
                    "https://www.khan.co.kr/rss/rssdata/culture_news.xml",      # 경향신문 / 문화
                    "https://www.khan.co.kr/rss/rssdata/kh_sports.xml",         # 경향신문 / 스포츠
                    "https://www.khan.co.kr/rss/rssdata/science_news.xml",      # 경향신문 / 과학&환경
                    "https://www.khan.co.kr/rss/rssdata/life_news.xml",         # 경향신문 / 라이프
                    "https://www.khan.co.kr/rss/rssdata/people_news.xml",       # 경향신문 / 사람
                    "https://www.khan.co.kr/rss/rssdata/english_news.xml",      # 경향신문 / 영문
                    "https://www.khan.co.kr/rss/rssdata/newsletter_news.xml",   # 경향신문 / 뉴스레터
                    "https://www.khan.co.kr/rss/rssdata/interactive_news.xml",  # 경향신문 / 인터랙티브
                    "https://www.kmib.co.kr/rss/data/kmibRssAll.xml",           # 국민일보 / 전체기사
                    "https://www.kmib.co.kr/rss/data/kmibPolRss.xml",           # 국민일보 / 정치
                    "https://www.kmib.co.kr/rss/data/kmibEcoRss.xml",           # 국민일보 / 경제
                    "https://www.kmib.co.kr/rss/data/kmibSocRss.xml",           # 국민일보 / 사회
                    "https://www.kmib.co.kr/rss/data/kmibIntRss.xml",           # 국민일보 / 국제
                    "https://www.kmib.co.kr/rss/data/kmibEntRss.xml",           # 국민일보 / 연예
                    "https://www.kmib.co.kr/rss/data/kmibSpoRss.xml",           # 국민일보 / 스포츠
                    "https://www.kmib.co.kr/rss/data/kmibGolfRss.xml",          # 국민일보 / 골프
                    "https://www.kmib.co.kr/rss/data/kmibLifeRss.xml",          # 국민일보 / 라이프
                    "https://www.kmib.co.kr/rss/data/kmibTraRss.xml",           # 국민일보 / 여행
                    "https://www.kmib.co.kr/rss/data/kmibEsportsRss.xml",       # 국민일보 / e스포츠
                    "https://www.kmib.co.kr/rss/data/kmibColRss.xml",           # 국민일보 / 사설/칼럼
                    "https://www.kmib.co.kr/rss/data/kmibChrRss.xml",           # 국민일보 / 더미션
                    ]

if __name__ == "__main__":
    time.sleep(60)
    while True:
        for url in RSS_FEED_URL_LIST:
            rss_n_kafka_producer(url)
            
        print("5분 대기 중...")
        time.sleep(300)