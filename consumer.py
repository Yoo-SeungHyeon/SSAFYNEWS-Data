import os
import re
import json
import requests
from openai import OpenAI
from datetime import datetime
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from kafka import KafkaConsumer
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine, Table, MetaData
from sqlalchemy.dialects.postgresql import insert as pg_insert

# pyflink가 제대로 동작하지 않아 일단 KafkaConfumer를 사용하여 데이터 처리

load_dotenv()  # .env 파일 읽어오기

client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))


# PostgreSQL 연결 URL
DATABASE_URL = "postgresql://ssafynews:ssafynews13@localhost:5433/news"

# 데이터베이스 엔진 생성
# 기존 코드와 동일
engine = create_engine(DATABASE_URL)
metadata = MetaData()

# ✅ 테이블이 존재하지 않으면 자동 생성
from sqlalchemy import Column, Integer, String, Text, DateTime, Float, ARRAY
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.exc import ProgrammingError

try:
    # 이미 존재하는 경우 로드
    articles_table = Table(
        "news_api_newsarticle",
        metadata,
        schema="public",
        autoload_with=engine
    )
except ProgrammingError:
    print("테이블이 존재하지 않아 생성합니다.")
    # 테이블 수동 정의
    articles_table = Table(
        "news_api_newsarticle",
        metadata,
        Column("id", Integer, primary_key=True, autoincrement=True),
        Column("title", String),
        Column("author", String),
        Column("link", String, unique=True, nullable=False),
        Column("summary", Text),
        Column("updated", DateTime),
        Column("full_text", Text),
        Column("category", String),
        Column("keywords", ARRAY(String)),  # 또는 JSONB
        Column("embedding", ARRAY(Float)),
        schema="public"
    )
    metadata.create_all(engine)
    print("✅ 테이블 생성 완료")

# 세션 생성
SessionLocal = sessionmaker(bind=engine)
def insert_article_ignore_duplicate(data: dict):
    session = SessionLocal()
    try:
        insert_stmt = pg_insert(articles_table).values(
            title=data["title"],
            author=data.get("author"),
            link=data["link"],
            summary=data.get("summary"),
            updated=data.get("updated", datetime.utcnow()),
            full_text=data.get("full_text"),
            category=data.get("category"),
            keywords=data.get("keywords"),
            embedding=data.get("embedding"),
        )

        # 중복 링크 있을 경우 무시
        do_nothing_stmt = insert_stmt.on_conflict_do_nothing(index_elements=['link'])
       
        session.execute(do_nothing_stmt)
        print(f"DB 적재 성공")
        session.commit()
    except Exception as e:
        session.rollback()
        raise e
    finally:
        session.close()

def Crawl_Article(url: str) -> str:
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36'
    }
    try:
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()
    except requests.RequestException as e:
        print(f"요청 실패: {e}")
        return ''
    
    soup = BeautifulSoup(response.text, 'html.parser')
    texts = soup.stripped_strings

    content_list = []
    for text in texts:
        # 한글 3자 이상 포함된 텍스트만 수집
        if re.search(r'[가-힣]{3,}', text):
            content_list.append(text)

    content = ' '.join(content_list)
    return content


def CategoryClassifier(article_text: str) -> str:
    categories = ["IT_과학", "건강", "경제", "교육", "국제", "라이프스타일", "문화", "사건사고", "사회일반",
                  "산업", "스포츠", "여성복지", "여행레저", "연예", "정치", "지역", "취미"]
    prompt = f"""
            다음 뉴스 내용을 가장 적절한 카테고리 하나로 분류해줘: {', '.join(categories)}
            뉴스 내용:
            {article_text}
            답변은 카테고리 이름만 출력해줘.
            """
    response = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[{"role": "user", "content": prompt}],
        max_tokens=20
    )
    output = response.choices[0].message.content.strip()
    return output if output in categories else "미분류"
    
    

def TagExtractor(article_text: str) -> list:
    prompt = f"다음 뉴스에서 핵심 키워드 5개를 쉼표로 구분하여 출력해주세요.\n\n{article_text}"
    response = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[{"role": "user", "content": prompt}],
        max_tokens=100
    )
    return [k.strip() for k in response.choices[0].message.content.split(",")]

def EmbeddingGenerator(article_text: str) -> list:
    response = client.embeddings.create(
        model="text-embedding-3-small",
        input=article_text
    )
    return response.data[0].embedding


consumer = KafkaConsumer(
    'article-topic',
    bootstrap_servers='localhost:9092',
    group_id='news-consumer',  # ⭐️ 고정된 group_id 추가
    auto_offset_reset='earliest',
    enable_auto_commit=False,  # 자동 커밋 비활성화. 대신 수동으로 offset 커밋
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)


for message in consumer:
    data = message.value
    print(f"메시지 수신: {data['title']}")
    data['full_text'] = Crawl_Article(data['link'])
    data['category'] = CategoryClassifier(data['summary'])
    data['keywords'] = TagExtractor(data['summary'])
    data['embedding'] = EmbeddingGenerator(data['summary'])
    
    # db에 저장
    insert_article_ignore_duplicate(data)
    
    # db에 저장되면 수동으로 커밋밋
    consumer.commit()
    
    print('--'*20)