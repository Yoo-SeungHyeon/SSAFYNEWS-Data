import os
import json
from datetime import datetime
from typing import Tuple, List

import ollama
from sentence_transformers import SentenceTransformer
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import (
    StreamTableEnvironment,
    EnvironmentSettings,
    DataTypes
)
from pyflink.table.expressions import col
from pyflink.table.udf import udf, ScalarFunction
from pyflink.table import RichScalarFunction  # 변경된 임포트
from hdfs import InsecureClient
from sqlalchemy import create_engine

# HDFS 설정
HDFS_URL = "http://localhost:9870"
HDFS_OUTPUT_PATH = f"{HDFS_URL}/batch/data/realtime"
try:
    HDFS_CLIENT = InsecureClient(HDFS_URL, user="root")
    USE_HDFS = True
except Exception as e:
    print(f"HDFS 연결 오류: {e}. HDFS 저장을 비활성화합니다.")
    HDFS_CLIENT = None
    USE_HDFS = False

# PostgreSQL 설정
DATABASE_URL = "postgresql://ssafynews:ssafynews13@localhost:5432/news"
ENGINE = create_engine(DATABASE_URL)

# 카테고리 목록
CATEGORIES = ["IT_과학", "건강", "경제", "교육", "국제", "라이프스타일", "문화",
              "사건사고", "사회일반", "산업", "스포츠", "여성복지", "여행레저",
              "연예", "정치", "지역", "취미"]

@udf(result_type=DataTypes.STRING())
def classify_category(text: str) -> str:
    """Ollama를 사용하여 텍스트를 카테고리로 분류"""
    prompt = f"""
    다음 텍스트를 가장 적절한 카테고리 하나로 분류해주세요: {', '.join(CATEGORIES)}
    텍스트:
    ---
    {text}
    ---
    답변은 카테고리 이름만 정확하게 출력해주세요.
    """
    try:
        response = ollama.chat(
            model='gemma3:4b-it-qat',
            messages=[{'role': 'user', 'content': prompt}]
        )
        output = response['message']['content'].strip()
        return output if output in CATEGORIES else "미분류"
    except Exception as e:
        print(f"카테고리 분류 오류: {e}")
        return "미분류"

@udf(result_type=DataTypes.ARRAY(DataTypes.STRING()))
def extract_keywords(text: str) -> List[str]:
    """Ollama를 사용하여 키워드 추출"""
    prompt = f"""
    다음 텍스트에서 핵심 키워드 5개를 추출하여 쉼표로 구분하여 답변해주세요.
    ---
    {text}
    ---
    """
    try:
        response = ollama.chat(
            model='gemma3:4b-it-qat',
            messages=[{'role': 'user', 'content': prompt}]
        )
        return [k.strip() for k in response['message']['content'].split(',')]
    except Exception as e:
        print(f"키워드 추출 오류: {e}")
        return []

class GenerateEmbedding(RichScalarFunction):
    def __init__(self):
        self.embedding_model = None

    def open(self, function_context):
        self.embedding_model = SentenceTransformer('all-mpnet-base-v2')

    def eval(self, text: str) -> List[float]:
        try:
            return self.embedding_model.encode(text).tolist()
        except Exception as e:
            print(f"임베딩 생성 오류: {e}")
            return []

generate_embedding_udf = udf(GenerateEmbedding(), result_type=DataTypes.ARRAY(DataTypes.FLOAT()))

def save_to_hdfs(data: dict, path: str):
    """HDFS에 데이터 저장"""
    try:
        with HDFS_CLIENT.write(path, encoding='utf-8') as writer:
            json.dump(data, writer, ensure_ascii=False)
    except Exception as e:
        print(f"HDFS 저장 오류: {e}")

def save_to_postgresql(data: dict):
    """PostgreSQL에 데이터 저장"""
    try:
        with ENGINE.connect() as conn:
            conn.execute("""
                INSERT INTO news_api_newsarticle
                (title, author, link, summary, updated, full_text, category, keywords, embedding)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (link) DO NOTHING
            """, (
                data['title'],
                data.get('author'),
                data['link'],
                data.get('summary'),
                datetime.now(),
                data.get('full_text'),
                data.get('category'),
                data.get('keywords'),
                data.get('embedding')
            ))
    except Exception as e:
        print(f"PostgreSQL 저장 오류: {e}")

def main():
    # Flink 환경 설정
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(3)  # 병렬 처리 수준 설정
    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    t_env = StreamTableEnvironment.create(env, environment_settings=settings)

    # Kafka 소스 테이블 생성
    t_env.execute_sql("""
        CREATE TABLE kafka_source (
            title STRING,
            author STRING,
            link STRING,
            summary STRING,
            full_text STRING,
            proctime AS PROCTIME()
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'article-topic',
            'properties.bootstrap.servers' = 'localhost:9092',
            'properties.group.id' = 'flink-news-consumer',
            'format' = 'json',
            'scan.startup.mode' = 'earliest-offset'
        )
    """)

    # 데이터 처리 파이프라인 구성
    table = t_env.from_path('kafka_source')

    result = table.add_columns(
        classify_category(col('summary')).alias('category'),
        extract_keywords(col('summary')).alias('keywords'),
        generate_embedding_udf(col('summary')).alias('embedding') # 변경된 UDF 사용
    )

    # PostgreSQL 싱크 테이블 생성
    t_env.execute_sql("""
        CREATE TABLE postgresql_sink (
            title STRING,
            author STRING,
            link STRING,
            summary STRING,
            updated TIMESTAMP,
            full_text STRING,
            category STRING,
            keywords ARRAY<STRING>,
            embedding ARRAY<FLOAT>,
            PRIMARY KEY (link) NOT ENFORCED
        ) WITH (
            'connector' = 'jdbc',
            'url' = 'jdbc:postgresql://localhost:5432/news',
            'table-name' = 'news_api_newsarticle',
            'username' = 'ssafynews',
            'password' = 'ssafynews13',
            'driver' = 'org.postgresql.Driver',
            'dialect' = 'postgresql',
            'on-conflict' = 'DO NOTHING'
        )
    """)

    # HDFS 싱크 테이블 생성 (USE_HDFS 조건부 활성화)
    if USE_HDFS:
        t_env.execute_sql(f"""
            CREATE TABLE hdfs_sink (
                title STRING,
                author STRING,
                link STRING,
                summary STRING,
                full_text STRING,
                category STRING,
                keywords ARRAY<STRING>,
                embedding ARRAY<FLOAT>,
                year_month_day STRING
            ) WITH (
                'connector' = 'filesystem',
                'path' = '{HDFS_OUTPUT_PATH}',
                'format' = 'json',
                'sink.partition-by' = 'year_month_day',
                'sink.rolling-policy.file-size' = '128MB',
                'sink.rolling-policy.rollover-interval' = '1 min'
            )
        """)

    # 결과 PostgreSQL로 저장
    result.select("""
        title, author, link, summary, CAST(proctime AS TIMESTAMP), full_text, category, CAST(keywords AS ARRAY<STRING>), CAST(embedding AS ARRAY<FLOAT>)
    """).insert_into('postgresql_sink')

    # 결과 HDFS로 저장 (USE_HDFS 조건부 실행)
    if USE_HDFS:
        result.select("""
            title, author, link, summary, full_text, category, CAST(keywords AS ARRAY<STRING>), CAST(embedding AS ARRAY<FLOAT>),
            DATE_FORMAT(proctime, 'yyyyMMdd')
        """).insert_into('hdfs_sink')

    # 실행
    env.execute("News Processing Pipeline")

if __name__ == "__main__":
    main()