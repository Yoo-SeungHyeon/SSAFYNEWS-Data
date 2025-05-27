import os
import sys
import argparse
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, count, desc, to_date
import matplotlib.pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages
from hdfs import InsecureClient

def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", required=True, help="YYYY-MM-DD")
    return parser.parse_args()

def main(report_date_str):
    # HDFS 및 경로 설정
    HDFS_HOSTPORT = "hdfs://hadoop-namenode:9000"
    WEBHDFS_URL = "http://hadoop-namenode:9870"  # WebHDFS REST API endpoint
    REALTIME_DIR = "/batch/data/realtime"
    ARCHIVE_DIR = "/batch/data/news_archive"
    HDFS_PDF_DIR = "/batch/data/pdf"
    LOCAL_PDF_DIR = "/tmp"
    INPUT_PATH = f"{HDFS_HOSTPORT}{REALTIME_DIR}/*.json"
    os.makedirs(LOCAL_PDF_DIR, exist_ok=True)

    spark = SparkSession.builder.appName("DailyNewsReport").getOrCreate()

    # 데이터 읽기
    try:
        df = spark.read.option("multiline", "true").json(INPUT_PATH)
    except Exception as e:
        print(f"데이터 읽기 실패: {e}")
        spark.stop()
        sys.exit(1)

    report_date = datetime.strptime(report_date_str, "%Y-%m-%d").date()
    df = df.withColumn("published_date", to_date(col("published_at")))

    # 해당 날짜 데이터만 필터링
    daily_df = df.filter(col("published_date") == report_date_str)

    # 집계
    summary = {}
    if daily_df.count() > 0:
        # 카테고리별 기사 수
        category_count = (
            daily_df.groupBy("category").count().orderBy(desc("count")).toPandas()
        )
        # 키워드별 빈도
        keywords_exploded = daily_df.withColumn("keyword", explode(col("keywords")))
        keyword_count = (
            keywords_exploded.groupBy("keyword").count().orderBy(desc("count")).toPandas()
        )
        # 기사 목록
        articles = daily_df.select("title", "content", "category", "published_at").toPandas()

        summary["category_count"] = category_count
        summary["keyword_count"] = keyword_count
        summary["articles"] = articles
    else:
        summary["category_count"] = None
        summary["keyword_count"] = None
        summary["articles"] = None

    # PDF 리포트 생성
    pdf_filename = f"daily_report_{report_date_str}.pdf"
    pdf_local_path = os.path.join(LOCAL_PDF_DIR, pdf_filename)
    with PdfPages(pdf_local_path) as pdf:
        plt.figure(figsize=(8, 4))
        plt.title(f"뉴스 카테고리별 기사 수 ({report_date_str})")
        if summary["category_count"] is not None and not summary["category_count"].empty:
            plt.bar(summary["category_count"]["category"], summary["category_count"]["count"])
            plt.xlabel("카테고리")
            plt.ylabel("기사 수")
            plt.tight_layout()
        else:
            plt.text(0.5, 0.5, "해당 날짜에 대한 데이터가 없습니다.", ha='center', va='center', fontsize=14)
        pdf.savefig()
        plt.close()

        plt.figure(figsize=(8, 4))
        plt.title(f"키워드별 빈도수 Top 10 ({report_date_str})")
        if summary["keyword_count"] is not None and not summary["keyword_count"].empty:
            top_keywords = summary["keyword_count"].head(10)
            plt.bar(top_keywords["keyword"], top_keywords["count"])
            plt.xlabel("키워드")
            plt.ylabel("빈도")
            plt.tight_layout()
        else:
            plt.text(0.5, 0.5, "해당 날짜에 대한 데이터가 없습니다.", ha='center', va='center', fontsize=14)
        pdf.savefig()
        plt.close()

        if summary["articles"] is not None and not summary["articles"].empty:
            plt.figure(figsize=(8, len(summary["articles"]) * 0.5 + 1))
            plt.axis('off')
            plt.title(f"기사 목록 ({report_date_str})", loc='left', fontsize=14)
            y = 1.0
            for _, row in summary["articles"].iterrows():
                plt.text(0, y, f"[{row['category']}] {row['title']} ({row['published_at']})", fontsize=10, wrap=True)
                y -= 0.06
                plt.text(0.02, y, row['content'], fontsize=9, wrap=True)
                y -= 0.09
            pdf.savefig()
            plt.close()

    print(f"PDF 리포트 저장(로컬): {pdf_local_path}")

    # PDF를 HDFS로 업로드
    client = InsecureClient(WEBHDFS_URL, user='root')  # user는 HDFS 권한에 맞게 조정
    try:
        with open(pdf_local_path, 'rb') as pdf_file:
            client.write(f"{HDFS_PDF_DIR}/{pdf_filename}", pdf_file, overwrite=True)
        print(f"PDF 리포트 업로드(HDFS): {HDFS_PDF_DIR}/{pdf_filename}")
    except Exception as e:
        print(f"PDF HDFS 업로드 실패: {e}")

    # 원본 JSON 파일 HDFS 내에서 이동 (realtime → news_archive)
    try:
        file_list = client.list(REALTIME_DIR, status=True)
        for filename, meta in file_list:
            src = f"{REALTIME_DIR}/{filename}"
            dst = f"{ARCHIVE_DIR}/{filename}"
            client.rename(src, dst)
            print(f"HDFS 파일 이동: {src} -> {dst}")
    except Exception as e:
        print(f"HDFS 파일 이동 실패: {e}")

    spark.stop()
    print("작업 완료")

if __name__ == "__main__":
    args = parse_args()
    main(args.date)
'''
airflow/
├── dags/
│   └── scripts/
│       └── spark_daily_report.py     ← 이 파일 위치
        └── pdf folder                ← PDF 리포트 저장 폴더
├── data/                            
│   ├── realtime/                    ← JSON 원본 데이터
│   └── news_archive/                ← 처리 완료된 파일 이동                       


mkdir -p dags/scripts         # Spark 스크립트
mkdir -p data/realtime        # JSON 수신
mkdir -p data/news_archive    # 이동 대상
mkdir -p data              # PDF 출력

'''

