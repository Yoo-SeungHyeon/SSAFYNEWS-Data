FROM python:3.10-slim

WORKDIR /app

# 필요한 패키지 설치
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# 코드 및 설정 복사
COPY producer.py consumer.py .env ./

# 기본은 bash 진입
CMD ["bash"]
