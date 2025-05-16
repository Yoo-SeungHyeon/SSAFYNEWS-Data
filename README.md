postgresql 컨테이너 접속
```bash
# 사용법
docker exec -it <컨테이너명> psql -U <user명> -d <db명>
```
```bash
docker exec -it pgvector_db psql -U ssafynews -d news
```