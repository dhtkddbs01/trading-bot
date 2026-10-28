# 수신기(토스 WebSocket → Kafka) 이미지 — 서버에서 docker compose 로 장 시간에만 실행
FROM python:3.12-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY src/ src/
# 키(.env)는 이미지에 넣지 않고 compose 의 env_file 로 넘긴다
CMD ["python", "-u", "-m", "src.exchange.web_socket"]
