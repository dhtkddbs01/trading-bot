"""토스증권 실시간 체결 수신 → Kafka — 착수 3 · 4단계 (WebSocket)."""
import asyncio
import json
import os
import signal
import sys
import time
from datetime import datetime, timedelta, timezone
import websockets
from websockets.exceptions import ConnectionClosed, InvalidStatus

from confluent_kafka import Producer
from src.exchange.toss import ROOT, get_token    # 2단계 토큰 함수 재사용 (24시간 저장·재사용) · repo 루트 경로

WS_URL = "wss://openapi-ws.tossinvest.com/ws/v1"
KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "127.0.0.1:9092")   # 서버 컨테이너에서는 compose 가 kafka:19092 를 넣어 줌
producer = Producer({'bootstrap.servers': KAFKA_BOOTSTRAP})
FALLBACK_DIR = ROOT / "data" / "fallback"         # Kafka 전송에 실패한 원본을 남기는 곳 (.gitignore 로 git 에서 빠짐)
KST = timezone(timedelta(hours=9))                # 파일 날짜는 한국 시간 기준 (서버 컨테이너는 UTC 라서)

async def keepalive(ws) -> None:
    """60초마다 'PING' 을 보냄. 서버는 180초 동안 아무것도 못 받으면 연결을 끊기 때문"""
    while True:
        await asyncio.sleep(60)
        await ws.send("PING")


def build_subscribe(codes: list[str]) -> str:
    """구독 선언 메시지(JSON 문자열)를 만듦"""
    result_json = [{"type": "trade:kr", "codes": codes}]
    return json.dumps(result_json)

def on_delivery(err, msg) -> None:
    """전송 결과 콜백 — 실패하면 원본을 파일에 남긴다"""
    if err is not None:
        print(f"Error occurred while sending message: {err}")
        FALLBACK_DIR.mkdir(parents=True, exist_ok=True)                     # 처음 실패하면 폴더부터 만듦
        path = FALLBACK_DIR / f"ticks_{datetime.now(KST):%Y-%m-%d}.jsonl"   # 날짜별 파일 (한 줄 = 메시지 하나)
        with open(path, "ab") as f:                                         # "ab" = 지우지 않고 뒤에 덧붙이기, bytes 그대로
            f.write(msg.value() + b"\n")                                    # msg.value() = produce 때 보낸 원본 (bytes)

def handle(raw: str) -> None:
    """서버가 보낸 메시지 하나를 처리"""
    data = json.loads(raw)
    if data["type"] == "pong":      # pong 메시지는 무시
        pass
    elif data["type"] == "message": # message 메시지는 채결 정보
        data_info = data["data"]
        topic = "ticks.raw"
        topic_code = data["topic"].split(':')[2]  # topic: trade:kr:005930 -> 005930
        timestamp = data_info["timestamp"]
        price = data_info["price"]
        volume = data_info["volume"]
        producer.produce(topic, key=topic_code, value=raw, on_delivery=on_delivery)
        producer.poll(0)  # 전송 완료 확인
        print(f"{topic_code} {timestamp} {price} {volume}")
    else:
        print(raw)


async def run(codes: list[str]) -> None:
    """연결 관리"""
    headers = {"Authorization": f"Bearer {get_token()}"}
    async with websockets.connect(WS_URL, additional_headers=headers) as ws:   # 연결 열기 (끝나면 자동으로 닫힘)
        await ws.send(build_subscribe(codes))              # 구독 선언 보내기
        ping_task = asyncio.create_task(keepalive(ws))     # PING 은 옆에서 따로 돌림
        try:
            async for raw in ws:                            # 메시지가 올 때마다 한 번씩 반복
                handle(raw)
        finally:
            timeout = 5.0
            left = producer.flush(timeout)                  # 종료 시점에 남은 메시지 전송 (못 보낸 개수를 돌려줌)
            print(f"연결 정리 — Kafka 로 못 보낸 메시지 {left}건")
            ping_task.cancel()                              # 끝날 때 PING 작업도 정리


async def run_forever(codes: list[str]) -> None:
    """끊기면 다시 연결 — 5초부터 두 배씩, 최대 60초 간격"""
    delay = 5
    while True:
        started = time.monotonic()
        try:
            await run(codes)                                # 서버가 정상적으로 닫아도 장중이면 다시 연결
            print("연결이 닫힘")
        except InvalidStatus as e:                          # 연결 단계에서 거절 (HTTP 상태코드)
            print(f"연결 거절: HTTP {e.response.status_code}")
            if e.response.status_code == 401:               # 다른 곳에서 토큰을 새로 받아 저장된 토큰이 무효가 된 경우
                get_token(force_new=True)
        except (ConnectionClosed, OSError) as e:            # 연결 중 끊김 · 네트워크 오류
            print(f"연결 끊김: {e!r}")
        if time.monotonic() - started > 60:                 # 1분 넘게 잘 붙어 있었으면 간격을 처음부터
            delay = 5
        print(f"{delay}초 뒤 다시 연결")
        await asyncio.sleep(delay)
        delay = min(delay * 2, 60)


if __name__ == "__main__":
    codes = sys.argv[1:] or ["005930"]                      # 인자가 비어 있으면(거짓) 005930
    signal.signal(signal.SIGTERM, signal.default_int_handler)   # docker stop · timeout 의 종료 신호도 Ctrl+C 처럼 → finally 의 flush 가 돈다
    try:
        asyncio.run(run_forever(codes))
    except KeyboardInterrupt:
        print("종료 (Ctrl+C · 종료 신호)")
