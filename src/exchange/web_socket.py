"""토스증권 실시간 체결 수신 — 착수 3단계 (WebSocket)."""
import asyncio
import json
import sys
from datetime import datetime, timedelta, timezone
import websockets

from confluent_kafka import Producer
from src.exchange.toss import ROOT, get_token    # 2단계 토큰 함수 재사용 (24시간 저장·재사용) · repo 루트 경로

WS_URL = "wss://openapi-ws.tossinvest.com/ws/v1"
producer = Producer({'bootstrap.servers': 'localhost:9092'})
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
            producer.flush(timeout)                         # 종료 시점에 남은 메시지 전송
            ping_task.cancel()                              # 끝날 때 PING 작업도 정리


if __name__ == "__main__":
    codes = sys.argv[1:] or ["005930"]                      # 인자가 비어 있으면(거짓) 005930
    try:
        asyncio.run(run(codes))
    except KeyboardInterrupt:
        print("종료 (Ctrl+C)")
