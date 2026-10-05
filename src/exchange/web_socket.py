"""토스증권 실시간 체결 수신 — 착수 3단계 (WebSocket)."""
import asyncio
import json
import sys

import websockets

from src.exchange.toss import get_token          # 2단계 토큰 함수 재사용 (24시간 저장·재사용)

WS_URL = "wss://openapi-ws.tossinvest.com/ws/v1"


async def keepalive(ws) -> None:
    """60초마다 'PING' 을 보냄. 서버는 180초 동안 아무것도 못 받으면 연결을 끊기 때문"""
    while True:
        await asyncio.sleep(60)
        await ws.send("PING")


def build_subscribe(codes: list[str]) -> str:
    """구독 선언 메시지(JSON 문자열)를 만듦"""
    result_json = [{"type": "trade:kr", "codes": codes}]
    return json.dumps(result_json)


def handle(raw: str) -> None:
    """서버가 보낸 메시지 하나를 처리"""
    data = json.loads(raw)
    if data["type"] == "pong":      # pong 메시지는 무시
        pass
    elif data["type"] == "message": # message 메시지는 채결 정보
        data_info = data["data"]
        topic = data["topic"]
        timestamp = data_info["timestamp"]
        price = data_info["price"]
        volume = data_info["volume"]
        print(f"{topic} {timestamp} {price} {volume}")
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
            ping_task.cancel()                              # 끝날 때 PING 작업도 정리


if __name__ == "__main__":
    codes = sys.argv[1:] or ["005930"]                      # 인자가 비어 있으면(거짓) 005930
    try:
        asyncio.run(run(codes))
    except KeyboardInterrupt:
        print("종료 (Ctrl+C)")
