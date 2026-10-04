"""토스증권 Open API 클라이언트 — 2단계: 토큰 발급 → 현재가 조회."""
import json
import os
import sys
import time
from pathlib import Path

import requests
from dotenv import load_dotenv

BASE_URL = "https://openapi.tossinvest.com"
ROOT = Path(__file__).resolve().parents[2]         # repo 루트 (src/exchange/toss.py 에서 두 단계 위)
TOKEN_CACHE = ROOT / ".cache" / "toss_token.json"   # .gitignore 로 git 에서 빠지는 자리

load_dotenv(ROOT / ".env")  # .env 에서 환경변수 읽기   

def _check(resp: requests.Response) -> None:
    """실패하면 상태코드 + 응답 본문(에러 코드)을 보여 주고 멈춘다."""
    if not resp.ok:
        raise RuntimeError(f"HTTP {resp.status_code}: {resp.text}")


def _issue_token() -> dict:
    """새 토큰 발급. ⚠️ 발급하는 순간 이 클라이언트의 이전 토큰은 무효가 된다."""
    resp = requests.post(
        f"{BASE_URL}/oauth2/token",
        data={                                       # data= 로 보내면 form-urlencoded 형식
            "grant_type": "client_credentials",
            "client_id": os.environ["TOSS_CLIENT_ID"],
            "client_secret": os.environ["TOSS_CLIENT_SECRET"],
        },
        timeout=10,
    )
    _check(resp)
    body = resp.json()
    # 만료 60초 전부터는 새로 받도록 여유를 둔다
    return {"access_token": body["access_token"], "expires_at": time.time() + body["expires_in"] - 60}


def get_token(force_new: bool = False) -> str:
    """저장된 토큰이 살아 있으면 다시 쓰고, 아니면 새로 발급해 저장한다."""
    if not force_new and TOKEN_CACHE.exists():
        cached = json.loads(TOKEN_CACHE.read_text())
        if cached["expires_at"] > time.time():
            return cached["access_token"]
    token = _issue_token()
    TOKEN_CACHE.parent.mkdir(exist_ok=True)
    TOKEN_CACHE.write_text(json.dumps(token))
    return token["access_token"]


def get_prices(symbols: list[str]) -> list[dict]:
    """현재가 조회. symbols 예: ["005930", "000660"] (최대 200개)."""
    def call(token: str) -> requests.Response:
        return requests.get(
            f"{BASE_URL}/api/v1/prices",
            params={"symbols": ",".join(symbols)},
            headers={"Authorization": f"Bearer {token}"},
            timeout=10,
        )

    resp = call(get_token())
    if resp.status_code == 401:                      # 저장된 토큰이 다른 곳의 재발급으로 무효가 된 경우
        resp = call(get_token(force_new=True))       # 한 번만 새로 받아 다시 시도
    _check(resp)
    return resp.json()["result"]


if __name__ == "__main__":
    symbols = sys.argv[1:] or ["005930"]              # 인자가 없으면 삼성전자
    for p in get_prices(symbols):
        print(p["symbol"], p["lastPrice"], p["currency"], p.get("timestamp"))