"""Diagnostic probe: raw DingTalk Stream protocol (ported from the proven
dingtalk-xbot-audit implementation) to A/B test whether group @ messages
reach a bare client. Logs EVERY frame verbatim. Ctrl+C to stop."""

import asyncio
import json
import os
import socket
import sys
import urllib.request
from pathlib import Path
from urllib.parse import quote_plus

import websockets

TOPIC = "/v1.0/im/bot/messages/get"
ENV_PATH = Path(__file__).resolve().parent.parent / ".env"


def load_env() -> dict:
    env = {}
    for line in ENV_PATH.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            k, _, v = line.partition("=")
            env[k.strip()] = v.strip()
    return env


def local_ip() -> str:
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("8.8.8.8", 80))
        return s.getsockname()[0]
    except Exception:
        return ""
    finally:
        s.close()


def http_post_json(url: str, payload: dict) -> dict:
    req = urllib.request.Request(
        url,
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=15) as resp:
        return json.loads(resp.read().decode("utf-8"))


def register(client_id: str, client_secret: str):
    payload = {
        "clientId": client_id,
        "clientSecret": client_secret,
        "subscriptions": [{"type": "CALLBACK", "topic": TOPIC}],
        "ua": "agenthub-probe/1.0",
        "localIp": local_ip(),
    }
    url = "https://api.dingtalk.com/v1.0/gateway/connections/open"
    data = http_post_json(url, payload)
    endpoint, ticket = (data or {}).get("endpoint", ""), (data or {}).get("ticket", "")
    if not endpoint or not ticket:
        raise RuntimeError(f"open connection failed: {data}")
    return endpoint, ticket


async def main() -> None:
    env = load_env()
    client_id = env.get("DINGTALK_CLIENT_ID", "")
    client_secret = env.get("DINGTALK_CLIENT_SECRET", "")
    if not (client_id and client_secret):
        print("missing DINGTALK_CLIENT_ID/SECRET in server/.env", flush=True)
        sys.exit(1)

    endpoint, ticket = register(client_id, client_secret)
    print(f"[probe] connected, endpoint={endpoint}", flush=True)
    async with websockets.connect(
        f"{endpoint}?ticket={quote_plus(ticket)}", max_size=4 * 1024 * 1024
    ) as ws:
        print("[probe] listening... send a private message AND a group @ now", flush=True)
        async for raw in ws:
            try:
                frame = json.loads(raw)
            except json.JSONDecodeError:
                print(f"[probe] RAW (non-json): {raw[:200]}", flush=True)
                continue
            headers = frame.get("headers") or {}
            ftype = frame.get("type")
            topic = headers.get("topic")
            data = frame.get("data")
            snippet = data if isinstance(data, str) else json.dumps(data, ensure_ascii=False)
            if isinstance(snippet, str):
                try:
                    parsed = json.loads(snippet)
                    conv = parsed.get("conversationType", "?")
                    text = (parsed.get("text") or {}).get("content", "")
                    sender = parsed.get("senderNick", "")
                    snippet = f"convType={conv} sender={sender} text={text!r}"
                except Exception:
                    snippet = snippet[:160]
            stamp = asyncio.get_event_loop().time()
            print(
                f"[probe] frame type={ftype} topic={topic} {snippet} (t={stamp:.0f}s)",
                flush=True,
            )
            # ACK everything (same as reference implementation)
            ack = {
                "code": 200,
                "message": "OK",
                "headers": {
                    "contentType": "application/json",
                    "messageId": headers.get("messageId"),
                },
            }
            await ws.send(json.dumps(ack))


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
