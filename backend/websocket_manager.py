import logging
from typing import List
from fastapi import WebSocket

logger = logging.getLogger("ws-manager")

ws_clients: List[WebSocket] = []

async def ws_broadcast(message: dict):
    import json
    msg = json.dumps(message)
    dead = []
    for ws in ws_clients:
        try:
            await ws.send_text(msg)
        except:
            dead.append(ws)

    for ws in dead:
        ws_clients.remove(ws)
