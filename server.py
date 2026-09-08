"""
Видеозвонок на WebRTC — сервер.

Делает две вещи одновременно, на одном порту:
1. Раздаёт index.html (обычная веб-страница).
2. Обрабатывает WebSocket-соединения на /ws — signaling для WebRTC
   (комнаты по коду, пересылка offer/answer/ICE между участниками).
   Само видео/аудио через сервер не идёт — только служебные данные.

Локальный запуск:
    pip install aiohttp
    python3 server.py
    Открыть: http://localhost:8765/index.html?room=test1

На Render (или похожем хостинге) порт берётся из переменной окружения
PORT — это настраивается автоматически, ничего менять не нужно.
"""

import json
import logging
import os
from pathlib import Path

from aiohttp import web, WSMsgType

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("signaling")

BASE_DIR = Path(__file__).parent

# room_id -> множество WebSocket-соединений в этой комнате
rooms: dict[str, set] = {}


async def index(request):
    return web.FileResponse(BASE_DIR / "index.html")


async def broadcast_to_room(room_id: str, message: dict, exclude=None):
    if room_id not in rooms:
        return
    data = json.dumps(message)
    dead = []
    for ws in rooms[room_id]:
        if ws is exclude:
            continue
        try:
            await ws.send_str(data)
        except ConnectionResetError:
            dead.append(ws)
    for ws in dead:
        rooms[room_id].discard(ws)


async def ws_handler(request):
    ws = web.WebSocketResponse(heartbeat=30)
    await ws.prepare(request)
    room_id = None

    try:
        async for raw in ws:
            if raw.type != WSMsgType.TEXT:
                continue
            try:
                msg = json.loads(raw.data)
            except json.JSONDecodeError:
                log.warning("Пришло не-JSON сообщение, игнорирую")
                continue

            msg_type = msg.get("type")

            if msg_type == "join":
                room_id = msg.get("room", "default")
                rooms.setdefault(room_id, set())

                # Ограничение: максимум 2 участника в комнате (P2P на двоих)
                if len(rooms[room_id]) >= 2:
                    await ws.send_str(json.dumps({"type": "room-full"}))
                    log.info(f"Комната {room_id} уже заполнена, отказ новому участнику")
                    return ws

                rooms[room_id].add(ws)
                log.info(f"Участник зашёл в комнату '{room_id}' (сейчас в комнате: {len(rooms[room_id])})")

                await broadcast_to_room(room_id, {"type": "peer-joined"}, exclude=ws)

                is_first = len(rooms[room_id]) == 1
                await ws.send_str(json.dumps({"type": "joined", "isInitiator": is_first}))

            elif msg_type in ("offer", "answer", "ice-candidate"):
                if room_id:
                    await broadcast_to_room(room_id, msg, exclude=ws)

            else:
                log.warning(f"Неизвестный тип сообщения: {msg_type}")

    finally:
        if room_id and room_id in rooms:
            rooms[room_id].discard(ws)
            log.info(f"Участник вышел из комнаты '{room_id}' (осталось: {len(rooms[room_id])})")
            await broadcast_to_room(room_id, {"type": "peer-left"})
            if not rooms[room_id]:
                del rooms[room_id]

    return ws


def create_app():
    app = web.Application()
    app.router.add_get("/", index)
    app.router.add_get("/index.html", index)
    app.router.add_get("/ws", ws_handler)
    return app


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8765))
    log.info(f"Запуск сервера на порту {port}")
    log.info("Оставьте это окно открытым, пока нужна возможность звонить (при локальном запуске).")
    web.run_app(create_app(), host="0.0.0.0", port=port, print=None)
