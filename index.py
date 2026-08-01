# Leva backend: телеметрия с ESP32, реестр устройств, Telegram-бот.
# Один FastAPI-приложение под Vercel Python runtime.

import json
import os
import time
from contextlib import contextmanager

import httpx
import psycopg
from fastapi import FastAPI, Header, HTTPException, Request
from psycopg.rows import dict_row

app = FastAPI()

DATABASE_URL = os.environ.get("DATABASE_URL", "")
LEVA_API_KEY = os.environ.get("LEVA_API_KEY", "")
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_WEBHOOK_SECRET = os.environ.get("TELEGRAM_WEBHOOK_SECRET", "")

TELEGRAM_API = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}"

# Вес пустой тары, грамм. Должен совпадать с EMPTY_CONTAINER_G в прошивке.
EMPTY_CONTAINER_G = 40.0
# Уведомляем, когда остатка хватит меньше чем на столько дней.
NOTIFY_DAYS_THRESHOLD = 2.0
# Не повторяем одно и то же уведомление чаще, чем раз в сутки.
NOTIFY_COOLDOWN_SECONDS = 24 * 3600

_schema_ready = False

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS devices (
    device_id TEXT PRIMARY KEY,
    name TEXT,
    owner_chat_id BIGINT,
    battery_mv INTEGER,
    last_weight_g REAL,
    full_reference_g REAL,
    avg_usage_g REAL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_seen TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS telemetry (
    id BIGSERIAL PRIMARY KEY,
    device_id TEXT NOT NULL,
    event TEXT NOT NULL,
    raw_hx711 BIGINT,
    weight_g REAL,
    previous_stable_g REAL,
    delta_g REAL,
    usage_g REAL,
    battery_mv INTEGER,
    stability_span_g REAL,
    sample_count INTEGER,
    wake_count BIGINT,
    device_millis BIGINT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS telemetry_device_time
    ON telemetry (device_id, created_at DESC);

CREATE TABLE IF NOT EXISTS notifications (
    id BIGSERIAL PRIMARY KEY,
    device_id TEXT NOT NULL,
    kind TEXT NOT NULL,
    sent_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS notifications_device_kind
    ON notifications (device_id, kind, sent_at DESC);
"""


@contextmanager
def db():
    conn = psycopg.connect(DATABASE_URL, row_factory=dict_row)
    try:
        global _schema_ready
        if not _schema_ready:
            conn.execute(SCHEMA_SQL)
            conn.commit()
            _schema_ready = True
        yield conn
        conn.commit()
    finally:
        conn.close()


def tg_send(chat_id: int, text: str) -> None:
    if not TELEGRAM_BOT_TOKEN:
        return
    try:
        httpx.post(
            f"{TELEGRAM_API}/sendMessage",
            json={"chat_id": chat_id, "text": text, "parse_mode": "HTML"},
            timeout=10,
        )
    except httpx.HTTPError:
        # Уведомление не критично для приёма телеметрии, не роняем запрос.
        pass


def compute_level_percent(weight_g: float, full_g: float) -> float:
    if not full_g or full_g <= EMPTY_CONTAINER_G:
        return 0.0
    percent = (weight_g - EMPTY_CONTAINER_G) / (full_g - EMPTY_CONTAINER_G) * 100.0
    return max(0.0, min(100.0, percent))


def estimate_daily_usage_g(conn, device_id: str) -> float:
    # Средний дневной расход по usage-событиям за последние 14 дней.
    row = conn.execute(
        """
        SELECT COALESCE(SUM(usage_g), 0) AS total,
               EXTRACT(EPOCH FROM (now() - MIN(created_at))) / 86400.0 AS days
        FROM telemetry
        WHERE device_id = %s
          AND event = 'usage'
          AND created_at > now() - INTERVAL '14 days'
        """,
        (device_id,),
    ).fetchone()
    total = float(row["total"] or 0)
    days = float(row["days"] or 0)
    if total <= 0 or days < 0.5:
        return 0.0
    return total / max(days, 1.0)


def maybe_notify_refill(conn, device: dict) -> None:
    chat_id = device.get("owner_chat_id")
    if not chat_id:
        return
    weight = device.get("last_weight_g")
    full = device.get("full_reference_g")
    if weight is None or full is None:
        return

    remaining_g = max(0.0, weight - EMPTY_CONTAINER_G)
    daily = estimate_daily_usage_g(conn, device["device_id"])
    if daily <= 0:
        return
    days_left = remaining_g / daily
    if days_left > NOTIFY_DAYS_THRESHOLD:
        return

    recent = conn.execute(
        """
        SELECT 1 FROM notifications
        WHERE device_id = %s AND kind = 'refill'
          AND sent_at > now() - make_interval(secs => %s)
        LIMIT 1
        """,
        (device["device_id"], NOTIFY_COOLDOWN_SECONDS),
    ).fetchone()
    if recent:
        return

    percent = compute_level_percent(weight, full)
    name = device.get("name") or device["device_id"]
    tg_send(
        chat_id,
        f"<b>{name}</b>: средство заканчивается.\n"
        f"Остаток примерно {percent:.0f}%, хватит на ~{days_left:.1f} дн.\n"
        f"Пора заказать новую упаковку.",
    )
    conn.execute(
        "INSERT INTO notifications (device_id, kind) VALUES (%s, 'refill')",
        (device["device_id"],),
    )


def maybe_notify_battery(conn, device: dict, battery_mv: int) -> None:
    chat_id = device.get("owner_chat_id")
    if not chat_id or battery_mv is None:
        return
    # Порог совпадает с BATTERY_RED_MIN_MV в прошивке (critical ниже 1050 мВ).
    if battery_mv >= 1050:
        return
    recent = conn.execute(
        """
        SELECT 1 FROM notifications
        WHERE device_id = %s AND kind = 'battery'
          AND sent_at > now() - make_interval(secs => %s)
        LIMIT 1
        """,
        (device["device_id"], NOTIFY_COOLDOWN_SECONDS),
    ).fetchone()
    if recent:
        return
    name = device.get("name") or device["device_id"]
    tg_send(chat_id, f"<b>{name}</b>: батарейки почти сели, замени их в ближайшее время.")
    conn.execute(
        "INSERT INTO notifications (device_id, kind) VALUES (%s, 'battery')",
        (device["device_id"],),
    )


@app.get("/api/health")
def health():
    return {"ok": True, "ts": int(time.time())}


@app.post("/api/telemetry")
async def telemetry(request: Request, x_api_key: str = Header(default="")):
    if not LEVA_API_KEY or x_api_key != LEVA_API_KEY:
        raise HTTPException(status_code=401, detail="bad api key")

    try:
        body = await request.json()
    except json.JSONDecodeError:
        raise HTTPException(status_code=400, detail="bad json")

    device_id = str(body.get("device_id", "")).strip()
    if not device_id or len(device_id) > 64:
        raise HTTPException(status_code=400, detail="bad device_id")

    event = str(body.get("event", "unknown"))[:64]

    def num(key):
        value = body.get(key)
        return value if isinstance(value, (int, float)) else None

    with db() as conn:
        conn.execute(
            """
            INSERT INTO devices (device_id, last_seen)
            VALUES (%s, now())
            ON CONFLICT (device_id) DO UPDATE SET last_seen = now()
            """,
            (device_id,),
        )

        conn.execute(
            """
            INSERT INTO telemetry (device_id, event, raw_hx711, weight_g,
                previous_stable_g, delta_g, usage_g, battery_mv,
                stability_span_g, sample_count, wake_count, device_millis)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                device_id, event, num("raw_hx711"), num("weight_g"),
                num("previous_stable_g"), num("delta_g"), num("usage_g"),
                num("battery_mv"), num("stability_span_g"),
                num("sample_count"), num("wake_count"), num("millis"),
            ),
        )

        weight = num("weight_g")
        battery = num("battery_mv")
        updates = {"battery_mv": battery}
        if event in ("usage", "no_change", "small_usage_accumulating",
                     "baseline_initialized", "bottle_changed") and weight is not None:
            updates["last_weight_g"] = weight
        if event in ("baseline_initialized", "bottle_changed") and weight is not None:
            updates["full_reference_g"] = weight

        set_parts = []
        params = []
        for column, value in updates.items():
            if value is not None:
                set_parts.append(f"{column} = %s")
                params.append(value)
        if set_parts:
            params.append(device_id)
            conn.execute(
                f"UPDATE devices SET {', '.join(set_parts)} WHERE device_id = %s",
                params,
            )

        device = conn.execute(
            "SELECT * FROM devices WHERE device_id = %s", (device_id,)
        ).fetchone()

        if event == "usage":
            maybe_notify_refill(conn, device)
        if battery is not None:
            maybe_notify_battery(conn, device, battery)

    return {"ok": True}


def handle_start(conn, chat_id: int, payload: str) -> str:
    payload = payload.strip()
    if not payload:
        return (
            "Привет! Это бот Leva.\n\n"
            "Чтобы привязать устройство, отсканируй QR-код на его корпусе "
            "или коробке. Если открыл бота вручную, найди QR и перейди по нему."
        )
    device = conn.execute(
        "SELECT * FROM devices WHERE device_id = %s", (payload,)
    ).fetchone()
    if device is None:
        # Устройство ещё ни разу не выходило в сеть: регистрируем заранее,
        # привязка сработает при первом же пакете телеметрии.
        conn.execute(
            "INSERT INTO devices (device_id, owner_chat_id) VALUES (%s, %s)",
            (payload, chat_id),
        )
        return (
            "Устройство привязано!\n\n"
            "Оно ещё не выходило на связь. Как только подключишь его к сети, "
            "данные начнут приходить сюда автоматически."
        )
    conn.execute(
        "UPDATE devices SET owner_chat_id = %s WHERE device_id = %s",
        (chat_id, payload),
    )
    return (
        "Устройство привязано!\n\n"
        "Теперь я буду присылать сюда уведомления, когда средство будет "
        "заканчиваться. Команда /status покажет текущий уровень."
    )


def handle_status(conn, chat_id: int) -> str:
    rows = conn.execute(
        "SELECT * FROM devices WHERE owner_chat_id = %s ORDER BY device_id",
        (chat_id,),
    ).fetchall()
    if not rows:
        return "У тебя пока нет привязанных устройств. Отсканируй QR на корпусе."
    lines = []
    for device in rows:
        name = device.get("name") or device["device_id"]
        weight = device.get("last_weight_g")
        full = device.get("full_reference_g")
        if weight is None or full is None:
            lines.append(f"<b>{name}</b>: данных пока нет")
            continue
        percent = compute_level_percent(weight, full)
        battery = device.get("battery_mv")
        battery_note = f", батарея {battery} мВ" if battery else ""
        lines.append(f"<b>{name}</b>: уровень ~{percent:.0f}%{battery_note}")
    return "\n".join(lines)


@app.post("/api/telegram/webhook")
async def telegram_webhook(request: Request):
    if TELEGRAM_WEBHOOK_SECRET:
        header = request.headers.get("x-telegram-bot-api-secret-token", "")
        if header != TELEGRAM_WEBHOOK_SECRET:
            raise HTTPException(status_code=401, detail="bad secret")

    update = await request.json()
    message = update.get("message") or update.get("edited_message")
    if not message:
        return {"ok": True}

    chat_id = message.get("chat", {}).get("id")
    text = (message.get("text") or "").strip()
    if not chat_id or not text:
        return {"ok": True}

    with db() as conn:
        if text.startswith("/start"):
            payload = text[len("/start"):].strip()
            reply = handle_start(conn, chat_id, payload)
        elif text.startswith("/status"):
            reply = handle_status(conn, chat_id)
        else:
            reply = (
                "Команды:\n"
                "/status - уровень средств на привязанных устройствах\n\n"
                "Чтобы привязать новое устройство, отсканируй его QR-код."
            )

    tg_send(chat_id, reply)
    return {"ok": True}
