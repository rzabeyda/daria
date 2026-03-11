import asyncio
import calendar
import os
import sqlite3
from datetime import datetime, timedelta
from dotenv import load_dotenv

load_dotenv()

from aiogram import Bot, Dispatcher, types, F
from aiogram.types import (
    InlineKeyboardButton, InlineKeyboardMarkup, FSInputFile,
    ReplyKeyboardMarkup, KeyboardButton,
)
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import StatesGroup, State
from aiogram.fsm.storage.memory import MemoryStorage

# ─── Конфиг ───────────────────────────────────────────────────────────────────

API_TOKEN   = os.getenv("API_TOKEN")

ADMIN_IDS = set()
for _key in ("ADMIN_ID", "ADMIN_ID2", "ADMIN_ID3"):
    _val = os.getenv(_key)
    if _val:
        ADMIN_IDS.add(int(_val))

bot = Bot(token=API_TOKEN)
dp  = Dispatcher(storage=MemoryStorage())

# ─── SQLite ───────────────────────────────────────────────────────────────────

DB_FILE = "bot.db"


def db_connect():
    return sqlite3.connect(DB_FILE)


def init_db():
    con = db_connect()
    cur = con.cursor()
    cur.executescript("""
        CREATE TABLE IF NOT EXISTS bookings (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id     INTEGER NOT NULL,
            service     TEXT NOT NULL,
            year        INTEGER NOT NULL,
            month       INTEGER NOT NULL,
            day         INTEGER NOT NULL,
            time        TEXT NOT NULL,
            name        TEXT NOT NULL,
            phone       TEXT NOT NULL,
            reminded_24 INTEGER DEFAULT 0,
            reminded_2  INTEGER DEFAULT 0,
            review_sent INTEGER DEFAULT 0
        );
        CREATE TABLE IF NOT EXISTS schedule (
            id      INTEGER PRIMARY KEY AUTOINCREMENT,
            type    TEXT NOT NULL,
            date    TEXT NOT NULL,
            time    TEXT
        );
        CREATE TABLE IF NOT EXISTS cancelled (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id     INTEGER NOT NULL,
            service     TEXT NOT NULL,
            year        INTEGER NOT NULL,
            month       INTEGER NOT NULL,
            day         INTEGER NOT NULL,
            time        TEXT NOT NULL,
            name        TEXT NOT NULL,
            phone       TEXT NOT NULL,
            cancelled_by TEXT NOT NULL,
            cancelled_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS transfers (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            booking_id  INTEGER NOT NULL,
            user_id     INTEGER NOT NULL,
            service     TEXT NOT NULL,
            old_date    TEXT NOT NULL,
            old_time    TEXT NOT NULL,
            new_date    TEXT NOT NULL,
            new_time    TEXT NOT NULL,
            transferred_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS reviews (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            booking_id  INTEGER NOT NULL,
            user_id     INTEGER NOT NULL,
            service     TEXT NOT NULL,
            rating      INTEGER NOT NULL,
            text        TEXT,
            created_at  TEXT NOT NULL
        );
    """)
    con.commit()
    con.close()


init_db()

# Миграция: добавляем новые колонки и таблицы если их нет
def migrate_db():
    con = db_connect()
    try:
        con.execute("ALTER TABLE bookings ADD COLUMN review_sent INTEGER DEFAULT 0")
        con.commit()
    except Exception:
        pass
    con.executescript("""
        CREATE TABLE IF NOT EXISTS cancelled (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id      INTEGER NOT NULL,
            service      TEXT NOT NULL,
            year         INTEGER NOT NULL,
            month        INTEGER NOT NULL,
            day          INTEGER NOT NULL,
            time         TEXT NOT NULL,
            name         TEXT NOT NULL,
            phone        TEXT NOT NULL,
            cancelled_by TEXT NOT NULL,
            cancelled_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS transfers (
            id             INTEGER PRIMARY KEY AUTOINCREMENT,
            booking_id     INTEGER NOT NULL,
            user_id        INTEGER NOT NULL,
            service        TEXT NOT NULL,
            old_date       TEXT NOT NULL,
            old_time       TEXT NOT NULL,
            new_date       TEXT NOT NULL,
            new_time       TEXT NOT NULL,
            transferred_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS reviews (
            id         INTEGER PRIMARY KEY AUTOINCREMENT,
            booking_id INTEGER NOT NULL,
            user_id    INTEGER NOT NULL,
            service    TEXT NOT NULL,
            rating     INTEGER NOT NULL,
            text       TEXT,
            created_at TEXT NOT NULL
        );
    """)
    con.commit()
    con.close()

migrate_db()



# ─── CRUD брони ───────────────────────────────────────────────────────────────

def add_booking(user_id: int, service: str, year: int, month: int, day: int,
                time: str, name: str, phone: str) -> int:
    con = db_connect()
    cur = con.cursor()
    cur.execute(
        "INSERT INTO bookings (user_id,service,year,month,day,time,name,phone) "
        "VALUES (?,?,?,?,?,?,?,?)",
        (user_id, service, year, month, day, time, name, phone)
    )
    bid = cur.lastrowid
    con.commit(); con.close()
    return bid


def remove_booking(bid: int):
    con = db_connect()
    con.execute("DELETE FROM bookings WHERE id=?", (bid,))
    con.commit(); con.close()


def get_booking(bid: int) -> dict | None:
    con = db_connect()
    cur = con.execute("SELECT * FROM bookings WHERE id=?", (bid,))
    row = cur.fetchone()
    con.close()
    return _row_to_booking(row) if row else None


def get_user_bookings(user_id: int) -> list[dict]:
    con = db_connect()
    cur = con.execute(
        "SELECT * FROM bookings WHERE user_id=? ORDER BY year,month,day,time",
        (user_id,)
    )
    rows = cur.fetchall(); con.close()
    return [_row_to_booking(r) for r in rows]


def get_all_bookings() -> list[dict]:
    con = db_connect()
    cur = con.execute("SELECT * FROM bookings ORDER BY year,month,day,time")
    rows = cur.fetchall(); con.close()
    return [_row_to_booking(r) for r in rows]


def update_booking_field(bid: int, field: str, value):
    allowed = {"service", "year", "month", "day", "time", "name", "phone"}
    if field not in allowed:
        return
    con = db_connect()
    con.execute(f"UPDATE bookings SET {field}=? WHERE id=?", (value, bid))
    con.commit(); con.close()


def _row_to_booking(row) -> dict:
    return {
        "id": row[0], "user_id": row[1], "service": row[2],
        "year": row[3], "month": row[4], "day": row[5],
        "time": row[6], "name": row[7], "phone": row[8],
        "reminded_24": row[9], "reminded_2": row[10],
    }


# ─── CRUD статистики / отзывов / переносов ────────────────────────────────────

def log_cancellation(b: dict, cancelled_by: str):
    con = db_connect()
    con.execute(
        "INSERT INTO cancelled (user_id,service,year,month,day,time,name,phone,cancelled_by,cancelled_at) "
        "VALUES (?,?,?,?,?,?,?,?,?,?)",
        (b["user_id"], b["service"], b["year"], b["month"], b["day"],
         b["time"], b["name"], b["phone"], cancelled_by,
         datetime.now().strftime("%Y-%m-%d %H:%M"))
    )
    con.commit(); con.close()


def log_transfer(bid: int, user_id: int, service: str,
                 old_date: str, old_time: str, new_date: str, new_time: str):
    con = db_connect()
    con.execute(
        "INSERT INTO transfers (booking_id,user_id,service,old_date,old_time,new_date,new_time,transferred_at) "
        "VALUES (?,?,?,?,?,?,?,?)",
        (bid, user_id, service, old_date, old_time, new_date, new_time,
         datetime.now().strftime("%Y-%m-%d %H:%M"))
    )
    con.commit(); con.close()


def add_review(booking_id: int, user_id: int, service: str, rating: int, text: str):
    con = db_connect()
    con.execute(
        "INSERT INTO reviews (booking_id,user_id,service,rating,text,created_at) VALUES (?,?,?,?,?,?)",
        (booking_id, user_id, service, rating, text, datetime.now().strftime("%Y-%m-%d %H:%M"))
    )
    con.commit(); con.close()


def get_stats() -> dict:
    con = db_connect()
    stats = {}
    stats["total_active"]    = con.execute("SELECT COUNT(*) FROM bookings").fetchone()[0]
    stats["total_cancelled"] = con.execute("SELECT COUNT(*) FROM cancelled").fetchone()[0]
    stats["total_transfers"] = con.execute("SELECT COUNT(*) FROM transfers").fetchone()[0]
    stats["total_reviews"]   = con.execute("SELECT COUNT(*) FROM reviews").fetchone()[0]
    avg = con.execute("SELECT AVG(rating) FROM reviews").fetchone()[0]
    stats["avg_rating"]      = round(avg, 1) if avg else 0
    # по услугам
    rows = con.execute(
        "SELECT service, COUNT(*) as cnt FROM bookings GROUP BY service ORDER BY cnt DESC"
    ).fetchall()
    stats["by_service"] = rows
    # отмены по инициатору
    stats["cancelled_by_client"] = con.execute(
        "SELECT COUNT(*) FROM cancelled WHERE cancelled_by='client'"
    ).fetchone()[0]
    stats["cancelled_by_master"] = con.execute(
        "SELECT COUNT(*) FROM cancelled WHERE cancelled_by='master'"
    ).fetchone()[0]
    # последние отзывы
    stats["last_reviews"] = con.execute(
        "SELECT rating, text, service, created_at FROM reviews ORDER BY id DESC LIMIT 5"
    ).fetchall()
    con.close()
    return stats


def get_client_visits(user_id: int) -> int:
    """Общее количество визитов клиента (активные + отменённые мастером учитываются, отменённые клиентом нет)."""
    con = db_connect()
    active = con.execute("SELECT COUNT(*) FROM bookings WHERE user_id=?", (user_id,)).fetchone()[0]
    # Из cancelled считаем только завершённые (отменённые мастером не считаем — клиент не пришёл)
    # Считаем только из таблицы cancelled где cancelled_by='client' — нет, лучше только active+завершённые
    # Простой вариант: все активные брони + все отменённые клиентом (раз уж дошёл до отмены — значит ходил)
    con.close()
    return active


def get_client_rank(visits: int) -> str:
    if visits >= 7:
        return "💎 VIP"
    elif visits >= 3:
        return "⭐ Постоянный"
    else:
        return "🆕 Новичок"


# ─── CRUD расписания ──────────────────────────────────────────────────────────

def block_day(date: str):
    con = db_connect()
    con.execute("INSERT OR IGNORE INTO schedule (type,date) VALUES ('day',?)", (date,))
    con.commit(); con.close()


def unblock_day(date: str):
    con = db_connect()
    con.execute("DELETE FROM schedule WHERE type='day' AND date=?", (date,))
    con.commit(); con.close()


def block_slot(date: str, time: str):
    con = db_connect()
    con.execute("INSERT OR IGNORE INTO schedule (type,date,time) VALUES ('slot',?,?)", (date, time))
    con.commit(); con.close()


def unblock_slot(date: str, time: str):
    con = db_connect()
    con.execute("DELETE FROM schedule WHERE type='slot' AND date=? AND time=?", (date, time))
    con.commit(); con.close()


def get_blocked_days() -> list[str]:
    con = db_connect()
    cur = con.execute("SELECT date FROM schedule WHERE type='day'")
    rows = cur.fetchall(); con.close()
    return [r[0] for r in rows]


def get_blocked_slots_for_date(date: str) -> list[str]:
    con = db_connect()
    cur = con.execute("SELECT time FROM schedule WHERE type='slot' AND date=?", (date,))
    rows = cur.fetchall(); con.close()
    return [r[0] for r in rows]


def get_all_blocked_slots() -> dict:
    con = db_connect()
    cur = con.execute("SELECT date,time FROM schedule WHERE type='slot' ORDER BY date,time")
    rows = cur.fetchall(); con.close()
    result = {}
    for date, time in rows:
        result.setdefault(date, []).append(time)
    return result

# ─── Контакты ─────────────────────────────────────────────────────────────────

CONTACTS_FULL = (
    "📞 +372 56 602 890\n"
    "💬 Telegram: @Vi_da_ch_iV\n"
    "📬 Dariashabelna@gmail.com"
)

CONTACTS_SHORT = (
    "📞 +372 56 602 890\n"
    "💬 Telegram: @Vi_da_ch_iV"
)

# ─── Услуги ───────────────────────────────────────────────────────────────────

services = [
    {"name": "Классический маникюр",    "price": "15€", "duration": "30 мин",  "duration_slots": 1, "img": "images/1.jpg"},
    {"name": "Гель-лак / Коррекция",    "price": "25€", "duration": "60 мин",  "duration_slots": 2, "img": "images/2.jpg"},
    {"name": "Наращивание ногтей",       "price": "35€", "duration": "120 мин", "duration_slots": 4, "img": "images/3.jpg"},
    {"name": "Гигиенический педикюр",    "price": "25€", "duration": "45 мин",  "duration_slots": 2, "img": "images/4.jpg"},
    {"name": "Педикюр с гель-лаком",    "price": "35€", "duration": "60 мин",  "duration_slots": 2, "img": "images/5.jpg"},
    {"name": "Мужской педикюр",         "price": "30€", "duration": "60 мин",  "duration_slots": 2, "img": "images/6.jpg"},
    {"name": "Снятие покрытия",         "price": "10€", "duration": "15 мин",  "duration_slots": 1, "img": "images/7.jpg"},
    {"name": "Ремонт одного ногтя",     "price": "2€",  "duration": "15 мин",  "duration_slots": 1, "img": "images/8.jpg"},
]

MONTHS = {
    1: "Январь",   2: "Февраль", 3: "Март",    4: "Апрель",
    5: "Май",      6: "Июнь",    7: "Июль",    8: "Август",
    9: "Сентябрь", 10: "Октябрь",11: "Ноябрь", 12: "Декабрь",
}

MONTHS_GEN = {
    1: "Января",   2: "Февраля", 3: "Марта",   4: "Апреля",
    5: "Мая",      6: "Июня",    7: "Июля",    8: "Августа",
    9: "Сентября", 10: "Октября",11: "Ноября", 12: "Декабря",
}

TIME_SLOTS = ["09:00","10:00","11:00","12:00","13:00",
              "14:00","15:00","16:00","17:00","18:00"]

# ─── FSM ──────────────────────────────────────────────────────────────────────

class Booking(StatesGroup):
    service = State()
    year    = State()
    month   = State()
    day     = State()
    time    = State()
    name    = State()
    phone   = State()


class EditBooking(StatesGroup):
    service = State()
    year    = State()
    month   = State()
    day     = State()
    time    = State()
    name    = State()
    phone   = State()


class Reschedule(StatesGroup):
    year  = State()
    month = State()
    day   = State()
    time  = State()


class Review(StatesGroup):
    rating = State()
    text   = State()


# ─── Вспомогательные функции ──────────────────────────────────────────────────

def get_service(name: str) -> dict | None:
    return next((s for s in services if s["name"] == name), None)


def date_key(year: int, month: int, day: int) -> str:
    return f"{year}-{month:02d}-{day:02d}"


def is_day_blocked(year: int, month: int, day: int) -> bool:
    return date_key(year, month, day) in get_blocked_days()


def duration_minutes(svc: dict | None) -> int:
    if not svc:
        return 60
    import re
    m = re.search(r"(\d+)", svc["duration"])
    return int(m.group(1)) if m else 60

def get_end_time(start_slot: str, duration_minutes: int) -> str:
    h, m = int(start_slot.split(":")[0]), int(start_slot.split(":")[1])
    total = h * 60 + m + duration_minutes
    return f"{total // 60:02d}:{total % 60:02d}"


def get_busy_slots(year: int, month: int, day: int,
                   exclude_bid: int | None = None) -> set[str]:
    """Слоты занятые существующими бронями (с учётом длительности)."""
    busy_minutes = set()
    for b in get_all_bookings():
        if exclude_bid and b["id"] == exclude_bid:
            continue
        if b["year"] != year or b["month"] != month or b["day"] != day:
            continue
        svc = get_service(b["service"])
        dur = duration_minutes(svc)
        h, m = int(b["time"].split(":")[0]), int(b["time"].split(":")[1])
        start = h * 60 + m
        # Помечаем все минуты занятые этой бронью
        for minute in range(start, start + dur):
            busy_minutes.add(minute)
    # Переводим в слоты
    busy = set()
    for slot in TIME_SLOTS:
        h, m = int(slot.split(":")[0]), int(slot.split(":")[1])
        if h * 60 + m in busy_minutes:
            busy.add(slot)
    return busy


def get_available_slots(year: int, month: int, day: int,
                        new_dur_min: int = 60,
                        exclude_bid: int | None = None) -> list[str]:
    """
    Базовые слоты каждый час (09:00..18:00).
    Добавляем HH:30 если бронь заканчивается в HH:30 — туда влезает новая услуга.
    """
    now = datetime.now()
    key = date_key(year, month, day)
    manual_blocked_min = set()
    for slot in get_blocked_slots_for_date(key):
        h, m = int(slot.split(":")[0]), int(slot.split(":")[1])
        manual_blocked_min.add(h * 60 + m)

    # Собираем занятые интервалы
    booked_intervals = []
    for b in get_all_bookings():
        if exclude_bid and b["id"] == exclude_bid:
            continue
        if b["year"] != year or b["month"] != month or b["day"] != day:
            continue
        svc = get_service(b["service"])
        dur = duration_minutes(svc)
        h, m = int(b["time"].split(":")[0]), int(b["time"].split(":")[1])
        start = h * 60 + m
        booked_intervals.append((start, start + dur))

    def is_free(start_min: int, dur: int) -> bool:
        if start_min > 18 * 60:
            return False
        end_min = start_min + dur
        for bs, be in booked_intervals:
            if start_min < be and end_min > bs:
                return False
        if start_min in manual_blocked_min:
            return False
        return True

    # Базовые почасовые кандидаты 09:00..18:00
    candidates = list(range(9 * 60, 18 * 60 + 1, 60))

    # Добавляем HH:30 после брони заканчивающейся на :30
    for _, be in booked_intervals:
        if be % 60 == 30 and 9 * 60 <= be <= 18 * 60:
            candidates.append(be)

    # Сегодня: убираем прошедшее
    if year == now.year and month == now.month and day == now.day:
        cutoff = now.hour * 60 + now.minute
        candidates = [m for m in candidates if m > cutoff]

    result = sorted(set(m for m in candidates if is_free(m, new_dur_min)))
    return [f"{m // 60:02d}:{m % 60:02d}" for m in result]


def get_blocked_slots(year: int, month: int, day: int, exclude_bid: int | None = None,
                      new_duration_slots: int = 1) -> set[str]:
    """Возвращает недоступные слоты (для совместимости со старым кодом)."""
    new_dur_min = new_duration_slots * 30
    available = set(get_available_slots(year, month, day, new_dur_min, exclude_bid))
    all_candidates = set(TIME_SLOTS)
    return all_candidates - available


def format_booking(b: dict, idx: int | None = None, username: str | None = None) -> str:
    month_name = MONTHS_GEN[b["month"]]
    prefix     = f"Бронь №{idx}\n" if idx else ""
    svc        = get_service(b["service"])
    dur_str    = svc["duration"] if svc else ""
    tg_line    = f" | 💬 @{username}" if username else ""
    addr_line = "\n\n😊 Я жду вас по адресу:\n🏠 Linnamäe tee 83-66" if username is None else ""
    return (
        f"{prefix}"
        f"💅 {b['service']}\n"
        f"⏱ Длительность: ~{dur_str}\n"
        f"🕐 {b['time']} | {b['day']} {month_name}\n"
        f"👤 {b['name']} 📞 {b['phone']}{tg_line}{addr_line}"
    ).strip()


# ─── Клавиатуры ───────────────────────────────────────────────────────────────

def bottom_kb(is_admin: bool = False) -> ReplyKeyboardMarkup:
    row = [
        KeyboardButton(text="💅 Услуги"),
        KeyboardButton(text="📋 Мои брони"),
        KeyboardButton(text="📞 Контакты"),
    ]
    buttons = [row]
    if is_admin:
        buttons.append([KeyboardButton(text="🔐 Админка")])
    return ReplyKeyboardMarkup(keyboard=buttons, resize_keyboard=True)


def main_menu_kb():
    rows = []
    for s in services:
        label = f"{s['name']} — {s['price']} (~{s['duration']})"
        rows.append([InlineKeyboardButton(text=label, callback_data=f"svc:{s['name']}")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def years_kb() -> InlineKeyboardMarkup:
    now = datetime.now()
    buttons = []
    for y in [now.year, now.year + 1]:
        buttons.append(InlineKeyboardButton(text=str(y), callback_data=f"year:{y}"))
    rows = [buttons, [InlineKeyboardButton(text="🏠 Главное меню", callback_data="main_menu")]]
    return InlineKeyboardMarkup(inline_keyboard=rows)


def months_kb(year: int) -> InlineKeyboardMarkup:
    now = datetime.now()
    rows, row = [], []
    shown = 0
    for num, name in MONTHS.items():
        if num < now.month:
            continue
        if shown >= 3:
            break
        row.append(InlineKeyboardButton(text=name, callback_data=f"month:{num}"))
        shown += 1
        if len(row) == 3:
            rows.append(row); row = []
    if row:
        rows.append(row)
    rows.append([InlineKeyboardButton(text="🏠 Главное меню", callback_data="main_menu")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def days_kb(year: int, month: int, new_duration_slots: int = 1) -> InlineKeyboardMarkup:
    now = datetime.now()
    _, days_in_month = calendar.monthrange(year, month)
    new_dur_min = new_duration_slots * 30
    rows, row = [], []
    for day in range(1, days_in_month + 1):
        if year == now.year and month == now.month and day < now.day:
            continue
        if is_day_blocked(year, month, day):
            continue
        available = get_available_slots(year, month, day, new_dur_min)
        if not available:
            continue
        row.append(InlineKeyboardButton(text=str(day), callback_data=f"day:{day}"))
        if len(row) == 7:
            rows.append(row); row = []
    if row:
        rows.append(row)
    if not rows:
        rows.append([InlineKeyboardButton(text="Нет доступных дней", callback_data="noop")])
    rows.append([
        InlineKeyboardButton(text="◀️ Назад",        callback_data="back_to_months"),
        InlineKeyboardButton(text="🏠 Главное меню", callback_data="main_menu"),
    ])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def time_kb(year: int, month: int, day: int, exclude_bid: int | None = None,
            new_duration_slots: int = 1) -> InlineKeyboardMarkup:
    new_dur_min  = new_duration_slots * 30
    available = get_available_slots(year, month, day, new_dur_min, exclude_bid)
    rows, row = [], []
    for slot in available:
        row.append(InlineKeyboardButton(text=slot, callback_data="t_" + slot.replace(":", "")))
        if len(row) == 3:
            rows.append(row); row = []
    if row:
        rows.append(row)
    if not rows:
        rows.append([InlineKeyboardButton(text="Нет свободного времени", callback_data="noop")])
    rows.append([
        InlineKeyboardButton(text="◀️ Назад",        callback_data="back_to_days"),
        InlineKeyboardButton(text="🏠 Главное меню", callback_data="main_menu"),
    ])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def back_to_menu_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🏠 Главное меню", callback_data="main_menu")],
    ])


def booking_list_kb(user_id: int) -> InlineKeyboardMarkup:
    user_bookings = get_user_bookings(user_id)
    rows = []
    for b in user_bookings:
        label = f"{b['time']} | {b['day']} {MONTHS_GEN[b['month']]} | {b['service'][:20]}"
        rows.append([InlineKeyboardButton(text=label, callback_data=f"viewb:{b['id']}")])
    rows.append([InlineKeyboardButton(text="🗑 Удалить все брони", callback_data="del_all_confirm")])
    rows.append([InlineKeyboardButton(text="🏠 Главное меню", callback_data="main_menu")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def booking_actions_kb(bid: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="🔄 Перенести",     callback_data=f"reschedule:{bid}"),
            InlineKeyboardButton(text="🗑 Удалить",        callback_data=f"del_booking:{bid}"),
        ],
        [
            InlineKeyboardButton(text="◀️ Все брони",    callback_data="my_booking"),
            InlineKeyboardButton(text="🏠 Главное меню", callback_data="main_menu"),
        ],
    ])


def confirm_delete_kb(bid: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text="✅ Да, удалить", callback_data=f"confirm_del:{bid}"),
        InlineKeyboardButton(text="❌ Отмена",       callback_data=f"viewb:{bid}"),
    ]])


def edit_options_kb(bid: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="💅 Изменить услугу",  callback_data=f"efield:service:{bid}")],
        [InlineKeyboardButton(text="📅 Изменить дату",    callback_data=f"efield:date:{bid}")],
        [InlineKeyboardButton(text="⏱ Изменить время",   callback_data=f"efield:time:{bid}")],
        [InlineKeyboardButton(text="👤 Изменить имя",     callback_data=f"efield:name:{bid}")],
        [InlineKeyboardButton(text="📞 Изменить телефон", callback_data=f"efield:phone:{bid}")],
        [InlineKeyboardButton(text="◀️ Назад",            callback_data=f"viewb:{bid}")],
    ])


def services_edit_kb(bid: int) -> InlineKeyboardMarkup:
    rows = []
    for s in services:
        label = f"{s['name']} — {s['price']} (~{s['duration']})"
        rows.append([InlineKeyboardButton(text=label, callback_data=f"esvc:{bid}:{s['name']}")])
    rows.append([InlineKeyboardButton(text="◀️ Назад", callback_data=f"edit_booking:{bid}")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def admin_panel_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📋 Все брони",        callback_data="admin_all")],
        [InlineKeyboardButton(text="📅 Брони на сегодня", callback_data="admin_today")],
        [InlineKeyboardButton(text="🔜 Брони на завтра",  callback_data="admin_tomorrow")],
        [InlineKeyboardButton(text="🗓 Моё расписание",   callback_data="admin_schedule")],
        [InlineKeyboardButton(text="📊 Статистика",       callback_data="admin_stats")],
    ])


def schedule_main_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🚫 Заблокировать день",   callback_data="sched_block_day")],
        [InlineKeyboardButton(text="✅ Разблокировать день",  callback_data="sched_unblock_day")],
        [InlineKeyboardButton(text="🚫 Заблокировать часы",   callback_data="sched_block_slots")],
        [InlineKeyboardButton(text="✅ Разблокировать часы",  callback_data="sched_unblock_slots")],
        [InlineKeyboardButton(text="📋 Показать расписание",  callback_data="sched_show")],
        [InlineKeyboardButton(text="🔓 Разблокировать всё",   callback_data="sched_unblock_all_confirm")],
        [InlineKeyboardButton(text="◀️ Назад",                callback_data="admin_back")],
    ])


def schedule_months_kb(action: str) -> InlineKeyboardMarkup:
    now = datetime.now()
    rows, row = [], []
    shown = 0
    for num, name in MONTHS.items():
        if num < now.month:
            continue
        if shown >= 3:
            break
        row.append(InlineKeyboardButton(text=name, callback_data=f"sm_{action}:{num}"))
        shown += 1
        if len(row) == 3:
            rows.append(row); row = []
    if row:
        rows.append(row)
    rows.append([InlineKeyboardButton(text="◀️ Назад", callback_data="admin_schedule")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def schedule_days_kb(month: int, action: str) -> InlineKeyboardMarkup:
    now = datetime.now()
    _, days_in_month = calendar.monthrange(now.year, month)
    rows, row = [], []
    for day in range(1, days_in_month + 1):
        if month == now.month and day < now.day:
            continue
        key   = date_key(now.year, month, day)
        label = f"🚫{day}" if key in get_blocked_days() else str(day)
        row.append(InlineKeyboardButton(text=label, callback_data=f"sd_{action}:{month}:{day}"))
        if len(row) == 7:
            rows.append(row); row = []
    if row:
        rows.append(row)
    rows.append([InlineKeyboardButton(text="◀️ Назад", callback_data="admin_schedule")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def schedule_slots_kb(month: int, day: int, action: str) -> InlineKeyboardMarkup:
    year           = datetime.now().year
    key            = date_key(year, month, day)
    blocked_manual = get_blocked_slots_for_date(key)
    booked_slots = set()
    for b in get_all_bookings():
        if b["year"] == year and b["month"] == month and b["day"] == day:
            svc = get_service(b["service"])
            dur = svc["duration_slots"] if svc else 1
            bi  = TIME_SLOTS.index(b["time"])
            for i in range(dur):
                idx = bi + i
                if 0 <= idx < len(TIME_SLOTS):
                    booked_slots.add(TIME_SLOTS[idx])
    rows, row = [], []
    for slot in TIME_SLOTS:
        if slot in blocked_manual:
            label = f"🔒{slot}"
        elif slot in booked_slots:
            label = f"👤{slot}"
        else:
            label = slot
        row.append(InlineKeyboardButton(
            text=label,
            callback_data=f"ss_{action}:{month}:{day}:{slot.replace(':', '')}"
        ))
        if len(row) == 3:
            rows.append(row); row = []
    if row:
        rows.append(row)
    rows.append([InlineKeyboardButton(text="◀️ Назад", callback_data="admin_schedule")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


# ─── /start ───────────────────────────────────────────────────────────────────

@dp.message(Command(commands=["start"]))
async def cmd_start(message: types.Message, state: FSMContext):
    await state.clear()
    is_admin = message.from_user.id in ADMIN_IDS
    photo = FSInputFile("images/darja.png")
    await message.answer_photo(
        photo=photo,
        caption=(
            "👋 Привет! Я помощник по записи\n"
            "к мастеру маникюра Дарье 💅"
        ),
        reply_markup=bottom_kb(is_admin)
    )
    await message.answer("💅 Выберите услугу:", reply_markup=main_menu_kb())


# ─── Нижние кнопки ────────────────────────────────────────────────────────────

@dp.message(F.text == "💅 Услуги")
async def btn_services(message: types.Message, state: FSMContext):
    await state.clear()
    await message.answer("Выберите услугу 👇", reply_markup=main_menu_kb())


@dp.message(F.text == "📋 Мои брони")
async def btn_my_bookings(message: types.Message, state: FSMContext):
    await state.clear()
    user_id       = message.from_user.id
    user_bookings = get_user_bookings(user_id)
    if not user_bookings:
        await message.answer("У вас нет активных броней.")
        return
    await message.answer("📋 Ваши брони:", reply_markup=booking_list_kb(user_id))


@dp.message(F.text == "📞 Контакты")
async def btn_contacts(message: types.Message):
    await message.answer("📇 Контакты мастера:\n\n" + CONTACTS_FULL)


@dp.message(F.text == "🔐 Админка")
async def btn_admin_panel(message: types.Message):
    if message.from_user.id not in ADMIN_IDS:
        await message.answer("⛔️ Нет доступа.")
        return
    all_b = get_all_bookings()
    await message.answer(
        f"🔐 Панель администратора\nВсего броней: {len(all_b)}",
        reply_markup=admin_panel_kb()
    )


# ─── Инлайн: меню ─────────────────────────────────────────────────────────────

@dp.callback_query(F.data == "main_menu")
async def go_main_menu(call: types.CallbackQuery, state: FSMContext):
    await state.clear()
    await call.message.answer("Выберите услугу 👇", reply_markup=main_menu_kb())
    await call.answer()


@dp.callback_query(F.data == "day_blocked")
async def day_blocked_cb(call: types.CallbackQuery):
    await call.answer("🚫 Этот день недоступен для записи.", show_alert=True)


@dp.callback_query(F.data == "slot_busy")
async def slot_busy(call: types.CallbackQuery):
    await call.answer("❌ Это время недоступно.", show_alert=True)


# ─── Инлайн: мои брони ────────────────────────────────────────────────────────

@dp.callback_query(F.data == "my_booking")
async def show_my_bookings(call: types.CallbackQuery, state: FSMContext):
    await state.clear()
    user_id       = call.from_user.id
    user_bookings = get_user_bookings(user_id)
    if not user_bookings:
        await call.answer("У вас нет активных броней.", show_alert=True)
        return
    await call.message.answer("📋 Ваши брони:", reply_markup=booking_list_kb(user_id))
    await call.answer()


@dp.callback_query(F.data.startswith("viewb:"))
async def view_booking(call: types.CallbackQuery):
    bid = int(call.data.split(":")[1])
    b   = get_booking(bid)
    if not b or b["user_id"] != call.from_user.id:
        await call.answer("Бронь не найдена.", show_alert=True)
        return
    user_bookings = get_user_bookings(call.from_user.id)
    idx = next((i + 1 for i, x in enumerate(user_bookings) if x["id"] == bid), 1)
    await call.message.answer(format_booking(b, idx), reply_markup=booking_actions_kb(bid))
    await call.answer()


# ─── Удаление брони ───────────────────────────────────────────────────────────

@dp.callback_query(F.data.startswith("del_booking:"))
async def delete_booking_confirm(call: types.CallbackQuery):
    bid = int(call.data.split(":")[1])
    await call.message.answer("❗️ Вы уверены, что хотите удалить эту бронь?",
                               reply_markup=confirm_delete_kb(bid))
    await call.answer()


@dp.callback_query(F.data.startswith("confirm_del:"))
async def confirm_delete(call: types.CallbackQuery):
    bid = int(call.data.split(":")[1])
    b   = get_booking(bid)
    if b and b["user_id"] == call.from_user.id:
        log_cancellation(b, "client")
        remove_booking(bid)
        for _aid in ADMIN_IDS:
            await bot.send_message(_aid, f"🗑 Клиент отменил бронь!\n\n{format_booking(b)}")
    await call.message.answer("✅ Бронь удалена.", reply_markup=back_to_menu_kb())
    await call.answer()


# ─── Удалить все брони ───────────────────────────────────────────────────────

@dp.callback_query(F.data == "del_all_confirm")
async def del_all_confirm(call: types.CallbackQuery):
    kb = InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text="✅ Да, удалить все", callback_data="del_all_yes"),
        InlineKeyboardButton(text="❌ Отмена",           callback_data="my_booking"),
    ]])
    await call.message.answer("❗️ Вы уверены? Все ваши брони будут удалены!", reply_markup=kb)
    await call.answer()


@dp.callback_query(F.data == "del_all_yes")
async def del_all_yes(call: types.CallbackQuery):
    user_id = call.from_user.id
    bookings = get_user_bookings(user_id)
    for b in bookings:
        remove_booking(b["id"])
        for _aid in ADMIN_IDS:
            try:
                await bot.send_message(_aid, f"🗑 Клиент удалил все брони!\n\n{format_booking(b)}")
            except Exception:
                pass
    await call.message.answer("✅ Все брони удалены.", reply_markup=back_to_menu_kb())
    await call.answer()


# ─── Редактирование брони ─────────────────────────────────────────────────────

@dp.callback_query(F.data.startswith("edit_booking:"))
async def edit_booking_menu(call: types.CallbackQuery):
    bid = int(call.data.split(":")[1])
    await call.message.answer("Что хотите изменить?", reply_markup=edit_options_kb(bid))
    await call.answer()


@dp.callback_query(F.data.startswith("efield:"))
async def edit_field_start(call: types.CallbackQuery, state: FSMContext):
    parts      = call.data.split(":", 2)
    field, bid = parts[1], int(parts[2])
    await state.update_data(edit_bid=bid)

    if field == "service":
        await call.message.answer("Выберите новую услугу:", reply_markup=services_edit_kb(bid))
        await state.set_state(EditBooking.service)
    elif field == "date":
        await call.message.answer("Выберите год:", reply_markup=years_kb())
        await state.set_state(EditBooking.year)
    elif field == "time":
        b = get_booking(bid)
        if b:
            await call.message.answer("Выберите новое время:",
                                      reply_markup=time_kb(b["year"], b["month"], b["day"], exclude_bid=bid))
        await state.set_state(EditBooking.time)
    elif field == "name":
        await call.message.answer("Введите новое имя:")
        await state.set_state(EditBooking.name)
    elif field == "phone":
        await call.message.answer("Введите новый номер телефона:")
        await state.set_state(EditBooking.phone)
    await call.answer()


async def notify_edit(bid: int):
    b = get_booking(bid)
    if not b:
        return
    user_bookings = get_user_bookings(b["user_id"])
    idx = next((i + 1 for i, x in enumerate(user_bookings) if x["id"] == bid), 1)
    await bot.send_message(b["user_id"],
                           f"✅ Ваша бронь обновлена!\n\n{format_booking(b, idx)}",
                           reply_markup=booking_actions_kb(bid))
    for _aid in ADMIN_IDS:
        await bot.send_message(_aid, f"✏️ Клиент изменил бронь!\n\n{format_booking(b)}")


@dp.callback_query(F.data.startswith("esvc:"), EditBooking.service)
async def edit_service_save(call: types.CallbackQuery, state: FSMContext):
    _, bid_str, service_name = call.data.split(":", 2)
    bid = int(bid_str)
    update_booking_field(bid, "service", service_name)
    await state.clear()
    await notify_edit(bid)
    await call.answer()


@dp.callback_query(F.data.startswith("year:"), EditBooking.year)
async def edit_year_save(call: types.CallbackQuery, state: FSMContext):
    year = int(call.data.split(":")[1])
    await state.update_data(edit_year=year)
    await call.message.answer(f"Выберите месяц ({year}):", reply_markup=months_kb(year))
    await state.set_state(EditBooking.month)
    await call.answer()


@dp.callback_query(F.data.startswith("month:"), EditBooking.month)
async def edit_month_save(call: types.CallbackQuery, state: FSMContext):
    month = int(call.data.split(":")[1])
    data  = await state.get_data()
    year  = data.get("edit_year", datetime.now().year)
    await state.update_data(edit_month=month)
    await call.message.answer(f"Выберите день:", reply_markup=days_kb(year, month))
    await state.set_state(EditBooking.day)
    await call.answer()


@dp.callback_query(F.data.startswith("day:"), EditBooking.day)
async def edit_day_save(call: types.CallbackQuery, state: FSMContext):
    day  = int(call.data.split(":")[1])
    data = await state.get_data()
    bid  = data["edit_bid"]
    year  = data.get("edit_year", datetime.now().year)
    month = data.get("edit_month")
    if month:
        update_booking_field(bid, "year",  year)
        update_booking_field(bid, "month", month)
        update_booking_field(bid, "day",   day)
    await state.clear()
    await notify_edit(bid)
    await call.answer()


@dp.callback_query(F.data.startswith("t_"), EditBooking.time)
async def edit_time_save(call: types.CallbackQuery, state: FSMContext):
    raw      = call.data[2:]
    time_str = raw[:2] + ":" + raw[2:]
    data     = await state.get_data()
    bid      = data["edit_bid"]
    update_booking_field(bid, "time", time_str)
    await state.clear()
    await notify_edit(bid)
    await call.answer()


@dp.message(EditBooking.name)
async def edit_name_save(message: types.Message, state: FSMContext):
    data = await state.get_data()
    bid  = data["edit_bid"]
    update_booking_field(bid, "name", message.text)
    await state.clear()
    await notify_edit(bid)


@dp.message(EditBooking.phone)
async def edit_phone_save(message: types.Message, state: FSMContext):
    data = await state.get_data()
    bid  = data["edit_bid"]
    update_booking_field(bid, "phone", message.text)
    await state.clear()
    await notify_edit(bid)


# ─── Новая бронь ──────────────────────────────────────────────────────────────

@dp.callback_query(F.data.startswith("svc:"))
async def service_choice(call: types.CallbackQuery, state: FSMContext):
    service_name = call.data[4:]
    svc          = get_service(service_name)
    if not svc:
        await call.answer("Услуга не найдена.", show_alert=True)
        return
    year = datetime.now().year
    await state.update_data(service=service_name, year=year)
    photo = FSInputFile(svc["img"])
    await call.message.answer_photo(
        photo=photo,
        caption=(f"✅ Вы выбрали: {service_name}\n"
                 f"💰 {svc['price']} | ⏱ {svc['duration']}\n\nВыберите месяц:"),
        reply_markup=months_kb(year)
    )
    await state.set_state(Booking.month)
    await call.answer()


@dp.callback_query(F.data == "back_to_months")
async def back_to_months(call: types.CallbackQuery, state: FSMContext):
    data = await state.get_data()
    year = data.get("year", datetime.now().year)
    await call.message.answer("Выберите месяц:", reply_markup=months_kb(year))
    await state.set_state(Booking.month)
    await call.answer()


@dp.callback_query(F.data.startswith("month:"), Booking.month)
async def month_choice(call: types.CallbackQuery, state: FSMContext):
    month = int(call.data.split(":")[1])
    data  = await state.get_data()
    year  = data.get("year", datetime.now().year)
    await state.update_data(month=month)
    svc_d = get_service(data.get("service",""))
    dur_d = svc_d["duration_slots"] if svc_d else 1
    await call.message.answer(f"Выберите день ({MONTHS[month]}):", reply_markup=days_kb(year, month, dur_d))
    await state.set_state(Booking.day)
    await call.answer()


@dp.callback_query(F.data == "back_to_days")
async def back_to_days(call: types.CallbackQuery, state: FSMContext):
    data  = await state.get_data()
    year  = data.get("year", datetime.now().year)
    month = data.get("month")
    if month:
        await call.message.answer(f"Выберите день ({MONTHS[month]}):", reply_markup=days_kb(year, month))
        await state.set_state(Booking.day)
    else:
        await call.message.answer("Выберите месяц:", reply_markup=months_kb(year))
        await state.set_state(Booking.month)
    await call.answer()


@dp.callback_query(F.data.startswith("day:"), Booking.day)
async def day_choice(call: types.CallbackQuery, state: FSMContext):
    day  = int(call.data.split(":")[1])
    data = await state.get_data()
    year  = data.get("year", datetime.now().year)
    month = data.get("month")
    await state.update_data(day=day)
    svc_data = get_service(data.get("service", ""))
    new_dur  = svc_data["duration_slots"] if svc_data else 1
    month_name = MONTHS[month] if month else ""
    await call.message.answer(
        f"✅ Вы выбрали: {day} {month_name}\n\nВыберите удобное время:",
        reply_markup=time_kb(year, month, day, new_duration_slots=new_dur)
    )
    await state.set_state(Booking.time)
    await call.answer()


@dp.callback_query(F.data.startswith("t_"), Booking.time)
async def time_choice(call: types.CallbackQuery, state: FSMContext):
    raw      = call.data[2:]
    time_str = raw[:2] + ":" + raw[2:]
    await state.update_data(time=time_str)
    await call.message.answer(f"✅ Вы выбрали: {time_str}\n\nВведите ваше имя:")
    await state.set_state(Booking.name)
    await call.answer()


@dp.message(Booking.name)
async def enter_name(message: types.Message, state: FSMContext):
    await state.update_data(name=message.text)
    await message.answer("Введите ваш номер телефона:")
    await state.set_state(Booking.phone)


@dp.message(Booking.phone)
async def enter_phone(message: types.Message, state: FSMContext):
    import re
    if re.search(r"[a-zA-Zа-яА-ЯёЁ]", message.text or ""):
        await message.answer("⚠️ Номер телефона не должен содержать буквы. Попробуйте ещё раз:")
        return
    data    = await state.get_data()
    user_id = message.from_user.id

    svc_chk  = get_service(data.get("service", ""))
    dur_chk  = svc_chk["duration_slots"] if svc_chk else 1
    yr  = data.get("year") or datetime.now().year
    mon = data.get("month")
    day = data.get("day")
    available_now = get_available_slots(yr, mon, day, dur_chk * 30)
    if data["time"] not in available_now:
        await message.answer(
            "⚠️ Это время уже заняли пока вы оформляли запись.\n"
            "Пожалуйста, начните запись заново 👇",
            reply_markup=main_menu_kb()
        )
        await state.clear()
        return

    bid = add_booking(
        user_id=user_id,
        service=data["service"],
        year=yr,
        month=mon,
        day=day,
        time=data["time"],
        name=data["name"],
        phone=message.text,
    )
    b = get_booking(bid)

    svc      = get_service(data["service"])
    dur      = svc["duration"] if svc else ""

    await message.answer(
        f"✅ Бронь подтверждена!\n\n"
        f"{format_booking(b)}\n\n"
        f"━━━━━━━━━━━━━━━━━\n"
        f"📍 Контакты Дарьи:\n{CONTACTS_SHORT}",
        reply_markup=back_to_menu_kb()
    )
    tg_username = message.from_user.username
    tg_line     = f"\n💬 @{tg_username}" if tg_username else ""
    visits      = get_client_visits(message.from_user.id)
    rank        = get_client_rank(visits)
    visit_word  = "визит" if visits == 1 else ("визита" if 2 <= visits <= 4 else "визитов")
    rank_line   = f"\n{rank} • {visits} {visit_word}"
    for _aid in ADMIN_IDS:
        await bot.send_message(
            _aid,
            f"🔔 Новая бронь!\n\n"
            f"💅 {data['service']}\n"
            f"⏱ Длительность: ~{dur}\n"
            f"🕐 {data['time']} | {data['day']} {MONTHS_GEN[data['month']]}\n"
            f"👤 {data['name']} 📞 {message.text}{tg_line}{rank_line}"
        )
    await state.clear()


# ─── Панель администратора ────────────────────────────────────────────────────


@dp.callback_query(F.data.startswith("admin_view:"))
async def admin_view_booking(call: types.CallbackQuery):
    if call.from_user.id not in ADMIN_IDS:
        await call.answer("⛔️ Нет доступа.", show_alert=True); return
    bid = int(call.data.split(":")[1])
    b   = get_booking(bid)
    if not b:
        await call.answer("Бронь не найдена.", show_alert=True); return
    svc      = get_service(b["service"])
    dur      = svc["duration"] if svc else ""
    visits     = get_client_visits(b["user_id"])
    rank       = get_client_rank(visits)
    visit_word = "визит" if visits == 1 else ("визита" if 2 <= visits <= 4 else "визитов")
    text = (
        f"📋 Бронь #{bid}\n\n"
        f"💅 {b['service']}\n"
        f"⏱ Длительность: ~{dur}\n"
        f"🕐 {b['time']} | {b['day']} {MONTHS_GEN[b['month']]}\n"
        f"👤 {b['name']} 📞 {b['phone']}\n"
        f"{rank} • {visits} {visit_word}"
    )
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✏️ Редактировать", callback_data=f"edit_booking:{bid}"),
         InlineKeyboardButton(text="🗑 Удалить",        callback_data=f"admin_del:{bid}")],
        [InlineKeyboardButton(text="◀️ Назад",          callback_data="admin_all")],
    ])
    await call.message.answer(text, reply_markup=kb)
    await call.answer()

@dp.callback_query(F.data.startswith("admin_"))
async def admin_actions(call: types.CallbackQuery, state: FSMContext):
    if call.from_user.id not in ADMIN_IDS:
        await call.answer("⛔️ Нет доступа.", show_alert=True)
        return
    action = call.data

    if action == "admin_all":
        all_b = get_all_bookings()
        if not all_b:
            await call.message.answer("Броней пока нет.")
            await call.answer(); return
        text = f"📋 Все брони ({len(all_b)} шт.):\n\n"
        rows = []
        for i, b in enumerate(all_b, 1):
            svc      = get_service(b["service"])
            dur      = svc["duration"] if svc else ""
            text += (f"💅 {b['service']}\n"
                     f"⏱ Длительность: ~{dur}\n"
                     f"🕐 {b['time']} | {b['day']} {MONTHS_GEN[b['month']]}\n"
                     f"👤 {b['name']} 📞 {b['phone']}\n\n")
            rows.append([InlineKeyboardButton(
                text=f"👁 #{i} — {b['name']} | {b['day']} {MONTHS[b['month']]} {b['time']}",
                callback_data=f"admin_view:{b['id']}"
            )])
        rows.append([InlineKeyboardButton(text="◀️ Назад", callback_data="admin_back")])
        await call.message.answer(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=rows))

    elif action == "admin_today":
        now     = datetime.now()
        today_b = [b for b in get_all_bookings()
                   if b["year"] == now.year and b["month"] == now.month and b["day"] == now.day]
        if not today_b:
            await call.message.answer("Сегодня броней нет.")
        else:
            text = f"📅 Сегодня ({now.day} {MONTHS[now.month]} {now.year}):\n\n"
            rows = []
            for b in today_b:
                svc      = get_service(b["service"])
                dur      = svc["duration"] if svc else ""
                text += (f"💅 {b['service']}\n"
                         f"⏱ Длительность: ~{dur}\n"
                         f"🕐 {b['time']} | {b['day']} {MONTHS_GEN[b['month']]}\n"
                         f"👤 {b['name']} 📞 {b['phone']}\n\n")
                rows.append([InlineKeyboardButton(
                    text=f"👁 {b['time']} — {b['name']}",
                    callback_data=f"admin_view:{b['id']}"
                )])
            rows.append([InlineKeyboardButton(text="◀️ Назад", callback_data="admin_back")])
            await call.message.answer(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=rows))

    elif action == "admin_tomorrow":
        tomorrow = datetime.now() + timedelta(days=1)
        tom_b = [b for b in get_all_bookings()
                 if b["year"] == tomorrow.year and b["month"] == tomorrow.month and b["day"] == tomorrow.day]
        if not tom_b:
            await call.message.answer("Завтра броней нет.")
        else:
            text = f"🔜 Завтра ({tomorrow.day} {MONTHS[tomorrow.month]} {tomorrow.year}):\n\n"
            rows = []
            for b in tom_b:
                svc      = get_service(b["service"])
                dur      = svc["duration"] if svc else ""
                text += (f"💅 {b['service']}\n"
                         f"⏱ Длительность: ~{dur}\n"
                         f"🕐 {b['time']} | {b['day']} {MONTHS_GEN[b['month']]}\n"
                         f"👤 {b['name']} 📞 {b['phone']}\n\n")
                rows.append([InlineKeyboardButton(
                    text=f"👁 {b['time']} — {b['name']}",
                    callback_data=f"admin_view:{b['id']}"
                )])
            rows.append([InlineKeyboardButton(text="◀️ Назад", callback_data="admin_back")])
            await call.message.answer(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=rows))

    elif action == "admin_schedule":
        await call.message.answer("🗓 Управление расписанием\n\nЧто хотите сделать?",
                                  reply_markup=schedule_main_kb())

    elif action == "admin_stats":
        try:
            s = get_stats()
        except Exception as e:
            await call.message.answer(f"❌ Ошибка статистики: {e}")
            await call.answer(); return
        text = (
            f"📊 Статистика\n\n"
            f"📋 Активных броней: {s['total_active']}\n"
            f"🗑 Отмен: {s['total_cancelled']} "
            f"(клиент: {s['cancelled_by_client']}, мастер: {s['cancelled_by_master']})\n"
            f"🔄 Переносов: {s['total_transfers']}\n"
            f"⭐ Отзывов: {s['total_reviews']}"
        )
        if s["avg_rating"]:
            text += f" | Средняя оценка: {s['avg_rating']} ⭐"
        if s["by_service"]:
            text += "\n\n💅 По услугам:\n"
            for svc_name, cnt in s["by_service"]:
                text += f"  • {svc_name}: {cnt}\n"
        if s["last_reviews"]:
            text += "\n⭐ Последние отзывы:\n"
            for rating, rev_text, svc_name, created_at in s["last_reviews"]:
                stars = "⭐" * rating
                rev   = f" — {rev_text}" if rev_text else ""
                text += f"  {stars} {svc_name}{rev}\n"
        await call.message.answer(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="◀️ Назад", callback_data="admin_back")]
        ]))

    elif action == "admin_back":
        all_b = get_all_bookings()
        await call.message.answer(f"🔐 Панель администратора\nВсего броней: {len(all_b)}",
                                  reply_markup=admin_panel_kb())

    elif action.startswith("admin_del:"):
        bid = int(action.split(":")[1])
        b   = get_booking(bid)
        if b:
            log_cancellation(b, "master")
            remove_booking(bid)
            try:
                await bot.send_message(b["user_id"],
                    f"❌ Ваша бронь была отменена мастером.\n\n{format_booking(b)}")
            except Exception:
                pass
            await call.message.answer("✅ Бронь отменена.")
        else:
            await call.message.answer("Бронь не найдена.")

    await call.answer()


# ─── Расписание ───────────────────────────────────────────────────────────────

@dp.callback_query(F.data.startswith("sched_"))
async def schedule_actions(call: types.CallbackQuery):
    if call.from_user.id not in ADMIN_IDS:
        await call.answer("⛔️ Нет доступа.", show_alert=True)
        return
    action = call.data
    if action == "sched_block_day":
        await call.message.answer("Выберите месяц:", reply_markup=schedule_months_kb("bday"))
    elif action == "sched_unblock_day":
        await call.message.answer("Выберите месяц:", reply_markup=schedule_months_kb("uday"))
    elif action == "sched_block_slots":
        await call.message.answer("Выберите месяц:", reply_markup=schedule_months_kb("bslot"))
    elif action == "sched_unblock_slots":
        await call.message.answer("Выберите месяц:", reply_markup=schedule_months_kb("uslot"))
    elif action == "sched_unblock_all_confirm":
        kb = InlineKeyboardMarkup(inline_keyboard=[[
            InlineKeyboardButton(text="✅ Да, разблокировать всё", callback_data="sched_unblock_all_yes"),
            InlineKeyboardButton(text="❌ Отмена", callback_data="admin_schedule"),
        ]])
        await call.message.answer("❗️ Разблокировать все дни и часы?", reply_markup=kb)
    elif action == "sched_unblock_all_yes":
        con = db_connect()
        con.execute("DELETE FROM schedule")
        con.commit(); con.close()
        await call.message.answer("✅ Всё расписание разблокировано.", reply_markup=schedule_main_kb())
    elif action == "sched_show":
        blocked = get_blocked_days()
        slots   = get_all_blocked_slots()
        text    = "📋 Текущие ограничения:\n\n"
        if blocked:
            text += "Заблокированные дни:\n"
            for d in sorted(blocked):
                text += f"  🚫 {d}\n"
            text += "\n"
        if slots:
            text += "Заблокированные часы:\n"
            for d, sl in sorted(slots.items()):
                text += f"  📅 {d}: {', '.join(sl)}\n"
        if not blocked and not slots:
            text += "Ограничений нет — все дни и часы открыты."
        await call.message.answer(text, reply_markup=schedule_main_kb())
    await call.answer()


@dp.callback_query(F.data.startswith("sm_"))
async def schedule_month_pick(call: types.CallbackQuery):
    _, rest  = call.data.split("_", 1)
    act, mon = rest.split(":")
    await call.message.answer(f"Выберите день ({MONTHS[int(mon)]}):",
                               reply_markup=schedule_days_kb(int(mon), act))
    await call.answer()


@dp.callback_query(F.data.startswith("sd_"))
async def schedule_day_pick(call: types.CallbackQuery):
    _, rest       = call.data.split("_", 1)
    act, mon, day = rest.split(":")
    month, day    = int(mon), int(day)
    key           = date_key(datetime.now().year, month, day)

    if act == "bday":
        yr = datetime.now().year
        has_bookings = any(
            b["year"] == yr and b["month"] == month and b["day"] == day
            for b in get_all_bookings()
        )
        if has_bookings:
            await call.answer(f"⚠️ На {day} {MONTHS[month]} есть брони — сначала отмените их", show_alert=True)
            return
        block_day(key)
        await call.answer(f"🚫 День {day} {MONTHS[month]} заблокирован")
        await call.message.edit_reply_markup(reply_markup=schedule_days_kb(month, act))
    elif act == "uday":
        unblock_day(key)
        await call.answer(f"✅ День {day} {MONTHS[month]} разблокирован")
        await call.message.edit_reply_markup(reply_markup=schedule_days_kb(month, act))
    elif act in ("bslot", "uslot"):
        await call.message.answer(f"Выберите часы для {day} {MONTHS[month]}:",
                                  reply_markup=schedule_slots_kb(month, day, act))
        await call.answer()


@dp.callback_query(F.data.startswith("ss_"))
async def schedule_slot_pick(call: types.CallbackQuery):
    _, rest            = call.data.split("_", 1)
    parts              = rest.split(":")
    act, mon, day, raw = parts[0], parts[1], parts[2], parts[3]
    month, day         = int(mon), int(day)
    time_str           = raw[:2] + ":" + raw[2:]
    key                = date_key(datetime.now().year, month, day)

    if act == "bslot":
        yr = datetime.now().year
        has_booking_at_slot = any(
            b["year"] == yr and b["month"] == month and b["day"] == day
            and b["time"] == time_str
            for b in get_all_bookings()
        )
        if has_booking_at_slot:
            await call.answer(f"⚠️ На {time_str} есть бронь — сначала отмените её", show_alert=True)
            return
        block_slot(key, time_str)
        await call.answer(f"🔒 {time_str} заблокировано")
    elif act == "uslot":
        unblock_slot(key, time_str)
        await call.answer(f"🔓 {time_str} разблокировано")

    await call.message.edit_reply_markup(reply_markup=schedule_slots_kb(month, day, act))


@dp.callback_query(F.data == "noop")
async def noop_cb(call: types.CallbackQuery):
    await call.answer()

# ─── Напоминания ─────────────────────────────────────────────────────────────

async def reminder_loop():
    while True:
        try:
            now = datetime.now()
            con = db_connect()
            rows_all = [_row_to_booking(r) for r in
                        con.execute("SELECT * FROM bookings").fetchall()]
            rows = [r for r in rows_all
                    if not r["reminded_24"] or not r["reminded_2"] or not r.get("review_sent")]
            con.close()

            for b in rows:
                try:
                    appt = datetime(b["year"], b["month"], b["day"],
                                    int(b["time"].split(":")[0]),
                                    int(b["time"].split(":")[1]))
                except Exception:
                    continue

                delta = appt - now
                total_minutes = delta.total_seconds() / 60
                svc = get_service(b["service"])
                dur = svc["duration"] if svc else ""

                if not b["reminded_24"] and 1430 <= total_minutes <= 1450:
                    try:
                        await bot.send_message(
                            b["user_id"],
                            f"🔔 Напоминание!\n\n"
                            f"Завтра у вас запись:\n"
                            f"💅 {b['service']} ({dur})\n"
                            f"📅 {b['day']} {MONTHS_GEN[b['month']]} {b['year']}\n"
                            f"⏱ {b['time']}\n\n"
                            f"📍 {CONTACTS_SHORT}"
                        )
                        con2 = db_connect()
                        con2.execute("UPDATE bookings SET reminded_24=1 WHERE id=?", (b["id"],))
                        con2.commit(); con2.close()
                    except Exception:
                        pass

                if not b["reminded_2"] and 110 <= total_minutes <= 130:
                    try:
                        await bot.send_message(
                            b["user_id"],
                            f"⏰ Через 2 часа ваша запись!\n\n"
                            f"💅 {b['service']} ({dur})\n"
                            f"📅 {b['day']} {MONTHS_GEN[b['month']]} {b['year']}\n"
                            f"⏱ {b['time']}\n\n"
                            f"📍 {CONTACTS_SHORT}"
                        )
                        con2 = db_connect()
                        con2.execute("UPDATE bookings SET reminded_2=1 WHERE id=?", (b["id"],))
                        con2.commit(); con2.close()
                    except Exception:
                        pass

                # Запрос отзыва — через 60 мин после окончания услуги
                svc_obj = get_service(b["service"])
                dur_m   = duration_minutes(svc_obj)
                end_appt = appt + timedelta(minutes=dur_m)
                mins_after_end = (now - end_appt).total_seconds() / 60
                if not b.get("review_sent") and 60 <= mins_after_end <= 80:
                    try:
                        await bot.send_message(
                            b["user_id"],
                            f"😊 Как прошёл ваш визит?\n\n"
                            f"💅 {b['service']}\n\n"
                            f"Пожалуйста, оставьте оценку 👇",
                            reply_markup=review_rating_kb(b["id"])
                        )
                        con2 = db_connect()
                        con2.execute("UPDATE bookings SET review_sent=1 WHERE id=?", (b["id"],))
                        con2.commit(); con2.close()
                    except Exception:
                        pass

            # Утреннее напоминание мастеру в 08:00
            if now.hour == 8 and now.minute < 5:
                today_b = [b2 for b2 in rows_all
                           if b2["year"] == now.year and b2["month"] == now.month
                           and b2["day"] == now.day]
                if today_b:
                    text = f"☀️ Доброе утро! Сегодня {now.day} {MONTHS_GEN[now.month]}:\n\n"
                    for b2 in today_b:
                        svc2 = get_service(b2["service"])
                        dur2 = svc2["duration"] if svc2 else ""
                        text += f"⏱ {b2['time']} — 💅 {b2['service']} (~{dur2})\n👤 {b2['name']} 📞 {b2['phone']}\n\n"
                    for _aid in ADMIN_IDS:
                        try:
                            await bot.send_message(_aid, text)
                        except Exception:
                            pass

        except Exception as e:
            print(f"Reminder error: {e}")

        await asyncio.sleep(300)


# ─── Перенос брони ───────────────────────────────────────────────────────────

@dp.callback_query(F.data.startswith("reschedule:"))
async def reschedule_start(call: types.CallbackQuery, state: FSMContext):
    bid = int(call.data.split(":")[1])
    b   = get_booking(bid)
    if not b or b["user_id"] != call.from_user.id:
        await call.answer("Бронь не найдена.", show_alert=True); return
    await state.update_data(reschedule_bid=bid,
                            reschedule_service=b["service"],
                            reschedule_old_date=f"{b['day']} {MONTHS_GEN[b['month']]}",
                            reschedule_old_time=b["time"])
    svc = get_service(b["service"])
    dur = svc["duration_slots"] if svc else 1
    year = datetime.now().year
    await state.update_data(year=year)
    await call.message.answer("📅 Выберите новую дату — месяц:", reply_markup=months_kb(year))
    await state.set_state(Reschedule.month)
    await call.answer()


@dp.callback_query(F.data.startswith("month:"), Reschedule.month)
async def reschedule_month(call: types.CallbackQuery, state: FSMContext):
    month = int(call.data.split(":")[1])
    data  = await state.get_data()
    year  = data.get("year", datetime.now().year)
    await state.update_data(month=month)
    svc = get_service(data.get("reschedule_service", ""))
    dur = svc["duration_slots"] if svc else 1
    bid = data.get("reschedule_bid")
    await call.message.answer(f"Выберите день ({MONTHS[month]}):",
                               reply_markup=days_kb(year, month, dur))
    await state.set_state(Reschedule.day)
    await call.answer()


@dp.callback_query(F.data.startswith("day:"), Reschedule.day)
async def reschedule_day(call: types.CallbackQuery, state: FSMContext):
    day  = int(call.data.split(":")[1])
    data = await state.get_data()
    year  = data.get("year", datetime.now().year)
    month = data.get("month")
    bid   = data.get("reschedule_bid")
    await state.update_data(day=day)
    svc = get_service(data.get("reschedule_service", ""))
    dur = svc["duration_slots"] if svc else 1
    month_name = MONTHS_GEN[month] if month else ""
    await call.message.answer(
        f"✅ Вы выбрали: {day} {month_name}\n\nВыберите удобное время:",
        reply_markup=time_kb(year, month, day, exclude_bid=bid, new_duration_slots=dur)
    )
    await state.set_state(Reschedule.time)
    await call.answer()


@dp.callback_query(F.data.startswith("t_"), Reschedule.time)
async def reschedule_time(call: types.CallbackQuery, state: FSMContext):
    raw      = call.data[2:]
    new_time = raw[:2] + ":" + raw[2:]
    data     = await state.get_data()
    bid      = data["reschedule_bid"]
    b        = get_booking(bid)
    if not b:
        await call.answer("Бронь не найдена.", show_alert=True); return

    year  = data.get("year", datetime.now().year)
    month = data.get("month")
    day   = data.get("day")
    old_date = f"{b['year']}-{b['month']:02d}-{b['day']:02d}"
    new_date = f"{year}-{month:02d}-{day:02d}"

    # Логируем перенос
    log_transfer(bid, b["user_id"], b["service"],
                 old_date, b["time"], new_date, new_time)

    # Обновляем бронь
    update_booking_field(bid, "year",  year)
    update_booking_field(bid, "month", month)
    update_booking_field(bid, "day",   day)
    update_booking_field(bid, "time",  new_time)

    b_new = get_booking(bid)
    await call.message.answer(
        f"✅ Бронь перенесена!\n\n{format_booking(b_new)}",
        reply_markup=booking_actions_kb(bid)
    )
    for _aid in ADMIN_IDS:
        await bot.send_message(
            _aid,
            f"🔄 Клиент перенёс бронь!\n\n"
            f"Было: {data['reschedule_old_date']} {data['reschedule_old_time']}\n"
            f"Стало: {day} {MONTHS_GEN[month]} {new_time}\n\n"
            f"{format_booking(b_new, username=None)}"
        )
    await state.clear()
    await call.answer()


# ─── Отзывы ───────────────────────────────────────────────────────────────────

def review_rating_kb(bid: int) -> InlineKeyboardMarkup:
    stars = ["⭐", "⭐⭐", "⭐⭐⭐", "⭐⭐⭐⭐", "⭐⭐⭐⭐⭐"]
    rows = [[InlineKeyboardButton(text=s, callback_data=f"rev_rating:{bid}:{i+1}")]
            for i, s in enumerate(stars)]
    rows.append([InlineKeyboardButton(text="❌ Пропустить", callback_data="rev_skip")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


@dp.callback_query(F.data.startswith("rev_rating:"))
async def review_rating(call: types.CallbackQuery, state: FSMContext):
    _, bid_str, rating_str = call.data.split(":")
    bid    = int(bid_str)
    rating = int(rating_str)
    b      = get_booking(bid)
    svc    = b["service"] if b else ""
    await state.update_data(review_bid=bid, review_rating=rating, review_service=svc)
    stars  = "⭐" * rating
    await call.message.answer(
        f"Вы поставили {stars}\n\nНапишите отзыв (или отправьте /skip чтобы пропустить):"
    )
    await state.set_state(Review.text)
    await call.answer()


@dp.callback_query(F.data == "rev_skip")
async def review_skip(call: types.CallbackQuery):
    await call.message.answer("Спасибо! Ждём вас снова 💅")
    await call.answer()


@dp.message(Review.text)
async def review_text(message: types.Message, state: FSMContext):
    data = await state.get_data()
    text = message.text if message.text != "/skip" else ""
    add_review(data["review_bid"], message.from_user.id,
               data["review_service"], data["review_rating"], text)
    stars = "⭐" * data["review_rating"]
    await message.answer("🙏 Спасибо за отзыв! Ждём вас снова 💅")
    for _aid in ADMIN_IDS:
        review_text = f"\n💬 {text}" if text else ""
        await bot.send_message(
            _aid,
            f"⭐ Новый отзыв!\n\n"
            f"💅 {data['review_service']}\n"
            f"{stars}{review_text}"
        )
    await state.clear()


# ─── Статистика ───────────────────────────────────────────────────────────────




@dp.message(Command("stats"))
async def cmd_stats(message: types.Message):
    if message.from_user.id not in ADMIN_IDS:
        return
    try:
        s = get_stats()
        await message.answer(
            f"📊 Статистика\n\n"
            f"📋 Активных броней: {s['total_active']}\n"
            f"🗑 Отмен: {s['total_cancelled']} (клиент: {s['cancelled_by_client']}, мастер: {s['cancelled_by_master']})\n"
            f"🔄 Переносов: {s['total_transfers']}\n"
            f"⭐ Отзывов: {s['total_reviews']}"
        )
    except Exception as e:
        await message.answer(f"❌ Ошибка: {e}")


# ─── Запуск ───────────────────────────────────────────────────────────────────

async def on_startup(bot: Bot):
    asyncio.create_task(reminder_loop())


async def main():
    dp.startup.register(on_startup)
    print("Бот запущен!")
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())