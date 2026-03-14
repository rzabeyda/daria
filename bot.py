import asyncio
import calendar
import os
import re
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

API_TOKEN = os.getenv("API_TOKEN")
ADMIN_IDS = set()
for _key in ("ADMIN_ID", "ADMIN_ID2", "ADMIN_ID3"):
    _val = os.getenv(_key)
    if _val:
        ADMIN_IDS.add(int(_val))

bot = Bot(token=API_TOKEN)
dp  = Dispatcher(storage=MemoryStorage())

DB_FILE = "bot.db"

def db_connect():
    return sqlite3.connect(DB_FILE)

def init_db():
    con = db_connect()
    con.executescript("""
        CREATE TABLE IF NOT EXISTS bookings (
            id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER NOT NULL,
            service TEXT NOT NULL, year INTEGER NOT NULL, month INTEGER NOT NULL,
            day INTEGER NOT NULL, time TEXT NOT NULL, name TEXT NOT NULL,
            phone TEXT NOT NULL, reminded_24 INTEGER DEFAULT 0,
            reminded_2 INTEGER DEFAULT 0, review_sent INTEGER DEFAULT 0
        );
        CREATE TABLE IF NOT EXISTS schedule (
            id INTEGER PRIMARY KEY AUTOINCREMENT, type TEXT NOT NULL,
            date TEXT NOT NULL, time TEXT
        );
        CREATE TABLE IF NOT EXISTS cancelled (
            id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER NOT NULL,
            service TEXT NOT NULL, year INTEGER NOT NULL, month INTEGER NOT NULL,
            day INTEGER NOT NULL, time TEXT NOT NULL, name TEXT NOT NULL,
            phone TEXT NOT NULL, cancelled_by TEXT NOT NULL, cancelled_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS transfers (
            id INTEGER PRIMARY KEY AUTOINCREMENT, booking_id INTEGER NOT NULL,
            user_id INTEGER NOT NULL, service TEXT NOT NULL, old_date TEXT NOT NULL,
            old_time TEXT NOT NULL, new_date TEXT NOT NULL, new_time TEXT NOT NULL,
            transferred_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS reviews (
            id INTEGER PRIMARY KEY AUTOINCREMENT, booking_id INTEGER NOT NULL,
            user_id INTEGER NOT NULL, service TEXT NOT NULL, rating INTEGER NOT NULL,
            text TEXT, created_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS services (
            id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT NOT NULL UNIQUE,
            price TEXT NOT NULL, duration TEXT NOT NULL, duration_min INTEGER NOT NULL,
            img TEXT NOT NULL, sort_order INTEGER DEFAULT 0, active INTEGER DEFAULT 1, description TEXT DEFAULT ''
        );
    """)
    con.commit(); con.close()

init_db()

def migrate_db():
    con = db_connect()
    for stmt in [
        "ALTER TABLE bookings ADD COLUMN review_sent INTEGER DEFAULT 0",
        "ALTER TABLE reviews ADD COLUMN username TEXT DEFAULT ''",
        "ALTER TABLE services ADD COLUMN description TEXT DEFAULT ''",
    ]:
        try: con.execute(stmt); con.commit()
        except: pass
    con.close()

migrate_db()

_DEFAULT_SERVICES = [
    ("Классический маникюр", "15€", "30 мин",  30,  "images/1.jpg", 1),
    ("Гель-лак / Коррекция", "25€", "60 мин",  60,  "images/2.jpg", 2),
    ("Наращивание ногтей",   "35€", "120 мин", 120, "images/3.jpg", 3),
    ("Гигиенический педикюр","25€", "45 мин",  45,  "images/4.jpg", 4),
    ("Педикюр с гель-лаком", "35€", "60 мин",  60,  "images/5.jpg", 5),
    ("Мужской педикюр",      "30€", "60 мин",  60,  "images/6.jpg", 6),
    ("Снятие покрытия",      "10€", "15 мин",  15,  "images/7.jpg", 7),
    ("Ремонт одного ногтя",  "2€",  "15 мин",  15,  "images/8.jpg", 8),
]

def _seed_services():
    con = db_connect()
    if con.execute("SELECT COUNT(*) FROM services").fetchone()[0] == 0:
        con.executemany(
            "INSERT OR IGNORE INTO services (name,price,duration,duration_min,img,sort_order) VALUES (?,?,?,?,?,?)",
            _DEFAULT_SERVICES
        )
        con.commit()
    con.close()

_seed_services()

def get_services_db():
    con = db_connect()
    rows = con.execute(
        "SELECT id,name,price,duration,duration_min,img FROM services WHERE active=1 ORDER BY sort_order,id"
    ).fetchall(); con.close()
    return [{"id":r[0],"name":r[1],"price":r[2],"duration":r[3],"duration_min":r[4],"img":r[5]} for r in rows]

def get_all_services_db():
    con = db_connect()
    rows = con.execute(
        "SELECT id,name,price,duration,duration_min,img,active FROM services ORDER BY sort_order,id"
    ).fetchall(); con.close()
    return [{"id":r[0],"name":r[1],"price":r[2],"duration":r[3],"duration_min":r[4],"img":r[5],"active":r[6]} for r in rows]

def get_service(name):
    con = db_connect()
    row = con.execute(
        "SELECT id,name,price,duration,duration_min,img,description FROM services WHERE name=? AND active=1", (name,)
    ).fetchone(); con.close()
    if not row: return None
    return {"id":row[0],"name":row[1],"price":row[2],"duration":row[3],"duration_min":row[4],"img":row[5],"description":row[6] if len(row)>6 else ""}

def get_service_by_id(sid):
    con = db_connect()
    row = con.execute(
        "SELECT id,name,price,duration,duration_min,img,active,description FROM services WHERE id=?", (sid,)
    ).fetchone(); con.close()
    if not row: return None
    return {"id":row[0],"name":row[1],"price":row[2],"duration":row[3],"duration_min":row[4],"img":row[5],"active":row[6],"description":row[7] if len(row)>7 else ""}

def add_service_db(name, price, duration, duration_min, img):
    try:
        con = db_connect()
        max_o = con.execute("SELECT MAX(sort_order) FROM services").fetchone()[0] or 0
        con.execute(
            "INSERT INTO services (name,price,duration,duration_min,img,sort_order,active) VALUES (?,?,?,?,?,?,1)",
            (name, price, duration, duration_min, img, max_o+1)
        ); con.commit(); con.close(); return True
    except: return False

def update_service_db(sid, field, value):
    if field not in {"name","price","duration","duration_min","img","active","description"}: return
    con = db_connect()
    con.execute(f"UPDATE services SET {field}=? WHERE id=?", (value, sid))
    con.commit(); con.close()

def deactivate_service_db(sid): update_service_db(sid, "active", 0)
def restore_service_db(sid):    update_service_db(sid, "active", 1)

def delete_service_db(sid):
    con = db_connect()
    con.execute("DELETE FROM services WHERE id=?", (sid,))
    con.commit(); con.close()

def add_booking(user_id, service, year, month, day, time, name, phone):
    con = db_connect(); cur = con.cursor()
    cur.execute(
        "INSERT INTO bookings (user_id,service,year,month,day,time,name,phone) VALUES (?,?,?,?,?,?,?,?)",
        (user_id,service,year,month,day,time,name,phone)
    ); bid = cur.lastrowid; con.commit(); con.close(); return bid

def remove_booking(bid):
    con = db_connect(); con.execute("DELETE FROM bookings WHERE id=?", (bid,)); con.commit(); con.close()

def get_booking(bid):
    con = db_connect(); row = con.execute("SELECT * FROM bookings WHERE id=?", (bid,)).fetchone(); con.close()
    return _row_to_booking(row) if row else None

def get_user_bookings(user_id):
    con = db_connect()
    rows = con.execute("SELECT * FROM bookings WHERE user_id=? ORDER BY year,month,day,time", (user_id,)).fetchall()
    con.close(); return [_row_to_booking(r) for r in rows]

def get_all_bookings():
    con = db_connect()
    rows = con.execute("SELECT * FROM bookings ORDER BY year,month,day,time").fetchall()
    con.close(); return [_row_to_booking(r) for r in rows]

def update_booking_field(bid, field, value):
    if field not in {"service","year","month","day","time","name","phone"}: return
    con = db_connect(); con.execute(f"UPDATE bookings SET {field}=? WHERE id=?", (value,bid)); con.commit(); con.close()

def _row_to_booking(row):
    return {"id":row[0],"user_id":row[1],"service":row[2],"year":row[3],"month":row[4],
            "day":row[5],"time":row[6],"name":row[7],"phone":row[8],
            "reminded_24":row[9],"reminded_2":row[10],"review_sent":row[11] if len(row)>11 else 0}

def log_cancellation(b, cancelled_by):
    con = db_connect()
    con.execute(
        "INSERT INTO cancelled (user_id,service,year,month,day,time,name,phone,cancelled_by,cancelled_at) VALUES (?,?,?,?,?,?,?,?,?,?)",
        (b["user_id"],b["service"],b["year"],b["month"],b["day"],b["time"],b["name"],b["phone"],
         cancelled_by, datetime.now().strftime("%Y-%m-%d %H:%M"))
    ); con.commit(); con.close()

def log_transfer(bid, user_id, service, old_date, old_time, new_date, new_time):
    con = db_connect()
    con.execute(
        "INSERT INTO transfers (booking_id,user_id,service,old_date,old_time,new_date,new_time,transferred_at) VALUES (?,?,?,?,?,?,?,?)",
        (bid,user_id,service,old_date,old_time,new_date,new_time,datetime.now().strftime("%Y-%m-%d %H:%M"))
    ); con.commit(); con.close()

def add_review(booking_id, user_id, service, rating, text, username=""):
    con = db_connect()
    cur = con.execute(
        "INSERT INTO reviews (booking_id,user_id,service,rating,text,created_at,username) VALUES (?,?,?,?,?,?,?)",
        (booking_id,user_id,service,rating,text,datetime.now().strftime("%Y-%m-%d %H:%M"),username)
    ); rid = cur.lastrowid; con.commit(); con.close(); return rid

def get_service_price_int(service_name):
    svc = get_service(service_name)
    if not svc: return 0
    m = re.search(r"(\d+)", svc["price"]); return int(m.group(1)) if m else 0

def get_stats():
    con = db_connect(); s = {}
    s["total_active"]    = con.execute("SELECT COUNT(*) FROM bookings").fetchone()[0]
    s["total_cancelled"] = con.execute("SELECT COUNT(*) FROM cancelled").fetchone()[0]
    s["total_transfers"] = con.execute("SELECT COUNT(*) FROM transfers").fetchone()[0]
    s["total_reviews"]   = con.execute("SELECT COUNT(*) FROM reviews").fetchone()[0]
    avg = con.execute("SELECT AVG(rating) FROM reviews").fetchone()[0]
    s["avg_rating"] = round(avg,1) if avg else 0
    rows = con.execute("SELECT service,COUNT(*) FROM bookings GROUP BY service ORDER BY COUNT(*) DESC").fetchall()
    s["by_service"] = rows
    s["total_revenue"] = sum(get_service_price_int(r[0]) for r in con.execute("SELECT service FROM bookings").fetchall())
    s["revenue_by_service"] = {svc:get_service_price_int(svc)*cnt for svc,cnt in rows}
    now = datetime.now()
    s["month_revenue"] = sum(get_service_price_int(r[0]) for r in con.execute(
        "SELECT service FROM bookings WHERE year=? AND month=?", (now.year,now.month)).fetchall())
    s["cancelled_by_client"] = con.execute("SELECT COUNT(*) FROM cancelled WHERE cancelled_by='client'").fetchone()[0]
    s["cancelled_by_master"] = con.execute("SELECT COUNT(*) FROM cancelled WHERE cancelled_by='master'").fetchone()[0]
    s["last_reviews"] = con.execute("SELECT rating,text,service,created_at FROM reviews ORDER BY id DESC LIMIT 5").fetchall()
    con.close(); return s

def get_client_visits(user_id):
    con = db_connect()
    n = con.execute("SELECT COUNT(*) FROM bookings WHERE user_id=?", (user_id,)).fetchone()[0]
    con.close(); return n

def get_client_rank(visits):
    if visits >= 7: return "💎 VIP"
    elif visits >= 3: return "⭐ Постоянный"
    else: return "🆕 Новичок"

def block_day(date):
    con = db_connect(); con.execute("INSERT OR IGNORE INTO schedule (type,date) VALUES ('day',?)", (date,)); con.commit(); con.close()

def unblock_day(date):
    con = db_connect(); con.execute("DELETE FROM schedule WHERE type='day' AND date=?", (date,)); con.commit(); con.close()

def block_slot(date, time):
    con = db_connect(); con.execute("INSERT OR IGNORE INTO schedule (type,date,time) VALUES ('slot',?,?)", (date,time)); con.commit(); con.close()

def unblock_slot(date, time):
    con = db_connect(); con.execute("DELETE FROM schedule WHERE type='slot' AND date=? AND time=?", (date,time)); con.commit(); con.close()

def get_blocked_days():
    con = db_connect(); rows = con.execute("SELECT date FROM schedule WHERE type='day'").fetchall(); con.close(); return [r[0] for r in rows]

def get_blocked_slots_for_date(date):
    con = db_connect(); rows = con.execute("SELECT time FROM schedule WHERE type='slot' AND date=?", (date,)).fetchall(); con.close(); return [r[0] for r in rows]

def get_all_blocked_slots():
    con = db_connect(); rows = con.execute("SELECT date,time FROM schedule WHERE type='slot' ORDER BY date,time").fetchall(); con.close()
    result = {}
    for date,time in rows: result.setdefault(date,[]).append(time)
    return result

CONTACTS_FULL  = "📞 +372 56 602 890\n💬 Telegram: @Vi_da_ch_iV\n📬 Dariashabelna@gmail.com"
CONTACTS_SHORT = "📞 +372 56 602 890\n💬 Telegram: @Vi_da_ch_iV"

MONTHS = {1:"Январь",2:"Февраль",3:"Март",4:"Апрель",5:"Май",6:"Июнь",
          7:"Июль",8:"Август",9:"Сентябрь",10:"Октябрь",11:"Ноябрь",12:"Декабрь"}
MONTHS_GEN = {1:"Января",2:"Февраля",3:"Марта",4:"Апреля",5:"Мая",6:"Июня",
              7:"Июля",8:"Августа",9:"Сентября",10:"Октября",11:"Ноября",12:"Декабря"}
TIME_SLOTS = ["09:00","10:00","11:00","12:00","13:00","14:00","15:00","16:00","17:00","18:00"]

class Booking(StatesGroup):
    service=State(); year=State(); month=State(); day=State(); time=State(); name=State(); phone=State()

class EditBooking(StatesGroup):
    service=State(); year=State(); month=State(); day=State(); time=State(); name=State(); phone=State()

class Reschedule(StatesGroup):
    year=State(); month=State(); day=State(); time=State()

class Review(StatesGroup):
    rating=State(); text=State()

class TipState(StatesGroup):
    amount = State()

class AdminReview(StatesGroup):
    text=State(); add_text=State(); add_rating=State(); add_service=State()

class AddService(StatesGroup):
    name=State(); price=State(); duration=State(); img=State()

class EditService(StatesGroup):
    value=State(); img=State()

def date_key(year, month, day): return f"{year}-{month:02d}-{day:02d}"
def is_day_blocked(year, month, day): return date_key(year,month,day) in get_blocked_days()

def duration_minutes(svc):
    if not svc: return 60
    if svc.get("duration_min"): return svc["duration_min"]
    m = re.search(r"(\d+)", svc.get("duration","60")); return int(m.group(1)) if m else 60

def get_end_time(start_slot, dur_min):
    h,m = int(start_slot.split(":")[0]), int(start_slot.split(":")[1])
    total = h*60+m+dur_min; return f"{total//60:02d}:{total%60:02d}"

def get_available_slots(year, month, day, new_dur_min=60, exclude_bid=None):
    now = datetime.now()
    key = date_key(year,month,day)
    manual_blocked = set()
    for slot in get_blocked_slots_for_date(key):
        h,m = int(slot.split(":")[0]), int(slot.split(":")[1]); manual_blocked.add(h*60+m)
    booked = []
    for b in get_all_bookings():
        if exclude_bid and b["id"]==exclude_bid: continue
        if b["year"]!=year or b["month"]!=month or b["day"]!=day: continue
        svc = get_service(b["service"]); dur = duration_minutes(svc)
        h,m = int(b["time"].split(":")[0]), int(b["time"].split(":")[1])
        booked.append((h*60+m, h*60+m+dur))
    def is_free(start, dur):
        if start > 18*60: return False
        for bs,be in booked:
            if start < be and start+dur > bs: return False
        if start in manual_blocked: return False
        return True
    candidates = list(range(9*60, 18*60+1, 60))
    for _,be in booked:
        if be%60==30 and 9*60<=be<=18*60: candidates.append(be)
    if year==now.year and month==now.month and day==now.day:
        cutoff = now.hour*60+now.minute
        candidates = [c for c in candidates if c > cutoff]
    result = sorted(set(c for c in candidates if is_free(c, new_dur_min)))
    return [f"{c//60:02d}:{c%60:02d}" for c in result]

def format_booking(b, idx=None, username=None):
    month_name = MONTHS_GEN[b["month"]]
    prefix = f"Бронь №{idx}\n" if idx else ""
    svc = get_service(b["service"]); dur_str = svc["duration"] if svc else ""
    tg_line = f" | 💬 @{username}" if username else ""
    addr = "\n\n😊 Я жду вас по адресу:\n🏠 Linnamäe tee 83-66" if username is None else ""
    return f"{prefix}💅 {b['service']}\n⏱ Длительность: ~{dur_str}\n🕐 {b['time']} | {b['day']} {month_name}\n👤 {b['name']} 📞 {b['phone']}{tg_line}{addr}".strip()

def bottom_kb(is_admin=False):
    row1 = [KeyboardButton(text="💅 Услуги"), KeyboardButton(text="📋 Мои брони"), KeyboardButton(text="⭐ Отзывы")]
    row2 = [KeyboardButton(text="💝 Чаевые"), KeyboardButton(text="👭 Друзья"), KeyboardButton(text="📞 Контакты")]
    buttons = [row1, row2]
    if is_admin: buttons.append([KeyboardButton(text="🔐 Админка")])
    return ReplyKeyboardMarkup(keyboard=buttons, resize_keyboard=True)

def main_menu_kb():
    rows = []
    for s in get_services_db():
        rows.append([InlineKeyboardButton(text=f"{s['name']} — {s['price']} (~{s['duration']})", callback_data=f"svc:{s['name']}")])
    return InlineKeyboardMarkup(inline_keyboard=rows)

def months_kb(year):
    now = datetime.now(); rows=[]; row=[]; shown=0
    for num,name in MONTHS.items():
        if num < now.month: continue
        if shown >= 3: break
        row.append(InlineKeyboardButton(text=name, callback_data=f"month:{num}")); shown+=1
        if len(row)==3: rows.append(row); row=[]
    if row: rows.append(row)
    rows.append([InlineKeyboardButton(text="🏠 Главное меню", callback_data="main_menu")])
    return InlineKeyboardMarkup(inline_keyboard=rows)

def days_kb(year, month, new_dur_min=60):
    now = datetime.now(); _,days_in_month = calendar.monthrange(year,month); rows=[]; row=[]
    for day in range(1, days_in_month+1):
        if year==now.year and month==now.month and day<now.day: continue
        if is_day_blocked(year,month,day): continue
        if not get_available_slots(year,month,day,new_dur_min): continue
        row.append(InlineKeyboardButton(text=str(day), callback_data=f"day:{day}"))
        if len(row)==7: rows.append(row); row=[]
    if row: rows.append(row)
    if not rows: rows.append([InlineKeyboardButton(text="Нет доступных дней", callback_data="noop")])
    rows.append([InlineKeyboardButton(text="◀️ Назад", callback_data="back_to_months"),
                 InlineKeyboardButton(text="🏠 Главное меню", callback_data="main_menu")])
    return InlineKeyboardMarkup(inline_keyboard=rows)

def time_kb(year, month, day, exclude_bid=None, new_dur_min=60):
    available = get_available_slots(year,month,day,new_dur_min,exclude_bid)
    rows=[]; row=[]
    for slot in available:
        row.append(InlineKeyboardButton(text=slot, callback_data="t_"+slot.replace(":","")))
        if len(row)==3: rows.append(row); row=[]
    if row: rows.append(row)
    if not rows: rows.append([InlineKeyboardButton(text="Нет свободного времени", callback_data="noop")])
    rows.append([InlineKeyboardButton(text="◀️ Назад", callback_data="back_to_days"),
                 InlineKeyboardButton(text="🏠 Главное меню", callback_data="main_menu")])
    return InlineKeyboardMarkup(inline_keyboard=rows)

def back_to_menu_kb():
    return InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🏠 Главное меню", callback_data="main_menu")]])

def booking_list_kb(user_id):
    rows = []
    for b in get_user_bookings(user_id):
        label = f"{b['time']} | {b['day']} {MONTHS_GEN[b['month']]} | {b['service'][:20]}"
        rows.append([InlineKeyboardButton(text=label, callback_data=f"viewb:{b['id']}")])
    rows.append([InlineKeyboardButton(text="🗑 Удалить все брони", callback_data="del_all_confirm")])
    rows.append([InlineKeyboardButton(text="🏠 Главное меню", callback_data="main_menu")])
    return InlineKeyboardMarkup(inline_keyboard=rows)

def booking_actions_kb(bid):
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔄 Перенести", callback_data=f"reschedule:{bid}"),
         InlineKeyboardButton(text="🗑 Удалить",   callback_data=f"del_booking:{bid}")],
        [InlineKeyboardButton(text="◀️ Все брони", callback_data="my_booking"),
         InlineKeyboardButton(text="🏠 Главное меню", callback_data="main_menu")]])

def confirm_delete_kb(bid):
    return InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text="✅ Да, удалить", callback_data=f"confirm_del:{bid}"),
        InlineKeyboardButton(text="❌ Отмена",       callback_data=f"viewb:{bid}")]])

def edit_options_kb(bid):
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="💅 Изменить услугу",      callback_data=f"efield:service:{bid}")],
        [InlineKeyboardButton(text="📅 Изменить дату",        callback_data=f"efield:date:{bid}")],
        [InlineKeyboardButton(text="⏱ Изменить время",       callback_data=f"efield:time:{bid}")],
        [InlineKeyboardButton(text="👤 Изменить имя",         callback_data=f"efield:name:{bid}")],
        [InlineKeyboardButton(text="📞 Изменить телефон",     callback_data=f"efield:phone:{bid}")],
        [InlineKeyboardButton(text="◀️ Назад",                callback_data=f"viewb:{bid}")]])

def services_edit_kb(bid):
    rows = []
    for s in get_services_db():
        rows.append([InlineKeyboardButton(text=f"{s['name']} — {s['price']} (~{s['duration']})", callback_data=f"esvc:{bid}:{s['name']}")])
    rows.append([InlineKeyboardButton(text="◀️ Назад", callback_data=f"edit_booking:{bid}")])
    return InlineKeyboardMarkup(inline_keyboard=rows)

def admin_panel_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📋 Все брони",           callback_data="admin_all")],
        [InlineKeyboardButton(text="📅 Брони на сегодня",    callback_data="admin_today")],
        [InlineKeyboardButton(text="🔜 Брони на завтра",     callback_data="admin_tomorrow")],
        [InlineKeyboardButton(text="🗓 Моё расписание",      callback_data="admin_schedule")],
        [InlineKeyboardButton(text="📊 Статистика",          callback_data="admin_stats")],
        [InlineKeyboardButton(text="💅 Управление услугами", callback_data="admin_services")],
        [InlineKeyboardButton(text="⭐ Управление отзывами", callback_data="admin_reviews")]])

def admin_services_kb():
    rows = []
    for s in get_all_services_db():
        icon = "✅" if s["active"] else "❌"
        rows.append([InlineKeyboardButton(text=f"{icon} {s['name']} — {s['price']} ({s['duration']})", callback_data=f"svc_manage:{s['id']}")])
    rows.append([InlineKeyboardButton(text="➕ Добавить услугу", callback_data="svc_add")])
    rows.append([InlineKeyboardButton(text="◀️ Назад",           callback_data="admin_back")])
    return InlineKeyboardMarkup(inline_keyboard=rows)

def svc_manage_kb(sid, active):
    toggle_text = "❌ Скрыть от клиентов" if active else "✅ Показать клиентам"
    toggle_cb   = f"svc_hide:{sid}" if active else f"svc_show:{sid}"
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✏️ Изменить название",    callback_data=f"svc_edit:name:{sid}")],
        [InlineKeyboardButton(text="💰 Изменить цену",        callback_data=f"svc_edit:price:{sid}")],
        [InlineKeyboardButton(text="⏱ Изменить длительность", callback_data=f"svc_edit:duration:{sid}")],
        [InlineKeyboardButton(text="📝 Изменить описание",    callback_data=f"svc_edit:description:{sid}")],
        [InlineKeyboardButton(text="🖼 Изменить картинку",    callback_data=f"svc_edit:img:{sid}")],
        [InlineKeyboardButton(text=toggle_text,               callback_data=toggle_cb)],
        [InlineKeyboardButton(text="🗑 Удалить услугу",         callback_data=f"svc_delete_confirm:{sid}")],
        [InlineKeyboardButton(text="◀️ Назад",                callback_data="admin_services")]])

def schedule_main_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🚫 Заблокировать день",   callback_data="sched_block_day")],
        [InlineKeyboardButton(text="✅ Разблокировать день",  callback_data="sched_unblock_day")],
        [InlineKeyboardButton(text="🚫 Заблокировать часы",   callback_data="sched_block_slots")],
        [InlineKeyboardButton(text="✅ Разблокировать часы",  callback_data="sched_unblock_slots")],
        [InlineKeyboardButton(text="📋 Показать расписание",  callback_data="sched_show")],
        [InlineKeyboardButton(text="🔓 Разблокировать всё",   callback_data="sched_unblock_all_confirm")],
        [InlineKeyboardButton(text="◀️ Назад",                callback_data="admin_back")]])

def schedule_months_kb(action):
    now=datetime.now(); rows=[]; row=[]; shown=0
    for num,name in MONTHS.items():
        if num<now.month: continue
        if shown>=3: break
        row.append(InlineKeyboardButton(text=name, callback_data=f"sm_{action}:{num}")); shown+=1
        if len(row)==3: rows.append(row); row=[]
    if row: rows.append(row)
    rows.append([InlineKeyboardButton(text="◀️ Назад", callback_data="admin_schedule")])
    return InlineKeyboardMarkup(inline_keyboard=rows)

def schedule_days_kb(month, action):
    now=datetime.now(); _,dim=calendar.monthrange(now.year,month); rows=[]; row=[]
    for day in range(1,dim+1):
        if month==now.month and day<now.day: continue
        key=date_key(now.year,month,day)
        label=f"🚫{day}" if key in get_blocked_days() else str(day)
        row.append(InlineKeyboardButton(text=label, callback_data=f"sd_{action}:{month}:{day}"))
        if len(row)==7: rows.append(row); row=[]
    if row: rows.append(row)
    rows.append([InlineKeyboardButton(text="◀️ Назад", callback_data="admin_schedule")])
    return InlineKeyboardMarkup(inline_keyboard=rows)

def schedule_slots_kb(month, day, action):
    year=datetime.now().year; key=date_key(year,month,day)
    blocked_manual=get_blocked_slots_for_date(key)
    booked_slots=set()
    for b in get_all_bookings():
        if b["year"]==year and b["month"]==month and b["day"]==day:
            svc=get_service(b["service"]); dur=duration_minutes(svc)
            h,m=int(b["time"].split(":")[0]),int(b["time"].split(":")[1])
            start=h*60+m
            for minute in range(start,start+dur,60):
                slot=f"{minute//60:02d}:00"
                if slot in TIME_SLOTS: booked_slots.add(slot)
    rows=[]; row=[]
    for slot in TIME_SLOTS:
        if slot in blocked_manual: label=f"🔒{slot}"
        elif slot in booked_slots: label=f"👤{slot}"
        else: label=slot
        row.append(InlineKeyboardButton(text=label, callback_data=f"ss_{action}:{month}:{day}:{slot.replace(':','')}"))
        if len(row)==3: rows.append(row); row=[]
    if row: rows.append(row)
    rows.append([InlineKeyboardButton(text="◀️ Назад", callback_data="admin_schedule")])
    return InlineKeyboardMarkup(inline_keyboard=rows)

@dp.message(Command("start"))
async def cmd_start(message: types.Message, state: FSMContext):
    await state.clear()
    is_admin = message.from_user.id in ADMIN_IDS
    photo = FSInputFile("images/darja.png")
    await message.answer_photo(photo=photo, caption="👋 Привет! Я помощник по записи\nк мастеру маникюра Дарье 💅", reply_markup=bottom_kb(is_admin))
    await message.answer("💅 Выберите услугу:", reply_markup=main_menu_kb())

@dp.message(F.text == "💅 Услуги")
async def btn_services(message: types.Message, state: FSMContext):
    await state.clear(); await message.answer("Выберите услугу 👇", reply_markup=main_menu_kb())

@dp.message(F.text == "📋 Мои брони")
async def btn_my_bookings(message: types.Message, state: FSMContext):
    await state.clear()
    if not get_user_bookings(message.from_user.id):
        await message.answer("У вас нет активных броней."); return
    await message.answer("📋 Ваши брони:", reply_markup=booking_list_kb(message.from_user.id))

@dp.message(F.text == "⭐ Отзывы")
async def btn_reviews(message: types.Message):
    con = db_connect()
    rows = con.execute(
        "SELECT rating, text, service, created_at, username FROM reviews ORDER BY id DESC LIMIT 20"
    ).fetchall()
    con.close()
    if not rows:
        await message.answer("😊 Отзывов пока нет — будьте первым!")
        return
    text = "⭐ Отзывы клиентов:\n\n"
    for row in rows:
        rating, rv, svc_name, created_at = row[0], row[1], row[2], row[3]
        username = row[4] if len(row) > 4 and row[4] else "Аноним"
        stars = "⭐" * rating
        date_str = created_at[:10] if created_at else ""
        text += f"{stars} — {svc_name} ({date_str})\n"
        text += f"👤 {username}\n"
        if rv:
            text += f"💬 {rv}"
        text += "\n\n"
    await message.answer(text.strip())

@dp.message(F.text == "📞 Контакты")
async def btn_contacts(message: types.Message):
    await message.answer("📇 Контакты мастера:\n\n" + CONTACTS_FULL)

@dp.message(F.text == "👭 Друзья")
async def btn_partners(message: types.Message):
    await message.answer(
        "👭 Партнёры Дарьи\n\n"
        "Здесь скоро появятся наши проверенные партнёры — мастера и салоны, которым мы доверяем 💅\n\n"
        "Следите за обновлениями!"
    )

@dp.message(F.text == "💝 Чаевые")
async def btn_tips(message: types.Message, state: FSMContext):
    await message.answer("🚧 В разработке!")

@dp.callback_query(F.data == "tip_cancel")
async def tip_cancel(call: types.CallbackQuery, state: FSMContext):
    await state.clear()
    await call.message.answer("Отменено.")
    await call.answer()

@dp.message(TipState.amount)
async def tip_amount(message: types.Message, state: FSMContext):
    await state.clear()
    if not message.text or not message.text.strip().isdigit():
        await message.answer("⚠️ Введите целое число, например: 10")
        await state.set_state(TipState.amount)
        return
    amount = int(message.text.strip())
    if amount < 1:
        await message.answer("⚠️ Минимальная сумма — 1 ⭐")
        await state.set_state(TipState.amount)
        return
    await state.clear()
    await message.answer_invoice(
        title="💝 Чаевые мастеру",
        description=f"Спасибо за визит! Вы отправляете {amount} ⭐ Дарье",
        payload="tip",
        currency="XTR",
        prices=[{"label": "Чаевые", "amount": amount}]
    )

@dp.pre_checkout_query()
async def pre_checkout(query: types.PreCheckoutQuery):
    await query.answer(ok=True)

@dp.message(F.successful_payment)
async def successful_payment(message: types.Message):
    stars = message.successful_payment.total_amount
    await message.answer(f"🙏 Спасибо за {stars} ⭐! Дарья очень рада 💅")
    for _aid in ADMIN_IDS:
        try:
            await bot.send_message(_aid, f"💝 Новые чаевые! {stars} ⭐ от @{message.from_user.username or message.from_user.first_name}")
        except: pass

@dp.message(F.text == "🔐 Админка")
async def btn_admin_panel(message: types.Message):
    if message.from_user.id not in ADMIN_IDS: await message.answer("⛔️ Нет доступа."); return
    await message.answer(f"🔐 Панель администратора\nВсего броней: {len(get_all_bookings())}", reply_markup=admin_panel_kb())

@dp.callback_query(F.data == "main_menu")
async def go_main_menu(call: types.CallbackQuery, state: FSMContext):
    await state.clear(); await call.message.answer("Выберите услугу 👇", reply_markup=main_menu_kb()); await call.answer()

@dp.callback_query(F.data == "my_booking")
async def show_my_bookings(call: types.CallbackQuery, state: FSMContext):
    await state.clear()
    if not get_user_bookings(call.from_user.id):
        await call.answer("У вас нет активных броней.", show_alert=True); return
    await call.message.answer("📋 Ваши брони:", reply_markup=booking_list_kb(call.from_user.id)); await call.answer()

@dp.callback_query(F.data.startswith("viewb:"))
async def view_booking(call: types.CallbackQuery):
    bid=int(call.data.split(":")[1]); b=get_booking(bid)
    if not b or b["user_id"]!=call.from_user.id:
        await call.answer("Бронь не найдена.", show_alert=True); return
    bookings=get_user_bookings(call.from_user.id)
    idx=next((i+1 for i,x in enumerate(bookings) if x["id"]==bid),1)
    await call.message.answer(format_booking(b,idx), reply_markup=booking_actions_kb(bid)); await call.answer()

@dp.callback_query(F.data.startswith("del_booking:"))
async def delete_booking_confirm(call: types.CallbackQuery):
    bid=int(call.data.split(":")[1])
    await call.message.answer("❗️ Вы уверены, что хотите удалить эту бронь?", reply_markup=confirm_delete_kb(bid)); await call.answer()

@dp.callback_query(F.data.startswith("confirm_del:"))
async def confirm_delete(call: types.CallbackQuery):
    bid=int(call.data.split(":")[1]); b=get_booking(bid)
    if b and b["user_id"]==call.from_user.id:
        log_cancellation(b,"client"); remove_booking(bid)
        for _aid in ADMIN_IDS:
            try: await bot.send_message(_aid, f"🗑 Клиент отменил бронь!\n\n{format_booking(b)}")
            except: pass
    await call.message.answer("✅ Бронь удалена.", reply_markup=back_to_menu_kb()); await call.answer()

@dp.callback_query(F.data == "del_all_confirm")
async def del_all_confirm(call: types.CallbackQuery):
    kb=InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text="✅ Да, удалить все", callback_data="del_all_yes"),
        InlineKeyboardButton(text="❌ Отмена", callback_data="my_booking")]])
    await call.message.answer("❗️ Все ваши брони будут удалены!", reply_markup=kb); await call.answer()

@dp.callback_query(F.data == "del_all_yes")
async def del_all_yes(call: types.CallbackQuery):
    for b in get_user_bookings(call.from_user.id):
        remove_booking(b["id"])
        for _aid in ADMIN_IDS:
            try: await bot.send_message(_aid, f"🗑 Клиент удалил все брони!\n\n{format_booking(b)}")
            except: pass
    await call.message.answer("✅ Все брони удалены.", reply_markup=back_to_menu_kb()); await call.answer()

@dp.callback_query(F.data.startswith("edit_booking:"))
async def edit_booking_menu(call: types.CallbackQuery):
    bid=int(call.data.split(":")[1])
    await call.message.answer("Что хотите изменить?", reply_markup=edit_options_kb(bid)); await call.answer()

@dp.callback_query(F.data.startswith("efield:"))
async def edit_field_start(call: types.CallbackQuery, state: FSMContext):
    parts=call.data.split(":",2); field,bid=parts[1],int(parts[2])
    await state.update_data(edit_bid=bid)
    cancel_kb=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="❌ Отмена", callback_data=f"viewb:{bid}")]])
    if field=="service":
        await call.message.answer("Выберите новую услугу:", reply_markup=services_edit_kb(bid)); await state.set_state(EditBooking.service)
    elif field=="date":
        await call.message.answer("Выберите год:", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
            InlineKeyboardButton(text=str(datetime.now().year), callback_data=f"year:{datetime.now().year}"),
            InlineKeyboardButton(text=str(datetime.now().year+1), callback_data=f"year:{datetime.now().year+1}")]])); await state.set_state(EditBooking.year)
    elif field=="time":
        b=get_booking(bid)
        if b:
            svc=get_service(b["service"]); dur=duration_minutes(svc)
            await call.message.answer("Выберите новое время:", reply_markup=time_kb(b["year"],b["month"],b["day"],exclude_bid=bid,new_dur_min=dur))
        await state.set_state(EditBooking.time)
    elif field=="name":
        await call.message.answer("Введите новое имя:", reply_markup=cancel_kb); await state.set_state(EditBooking.name)
    elif field=="phone":
        await call.message.answer("Введите новый телефон:", reply_markup=cancel_kb); await state.set_state(EditBooking.phone)
    await call.answer()

async def notify_edit(bid):
    b=get_booking(bid)
    if not b: return
    bookings=get_user_bookings(b["user_id"]); idx=next((i+1 for i,x in enumerate(bookings) if x["id"]==bid),1)
    try: await bot.send_message(b["user_id"], f"✅ Ваша бронь обновлена!\n\n{format_booking(b,idx)}", reply_markup=booking_actions_kb(bid))
    except: pass
    for _aid in ADMIN_IDS:
        try: await bot.send_message(_aid, f"✏️ Клиент изменил бронь!\n\n{format_booking(b)}")
        except: pass

@dp.callback_query(F.data.startswith("esvc:"), EditBooking.service)
async def edit_service_save(call: types.CallbackQuery, state: FSMContext):
    _,bid_str,service_name=call.data.split(":",2); bid=int(bid_str)
    update_booking_field(bid,"service",service_name); await state.clear(); await notify_edit(bid); await call.answer()

@dp.callback_query(F.data.startswith("year:"), EditBooking.year)
async def edit_year_save(call: types.CallbackQuery, state: FSMContext):
    year=int(call.data.split(":")[1]); await state.update_data(edit_year=year)
    await call.message.answer(f"Выберите месяц ({year}):", reply_markup=months_kb(year)); await state.set_state(EditBooking.month); await call.answer()

@dp.callback_query(F.data.startswith("month:"), EditBooking.month)
async def edit_month_save(call: types.CallbackQuery, state: FSMContext):
    month=int(call.data.split(":")[1]); data=await state.get_data(); year=data.get("edit_year",datetime.now().year)
    await state.update_data(edit_month=month)
    await call.message.answer("Выберите день:", reply_markup=days_kb(year,month)); await state.set_state(EditBooking.day); await call.answer()

@dp.callback_query(F.data.startswith("day:"), EditBooking.day)
async def edit_day_save(call: types.CallbackQuery, state: FSMContext):
    day=int(call.data.split(":")[1]); data=await state.get_data(); bid=data["edit_bid"]
    year=data.get("edit_year",datetime.now().year); month=data.get("edit_month")
    if month:
        update_booking_field(bid,"year",year); update_booking_field(bid,"month",month); update_booking_field(bid,"day",day)
    await state.clear(); await notify_edit(bid); await call.answer()

@dp.callback_query(F.data.startswith("t_"), EditBooking.time)
async def edit_time_save(call: types.CallbackQuery, state: FSMContext):
    raw=call.data[2:]; time_str=raw[:2]+":"+raw[2:]; data=await state.get_data(); bid=data["edit_bid"]
    update_booking_field(bid,"time",time_str); await state.clear(); await notify_edit(bid); await call.answer()

@dp.message(EditBooking.name)
async def edit_name_save(message: types.Message, state: FSMContext):
    data=await state.get_data(); update_booking_field(data["edit_bid"],"name",message.text); await state.clear(); await notify_edit(data["edit_bid"])

@dp.message(EditBooking.phone)
async def edit_phone_save(message: types.Message, state: FSMContext):
    data=await state.get_data(); update_booking_field(data["edit_bid"],"phone",message.text); await state.clear(); await notify_edit(data["edit_bid"])

@dp.callback_query(F.data.startswith("svc:"))
async def service_choice(call: types.CallbackQuery, state: FSMContext):
    service_name=call.data[4:]; svc=get_service(service_name)
    if not svc: await call.answer("Услуга не найдена.", show_alert=True); return
    year=datetime.now().year; await state.update_data(service=service_name,year=year)
    photo=FSInputFile(svc["img"])
    desc_line = f"\n\n📝 {svc['description']}" if svc.get("description") else ""
    await call.message.answer_photo(photo=photo,
        caption=f"✅ Вы выбрали: {service_name}\n💰 {svc['price']} | ⏱ {svc['duration']}{desc_line}\n\nВыберите месяц:",
        reply_markup=months_kb(year))
    await state.set_state(Booking.month); await call.answer()

@dp.callback_query(F.data == "back_to_months")
async def back_to_months(call: types.CallbackQuery, state: FSMContext):
    data=await state.get_data(); year=data.get("year",datetime.now().year)
    await call.message.answer("Выберите месяц:", reply_markup=months_kb(year)); await state.set_state(Booking.month); await call.answer()

@dp.callback_query(F.data.startswith("month:"), Booking.month)
async def month_choice(call: types.CallbackQuery, state: FSMContext):
    month=int(call.data.split(":")[1]); data=await state.get_data(); year=data.get("year",datetime.now().year)
    await state.update_data(month=month); svc=get_service(data.get("service","")); dur=duration_minutes(svc)
    await call.message.answer(f"Выберите день ({MONTHS[month]}):", reply_markup=days_kb(year,month,dur))
    await state.set_state(Booking.day); await call.answer()

@dp.callback_query(F.data == "back_to_days")
async def back_to_days(call: types.CallbackQuery, state: FSMContext):
    data=await state.get_data(); year=data.get("year",datetime.now().year); month=data.get("month")
    if month:
        await call.message.answer(f"Выберите день ({MONTHS[month]}):", reply_markup=days_kb(year,month)); await state.set_state(Booking.day)
    else:
        await call.message.answer("Выберите месяц:", reply_markup=months_kb(year)); await state.set_state(Booking.month)
    await call.answer()

@dp.callback_query(F.data.startswith("day:"), Booking.day)
async def day_choice(call: types.CallbackQuery, state: FSMContext):
    day=int(call.data.split(":")[1]); data=await state.get_data()
    year=data.get("year",datetime.now().year); month=data.get("month")
    await state.update_data(day=day); svc=get_service(data.get("service","")); dur=duration_minutes(svc)
    await call.message.answer(f"✅ Вы выбрали: {day} {MONTHS[month]}\n\nВыберите удобное время:",
        reply_markup=time_kb(year,month,day,new_dur_min=dur))
    await state.set_state(Booking.time); await call.answer()

@dp.callback_query(F.data.startswith("t_"), Booking.time)
async def time_choice(call: types.CallbackQuery, state: FSMContext):
    raw=call.data[2:]; time_str=raw[:2]+":"+raw[2:]
    await state.update_data(time=time_str)
    await call.message.answer(f"✅ Вы выбрали: {time_str}\n\nВведите ваше имя:")
    await state.set_state(Booking.name); await call.answer()

@dp.message(Booking.name)
async def enter_name(message: types.Message, state: FSMContext):
    await state.update_data(name=message.text)
    await message.answer("Введите ваш номер телефона:"); await state.set_state(Booking.phone)

@dp.message(Booking.phone)
async def enter_phone(message: types.Message, state: FSMContext):
    if re.search(r"[a-zA-Zа-яА-ЯёЁ]", message.text or ""):
        await message.answer("⚠️ Номер не должен содержать буквы. Попробуйте ещё раз:"); return
    data=await state.get_data(); user_id=message.from_user.id
    svc=get_service(data.get("service","")); dur=duration_minutes(svc)
    yr=data.get("year") or datetime.now().year; mon=data.get("month"); day=data.get("day")
    available=get_available_slots(yr,mon,day,dur)
    if data["time"] not in available:
        await message.answer("⚠️ Это время уже заняли пока вы оформляли запись.\nПожалуйста, начните заново 👇", reply_markup=main_menu_kb())
        await state.clear(); return
    bid=add_booking(user_id=user_id,service=data["service"],year=yr,month=mon,day=day,time=data["time"],name=data["name"],phone=message.text)
    b=get_booking(bid)
    await message.answer(f"✅ Бронь подтверждена!\n\n{format_booking(b)}\n\n━━━━━━━━━━━━━━━━━\n📍 Контакты Дарьи:\n{CONTACTS_SHORT}", reply_markup=back_to_menu_kb())
    tg=message.from_user.username; tg_line=f"\n💬 @{tg}" if tg else ""
    visits=get_client_visits(user_id); rank=get_client_rank(visits)
    vw="визит" if visits==1 else ("визита" if 2<=visits<=4 else "визитов")
    for _aid in ADMIN_IDS:
        try: await bot.send_message(_aid, f"🔔 Новая бронь!\n\n💅 {data['service']}\n⏱ ~{svc['duration'] if svc else ''}\n🕐 {data['time']} | {day} {MONTHS_GEN[mon]}\n👤 {data['name']} 📞 {message.text}{tg_line}\n{rank} • {visits} {vw}")
        except: pass
    await state.clear()

@dp.callback_query(F.data.startswith("admin_view:"))
async def admin_view_booking(call: types.CallbackQuery):
    if call.from_user.id not in ADMIN_IDS: await call.answer("⛔️", show_alert=True); return
    bid=int(call.data.split(":")[1]); b=get_booking(bid)
    if not b: await call.answer("Бронь не найдена.", show_alert=True); return
    svc=get_service(b["service"]); dur=svc["duration"] if svc else ""
    visits=get_client_visits(b["user_id"]); rank=get_client_rank(visits)
    vw="визит" if visits==1 else ("визита" if 2<=visits<=4 else "визитов")
    text=f"📋 Бронь #{bid}\n\n💅 {b['service']}\n⏱ ~{dur}\n🕐 {b['time']} | {b['day']} {MONTHS_GEN[b['month']]}\n👤 {b['name']} 📞 {b['phone']}\n{rank} • {visits} {vw}"
    kb=InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✏️ Редактировать", callback_data=f"edit_booking:{bid}"),
         InlineKeyboardButton(text="🗑 Удалить",        callback_data=f"admin_del:{bid}")],
        [InlineKeyboardButton(text="◀️ Назад",          callback_data="admin_all")]])
    await call.message.answer(text, reply_markup=kb); await call.answer()

@dp.callback_query(F.data.startswith("admin_"))
async def admin_actions(call: types.CallbackQuery):
    if call.from_user.id not in ADMIN_IDS: await call.answer("⛔️", show_alert=True); return
    action=call.data
    if action=="admin_all":
        all_b=get_all_bookings()
        if not all_b: await call.message.answer("Броней пока нет."); await call.answer(); return
        rows=[[InlineKeyboardButton(text=f"👁 {b['time']} {b['day']} {MONTHS[b['month']]} — {b['name']}", callback_data=f"admin_view:{b['id']}")] for b in all_b]
        rows.append([InlineKeyboardButton(text="◀️ Назад", callback_data="admin_back")])
        await call.message.answer(f"📋 Все брони ({len(all_b)} шт.):", reply_markup=InlineKeyboardMarkup(inline_keyboard=rows))
    elif action=="admin_today":
        now=datetime.now(); today_b=[b for b in get_all_bookings() if b["year"]==now.year and b["month"]==now.month and b["day"]==now.day]
        if not today_b: await call.message.answer("Сегодня броней нет.")
        else:
            rows=[[InlineKeyboardButton(text=f"👁 {b['time']} — {b['name']} | {b['service'][:20]}", callback_data=f"admin_view:{b['id']}")] for b in today_b]
            rows.append([InlineKeyboardButton(text="◀️ Назад", callback_data="admin_back")])
            await call.message.answer(f"📅 Сегодня ({now.day} {MONTHS[now.month]}):", reply_markup=InlineKeyboardMarkup(inline_keyboard=rows))
    elif action=="admin_tomorrow":
        tom=datetime.now()+timedelta(days=1); tom_b=[b for b in get_all_bookings() if b["year"]==tom.year and b["month"]==tom.month and b["day"]==tom.day]
        if not tom_b: await call.message.answer("Завтра броней нет.")
        else:
            rows=[[InlineKeyboardButton(text=f"👁 {b['time']} — {b['name']} | {b['service'][:20]}", callback_data=f"admin_view:{b['id']}")] for b in tom_b]
            rows.append([InlineKeyboardButton(text="◀️ Назад", callback_data="admin_back")])
            await call.message.answer(f"🔜 Завтра ({tom.day} {MONTHS[tom.month]}):", reply_markup=InlineKeyboardMarkup(inline_keyboard=rows))
    elif action=="admin_schedule":
        await call.message.answer("🗓 Управление расписанием:", reply_markup=schedule_main_kb())
    elif action=="admin_services":
        await call.message.answer("💅 Управление услугами:", reply_markup=admin_services_kb())
    elif action=="admin_stats":
        try: s=get_stats()
        except Exception as e: await call.message.answer(f"❌ Ошибка: {e}"); await call.answer(); return
        now=datetime.now()
        text=(f"📊 Статистика\n\n📋 Активных: {s['total_active']}\n🗑 Отмен: {s['total_cancelled']} (клиент: {s['cancelled_by_client']}, мастер: {s['cancelled_by_master']})\n"
              f"🔄 Переносов: {s['total_transfers']}\n⭐ Отзывов: {s['total_reviews']}")
        if s["avg_rating"]: text+=f" | Оценка: {s['avg_rating']} ⭐"
        text+=f"\n\n💰 За {MONTHS[now.month]}: {s['month_revenue']}€\n📊 Всего: {s['total_revenue']}€"
        if s["by_service"]:
            text+="\n\n💅 По услугам:\n"
            for svc_name,cnt in s["by_service"]:
                text+=f"  • {svc_name}: {cnt} шт. → {s['revenue_by_service'].get(svc_name,0)}€\n"
        if s["last_reviews"]:
            text+="\n⭐ Последние отзывы:\n"
            for rating,rv,svc_name,_ in s["last_reviews"]:
                text+=f"  {'⭐'*rating} {svc_name}{(' — '+rv) if rv else ''}\n"
        await call.message.answer(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="◀️ Назад", callback_data="admin_back")]]))
    elif action=="admin_reviews":
        con=db_connect()
        rows=con.execute("SELECT id,rating,text,service,created_at,username FROM reviews ORDER BY id DESC LIMIT 30").fetchall()
        con.close()
        if not rows:
            await call.message.answer("⭐ Отзывов пока нет.",
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(text="✍️ Написать отзыв", callback_data="admin_rev_add")],
                    [InlineKeyboardButton(text="◀️ Назад", callback_data="admin_back")]]))
            await call.answer(); return
        text="⭐ Все отзывы:\n\n"
        kb_rows=[]
        for row in rows:
            rev_id,rating,rv,svc_name,created_at = row[0],row[1],row[2],row[3],row[4]
            username = row[5] if len(row) > 5 and row[5] else "Аноним"
            stars="⭐"*rating
            date_str=created_at[:10] if created_at else ""
            text+=f"#{rev_id} {stars} — {svc_name} ({date_str})\n"
            text+=f"👤 {username}\n"
            if rv: text+=f"💬 {rv}\n"
            text+="\n"
            kb_rows.append([
                InlineKeyboardButton(text=f"✏️ #{rev_id}", callback_data=f"admin_rev_edit:{rev_id}"),
                InlineKeyboardButton(text=f"🗑 #{rev_id}", callback_data=f"admin_rev_del:{rev_id}")])
        kb_rows.append([InlineKeyboardButton(text="✍️ Написать отзыв", callback_data="admin_rev_add")])
        kb_rows.append([InlineKeyboardButton(text="◀️ Назад", callback_data="admin_back")])
        await call.message.answer(text.strip(), reply_markup=InlineKeyboardMarkup(inline_keyboard=kb_rows))
    elif action.startswith("admin_rev_del:"):
        rev_id=int(action.split(":")[1])
        con=db_connect(); con.execute("DELETE FROM reviews WHERE id=?", (rev_id,)); con.commit(); con.close()
        await call.message.answer("🗑 Отзыв удалён.", reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="◀️ К отзывам", callback_data="admin_reviews")]]))
    elif action.startswith("admin_rev_edit:"):
        rev_id=int(action.split(":")[1])
        await state.update_data(edit_rev_id=rev_id)
        await state.set_state(AdminReview.text)
        await call.message.answer("✏️ Введите новый текст отзыва (или /skip чтобы оставить без текста):",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="❌ Отмена", callback_data="admin_reviews")]]))
    elif action=="admin_rev_add":
        await state.set_state(AdminReview.add_text)
        await call.message.answer("✍️ Введите текст отзыва от мастера:",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="❌ Отмена", callback_data="admin_reviews")]]))
    elif action=="admin_back":
        await call.message.answer(f"🔐 Панель администратора\nВсего броней: {len(get_all_bookings())}", reply_markup=admin_panel_kb())
    elif action.startswith("admin_del:"):
        bid=int(action.split(":")[1]); b=get_booking(bid)
        if b:
            log_cancellation(b,"master"); remove_booking(bid)
            try: await bot.send_message(b["user_id"], f"❌ Ваша бронь отменена мастером.\n\n{format_booking(b)}")
            except: pass
            await call.message.answer("✅ Бронь отменена.")
        else: await call.message.answer("Бронь не найдена.")
    await call.answer()

@dp.callback_query(F.data.startswith("svc_manage:"))
async def svc_manage(call: types.CallbackQuery):
    if call.from_user.id not in ADMIN_IDS: await call.answer("⛔️", show_alert=True); return
    sid=int(call.data.split(":")[1]); s=get_service_by_id(sid)
    if not s: await call.answer("Услуга не найдена.", show_alert=True); return
    status="✅ Активна" if s["active"] else "❌ Скрыта"
    desc_line = f"\n📝 {s['description']}" if s.get("description") else ""
    await call.message.answer(f"💅 {s['name']}\n💰 {s['price']} | ⏱ {s['duration']}{desc_line}\nСтатус: {status}", reply_markup=svc_manage_kb(sid,s["active"])); await call.answer()

@dp.callback_query(F.data.startswith("svc_hide:"))
async def svc_hide(call: types.CallbackQuery):
    if call.from_user.id not in ADMIN_IDS: await call.answer("⛔️", show_alert=True); return
    sid=int(call.data.split(":")[1]); deactivate_service_db(sid)
    await call.answer("❌ Услуга скрыта"); await call.message.edit_reply_markup(reply_markup=svc_manage_kb(sid,0))

@dp.callback_query(F.data.startswith("svc_show:"))
async def svc_show(call: types.CallbackQuery):
    if call.from_user.id not in ADMIN_IDS: await call.answer("⛔️", show_alert=True); return
    sid=int(call.data.split(":")[1]); restore_service_db(sid)
    await call.answer("✅ Услуга видна"); await call.message.edit_reply_markup(reply_markup=svc_manage_kb(sid,1))

@dp.callback_query(F.data.startswith("svc_edit:"))
async def svc_edit_start(call: types.CallbackQuery, state: FSMContext):
    if call.from_user.id not in ADMIN_IDS: await call.answer("⛔️", show_alert=True); return
    _,field,sid_str=call.data.split(":"); sid=int(sid_str); s=get_service_by_id(sid)
    if not s: await call.answer("Услуга не найдена.", show_alert=True); return
    await state.update_data(edit_svc_id=sid, edit_svc_field=field)
    cancel_kb=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="❌ Отмена", callback_data="admin_services")]])
    if field=="img":
        await call.message.answer("🖼 Отправьте новую картинку для услуги фото-сообщением:", reply_markup=cancel_kb)
        await state.set_state(EditService.img)
    else:
        cur_desc = s.get("description","") or "нет"
        prompts={"name":f"Текущее: {s['name']}\n\nВведите новое название:","price":f"Текущая: {s['price']}\n\nВведите цену (пример: 20€):","duration":f"Текущая: {s['duration']}\n\nВведите длительность (пример: 45 мин):","description":f"Текущее описание: {cur_desc}\n\nВведите новое описание услуги (или напишите - чтобы убрать):"}
        await call.message.answer(prompts.get(field,"Введите новое значение:"), reply_markup=cancel_kb)
        await state.set_state(EditService.value)
    await call.answer()

@dp.message(EditService.value)
async def svc_edit_save(message: types.Message, state: FSMContext):
    data=await state.get_data(); sid=data["edit_svc_id"]; field=data["edit_svc_field"]; value=message.text.strip()
    if field=="price" and "€" not in value: value=value+"€"
    if field=="description" and value=="-": value=""
    if field=="duration":
        if "мин" not in value.lower(): value=value+" мин"
        m=re.search(r"(\d+)",value); update_service_db(sid,"duration_min",int(m.group(1)) if m else 60)
    update_service_db(sid,field,value); s=get_service_by_id(sid); await state.clear()
    await message.answer(f"✅ Обновлено!\n\n💅 {s['name']}\n💰 {s['price']} | ⏱ {s['duration']}",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="◀️ К услугам", callback_data="admin_services")]]))

@dp.message(EditService.img)
async def svc_edit_img_save(message: types.Message, state: FSMContext):
    if not message.photo: await message.answer("⚠️ Отправьте именно фотографию."); return
    data=await state.get_data(); sid=data["edit_svc_id"]; s=get_service_by_id(sid)
    os.makedirs("images",exist_ok=True); file_path=f"images/svc_{sid}.jpg"
    photo=message.photo[-1]; file_info=await bot.get_file(photo.file_id)
    await bot.download_file(file_info.file_path, destination=file_path)
    update_service_db(sid,"img",file_path); await state.clear()
    await message.answer(f"✅ Картинка обновлена!\n\n💅 {s['name']}",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="◀️ К услугам", callback_data="admin_services")]]))

@dp.callback_query(F.data.startswith("svc_delete_confirm:"))
async def svc_delete_confirm(call: types.CallbackQuery):
    if call.from_user.id not in ADMIN_IDS: await call.answer("⛔️", show_alert=True); return
    sid=int(call.data.split(":")[1]); s=get_service_by_id(sid)
    if not s: await call.answer("Услуга не найдена.", show_alert=True); return
    kb=InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text="✅ Да, удалить", callback_data=f"svc_delete_yes:{sid}"),
        InlineKeyboardButton(text="❌ Отмена",       callback_data=f"svc_manage:{sid}")]])
    await call.message.answer(
        f"❗️ Удалить услугу «{s['name']}»?\n\nВсе существующие брони сохранятся.",
        reply_markup=kb); await call.answer()

@dp.callback_query(F.data.startswith("svc_delete_yes:"))
async def svc_delete_yes(call: types.CallbackQuery):
    if call.from_user.id not in ADMIN_IDS: await call.answer("⛔️", show_alert=True); return
    sid=int(call.data.split(":")[1]); s=get_service_by_id(sid)
    if not s: await call.answer("Услуга не найдена.", show_alert=True); return
    name=s["name"]; delete_service_db(sid)
    await call.message.answer(
        f"✅ Услуга «{name}» удалена.",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
            InlineKeyboardButton(text="◀️ К услугам", callback_data="admin_services")]])); await call.answer()

@dp.callback_query(F.data == "svc_add")
async def svc_add_start(call: types.CallbackQuery, state: FSMContext):
    if call.from_user.id not in ADMIN_IDS: await call.answer("⛔️", show_alert=True); return
    await call.message.answer("➕ Новая услуга\n\nВведите название:",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="❌ Отмена", callback_data="admin_services")]]))
    await state.set_state(AddService.name); await call.answer()

@dp.message(AddService.name)
async def svc_add_name(message: types.Message, state: FSMContext):
    await state.update_data(new_name=message.text.strip()); await message.answer("Введите цену (пример: 25€):"); await state.set_state(AddService.price)

@dp.message(AddService.price)
async def svc_add_price(message: types.Message, state: FSMContext):
    await state.update_data(new_price=message.text.strip()); await message.answer("Введите длительность (пример: 60 мин):"); await state.set_state(AddService.duration)

@dp.message(AddService.duration)
async def svc_add_duration(message: types.Message, state: FSMContext):
    dur=message.text.strip(); m=re.search(r"(\d+)",dur)
    await state.update_data(new_duration=dur,new_dur_min=int(m.group(1)) if m else 60)
    await message.answer("Отправьте картинку для услуги фото-сообщением:\n(или напишите skip)"); await state.set_state(AddService.img)

@dp.message(AddService.img)
async def svc_add_img(message: types.Message, state: FSMContext):
    data=await state.get_data()
    if message.photo:
        os.makedirs("images",exist_ok=True)
        photo=message.photo[-1]; file_info=await bot.get_file(photo.file_id)
        img=f"images/new_{photo.file_id[-8:]}.jpg"
        await bot.download_file(file_info.file_path, destination=img)
    elif message.text and message.text.lower()=="skip":
        img="images/1.jpg"
    else:
        await message.answer("Отправьте фото или напишите skip:"); return
    ok=add_service_db(data["new_name"],data["new_price"],data["new_duration"],data["new_dur_min"],img)
    await state.clear()
    if ok: await message.answer(f"✅ Услуга добавлена!\n\n💅 {data['new_name']}\n💰 {data['new_price']} | ⏱ {data['new_duration']}",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="◀️ К услугам", callback_data="admin_services")]]))
    else: await message.answer("❌ Ошибка — услуга с таким названием уже существует.")

@dp.callback_query(F.data.startswith("sched_"))
async def schedule_actions(call: types.CallbackQuery):
    if call.from_user.id not in ADMIN_IDS: await call.answer("⛔️", show_alert=True); return
    action=call.data
    if action=="sched_block_day": await call.message.answer("Выберите месяц:", reply_markup=schedule_months_kb("bday"))
    elif action=="sched_unblock_day": await call.message.answer("Выберите месяц:", reply_markup=schedule_months_kb("uday"))
    elif action=="sched_block_slots": await call.message.answer("Выберите месяц:", reply_markup=schedule_months_kb("bslot"))
    elif action=="sched_unblock_slots": await call.message.answer("Выберите месяц:", reply_markup=schedule_months_kb("uslot"))
    elif action=="sched_unblock_all_confirm":
        kb=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="✅ Да", callback_data="sched_unblock_all_yes"),InlineKeyboardButton(text="❌ Отмена", callback_data="admin_schedule")]])
        await call.message.answer("❗️ Разблокировать все дни и часы?", reply_markup=kb)
    elif action=="sched_unblock_all_yes":
        con=db_connect(); con.execute("DELETE FROM schedule"); con.commit(); con.close()
        await call.message.answer("✅ Всё разблокировано.", reply_markup=schedule_main_kb())
    elif action=="sched_show":
        blocked=get_blocked_days(); slots=get_all_blocked_slots()
        text="📋 Текущие ограничения:\n\n"
        if blocked: text+="Заблокированные дни:\n"+"".join(f"  🚫 {d}\n" for d in sorted(blocked))+"\n"
        if slots: text+="Заблокированные часы:\n"+"".join(f"  📅 {d}: {', '.join(sl)}\n" for d,sl in sorted(slots.items()))
        if not blocked and not slots: text+="Ограничений нет."
        await call.message.answer(text, reply_markup=schedule_main_kb())
    await call.answer()

@dp.callback_query(F.data.startswith("sm_"))
async def schedule_month_pick(call: types.CallbackQuery):
    _,rest=call.data.split("_",1); act,mon=rest.split(":")
    await call.message.answer(f"Выберите день ({MONTHS[int(mon)]}):", reply_markup=schedule_days_kb(int(mon),act)); await call.answer()

@dp.callback_query(F.data.startswith("sd_"))
async def schedule_day_pick(call: types.CallbackQuery):
    _,rest=call.data.split("_",1); act,mon,day=rest.split(":")
    month,day=int(mon),int(day); key=date_key(datetime.now().year,month,day)
    if act=="bday":
        yr=datetime.now().year
        if any(b["year"]==yr and b["month"]==month and b["day"]==day for b in get_all_bookings()):
            await call.answer(f"⚠️ На {day} {MONTHS[month]} есть брони — сначала отмените их", show_alert=True); return
        block_day(key); await call.answer(f"🚫 {day} {MONTHS[month]} заблокирован")
        await call.message.edit_reply_markup(reply_markup=schedule_days_kb(month,act))
    elif act=="uday":
        unblock_day(key); await call.answer(f"✅ {day} {MONTHS[month]} разблокирован")
        await call.message.edit_reply_markup(reply_markup=schedule_days_kb(month,act))
    elif act in ("bslot","uslot"):
        await call.message.answer(f"Выберите часы для {day} {MONTHS[month]}:", reply_markup=schedule_slots_kb(month,day,act)); await call.answer()

@dp.callback_query(F.data.startswith("ss_"))
async def schedule_slot_pick(call: types.CallbackQuery):
    _,rest=call.data.split("_",1); parts=rest.split(":")
    act,mon,day,raw=parts[0],parts[1],parts[2],parts[3]
    month,day=int(mon),int(day); time_str=raw[:2]+":"+raw[2:]
    key=date_key(datetime.now().year,month,day)
    if act=="bslot":
        yr=datetime.now().year
        if any(b["year"]==yr and b["month"]==month and b["day"]==day and b["time"]==time_str for b in get_all_bookings()):
            await call.answer(f"⚠️ На {time_str} есть бронь", show_alert=True); return
        block_slot(key,time_str); await call.answer(f"🔒 {time_str} заблокировано")
    elif act=="uslot":
        unblock_slot(key,time_str); await call.answer(f"🔓 {time_str} разблокировано")
    await call.message.edit_reply_markup(reply_markup=schedule_slots_kb(month,day,act))

@dp.callback_query(F.data == "noop")
async def noop_cb(call: types.CallbackQuery): await call.answer()

@dp.callback_query(F.data.startswith("reschedule:"))
async def reschedule_start(call: types.CallbackQuery, state: FSMContext):
    bid=int(call.data.split(":")[1]); b=get_booking(bid)
    if not b or b["user_id"]!=call.from_user.id: await call.answer("Бронь не найдена.", show_alert=True); return
    svc=get_service(b["service"]); dur=duration_minutes(svc); year=datetime.now().year
    await state.update_data(reschedule_bid=bid,reschedule_service=b["service"],
        reschedule_old_date=f"{b['day']} {MONTHS_GEN[b['month']]}",reschedule_old_time=b["time"],year=year)
    await call.message.answer("📅 Выберите новую дату — месяц:", reply_markup=months_kb(year))
    await state.set_state(Reschedule.month); await call.answer()

@dp.callback_query(F.data.startswith("month:"), Reschedule.month)
async def reschedule_month(call: types.CallbackQuery, state: FSMContext):
    month=int(call.data.split(":")[1]); data=await state.get_data(); year=data.get("year",datetime.now().year)
    await state.update_data(month=month); svc=get_service(data.get("reschedule_service","")); dur=duration_minutes(svc)
    await call.message.answer(f"Выберите день ({MONTHS[month]}):", reply_markup=days_kb(year,month,dur))
    await state.set_state(Reschedule.day); await call.answer()

@dp.callback_query(F.data.startswith("day:"), Reschedule.day)
async def reschedule_day(call: types.CallbackQuery, state: FSMContext):
    day=int(call.data.split(":")[1]); data=await state.get_data()
    year=data.get("year",datetime.now().year); month=data.get("month"); bid=data.get("reschedule_bid")
    await state.update_data(day=day); svc=get_service(data.get("reschedule_service","")); dur=duration_minutes(svc)
    await call.message.answer(f"✅ {day} {MONTHS_GEN[month]}\n\nВыберите время:",
        reply_markup=time_kb(year,month,day,exclude_bid=bid,new_dur_min=dur))
    await state.set_state(Reschedule.time); await call.answer()

@dp.callback_query(F.data.startswith("t_"), Reschedule.time)
async def reschedule_time(call: types.CallbackQuery, state: FSMContext):
    raw=call.data[2:]; new_time=raw[:2]+":"+raw[2:]; data=await state.get_data()
    bid=data["reschedule_bid"]; b=get_booking(bid)
    if not b: await call.answer("Бронь не найдена.", show_alert=True); return
    year=data.get("year",datetime.now().year); month=data.get("month"); day=data.get("day")
    old_date=f"{b['year']}-{b['month']:02d}-{b['day']:02d}"; new_date=f"{year}-{month:02d}-{day:02d}"
    log_transfer(bid,b["user_id"],b["service"],old_date,b["time"],new_date,new_time)
    update_booking_field(bid,"year",year); update_booking_field(bid,"month",month)
    update_booking_field(bid,"day",day); update_booking_field(bid,"time",new_time)
    b_new=get_booking(bid)
    await call.message.answer(f"✅ Бронь перенесена!\n\n{format_booking(b_new)}", reply_markup=booking_actions_kb(bid))
    for _aid in ADMIN_IDS:
        try: await bot.send_message(_aid, f"🔄 Перенос брони!\n\nБыло: {data['reschedule_old_date']} {data['reschedule_old_time']}\nСтало: {day} {MONTHS_GEN[month]} {new_time}\n\n{format_booking(b_new)}")
        except: pass
    await state.clear(); await call.answer()

def review_rating_kb(bid):
    stars=["⭐","⭐⭐","⭐⭐⭐","⭐⭐⭐⭐","⭐⭐⭐⭐⭐"]
    rows=[[InlineKeyboardButton(text=s, callback_data=f"rev_rating:{bid}:{i+1}")] for i,s in enumerate(stars)]
    rows.append([InlineKeyboardButton(text="❌ Пропустить", callback_data="rev_skip")])
    return InlineKeyboardMarkup(inline_keyboard=rows)

@dp.callback_query(F.data.startswith("rev_rating:"))
async def review_rating(call: types.CallbackQuery, state: FSMContext):
    _,bid_str,rating_str=call.data.split(":"); bid=int(bid_str); rating=int(rating_str)
    b=get_booking(bid); svc=b["service"] if b else ""
    username = call.from_user.username or call.from_user.first_name or "Аноним"
    review_id=add_review(bid,call.from_user.id,svc,rating,"",username)
    await state.update_data(review_bid=bid,review_rating=rating,review_service=svc,review_id=review_id)
    stars="⭐"*rating
    for _aid in ADMIN_IDS:
        try: await bot.send_message(_aid, f"⭐ Новый отзыв!\n\n💅 {svc}\n{stars}")
        except: pass
    await call.message.answer(f"Вы поставили {stars}\n\nНапишите комментарий (или нажмите «Пропустить»):",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="❌ Пропустить", callback_data="rev_skip")]]))
    await state.set_state(Review.text); await call.answer()

@dp.callback_query(F.data == "rev_skip")
async def review_skip(call: types.CallbackQuery, state: FSMContext):
    await state.clear(); await call.message.answer("Спасибо! Ждём вас снова 💅"); await call.answer()

@dp.message(Review.text)
async def review_text(message: types.Message, state: FSMContext):
    data=await state.get_data(); text=message.text.strip() if message.text and message.text!="/skip" else ""
    review_id=data.get("review_id")
    if review_id and text:
        con=db_connect(); con.execute("UPDATE reviews SET text=? WHERE id=?", (text,review_id)); con.commit(); con.close()
    stars="⭐"*data["review_rating"]
    await message.answer("🙏 Спасибо за отзыв! Ждём вас снова 💅")
    if text:
        for _aid in ADMIN_IDS:
            try: await bot.send_message(_aid, f"💬 Комментарий к отзыву:\n\n💅 {data['review_service']}\n{stars}\n{text}")
            except: pass
    await state.clear()

@dp.message(AdminReview.text)
async def admin_rev_edit_text(message: types.Message, state: FSMContext):
    data = await state.get_data()
    rev_id = data.get("edit_rev_id")
    new_text = "" if (not message.text or message.text == "/skip") else message.text.strip()
    con = db_connect()
    con.execute("UPDATE reviews SET text=? WHERE id=?", (new_text, rev_id))
    con.commit(); con.close()
    await state.clear()
    await message.answer("✅ Отзыв обновлён!", reply_markup=InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="◀️ К отзывам", callback_data="admin_reviews")]]))

@dp.message(AdminReview.add_text)
async def admin_rev_add_text(message: types.Message, state: FSMContext):
    await state.update_data(new_rev_text=message.text.strip() if message.text else "")
    await state.set_state(AdminReview.add_rating)
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=s, callback_data=f"admin_rev_rating:{i+1}")]
        for i, s in enumerate(["⭐","⭐⭐","⭐⭐⭐","⭐⭐⭐⭐","⭐⭐⭐⭐⭐"])])
    await message.answer("Выберите рейтинг:", reply_markup=kb)

@dp.callback_query(F.data.startswith("admin_rev_rating:"), AdminReview.add_rating)
async def admin_rev_add_rating(call: types.CallbackQuery, state: FSMContext):
    rating = int(call.data.split(":")[1])
    await state.update_data(new_rev_rating=rating)
    await state.set_state(AdminReview.add_service)
    services = get_all_services_db()
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=s["name"], callback_data=f"admin_rev_svc:{s['name']}")]
        for s in services])
    await call.message.answer("Выберите услугу для отзыва:", reply_markup=kb)
    await call.answer()

@dp.callback_query(F.data.startswith("admin_rev_svc:"), AdminReview.add_service)
async def admin_rev_add_service(call: types.CallbackQuery, state: FSMContext):
    svc_name = call.data.split(":", 1)[1]
    data = await state.get_data()
    con = db_connect()
    con.execute(
        "INSERT INTO reviews (booking_id, user_id, service, rating, text, created_at) VALUES (?,?,?,?,?,?)",
        (0, 0, svc_name, data["new_rev_rating"], data.get("new_rev_text", ""), datetime.now().strftime("%Y-%m-%d %H:%M")))
    con.commit(); con.close()
    await state.clear()
    await call.message.answer("✅ Отзыв добавлен!", reply_markup=InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="◀️ К отзывам", callback_data="admin_reviews")]]))
    await call.answer()

async def reminder_loop():
    while True:
        try:
            now=datetime.now()
            con=db_connect(); rows_all=[_row_to_booking(r) for r in con.execute("SELECT * FROM bookings").fetchall()]; con.close()
            for b in rows_all:
                try: appt=datetime(b["year"],b["month"],b["day"],int(b["time"].split(":")[0]),int(b["time"].split(":")[1]))
                except: continue
                delta=appt-now; total_min=delta.total_seconds()/60
                svc=get_service(b["service"]); dur_str=svc["duration"] if svc else ""
                if not b["reminded_24"] and 1430<=total_min<=1450:
                    try:
                        await bot.send_message(b["user_id"], f"🔔 Напоминание!\n\nЗавтра у вас запись:\n💅 {b['service']} ({dur_str})\n📅 {b['day']} {MONTHS_GEN[b['month']]}\n⏱ {b['time']}\n\n📍 {CONTACTS_SHORT}")
                        con2=db_connect(); con2.execute("UPDATE bookings SET reminded_24=1 WHERE id=?", (b["id"],)); con2.commit(); con2.close()
                    except: pass
                if not b["reminded_2"] and 110<=total_min<=130:
                    try:
                        await bot.send_message(b["user_id"], f"⏰ Через 2 часа ваша запись!\n\n💅 {b['service']} ({dur_str})\n📅 {b['day']} {MONTHS_GEN[b['month']]}\n⏱ {b['time']}\n\n📍 {CONTACTS_SHORT}")
                        con2=db_connect(); con2.execute("UPDATE bookings SET reminded_2=1 WHERE id=?", (b["id"],)); con2.commit(); con2.close()
                    except: pass
                svc_obj=get_service(b["service"]); dur_m=duration_minutes(svc_obj)
                end_appt=appt+timedelta(minutes=dur_m); mins_after=(now-end_appt).total_seconds()/60
                if not b.get("review_sent") and 180<=mins_after<=200:
                    try:
                        await bot.send_message(b["user_id"], f"😊 Как прошёл визит?\n\n💅 {b['service']}\n\nОставьте оценку 👇", reply_markup=review_rating_kb(b["id"]))
                        con2=db_connect(); con2.execute("UPDATE bookings SET review_sent=1 WHERE id=?", (b["id"],)); con2.commit(); con2.close()
                    except: pass
            if now.hour==8 and now.minute<5:
                today_b=[b for b in rows_all if b["year"]==now.year and b["month"]==now.month and b["day"]==now.day]
                if today_b:
                    text=f"☀️ Доброе утро! Сегодня {now.day} {MONTHS_GEN[now.month]}:\n\n"
                    for b in today_b:
                        svc2=get_service(b["service"]); dur2=svc2["duration"] if svc2 else ""
                        text+=f"⏱ {b['time']} — 💅 {b['service']} (~{dur2})\n👤 {b['name']} 📞 {b['phone']}\n\n"
                    for _aid in ADMIN_IDS:
                        try: await bot.send_message(_aid, text)
                        except: pass
        except Exception as e: print(f"Reminder error: {e}")
        await asyncio.sleep(300)

@dp.message(Command("stats"))
async def cmd_stats(message: types.Message):
    if message.from_user.id not in ADMIN_IDS: return
    try:
        s=get_stats(); now=datetime.now()
        await message.answer(f"📊 Статистика\n\n📋 Активных: {s['total_active']}\n🗑 Отмен: {s['total_cancelled']}\n🔄 Переносов: {s['total_transfers']}\n⭐ Отзывов: {s['total_reviews']}\n\n💰 За {MONTHS[now.month]}: {s['month_revenue']}€\n💰 Всего: {s['total_revenue']}€")
    except Exception as e: await message.answer(f"❌ Ошибка: {e}")

async def on_startup(bot: Bot):
    asyncio.create_task(reminder_loop())

async def main():
    dp.startup.register(on_startup); print("Бот запущен!")
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
