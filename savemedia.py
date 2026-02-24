import telebot
from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton
import psycopg2
from datetime import datetime
import os
import threading
import time
# ================= CONFIG ================= #

BOT_TOKEN = "8606303101:AAGw3fHdI5jpZOOuFCSoHlPKb1Urj4Oidk4"
# DATABASE_URL = "YOUR_POSTGRES_URL"
DATABASE_URL = os.getenv("DATABASE_URL")
ADMIN_ID = 8305774350  # Your Telegram ID
user_sessions = {}
user_timers = {}
FILES_PER_PAGE = 5
bot = telebot.TeleBot(BOT_TOKEN)
session_lock = threading.Lock()
# ================= DATABASE ================= #

def get_connection():
    return psycopg2.connect(DATABASE_URL)

def init_db():
    conn = get_connection()
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS users (
            user_id BIGINT PRIMARY KEY,
            username TEXT,
            joined_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS stored_media (
            id SERIAL PRIMARY KEY,
            user_id BIGINT REFERENCES users(user_id) ON DELETE CASCADE,
            file_id TEXT NOT NULL,
            file_type TEXT NOT NULL,
            caption TEXT,
            saved_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(user_id, file_id),
            file_size BIGINT DEFAULT 0
        );
    """)

    cur.execute("CREATE INDEX IF NOT EXISTS idx_user_media ON stored_media(user_id);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_saved_at ON stored_media(saved_at);")

    conn.commit()
    cur.close()
    conn.close()
    
# ================= ADMIN PANEL Helper ================= #
def admin_panel_text():
    return "🛠 Admin Panel\n\nSelect an option:"

def admin_panel_markup():
    markup = InlineKeyboardMarkup()
    markup.add(InlineKeyboardButton("📊 Bot Stats", callback_data="admin_stats"))
    markup.add(InlineKeyboardButton("👥 Total Users", callback_data="admin_users"))
    markup.add(InlineKeyboardButton("📦 Total Files", callback_data="admin_files"))
    markup.add(InlineKeyboardButton("🔙 Back", callback_data="menu_main"))
    return markup
# ================= DB HELPERS ================= #
def get_storage_used(user_id):
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        "SELECT COALESCE(SUM(file_size), 0) FROM stored_media WHERE user_id = %s",
        (user_id,)
    )
    total_size = cur.fetchone()[0]
    cur.close()
    conn.close()
    return total_size
def format_size(size):
    for unit in ['B', 'KB', 'MB', 'GB']:
        if size < 1024:
            return f"{size:.2f} {unit}"
        size /= 1024
    return f"{size:.2f} TB"
def save_user(user):
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO users (user_id, username) VALUES (%s, %s) ON CONFLICT DO NOTHING",
        (user.id, user.username)
    )
    conn.commit()
    cur.close()
    conn.close()

def save_media(user_id, file_id, file_type, caption,file_size):
    conn = get_connection()
    cur = conn.cursor()

    cur.execute("""
        INSERT INTO stored_media (user_id, file_id, file_type, caption, file_size)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (user_id, file_id) DO NOTHING
        RETURNING id;
    """, (user_id, file_id, file_type, caption, file_size))

    result = cur.fetchone()

    conn.commit()
    cur.close()
    conn.close()

    return result is not None

def get_total_files(user_id):
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM stored_media WHERE user_id = %s", (user_id,))
    count = cur.fetchone()[0]
    cur.close()
    conn.close()
    return count

def get_category_counts(user_id):
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("""
        SELECT file_type, COUNT(*)
        FROM stored_media
        WHERE user_id = %s
        GROUP BY file_type
    """, (user_id,))
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return dict(rows)

# ================= DASHBOARD ================= #

def dashboard_text(user_id):
    total_files = get_total_files(user_id)
    total_size = format_size(get_storage_used(user_id))
    return f"📦 Cloud Vault\n\n📊 Your Storage:\n• Total Files: {total_files}\n\nChoose an option:"

def dashboard_markup(user_id):
    markup = InlineKeyboardMarkup()
    markup.add(InlineKeyboardButton("📁 My Files", callback_data="menu_files"))

    if user_id == ADMIN_ID:
        markup.add(InlineKeyboardButton("🛠 Admin Panel", callback_data="admin_panel"))

    return markup

@bot.message_handler(commands=['start'])
def start(message):
    save_user(message.from_user)
    bot.send_message(
        message.chat.id,
        dashboard_text(message.from_user.id),
        reply_markup=dashboard_markup(message.from_user.id)
    )

# ================= AUTO SAVE ================= #

import threading
import time

media_groups = {}
def finalize_user_upload(user_id, chat_id):
    session = user_sessions.pop(user_id, None)
    user_timers.pop(user_id, None)

    if not session:
        return

    total_files = get_total_files(user_id)
    total_size = format_size(get_storage_used(user_id))
    message_id = session["message_id"]

    if session["duplicate"] > 0:
        text = (
            f"📦 Upload Completed\n\n"
            f"Total Sent: {session['total']}\n"
            f"✅ Saved: {session['saved']}\n"
            f"♻️ Skipped (Duplicates): {session['duplicate']}\n\n"
            f"📦 Total Files: {total_files}\n"
            f"💾 Total Size: {total_size}"
        )
    else:
        text = (
            f"✅ {session['saved']} file(s) saved\n"
            f"📦 Total Files: {total_files}\n"
            f"💾 Total Size: {total_size}"
        )

    bot.edit_message_text(
        text,
        chat_id,
        message_id
    )

@bot.message_handler(content_types=['photo', 'video', 'document', 'audio'])
def handle_media(message):
    save_user(message.from_user)

    file_type = message.content_type
    caption = message.caption

    if file_type == "photo":
        file_id = message.photo[-1].file_id
        file_size = message.photo[-1].file_size
    elif file_type == "video":
        file_id = message.video.file_id
        file_size = message.video.file_size
    elif file_type == "document":
        file_id = message.document.file_id
        file_size = message.document.file_size
    elif file_type == "audio":
        file_id = message.audio.file_id
        file_size = message.audio.file_size
    else:
        return

    result = save_media(message.from_user.id, file_id, file_type, caption, file_size)

    user_id = message.from_user.id
    chat_id = message.chat.id

    with session_lock:
        if user_id not in user_sessions:
            processing_msg = bot.send_message(chat_id, "⏳ Processing uploads...")

            user_sessions[user_id] = {
                "total": 0,
                "saved": 0,
                "duplicate": 0,
                "message_id": processing_msg.message_id
            }

    session = user_sessions[user_id]


    session["total"] += 1
    if result:
        session["saved"] += 1
    else:
        session["duplicate"] += 1

    if user_id in user_timers:
        user_timers[user_id].cancel()

    timer = threading.Timer(
        1.0,
        finalize_user_upload,
        args=(user_id, chat_id)
    )

    user_timers[user_id] = timer
    timer.start()

# ================= CATEGORY MENU ================= #

def category_menu(user_id):
    counts = get_category_counts(user_id)

    markup = InlineKeyboardMarkup()
    markup.add(InlineKeyboardButton(f"📷 Photos ({counts.get('photo',0)})", callback_data="cat_photo_0"))
    markup.add(InlineKeyboardButton(f"🎥 Videos ({counts.get('video',0)})", callback_data="cat_video_0"))
    markup.add(InlineKeyboardButton(f"📄 Documents ({counts.get('document',0)})", callback_data="cat_document_0"))
    markup.add(InlineKeyboardButton(f"🎵 Audio ({counts.get('audio',0)})", callback_data="cat_audio_0"))
    markup.add(InlineKeyboardButton("🔙 Back", callback_data="menu_main"))
    return markup

# ================= CATEGORY PAGE ================= #

def category_page(user_id, file_type, page):
    conn = get_connection()
    cur = conn.cursor()

    offset = page * FILES_PER_PAGE

    cur.execute("""
        SELECT id, file_id, saved_at
        FROM stored_media
        WHERE user_id = %s AND file_type = %s
        ORDER BY id DESC
        LIMIT %s OFFSET %s
    """, (user_id, file_type, FILES_PER_PAGE, offset))

    rows = cur.fetchall()
    cur.close()
    conn.close()

    markup = InlineKeyboardMarkup()

    for media_id, file_id, saved_at in rows:
        date_str = saved_at.strftime("%d %b %H:%M")
        markup.add(
            InlineKeyboardButton(
                f"{date_str}",
                callback_data=f"get_{file_type}_{media_id}"
            )
        )

    if page > 0:
        markup.add(InlineKeyboardButton("⬅ Prev", callback_data=f"cat_{file_type}_{page-1}"))

    if len(rows) == FILES_PER_PAGE:
        markup.add(InlineKeyboardButton("Next ➡", callback_data=f"cat_{file_type}_{page+1}"))

    markup.add(InlineKeyboardButton("🔙 Back", callback_data="menu_files"))

    text = f"{file_type.upper()}\nPage: {page+1}"
    return text, markup

# ================= CALLBACKS ================= #

@bot.callback_query_handler(func=lambda call: True)
def callback_handler(call):
    data = call.data
    
    if data == "menu_main":
        bot.edit_message_text(
            dashboard_text(call.from_user.id),
            call.message.chat.id,
            call.message.message_id,
            reply_markup=dashboard_markup(call.from_user.id)
        )
    elif data == "admin_panel":
        if call.from_user.id != ADMIN_ID:
            bot.answer_callback_query(call.id, "Unauthorized")
            return
        bot.edit_message_text(
            admin_panel_text(),
            call.message.chat.id,
            call.message.message_id,
            reply_markup=admin_panel_markup()
        )
    elif data == "admin_stats":
        if call.from_user.id != ADMIN_ID:
            bot.answer_callback_query(call.id, "Unauthorized")
            return
        conn  = get_connection()
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM users")
        total_users = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM stored_media")
        total_files = cur.fetchone()[0]
        cur.close()
        conn.close()
        bot.edit_message_text(
            f"📊 Bot Statistics\n\n"
            f"👥 Total Users: {total_files}\n"
            f"📦 Total Files: {total_users}",
            call.message.chat.id,
            call.message.message_id,
            reply_markup=admin_panel_markup()
        )
    elif data == "admin_users":
        if call.from_user.id != ADMIN_ID:
            bot.answer_callback_query(call.id, "Unauthorized")
            return
        conn  = get_connection()
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM users")
        total_users = cur.fetchone()[0]
        cur.close()
        conn.close()
        bot.edit_message_text(
            f"👥 Total Users: {total_users}",
            call.message.chat.id,
            call.message.message_id,
            reply_markup=admin_panel_markup()
        )
    elif data == "admin_files":
        if call.from_user.id != ADMIN_ID:
            bot.answer_callback_query(call.id, "Unauthorized")
            return
        conn  = get_connection()
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM stored_media")
        total_files = cur.fetchone()[0]
        cur.close()
        conn.close()
        bot.edit_message_text(
            f"📦 Total Files: {total_files}",
            call.message.chat.id,
            call.message.message_id,
            reply_markup=admin_panel_markup()
        )
    elif data == "menu_files":
        bot.edit_message_text(
            "📂 Select Category",
            call.message.chat.id,
            call.message.message_id,
            reply_markup=category_menu(call.from_user.id)
        )

    elif data.startswith("cat_"):
        _, file_type, page = data.split("_")
        page = int(page)
        text, markup = category_page(call.from_user.id, file_type, page)
        bot.edit_message_text(
            text,
            call.message.chat.id,
            call.message.message_id,
            reply_markup=markup
        )

    elif data.startswith("get_"):
        _, file_type, media_id = data.split("_")

        conn = get_connection()
        cur = conn.cursor()
        cur.execute(
            "SELECT file_id FROM stored_media WHERE id = %s AND user_id = %s",
            (media_id, call.from_user.id)
        )
        result = cur.fetchone()
        cur.close()
        conn.close()

        if result:
            file_id = result[0]

            if file_type == "photo":
                bot.send_photo(call.message.chat.id, file_id)
            elif file_type == "video":
                bot.send_video(call.message.chat.id, file_id)
            elif file_type == "document":
                bot.send_document(call.message.chat.id, file_id)
            elif file_type == "audio":
                bot.send_audio(call.message.chat.id, file_id)
    
# ================= ADMIN STATS ================= #

@bot.message_handler(commands=['stats'])
def stats(message):
    if message.from_user.id != ADMIN_ID:
        return

    conn = get_connection()
    cur = conn.cursor()

    cur.execute("SELECT COUNT(*) FROM users")
    total_users = cur.fetchone()[0]

    cur.execute("SELECT COUNT(*) FROM stored_media")
    total_files = cur.fetchone()[0]

    cur.execute("SELECT COUNT(*) FROM stored_media WHERE saved_at::date = CURRENT_DATE")
    today_uploads = cur.fetchone()[0]

    cur.execute("SELECT COUNT(*) FROM users WHERE joined_at::date = CURRENT_DATE")
    new_users_today = cur.fetchone()[0]

    cur.close()
    conn.close()

    bot.reply_to(
        message,
        f"📊 Bot Statistics\n\n"
        f"👥 Total Users: {total_users}\n"
        f"📦 Total Files: {total_files}\n"
        f"📅 Uploads Today: {today_uploads}\n"
        f"🆕 New Users Today: {new_users_today}"
    )

# ================= START BOT ================= #

if __name__ == "__main__":
    init_db()
    bot.remove_webhook()
    print("Bot is running...")
    bot.infinity_polling(skip_pending=True)
