import telebot
from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton
from telebot.types import InputMediaPhoto, InputMediaVideo, InputMediaDocument, InputMediaAudio
import psycopg2
from datetime import datetime
import os
import threading
import time
from queue import Queue

# ================= CONFIG ================= #

BOT_TOKEN = "8606303101:AAGw3fHdI5jpZOOuFCSoHlPKb1Urj4Oidk4"
# DATABASE_URL = "YOUR_POSTGRES_URL"
DATABASE_URL = os.getenv("DATABASE_URL")
ADMIN_ID = 8305774350  # Your Telegram ID
user_sessions = {}
user_timers = {}
live_jobs = {}
FILES_PER_PAGE = 5
bot = telebot.TeleBot(BOT_TOKEN)
session_lock = threading.Lock()
admin_send_state = {}
admin_active_jobs = {}


job_queue = Queue()
job_status_cache = {}
job_status_lock = threading.Lock()
worker_running = False
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
            media_group_id TEXT,
            saved_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(user_id, file_id),
            file_size BIGINT DEFAULT 0
        );
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS send_jobs (
            id SERIAL PRIMARY KEY,
            admin_id BIGINT,
            target_user BIGINT,
            group_id BIGINT,
            last_sent_id BIGINT DEFAULT 0,
            is_active BOOLEAN DEFAULT TRUE,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """)
    cur.execute("""
        ALTER TABLE send_jobs
        ADD COLUMN IF NOT EXISTS status TEXT DEFAULT 'running'
    """)  
    cur.execute("""
        CREATE TABLE IF NOT EXISTS user_send_groups (
            id SERIAL PRIMARY KEY,
            target_user BIGINT,
            group_id BIGINT,
            group_title TEXT,
            UNIQUE(target_user, group_id)
        );
    """) 
    cur.execute("""
        ALTER TABLE user_send_groups
        ADD COLUMN IF NOT EXISTS group_title TEXT;
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
    markup.add(InlineKeyboardButton("👤 View Users", callback_data="admin_userlist_0"))
    markup.add(InlineKeyboardButton("📊 Storage Analytics", callback_data="admin_analytics"))
    markup.add(InlineKeyboardButton("🔙 Back", callback_data="menu_main"))
    return markup
USERS_PER_PAGE = 10

def get_users_page(page):
    conn = get_connection()
    cur = conn.cursor()

    offset = page * USERS_PER_PAGE

    cur.execute("""
        SELECT user_id, username
        FROM users
        ORDER BY joined_at DESC
        LIMIT %s OFFSET %s
    """, (USERS_PER_PAGE, offset))

    users = cur.fetchall()

    cur.close()
    conn.close()

    return users
def get_total_storage():
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("SELECT COALESCE(SUM(file_size), 0) FROM stored_media")
    total_size = cur.fetchone()[0]
    cur.close()
    conn.close()
    return total_size
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

def save_media(user_id, file_id, file_type, caption, file_size, media_group_id=None):
    conn = get_connection()
    cur = conn.cursor()

    cur.execute("""
        INSERT INTO stored_media
        (user_id, file_id, file_type, caption, file_size, media_group_id)
        VALUES (%s,%s,%s,%s,%s,%s)
        ON CONFLICT (user_id, file_id) DO NOTHING
        RETURNING id;
    """, (user_id, file_id, file_type, caption, file_size, media_group_id))

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
    if total_files == 0:
        return "Welcome to Rock Cloud Vault!\n\n📦 Your storage is empty.\nStart by sending any media file to save it here."
    else:
        return f"Wellcome To Rock Cloud Vault\n\n📦 Your Storage:\n•📄 Total Files: {total_files}\n•💾 Total Size: {total_size}\n\nChoose an option:"

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
@bot.message_handler(func=lambda m: m.from_user.id == ADMIN_ID and m.from_user.id in admin_send_state)
def admin_group_input(message):
    
    if message.from_user.id in admin_send_state and "changing_job" in admin_send_state[message.from_user.id]:

        job_id = admin_send_state[message.from_user.id]["changing_job"]

        if message.forward_from_chat:
            group_id = message.forward_from_chat.id
            group_title = message.forward_from_chat.title
        else:
            group_id = int(message.text)
            chat = bot.get_chat(group_id)
            group_title = chat.title

        # Update live job
        live_jobs[job_id]["group_id"] = group_id
        live_jobs[job_id]["group_title"] = group_title

        # Update DB
    # Update DB every 10 files to reduce load
        conn = get_connection()
        cur = conn.cursor()
        cur.execute("""
            UPDATE send_jobs
            SET group_id = %s
            WHERE id = %s
        """, (group_id, job_id))
        conn.commit()
        cur.close()
        conn.close()

        bot.send_message(
            message.chat.id,
            f"✅ Group changed to: {group_title}\nPress Resume to continue."
        )

        del admin_send_state[message.from_user.id]["changing_job"]
        return

    if message.from_user.id not in admin_send_state:
        return

    state = admin_send_state[message.from_user.id]

    # detect forwarded message
    if message.forward_from_chat:

        group_id = message.forward_from_chat.id
        group_title = message.forward_from_chat.title
        state["group_id"] = group_id

    else:
        # admin typed ID manually
        try:
            group_id = int(message.text)
            state["group_id"] = group_id
        except:
            bot.reply_to(message, "❌ Invalid group ID")
            return

    markup = InlineKeyboardMarkup()
    markup.add(
        InlineKeyboardButton(
            "🚀 SEND ALL MEDIA NOW",
            callback_data="admin_confirm_send"
        )
    )

    bot.send_message(
        message.chat.id,
        f"✅ Group saved: `{group_id}`\nPress button to send media",
        parse_mode="Markdown",
        reply_markup=markup
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
            f"📦 Total Files: {total_files}\n"
            f"🎬 Total Video Files: {session['video']} \n"
            f"💾 Total Size: {total_size}"
        )
    else:
        text = (
            f"📦 Upload Completed\n\n"
            f"Total Sent: {session['total']}\n"
            f"✅ Saved: {session['saved']}\n\n"
            f"📦 Total Files: {total_files}\n"
            f"🎬 Total Video Files: {session['video']} \n"
            f"🖼️ Total Photo Files: {session['photo']} \n"
            f"💾 Total Size: {total_size}"
        )

    bot.edit_message_text(
        text,
        chat_id,
        message_id
    )

album_buffer = {}
album_timers = {}

@bot.message_handler(content_types=['photo','video','document','audio'])
def handle_media(message):

    save_user(message.from_user)

    media_group_id = message.media_group_id
    

    # ---------- detect file ----------
    if message.content_type == "photo":
        file_id = message.photo[-1].file_id
        file_size = message.photo[-1].file_size
        file_type = "photo"

    elif message.content_type == "video":
        file_id = message.video.file_id
        file_size = message.video.file_size
        file_type = "video"

    elif message.content_type == "document":
        file_id = message.document.file_id
        file_size = message.document.file_size
        file_type = "document"

    elif message.content_type == "audio":
        file_id = message.audio.file_id
        file_size = message.audio.file_size
        file_type = "audio"

    else:
        return

    caption = message.caption
    user_id = message.from_user.id

    # =========================
    # ALBUM DETECTED
    # =========================
    if media_group_id:

        if media_group_id not in album_buffer:
            album_buffer[media_group_id] = []

        album_buffer[media_group_id].append(
            (message.chat.id, user_id, file_id, file_type, caption, file_size, media_group_id)
        )

        # Cancel old timer
        if media_group_id in album_timers:
            album_timers[media_group_id].cancel()

        # SAFE finalize function
        def finalize_album(mgid):
            items = album_buffer.pop(mgid, [])
            if not items:
                return

            chat_id = items[0][0]
            user_id = items[0][1]

            with session_lock:
                if user_id not in user_sessions:
                    msg = bot.send_message(chat_id, "📥 Saving files...")
                    user_sessions[user_id] = {
                        "total": 0,
                        "saved": 0,
                        "duplicate": 0,
                        "photo": 0,
                        "video": 0,
                        "document": 0,
                        "audio": 0,
                        "message_id": msg.message_id
                    }

            for _, u_id, file_id, file_type, caption, file_size, mgid in items:
                result = save_media(u_id, file_id, file_type, caption, file_size, mgid)

                if result:
                    user_sessions[user_id]["saved"] += 1
                    user_sessions[user_id][file_type] += 1
                else:
                    user_sessions[user_id]["duplicate"] += 1

            user_sessions[user_id]["total"] += len(items)

            if user_id in user_timers:
                user_timers[user_id].cancel()

            t2 = threading.Timer(
                2.0,
                finalize_user_upload,
                args=(user_id, chat_id)
            )
            user_timers[user_id] = t2
            t2.start()
        # Start timer properly with argument
        t = threading.Timer(1.2, finalize_album, args=(media_group_id,))
        album_timers[media_group_id] = t
        t.start()
    else:
        # single media
        chat_id = message.chat.id
        
        is_saved = save_media(user_id, file_id, file_type, caption, file_size, None)

        with session_lock:
            if user_id not in user_sessions:
                msg = bot.send_message(chat_id, "📥 Saving files...")
                user_sessions[user_id] = {
                    "total": 0,
                    "saved": 0,
                    "duplicate": 0,
                    "photo": 0,
                    "video": 0,
                    "document": 0,
                    "audio": 0,
                    "message_id": msg.message_id
                }
            user_sessions[user_id]["total"] += 1

            if is_saved:
                user_sessions[user_id]["saved"] += 1
                user_sessions[user_id][file_type] += 1
            else:
                user_sessions[user_id]["duplicate"] += 1

            # reset timer
            if user_id in user_timers:
                user_timers[user_id].cancel()
            t = threading.Timer(
                2.0,
                finalize_user_upload,
                args=(user_id, message.chat.id)
            )
            user_timers[user_id] = t
            t.start()

        
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
        total_size = format_size(get_total_storage())
        bot.edit_message_text(
            f"📊 Bot Statistics\n\n"
            f"👥 Total Users: {total_users}\n"
            f"📦 Total Files: {total_files}\n"
            f"💾 Total Storage Used: {total_size}",
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
    elif data == "admin_analytics":

        if call.from_user.id != ADMIN_ID:
            bot.answer_callback_query(call.id, "Unauthorized")
            return

        conn = get_connection()
        cur = conn.cursor()

        # Total storage
        cur.execute("SELECT COALESCE(SUM(file_size),0) FROM stored_media")
        total_storage = cur.fetchone()[0]

        # Storage by type
        cur.execute("""
            SELECT file_type, COALESCE(SUM(file_size),0)
            FROM stored_media
            GROUP BY file_type
        """)
        type_data = cur.fetchall()

        # Top 5 users
        cur.execute("""
            SELECT user_id, COALESCE(SUM(file_size),0) as total
            FROM stored_media
            GROUP BY user_id
            ORDER BY total DESC
            LIMIT 5
        """)
        top_users = cur.fetchall()

        # Last 7 days uploads
        cur.execute("""
            SELECT saved_at::date, COUNT(*)
            FROM stored_media
            WHERE saved_at >= CURRENT_DATE - INTERVAL '7 days'
            GROUP BY saved_at::date
            ORDER BY saved_at::date
        """)
        daily_uploads = cur.fetchall()

        cur.close()
        conn.close()

        # Format results
        text = "📊 STORAGE ANALYTICS\n\n"

        text += f"📦 Total Storage Used: {format_size(total_storage)}\n\n"

        text += "📁 Storage by Type:\n"
        for file_type, size in type_data:
            text += f"• {file_type.capitalize()}: {format_size(size)}\n"

        text += "\n👑 Top 5 Users:\n"
        for user_id, size in top_users:
            text += f"• {user_id} → {format_size(size)}\n"

        text += "\n📅 Uploads Last 7 Days:\n"
        for date, count in daily_uploads:
            text += f"• {date} → {count} files\n"

        markup = InlineKeyboardMarkup()
        markup.add(InlineKeyboardButton("🔙 Back", callback_data="admin_panel"))

        bot.edit_message_text(
            text,
            call.message.chat.id,
            call.message.message_id,
            reply_markup=markup
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
    elif data.startswith("admin_userlist_"):
        if call.from_user.id != ADMIN_ID:
            bot.answer_callback_query(call.id, "Unauthorized")
            return

        page = int(data.split("_")[-1])
        users = get_users_page(page)

        text = f"👥 Select User (Page {page+1})"

        markup = InlineKeyboardMarkup()

        for user_id, username in users:

            if username:
                label = f"@{username}"
            else:
                label = f"User {user_id}"

            markup.add(
                InlineKeyboardButton(
                    label,
                    callback_data=f"admin_openuser_{user_id}"
                )
            )

        if page > 0:
            markup.add(
                InlineKeyboardButton("⬅ Prev", callback_data=f"admin_userlist_{page-1}")
            )

        if len(users) == USERS_PER_PAGE:
            markup.add(
                InlineKeyboardButton("Next ➡", callback_data=f"admin_userlist_{page+1}")
            )

        markup.add(InlineKeyboardButton("🔙 Back", callback_data="admin_panel"))

        bot.edit_message_text(
            text,
            call.message.chat.id,
            call.message.message_id,
            reply_markup=markup
        )
    elif data.startswith("admin_openuser_"):
        if call.from_user.id != ADMIN_ID:
            return

        user_id = int(data.split("_")[-1])

        total = get_total_files(user_id)
        cats = get_category_counts(user_id)

        text = (
            f"👤 User ID: {user_id}\n\n"
            f"📦 Total Files: {total}\n"
            f"💾 Storage Used: {format_size(get_storage_used(user_id))}\n\n"
            f"📷 Photos: {cats.get('photo',0)}\n"
            f"🎥 Videos: {cats.get('video',0)}\n"
            f"📄 Documents: {cats.get('document',0)}\n"
            f"🎵 Audio: {cats.get('audio',0)}"
        )

        markup = InlineKeyboardMarkup()

        markup.add(
            InlineKeyboardButton(
                "📂 View Files",
                callback_data=f"admin_userfiles_{user_id}"
            )
        )
        markup.add(
            InlineKeyboardButton(
                "📤 Send Media",
                callback_data=f"admin_sendmedia_{user_id}"
            )
        )

        markup.add(
            InlineKeyboardButton(
                "🔙 Back to users",
                callback_data="admin_userlist_0"
            )
        )

        bot.edit_message_text(
            text,
            call.message.chat.id,
            call.message.message_id,
            reply_markup=markup
        )
    elif data.startswith("admin_userfiles_"):
        if call.from_user.id != ADMIN_ID:
            return

        user_id = int(data.split("_")[-1])

        text = "📂 Select category"

        bot.edit_message_text(
            text,
            call.message.chat.id,
            call.message.message_id,
            reply_markup=category_menu(user_id)
        )
    elif data.startswith("admin_sendmedia_"):
        if call.from_user.id != ADMIN_ID:
            return

        user_id = int(data.split("_")[-1])

        admin_send_state[call.from_user.id] = {
            "target_user": user_id
        }
        markup = InlineKeyboardMarkup()
        markup.add(
            InlineKeyboardButton("🚀 Fast", callback_data="speed_fast"),
            InlineKeyboardButton("⚖ Safe", callback_data="speed_safe"),
        )
        markup.add(
            InlineKeyboardButton("🐢 Ultra Safe", callback_data="speed_ultra")
        )

        bot.send_message(
            call.message.chat.id,
            "⚙ Select sending speed:",
            reply_markup=markup
        )
    elif data == "admin_cancel_send":

        if call.from_user.id in admin_active_jobs:
            admin_active_jobs[call.from_user.id]["cancel"] = True
    elif data.startswith("speed_"):

        if call.from_user.id not in admin_send_state:
            return

        speed_map = {
            "speed_fast": 0.3,
            "speed_safe": 1,
            "speed_ultra": 2
        }

        selected_speed = speed_map.get(data, 1)

        admin_send_state[call.from_user.id]["speed"] = selected_speed

        target_user = admin_send_state[call.from_user.id]["target_user"]

        conn = get_connection()
        cur = conn.cursor()
        cur.execute("""
            SELECT group_id, group_title FROM user_send_groups
            WHERE target_user = %s
            ORDER BY id DESC
            LIMIT 5
        """, (target_user,))
        groups = cur.fetchall()
        cur.close()
        conn.close()

        markup = InlineKeyboardMarkup()

        # Add previous groups as buttons
        for g_id, g_title in groups:
            markup.add(
                InlineKeyboardButton(
                    f"📂 {g_title}",
                    callback_data=f"use_group_{g_id}"
                )
            )
        # Add manual entry option
        markup.add(
            InlineKeyboardButton("➕ Use New Group", callback_data="enter_new_group")
        )

        bot.send_message(
            call.message.chat.id,
            "📤 Select a previous group or add new one:",
            reply_markup=markup
        )
    elif data.startswith("use_group_"):

        group_id = int(data.split("_")[-1])

        if call.from_user.id not in admin_send_state:
            bot.answer_callback_query(call.id, "Session expired")
            return

        admin_send_state[call.from_user.id]["group_id"] = group_id

        markup = InlineKeyboardMarkup()
        markup.add(
            InlineKeyboardButton(
                "🚀 SEND ALL MEDIA NOW",
                callback_data="admin_confirm_send"
            )
        )

        bot.send_message(
            call.message.chat.id,
            f"✅ Group selected: `{group_id}`\n\nPress the button below to start sending.",
            parse_mode="Markdown",
            reply_markup=markup
        )


    elif data == "enter_new_group":

        bot.send_message(
            call.message.chat.id,
            "📩 Forward ANY message from target group\nOR send group ID."
        )
    elif data == "admin_confirm_send":

        if call.from_user.id not in admin_send_state:
            bot.answer_callback_query(call.id, "Session expired")
            return

        state = admin_send_state[call.from_user.id]

        user_id = state["target_user"]
        group_id = state.get("group_id")
        speed = state.get("speed", 1)

        if not group_id:
            bot.answer_callback_query(call.id, "Send group first")
            return

        try:
            chat = bot.get_chat(group_id)
            group_title = chat.title
        except:
            group_title = str(group_id)

        bot.send_message(call.message.chat.id, "📥 Preparing job...")

        conn = get_connection()
        cur = conn.cursor()

        # Create job FIRST
        cur.execute("""
            INSERT INTO send_jobs (admin_id, target_user, group_id)
            VALUES (%s,%s,%s)
            RETURNING id
        """, (call.from_user.id, user_id, group_id))

        job_id = cur.fetchone()[0]

        # Save group history
        cur.execute("""
            INSERT INTO user_send_groups (target_user, group_id, group_title)
            VALUES (%s, %s, %s)
            ON CONFLICT (target_user, group_id)
            DO UPDATE SET group_title = EXCLUDED.group_title
        """, (user_id, group_id, group_title))

        conn.commit()
        cur.close()
        conn.close()

        with job_status_lock:
            job_status_cache[job_id] = "running"

        job_queue.put({
            "job_id": job_id,
            "group_id": group_id,
            "group_title": group_title,
            "target_user": user_id,
            "speed": speed,
            "chat_id": call.message.chat.id
        })

        start_worker()

        bot.send_message(
            call.message.chat.id,
            "🚀 Sending started."
        )

        admin_send_state.pop(call.from_user.id, None)
    
    elif data == "menu_files":
        bot.edit_message_text(
            "📂 Select Category",
            call.message.chat.id,
            call.message.message_id,
            reply_markup=category_menu(call.from_user.id)
        )
    elif data.startswith("pause_job_"):

        job_id = int(data.split("_")[-1])

        with job_status_lock:
            job_status_cache[job_id] = "paused"

        job = live_jobs.get(job_id)
        if not job:
            return

        sent = job["sent"]
        total = job["total"]
        group_title = job["group_title"]
        chat_id = job["chat_id"]
        message_id = job["message_id"]

        percent = int((sent / total) * 100) if total else 0
        bar = build_progress_bar(percent)

        markup = InlineKeyboardMarkup()
        markup.add(
            InlineKeyboardButton("🔁 Change Group", callback_data=f"change_group_{job_id}"),
            InlineKeyboardButton("▶ Resume", callback_data=f"resume_job_{job_id}")
        )

        text = (
            f"⏸ Sending to: {group_title} (Paused)\n\n"
            f"[{bar}] {percent}%\n\n"
            f"📊 {sent} / {total} files sent"
        )

        bot.edit_message_text(text, chat_id, message_id, reply_markup=markup)
    elif data.startswith("change_group_"):

        job_id = int(data.split("_")[-1])

        if job_id not in live_jobs:
            bot.answer_callback_query(call.id, "Job not found")
            return

        admin_send_state[call.from_user.id] = {
            "changing_job": job_id
        }

        bot.send_message(
            call.message.chat.id,
            "📩 Forward ANY message from new group\nOR send new group ID."
        )
    elif data.startswith("resume_job_"):

        job_id = int(data.split("_")[-1])

        with job_status_lock:
            job_status_cache[job_id] = "running"

        job = live_jobs.get(job_id)

        if job:
            sent = job["sent"]
            total = job["total"]
            group_title = job["group_title"]
            chat_id = job["chat_id"]
            message_id = job["message_id"]

            percent = int((sent / total) * 100) if total else 0
            bar = build_progress_bar(percent)

            text = (
                f"▶ Sending to: {group_title}\n\n"
                f"[{bar}] {percent}%\n\n"
                f"📊 {sent} / {total} files sent"
            )

            bot.edit_message_text(text, chat_id, message_id)

        bot.answer_callback_query(call.id, "Resumed")
    
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
def resume_jobs():
    conn = get_connection()
    cur = conn.cursor()

    cur.execute("""
        SELECT id, target_user, group_id
        FROM send_jobs
        WHERE is_active = TRUE
    """)
    jobs = cur.fetchall()

    cur.close()
    conn.close()

    for job_id, target_user, group_id in jobs:

        try:
            chat = bot.get_chat(group_id)
            group_title = chat.title
        except:
            group_title = str(group_id)

        with job_status_lock:
            job_status_cache[job_id] = "running"

        job_queue.put({
            "job_id": job_id,
            "group_id": group_id,
            "group_title": group_title,
            "target_user": target_user,
            "speed": 1,
            "chat_id": ADMIN_ID
        })

    if not job_queue.empty():
        start_worker()
def start_worker():
    global worker_running

    if worker_running:
        return

    worker_running = True
    threading.Thread(target=queue_worker).start()
def build_progress_bar(percent, length=20):
    filled = int(length * percent / 100)
    empty = length - filled
    return "█" * filled + "░" * empty

def queue_worker():
    global worker_running

    while True:
        try:
            job = job_queue.get(timeout=1)
        except:
            break

        job_id = job["job_id"]
        target_user = job["target_user"]
        delay = job.get("speed", 1)
        chat_id = job["chat_id"]
        pending_album = []
        pending_mgid = None
        pending_last_id = None
        # Get group dynamically
        group_id = job["group_id"]
        group_title = job["group_title"]

        # Get total count
        conn = get_connection()
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM stored_media WHERE user_id=%s", (target_user,))
        total = cur.fetchone()[0]

        cur.execute("SELECT last_sent_id FROM send_jobs WHERE id=%s", (job_id,))
        last_sent_id = cur.fetchone()[0]

        cur.close()
        conn.close()

        progress_message = bot.send_message(chat_id, "📤 Sending started...")

        live_jobs[job_id] = {
            "sent": 0,
            "total": total,
            "group_id": group_id,
            "group_title": group_title,
            "message_id": progress_message.message_id,
            "chat_id": chat_id
        }

        sent = 0
        BATCH_SIZE = 200

        while True:

            # Pause handling
            while job_status_cache.get(job_id) == "paused":
                time.sleep(1)

            conn = get_connection()
            cur = conn.cursor()
            cur.execute("""
                SELECT id, file_id, file_type, caption, media_group_id
                FROM stored_media
                WHERE user_id=%s AND id > %s
                ORDER BY id ASC
                LIMIT %s
            """, (target_user, last_sent_id, BATCH_SIZE))

            rows = cur.fetchall()
            cur.close()
            conn.close()

            if not rows:
                break

            i = 0
            while i < len(rows):

                media_id, file_id, file_type, caption, mgid = rows[i]
                current_group = live_jobs[job_id]["group_id"]

                try:
                    if pending_mgid and mgid != pending_mgid:
                        # Send leftover pending album first
                        bot.send_media_group(current_group, pending_album)
                        sent += len(pending_album)
                        last_sent_id = pending_last_id

                        pending_album = []
                        pending_mgid = None
                        pending_last_id = None

                    # ================= CONTINUE PENDING ALBUM =================
                    if pending_mgid and mgid == pending_mgid:

                        if file_type == "photo":
                            pending_album.append(
                                InputMediaPhoto(file_id, caption=None)
                            )
                        elif file_type == "video":
                            pending_album.append(
                                InputMediaVideo(file_id, caption=None)
                            )

                        pending_last_id = media_id
                        i += 1

                        if len(pending_album) == 10:
                            bot.send_media_group(current_group, pending_album)
                            sent += len(pending_album)
                            last_sent_id = pending_last_id

                            pending_album = []
                            pending_mgid = None
                            pending_last_id = None

                        continue

                    # ================= NEW ALBUM =================
                    if mgid:

                        album_items = []
                        album_last_id = media_id

                        if file_type == "photo":
                            album_items.append(
                                InputMediaPhoto(file_id, caption=caption)
                            )
                        elif file_type == "video":
                            album_items.append(
                                InputMediaVideo(file_id, caption=caption)
                            )

                        i += 1

                        while i < len(rows) and rows[i][4] == mgid and len(album_items) < 10:
                            m_id, f_id, f_type, cap, _ = rows[i]

                            if f_type == "photo":
                                album_items.append(InputMediaPhoto(f_id))
                            elif f_type == "video":
                                album_items.append(InputMediaVideo(f_id))

                            album_last_id = m_id
                            i += 1
                        # If batch ended but album not finished → store pending
                        # Check if album might continue in next batch
                        if i == len(rows):

                            conn = get_connection()
                            cur = conn.cursor()
                            cur.execute("""
                                SELECT media_group_id
                                FROM stored_media
                                WHERE user_id=%s AND id > %s
                                ORDER BY id ASC
                                LIMIT 1
                            """, (target_user, album_last_id))

                            next_row = cur.fetchone()
                            cur.close()
                            conn.close()

                            if next_row and next_row[0] == mgid:
                                # Album continues
                                pending_album = album_items
                                pending_mgid = mgid
                                pending_last_id = album_last_id
                                break
                            else:
                                # Album finished
                                bot.send_media_group(current_group, album_items)
                                sent += len(album_items)
                                last_sent_id = album_last_id
                        else:
                            # Normal case (not end of batch)
                            bot.send_media_group(current_group, album_items)
                            sent += len(album_items)
                            last_sent_id = album_last_id
                        # Otherwise send immediately
                        bot.send_media_group(current_group, album_items)
                        sent += len(album_items)
                        last_sent_id = album_last_id

                    # ================= SINGLE =================
                    else:

                        bot.send_photo(current_group, file_id, caption=caption) \
                            if file_type == "photo" else \
                        bot.send_video(current_group, file_id, caption=caption) \
                            if file_type == "video" else \
                        bot.send_document(current_group, file_id, caption=caption) \
                            if file_type == "document" else \
                        bot.send_audio(current_group, file_id, caption=caption)

                        sent += 1
                        last_sent_id = media_id
                        i += 1

                    # Update DB every 10 files
                    if sent % 10 == 0:
                        conn = get_connection()
                        cur = conn.cursor()
                        cur.execute("""
                            UPDATE send_jobs
                            SET last_sent_id=%s
                            WHERE id=%s
                        """, (last_sent_id, job_id))
                        conn.commit()
                        cur.close()
                        conn.close()

                    time.sleep(delay)

                except telebot.apihelper.ApiTelegramException as e:
                    if e.error_code == 429:
                        retry = int(e.result_json["parameters"]["retry_after"])
                        time.sleep(retry)
                    else:
                        print("Telegram error:", e)
                        time.sleep(2)
        
        if pending_album:
            bot.send_media_group(current_group, pending_album)
            sent += len(pending_album)
            last_sent_id = pending_last_id

            conn = get_connection()
            cur = conn.cursor()
            cur.execute("""
                UPDATE send_jobs
                SET last_sent_id=%s
                WHERE id=%s
            """, (last_sent_id, job_id))
            conn.commit()
            cur.close()
            conn.close()
        # Now mark inactive
        conn = get_connection()
        cur = conn.cursor()
        cur.execute("""
            UPDATE send_jobs
            SET is_active = FALSE
            WHERE id = %s
        """, (job_id,))
        conn.commit()
        cur.close()
        conn.close()
        # sent = 0        
        bot.edit_message_text(
            f"✅ Sending completed.\n\n{sent} files sent.",
            chat_id,
            live_jobs[job_id]["message_id"]
        )

        job_queue.task_done()

    worker_running = False
        # reuse your sender logic here
# ================= START BOT ================= #

if __name__ == "__main__":
    init_db()
    resume_jobs()   # ADD THIS LINE
    bot.remove_webhook()
    print("Bot is running...")
    bot.infinity_polling(skip_pending=True)
