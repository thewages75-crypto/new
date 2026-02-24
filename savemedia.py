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
admin_send_state = {}
admin_active_jobs = {}
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
    markup.add(InlineKeyboardButton("👤 View Users", callback_data="admin_userlist_0"))
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
@bot.message_handler(func=lambda m: m.from_user.id == ADMIN_ID and m.from_user.id in admin_send_state)
def admin_group_input(message):

    if message.from_user.id not in admin_send_state:
        return

    state = admin_send_state[message.from_user.id]

    # detect forwarded message
    if message.forward_from_chat:
        group_id = message.forward_from_chat.id
    elif message.forward_sender_name:
        bot.reply_to(message, "❌ Forward directly from the group, not anonymous admin.")
        return
    elif message.text:
        try:
            group_id = int(message.text)
        except:
            bot.reply_to(message, "❌ Invalid group ID")
            return
    else:
        bot.reply_to(message, "❌ Send group ID or forward a message from the group.")
        return

    state["group_id"] = group_id
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

    def safe_edit(text, markup=None):
        try:
            bot.edit_message_text(
                text,
                call.message.chat.id,
                call.message.message_id,
                reply_markup=markup
            )
        except:
            bot.send_message(
                call.message.chat.id,
                text,
                reply_markup=markup
            )

    # ================= MAIN MENU =================

    if data == "menu_main":
        safe_edit(
            dashboard_text(call.from_user.id),
            dashboard_markup(call.from_user.id)
        )

    elif data == "menu_files":
        safe_edit("📂 Select Category", category_menu(call.from_user.id))


    # ================= USER CATEGORY =================

    elif data.startswith("cat_"):
        _, file_type, page = data.split("_")
        text, markup = category_page(call.from_user.id, file_type, int(page))
        safe_edit(text, markup)


    elif data.startswith("get_"):

        _, file_type, media_id = data.split("_")

        conn = get_connection()
        cur = conn.cursor()
        cur.execute(
            "SELECT file_id FROM stored_media WHERE id=%s AND user_id=%s",
            (media_id, call.from_user.id)
        )
        r = cur.fetchone()
        cur.close()
        conn.close()

        if not r:
            return

        file_id = r[0]

        if file_type == "photo":
            bot.send_photo(call.message.chat.id, file_id)
        elif file_type == "video":
            bot.send_video(call.message.chat.id, file_id)
        elif file_type == "document":
            bot.send_document(call.message.chat.id, file_id)
        elif file_type == "audio":
            bot.send_audio(call.message.chat.id, file_id)


    # ================= ADMIN PANEL =================

    elif data == "admin_panel":

        if call.from_user.id != ADMIN_ID:
            return

        safe_edit(admin_panel_text(), admin_panel_markup())


    # ================= ADMIN USER LIST =================

    elif data.startswith("admin_userlist_"):

        if call.from_user.id != ADMIN_ID:
            return

        page = int(data.split("_")[-1])
        users = get_users_page(page)

        markup = InlineKeyboardMarkup()

        for uid, username in users:
            label = f"@{username}" if username else f"User {uid}"
            markup.add(
                InlineKeyboardButton(
                    label,
                    callback_data=f"admin_openuser_{uid}"
                )
            )

        if page > 0:
            markup.add(InlineKeyboardButton("⬅ Prev", callback_data=f"admin_userlist_{page-1}"))

        if len(users) == USERS_PER_PAGE:
            markup.add(InlineKeyboardButton("Next ➡", callback_data=f"admin_userlist_{page+1}"))

        markup.add(InlineKeyboardButton("🔙 Back", callback_data="admin_panel"))

        safe_edit(f"👥 Select User (Page {page+1})", markup)


    # ================= ADMIN OPEN USER =================

    elif data.startswith("admin_openuser_"):

        if call.from_user.id != ADMIN_ID:
            return

        uid = int(data.split("_")[-1])
        cats = get_category_counts(uid)

        text = (
            f"👤 User ID: {uid}\n\n"
            f"📦 Total Files: {get_total_files(uid)}\n"
            f"📷 Photos: {cats.get('photo',0)}\n"
            f"🎥 Videos: {cats.get('video',0)}\n"
            f"📄 Documents: {cats.get('document',0)}\n"
            f"🎵 Audio: {cats.get('audio',0)}"
        )

        markup = InlineKeyboardMarkup()
        markup.add(InlineKeyboardButton("📂 View Files", callback_data=f"admin_userfiles_{uid}"))
        markup.add(InlineKeyboardButton("📤 Send Media", callback_data=f"admin_sendmedia_{uid}"))
        markup.add(InlineKeyboardButton("🔙 Back", callback_data="admin_userlist_0"))

        safe_edit(text, markup)


    # ================= ADMIN VIEW FILES =================

    elif data.startswith("admin_userfiles_"):

        uid = int(data.split("_")[-1])
        cats = get_category_counts(uid)

        markup = InlineKeyboardMarkup()
        markup.add(InlineKeyboardButton(f"📷 Photos ({cats.get('photo',0)})", callback_data=f"admin_cat_{uid}_photo_0"))
        markup.add(InlineKeyboardButton(f"🎥 Videos ({cats.get('video',0)})", callback_data=f"admin_cat_{uid}_video_0"))
        markup.add(InlineKeyboardButton(f"📄 Documents ({cats.get('document',0)})", callback_data=f"admin_cat_{uid}_document_0"))
        markup.add(InlineKeyboardButton(f"🎵 Audio ({cats.get('audio',0)})", callback_data=f"admin_cat_{uid}_audio_0"))
        markup.add(InlineKeyboardButton("🔙 Back", callback_data=f"admin_openuser_{uid}"))

        safe_edit("📂 Select category", markup)


    # ================= ADMIN CATEGORY PAGE =================

    elif data.startswith("admin_cat_"):

        _, uid, file_type, page = data.split("_",3)
        uid = int(uid)
        page = int(page)

        conn = get_connection()
        cur = conn.cursor()

        offset = page * FILES_PER_PAGE

        cur.execute("""
            SELECT file_id, file_type
            FROM stored_media
            WHERE user_id=%s AND file_type=%s
            ORDER BY id DESC
            LIMIT %s OFFSET %s
        """, (uid, file_type, FILES_PER_PAGE, offset))

        rows = cur.fetchall()
        cur.close()
        conn.close()

        markup = InlineKeyboardMarkup()

        for file_id, t in rows:
            markup.add(
                InlineKeyboardButton(
                    "📁 Open File",
                    callback_data=f"admin_get_{uid}_{t}_{file_id}"
                )
            )

        if page > 0:
            markup.add(InlineKeyboardButton("⬅ Prev", callback_data=f"admin_cat_{uid}_{file_type}_{page-1}"))

        if len(rows) == FILES_PER_PAGE:
            markup.add(InlineKeyboardButton("Next ➡", callback_data=f"admin_cat_{uid}_{file_type}_{page+1}"))

        markup.add(InlineKeyboardButton("🔙 Back", callback_data=f"admin_userfiles_{uid}"))

        safe_edit(f"{file_type.upper()} page {page+1}", markup)


    # ================= ADMIN GET FILE =================

    elif data.startswith("admin_get_"):

        _, uid, file_type, file_id = data.split("_",3)

        if file_type == "photo":
            bot.send_photo(call.message.chat.id, file_id)
        elif file_type == "video":
            bot.send_video(call.message.chat.id, file_id)
        elif file_type == "document":
            bot.send_document(call.message.chat.id, file_id)
        elif file_type == "audio":
            bot.send_audio(call.message.chat.id, file_id)


    # ================= ADMIN SEND MEDIA =================

    elif data.startswith("admin_sendmedia_"):

        uid = int(data.split("_")[-1])
        admin_send_state[call.from_user.id] = {"target_user": uid}

        bot.send_message(
            call.message.chat.id,
            "📩 Forward ANY message from the group OR send group ID"
        )


    elif data == "admin_cancel_send":
        if call.from_user.id in admin_active_jobs:
            admin_active_jobs[call.from_user.id]["cancel"] = True
    elif data == "admin_confirm_send":

        if call.from_user.id not in admin_send_state:
            return

        state = admin_send_state[call.from_user.id]
        uid = state["target_user"]
        group_id = state.get("group_id")

        if not group_id:
            bot.answer_callback_query(call.id, "Send group first")
            return

        conn = get_connection()
        cur = conn.cursor()
        cur.execute("""
            SELECT file_id, file_type, caption
            FROM stored_media
            WHERE user_id=%s
            ORDER BY id ASC
        """, (uid,))
        rows = cur.fetchall()
        cur.close()
        conn.close()

        admin_active_jobs[call.from_user.id] = {"cancel": False}

        bot.send_message(call.message.chat.id, f"🚀 Sending {len(rows)} files...")

        from telebot.types import (
            InputMediaPhoto, InputMediaVideo,
            InputMediaDocument, InputMediaAudio
        )
        markup = InlineKeyboardMarkup()
        markup.add(
            InlineKeyboardButton(
                "🛑 CANCEL SENDING",
                callback_data="admin_cancel_send"
            )
        )

        bot.send_message(
            call.message.chat.id,
            f"🚀 Sending {len(rows)} files...",
            reply_markup=markup
        )

    def sender():

        batch = []

        def flush():

            nonlocal batch

            if not batch:
                return

            if len(batch) == 1:
                m = batch[0]

                if isinstance(m, InputMediaPhoto):
                    bot.send_photo(group_id, m.media, caption=m.caption)

                elif isinstance(m, InputMediaVideo):
                    bot.send_video(group_id, m.media, caption=m.caption)

                elif isinstance(m, InputMediaDocument):
                    bot.send_document(group_id, m.media, caption=m.caption)

                elif isinstance(m, InputMediaAudio):
                    bot.send_audio(group_id, m.media, caption=m.caption)

            else:
                bot.send_media_group(group_id, batch)

            batch = []
            time.sleep(1)   # ⭐ one second per send

        for file_id, t, caption in rows:
            if admin_active_jobs[call.from_user.id]["cancel"]:
                bot.send_message(call.message.chat.id, "🛑 Cancelled")
                return

            if t == "photo":
                batch.append(InputMediaPhoto(file_id, caption=caption))
            elif t == "video":
                batch.append(InputMediaVideo(file_id, caption=caption))
            elif t == "document":
                batch.append(InputMediaDocument(file_id, caption=caption))
            elif t == "audio":
                batch.append(InputMediaAudio(file_id, caption=caption))

            if len(batch) == 10:
                flush()

        flush()

        bot.send_message(call.message.chat.id, "✅ Done")

        threading.Thread(target=sender).start()
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
