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

FILES_PER_PAGE = 5
media_groups = {}
bot = telebot.TeleBot(BOT_TOKEN)

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
            UNIQUE(user_id, file_id)
        );
    """)

    cur.execute("CREATE INDEX IF NOT EXISTS idx_user_media ON stored_media(user_id);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_saved_at ON stored_media(saved_at);")

    conn.commit()
    cur.close()
    conn.close()

# ================= DB HELPERS ================= #

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

def save_media(user_id, file_id, file_type, caption):
    conn = get_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            "INSERT INTO stored_media (user_id, file_id, file_type, caption) VALUES (%s, %s, %s, %s)",
            (user_id, file_id, file_type, caption)
        )
        conn.commit()
        cur.close()
        conn.close()
        return True  # saved
    except psycopg2.errors.UniqueViolation:
        conn.rollback()
        cur.close()
        conn.close()
        return False  # duplicate

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
    total = get_total_files(user_id)
    return f"📦 Cloud Vault\n\n📊 Your Storage:\n• Total Files: {total}\n\nChoose an option:"

def dashboard_markup():
    markup = InlineKeyboardMarkup()
    markup.add(InlineKeyboardButton("📁 My Files", callback_data="menu_files"))
    return markup

@bot.message_handler(commands=['start'])
def start(message):
    save_user(message.from_user)
    bot.send_message(
        message.chat.id,
        dashboard_text(message.from_user.id),
        reply_markup=dashboard_markup()
    )

# ================= AUTO SAVE ================= #

import threading
import time

media_groups = {}

def process_single_media(message):
    file_type = message.content_type
    caption = message.caption

    if file_type == "photo":
        file_id = message.photo[-1].file_id
    elif file_type == "video":
        file_id = message.video.file_id
    elif file_type == "document":
        file_id = message.document.file_id
    elif file_type == "audio":
        file_id = message.audio.file_id
    else:
        return

    save_media(message.from_user.id, file_id, file_type, caption)


def process_album(media_group_id, user_id):
    time.sleep(1)

    messages = media_groups.pop(media_group_id, [])

    if not messages:
        return

    saved_count = 0
    duplicate_count = 0

    for msg in messages:
        file_type = msg.content_type
        caption = msg.caption

        if file_type == "photo":
            file_id = msg.photo[-1].file_id
        elif file_type == "video":
            file_id = msg.video.file_id
        elif file_type == "document":
            file_id = msg.document.file_id
        elif file_type == "audio":
            file_id = msg.audio.file_id
        else:
            continue

        result = save_media(user_id, file_id, file_type, caption)

        if result:
            saved_count += 1
        else:
            duplicate_count += 1

    total = get_total_files(user_id)

    chat_id = messages[0].chat.id

    if duplicate_count > 0:
        bot.send_message(
            chat_id,
            f"📦 Album Processed\n\n"
            f"Total Media: {len(messages)}\n"
            f"✅ Saved: {saved_count}\n"
            f"♻️ Duplicates: {duplicate_count}\n\n"
            f"📦 Total Files: {total}"
        )
    else:
        bot.send_message(
            chat_id,
            f"✅ Album Saved Successfully\n"
            f"📦 Total Files: {total}"
        )


@bot.message_handler(content_types=['photo', 'video', 'document', 'audio'])
def handle_media(message):
    save_user(message.from_user)

    # If album
    if message.media_group_id:
        group_id = message.media_group_id

        if group_id not in media_groups:
            media_groups[group_id] = []

            # Start background thread for album processing
            threading.Thread(
                target=process_album,
                args=(group_id, message.from_user.id)
            ).start()

        media_groups[group_id].append(message)

    else:
        # Single media
        def delayed_save():
            time.sleep(1)

            file_type = message.content_type
            caption = message.caption

            if file_type == "photo":
                file_id = message.photo[-1].file_id
            elif file_type == "video":
                file_id = message.video.file_id
            elif file_type == "document":
                file_id = message.document.file_id
            elif file_type == "audio":
                file_id = message.audio.file_id
            else:
                return

            result = save_media(message.from_user.id, file_id, file_type, caption)
            total = get_total_files(message.from_user.id)

            if result:
                bot.send_message(
                    message.chat.id,
                    f"✅ Saved Successfully\n📦 Total Files: {total}"
                )
            else:
                bot.send_message(
                    message.chat.id,
                    f"♻️ Duplicate Media Detected\n\n"
                    f"Total Media: 1\n"
                    f"✅ Saved: 0\n"
                    f"♻️ Duplicates: 1\n\n"
                    f"📦 Total Files: {total}"
                )

        threading.Thread(target=delayed_save).start()

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
            reply_markup=dashboard_markup()
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
