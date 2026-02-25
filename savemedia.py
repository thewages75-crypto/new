# ================= CORE_BOOT =================

from time import time

import telebot
import psycopg2
import os
ADMIN_ID = 8305774350   # ← PUT YOUR TELEGRAM ID
BOT_TOKEN = "8606303101:AAGw3fHdI5jpZOOuFCSoHlPKb1Urj4Oidk4"
DATABASE_URL = os.getenv("DATABASE_URL")
admin_jobs = {}
bot = telebot.TeleBot(BOT_TOKEN)
admin_send_state = {}
def get_connection():
    return psycopg2.connect(DATABASE_URL)


def init_db():

    conn = get_connection()
    cur = conn.cursor()

    # ---- USERS TABLE ----
    cur.execute("""
    CREATE TABLE IF NOT EXISTS users(
        user_id BIGINT PRIMARY KEY,
        username TEXT,
        joined_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """)

    # ---- MEDIA TABLE (ALBUM READY) ----
    cur.execute("""
    CREATE TABLE IF NOT EXISTS stored_media(
        id SERIAL PRIMARY KEY,
        user_id BIGINT,
        file_id TEXT,
        file_type TEXT,
        caption TEXT,
        file_size BIGINT,
        media_group_id TEXT,
        saved_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """)

    conn.commit()
    cur.close()
    conn.close()
# ================= USER_TRACKER =================

def save_user(user):

    conn = get_connection()
    cur = conn.cursor()

    cur.execute("""
        INSERT INTO users(user_id, username)
        VALUES(%s,%s)
        ON CONFLICT(user_id) DO NOTHING
    """, (user.id, user.username))

    conn.commit()
    cur.close()
    conn.close()
# ================= MEDIA_STORAGE_ENGINE =================

def save_media(user_id, file_id, file_type, caption, file_size, media_group_id):

    conn = get_connection()
    cur = conn.cursor()

    cur.execute("""
        INSERT INTO stored_media
        (user_id,file_id,file_type,caption,file_size,media_group_id)
        VALUES(%s,%s,%s,%s,%s,%s)
        ON CONFLICT DO NOTHING
    """, (user_id,file_id,file_type,caption,file_size,media_group_id))

    conn.commit()
    cur.close()
    conn.close()


@bot.message_handler(content_types=['photo','video','document','audio'])
def media_handler(message):

    # always track user
    save_user(message.from_user)

    caption = message.caption
    media_group_id = message.media_group_id

    file_id = None
    file_size = 0
    file_type = None

    # ---- SAFE TELEGRAM MEDIA DETECTION ----
    if message.photo:
        file_type="photo"
        file_id=message.photo[-1].file_id
        file_size=message.photo[-1].file_size

    elif message.video:
        file_type="video"
        file_id=message.video.file_id
        file_size=message.video.file_size

    elif message.document:
        file_type="document"
        file_id=message.document.file_id
        file_size=message.document.file_size

    elif message.audio:
        file_type="audio"
        file_id=message.audio.file_id
        file_size=message.audio.file_size

    # ignore if nothing valid
    if not file_id:
        return

    save_media(
        message.from_user.id,
        file_id,
        file_type,
        caption,
        file_size,
        media_group_id
    )

    bot.reply_to(message,"✅ Saved")
# ================= UPLOAD_SESSION_MANAGER =================

import threading

user_sessions = {}
user_timers = {}
session_lock = threading.Lock()


def finalize_upload(user_id, chat_id):

    session = user_sessions.pop(user_id, None)
    user_timers.pop(user_id, None)

    if not session:
        return

    bot.edit_message_text(
        f"✅ Upload complete\n\nSaved files: {session['count']}",
        chat_id,
        session["msg_id"]
    )
# ================= USER_DASHBOARD_UI =================

from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton


def get_total_files(user_id):

    conn=get_connection()
    cur=conn.cursor()

    cur.execute("SELECT COUNT(*) FROM stored_media WHERE user_id=%s",(user_id,))
    count=cur.fetchone()[0]

    cur.close()
    conn.close()

    return count


def get_category_counts(user_id):

    conn=get_connection()
    cur=conn.cursor()

    cur.execute("""
        SELECT file_type,COUNT(*)
        FROM stored_media
        WHERE user_id=%s
        GROUP BY file_type
    """,(user_id,))

    rows=cur.fetchall()

    cur.close()
    conn.close()

    return dict(rows)


def dashboard_markup(user_id):

    markup=InlineKeyboardMarkup()

    markup.add(
        InlineKeyboardButton("📁 My Files",callback_data="menu_files")
    )

    if user_id==ADMIN_ID:
        markup.add(
            InlineKeyboardButton("🛠 Admin Panel",callback_data="admin_panel")
        )

    return markup
def category_menu(user_id):

    counts=get_category_counts(user_id)

    markup=InlineKeyboardMarkup()

    markup.add(InlineKeyboardButton(f"📷 Photos ({counts.get('photo',0)})",callback_data="cat_photo"))
    markup.add(InlineKeyboardButton(f"🎥 Videos ({counts.get('video',0)})",callback_data="cat_video"))
    markup.add(InlineKeyboardButton(f"📄 Documents ({counts.get('document',0)})",callback_data="cat_document"))
    markup.add(InlineKeyboardButton(f"🎵 Audio ({counts.get('audio',0)})",callback_data="cat_audio"))

    markup.add(InlineKeyboardButton("🔙 Back",callback_data="menu_main"))

    return markup
# @bot.message_handler(commands=['start'])
# def start(msg):

#     save_user(msg.from_user)

#     user_id = message.from_user.id
#     chat_id = message.chat.id

#     with session_lock:
#         if user_id not in user_sessions:

#             processing = bot.send_message(chat_id,"⏳ Processing upload...")

#             user_sessions[user_id] = {
#                 "count":0,
#                 "msg_id":processing.message_id
#             }

#     session = user_sessions[user_id]
#     session["count"] += 1


#     # restart timer (album-safe)
#     if user_id in user_timers:
#         user_timers[user_id].cancel()

#     timer = threading.Timer(
#         1.3,
#         finalize_upload,
#         args=(user_id,chat_id)
#     )

#     user_timers[user_id]=timer
#     timer.start()
@bot.message_handler(commands=['start'])
def start(msg):

    save_user(msg.from_user)

    total=get_total_files(msg.from_user.id)

    bot.send_message(
        msg.chat.id,
        f"📦 Cloud Vault\n\nStored files: {total}",
        reply_markup=dashboard_markup(msg.from_user.id)
    )
# ================= ADMIN_PROGRESS_SENDER =================

@bot.message_handler(func=lambda m: m.from_user and m.from_user.id in admin_send_state)
def receive_group(message):

    group_id=int(message.text)
    user_id=admin_send_state.pop(message.from_user.id)

    conn=get_connection()
    cur=conn.cursor()

    cur.execute("""
        SELECT file_id,file_type,caption,media_group_id
        FROM stored_media
        WHERE user_id=%s
        ORDER BY id
    """,(user_id,))

    rows=cur.fetchall()

    cur.close()
    conn.close()

    total=len(rows)

    from telebot.types import InlineKeyboardMarkup,InlineKeyboardButton

    markup=InlineKeyboardMarkup()
    markup.add(InlineKeyboardButton("🛑 CANCEL",callback_data="cancel_send"))

    progress=bot.send_message(
        message.chat.id,
        f"🚀 Sending media...\n0/{total}",
        reply_markup=markup
    )

    progress_id=progress.message_id
    admin_jobs[message.from_user.id]={"cancel":False}
    # ---- rebuild albums ----
    from collections import defaultdict
    from telebot.types import InputMediaPhoto,InputMediaVideo,InputMediaDocument,InputMediaAudio

    groups=defaultdict(list)

    for file_id,file_type,caption,mgid in rows:
        groups[mgid].append((file_id,file_type,caption))

    sent=0
    if admin_jobs.get(message.from_user.id,{}).get("cancel"):

        bot.edit_message_text(
            "🛑 Sending cancelled",
            message.chat.id,
            progress_id
        )

        admin_jobs.pop(message.from_user.id,None)
        return
    for mgid,items in groups.items():

        # SINGLE
        if mgid is None or len(items)==1:

            f,t,c=items[0]

            if t=="photo": bot.send_photo(group_id,f,caption=c)
            elif t=="video": bot.send_video(group_id,f,caption=c)
            elif t=="document": bot.send_document(group_id,f,caption=c)
            elif t=="audio": bot.send_audio(group_id,f,caption=c)

            sent+=1

        # ALBUM
        else:

            batch=[]

            for i,(f,t,c) in enumerate(items):

                cap=c if i==0 else None

                if t=="photo": batch.append(InputMediaPhoto(f,caption=cap))
                elif t=="video": batch.append(InputMediaVideo(f,caption=cap))
                elif t=="document": batch.append(InputMediaDocument(f,caption=cap))
                elif t=="audio": batch.append(InputMediaAudio(f,caption=cap))

            bot.send_media_group(group_id,batch)

            sent+=len(items)

        # ---- UPDATE PROGRESS EVERY 5 FILES ----
        if sent%5==0 or sent==total:

            percent=int(sent*100/total)

            try:
                bot.edit_message_text(
                    f"🚀 Sending media...\n{sent}/{total}\n{percent}%",
                    message.chat.id,
                    progress_id
                )
            except:
                passa

        time.sleep(0.05)   # anti-flood safety

    bot.edit_message_text(
        f"✅ Done!\nSent {total} files",
        message.chat.id,
        progress_id
    )
    admin_jobs.pop(message.from_user.id,None)
    
# ================= ADMIN_STATS_ENGINE =================

def get_total_users():

    conn=get_connection()
    cur=conn.cursor()

    cur.execute("SELECT COUNT(*) FROM users")
    total=cur.fetchone()[0]

    cur.close()
    conn.close()

    return total


def get_total_files_all():

    conn=get_connection()
    cur=conn.cursor()

    cur.execute("SELECT COUNT(*) FROM stored_media")
    total=cur.fetchone()[0]

    cur.close()
    conn.close()

    return total
@bot.callback_query_handler(func=lambda call:True)
def callbacks(call):

    if call.data=="menu_main":

        total=get_total_files(call.from_user.id)

        bot.edit_message_text(
            f"📦 Cloud Vault\n\nStored files: {total}",
            call.message.chat.id,
            call.message.message_id,
            reply_markup=dashboard_markup(call.from_user.id)
        )

    elif call.data=="admin_panel":

        if call.from_user.id!=ADMIN_ID:
            return

        users=get_total_users()
        files=get_total_files_all()

        text=(
            "🛠 Admin Panel\n\n"
            f"👥 Total Users: {users}\n"
            f"📦 Total Files: {files}\n\n"
            "Choose option:"
        )

        markup=InlineKeyboardMarkup()

        markup.add(InlineKeyboardButton("👥 View Users",callback_data="admin_users"))
        markup.add(InlineKeyboardButton("🔙 Back",callback_data="menu_main"))

        bot.edit_message_text(
            text,
            call.message.chat.id,
            call.message.message_id,
            reply_markup=markup
        )
    elif call.data=="admin_users":

        users=get_users()

        markup=InlineKeyboardMarkup()

        for uid,username in users:

            label=f"@{username}" if username else f"User {uid}"

            markup.add(
                InlineKeyboardButton(
                    label,
                    callback_data=f"admin_open_{uid}"
                )
            )

        markup.add(InlineKeyboardButton("🔙 Back",callback_data="admin_panel"))

        bot.edit_message_text(
            "Select user",
            call.message.chat.id,
            call.message.message_id,
            reply_markup=markup
        )
    elif call.data.startswith("admin_open_"):

        uid=int(call.data.split("_")[2])

        markup=InlineKeyboardMarkup()

        markup.add(
            InlineKeyboardButton(
                "📤 Send all media to group",
                callback_data=f"admin_send_{uid}"
            )
        )

        markup.add(InlineKeyboardButton("🔙 Back",callback_data="admin_users"))

        bot.edit_message_text(
            f"User {uid}",
            call.message.chat.id,
            call.message.message_id,
            reply_markup=markup
        )
    elif call.data.startswith("admin_send_"):

        if call.from_user.id!=ADMIN_ID:
            return

        uid=int(call.data.split("_")[2])

        bot.send_message(call.message.chat.id,"Send target GROUP ID")

        admin_send_state[call.from_user.id]=uid
    elif call.data=="cancel_send":

        if call.from_user.id in admin_jobs:
            admin_jobs[call.from_user.id]["cancel"]=True

        bot.answer_callback_query(call.id,"Stopping...")
    elif call.data.startswith("cat_"):

        file_type = call.data.split("_")[1]
        page = 0

        rows = get_files(call.from_user.id,file_type,page)

        markup = InlineKeyboardMarkup()

        for media_id,saved_at in rows:

            markup.add(
                InlineKeyboardButton(
                    saved_at.strftime("%d %b %H:%M"),
                    callback_data=f"open_{media_id}"
                )
            )

        markup.add(InlineKeyboardButton("🔙 Back",callback_data="menu_files"))

        bot.edit_message_text(
            f"{file_type.upper()} FILES",
            call.message.chat.id,
            call.message.message_id,
            reply_markup=markup
        )


    elif call.data.startswith("open_"):

        media_id=int(call.data.split("_")[1])

        conn=get_connection()
        cur=conn.cursor()

        cur.execute("""
            SELECT file_id,file_type,caption
            FROM stored_media
            WHERE id=%s AND user_id=%s
        """,(media_id,call.from_user.id))

        row=cur.fetchone()

        cur.close()
        conn.close()

        if not row:
            return

        file_id,file_type,caption=row

        if file_type=="photo":
            bot.send_photo(call.message.chat.id,file_id,caption=caption)
        elif file_type=="video":
            bot.send_video(call.message.chat.id,file_id,caption=caption)
        elif file_type=="document":
            bot.send_document(call.message.chat.id,file_id,caption=caption)
        elif file_type=="audio":
            bot.send_audio(call.message.chat.id,file_id,caption=caption)
# --- ADMIN DB HELPERS ---

def get_users():

    conn=get_connection()
    cur=conn.cursor()

    cur.execute("SELECT user_id,username FROM users ORDER BY joined_at DESC")

    rows=cur.fetchall()

    cur.close()
    conn.close()

    return rows
# ================= USER_FILE_BROWSER =================

FILES_PER_PAGE = 6


def get_files(user_id, file_type, page):

    offset = page * FILES_PER_PAGE

    conn = get_connection()
    cur = conn.cursor()

    cur.execute("""
        SELECT id,saved_at
        FROM stored_media
        WHERE user_id=%s AND file_type=%s
        ORDER BY id DESC
        LIMIT %s OFFSET %s
    """,(user_id,file_type,FILES_PER_PAGE,offset))

    rows = cur.fetchall()

    cur.close()
    conn.close()

    return rows
if __name__ == "__main__":
    init_db()
    print("BOT STARTED")
    bot.infinity_polling()
