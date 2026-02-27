# =====================================================
# ===================== CONFIG ========================
# =====================================================

import telebot
from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton
import psycopg2
from psycopg2 import pool
import threading
import time
from datetime import datetime

BOT_TOKEN = "8606303101:AAGw3fHdI5jpZOOuFCSoHlPKb1Urj4Oidk4"
# DATABASE_URL = "YOUR_RAILWAY_POSTGRES_URL"
ADMIN_ID = 8305774350  # replace with your Telegram ID

bot = telebot.TeleBot(BOT_TOKEN, parse_mode="HTML")

# =====================================================
# ================= DATABASE LAYER ====================
# =====================================================

db_pool = pool.SimpleConnectionPool(
    minconn=1,
    maxconn=20,
    dsn=DATABASE_URL
)

def get_connection():
    return db_pool.getconn()

def release_connection(conn):
    db_pool.putconn(conn)

# =====================================================
# ================= TABLE INITIALIZATION ==============
# =====================================================

def init_db():
    conn = get_connection()
    cur = conn.cursor()

    # USERS
    cur.execute("""
    CREATE TABLE IF NOT EXISTS users (
        user_id BIGINT PRIMARY KEY,
        username TEXT,
        first_seen TIMESTAMP DEFAULT NOW(),
        total_files INTEGER DEFAULT 0,
        total_size BIGINT DEFAULT 0
    );
    """)

    # STORED MEDIA
    cur.execute("""
    CREATE TABLE IF NOT EXISTS stored_media (
        id SERIAL PRIMARY KEY,
        user_id BIGINT REFERENCES users(user_id) ON DELETE CASCADE,
        file_id TEXT NOT NULL,
        file_type TEXT NOT NULL,
        caption TEXT,
        media_group_id TEXT,
        file_size BIGINT DEFAULT 0,
        created_at TIMESTAMP DEFAULT NOW()
    );
    """)
        # SEND JOBS TABLE
    cur.execute("""
    CREATE TABLE IF NOT EXISTS send_jobs (
        id SERIAL PRIMARY KEY,
        admin_id BIGINT NOT NULL,
        target_user BIGINT NOT NULL,
        group_id BIGINT NOT NULL,
        last_sent_id INTEGER DEFAULT 0,
        total_files INTEGER DEFAULT 0,
        sent_count INTEGER DEFAULT 0,
        status TEXT DEFAULT 'pending',
        created_at TIMESTAMP DEFAULT NOW(),
        updated_at TIMESTAMP DEFAULT NOW()
    );
    """)

    cur.execute("""
    CREATE INDEX IF NOT EXISTS idx_job_status
    ON send_jobs(status);
    """)

    # PERFORMANCE INDEXES
    cur.execute("""
    CREATE INDEX IF NOT EXISTS idx_user_media
    ON stored_media(user_id, id);
    """)

    cur.execute("""
    CREATE INDEX IF NOT EXISTS idx_media_group
    ON stored_media(media_group_id);
    """)

    conn.commit()
    cur.close()
    release_connection(conn)

# =====================================================
# ================= USER + STORAGE ====================
# =====================================================

def register_user(message):
    conn = get_connection()
    cur = conn.cursor()

    cur.execute("""
        INSERT INTO users (user_id, username)
        VALUES (%s, %s)
        ON CONFLICT (user_id)
        DO UPDATE SET username = EXCLUDED.username
    """, (message.from_user.id, message.from_user.username))

    conn.commit()
    cur.close()
    release_connection(conn)

def save_media(user_id, file_id, file_type,
               caption=None,
               media_group_id=None,
               file_size=0):

    conn = get_connection()
    cur = conn.cursor()

    cur.execute("""
        INSERT INTO stored_media
        (user_id, file_id, file_type, caption, media_group_id, file_size)
        VALUES (%s, %s, %s, %s, %s, %s)
    """, (user_id, file_id, file_type, caption, media_group_id, file_size))

    cur.execute("""
        UPDATE users
        SET total_files = total_files + 1,
            total_size = total_size + %s
        WHERE user_id = %s
    """, (file_size, user_id))

    conn.commit()
    cur.close()
    release_connection(conn)

# =====================================================
# ================= USER DASHBOARD ====================
# =====================================================
admin_state = {}
@bot.message_handler(commands=['start'])
def start_handler(message):
    register_user(message)

    user_id = message.from_user.id

    conn = get_connection()
    cur = conn.cursor()

    cur.execute("""
        SELECT total_files, total_size
        FROM users
        WHERE user_id = %s
    """, (user_id,))
    result = cur.fetchone()

    cur.close()
    release_connection(conn)

    total_files = result[0] if result else 0
    total_size = result[1] if result else 0

    total_size_mb = round(total_size / (1024 * 1024), 2)

    text = (
        "📦 <b>Your Archive</b>\n\n"
        f"📁 Total Files: <b>{total_files}</b>\n"
        f"💾 Total Storage: <b>{total_size_mb} MB</b>\n"
    )

    markup = InlineKeyboardMarkup()
    markup.add(
        InlineKeyboardButton("📂 View My Files", callback_data="view_my_files")
    )

    if user_id == ADMIN_ID:
        markup.add(
            InlineKeyboardButton("👨‍💼 Admin Panel", callback_data="admin_panel")
        )

    bot.send_message(message.chat.id, text, reply_markup=markup)
@bot.callback_query_handler(func=lambda call: call.data == "admin_panel")
def admin_panel(call):
    if call.from_user.id != ADMIN_ID:
        return

    markup = InlineKeyboardMarkup()
    markup.add(
        InlineKeyboardButton("📋 View Users", callback_data="admin_users"),
        InlineKeyboardButton("📊 View Jobs", callback_data="admin_jobs")
    )

    bot.edit_message_text(
        "👨‍💼 <b>Admin Panel</b>",
        call.message.chat.id,
        call.message.message_id,
        reply_markup=markup
    )
@bot.callback_query_handler(func=lambda call: call.data == "admin_users")
def admin_users(call):
    if call.from_user.id != ADMIN_ID:
        return

    conn = get_connection()
    cur = conn.cursor()

    cur.execute("""
        SELECT user_id, total_files
        FROM users
        ORDER BY total_files DESC
        LIMIT 20
    """)

    users = cur.fetchall()
    cur.close()
    release_connection(conn)

    if not users:
        bot.answer_callback_query(call.id, "No users found.")
        return

    markup = InlineKeyboardMarkup()

    for user_id, total_files in users:
        markup.add(
            InlineKeyboardButton(
                f"{user_id} ({total_files})",
                callback_data=f"select_user_{user_id}"
            )
        )

    bot.edit_message_text(
        "📋 Select User:",
        call.message.chat.id,
        call.message.message_id,
        reply_markup=markup
    )
@bot.callback_query_handler(func=lambda call: call.data.startswith("select_user_"))
def select_user(call):
    user_id = int(call.data.split("_")[-1])

    admin_state[call.from_user.id] = {"target_user": user_id}

    bot.answer_callback_query(call.id)

    bot.send_message(
        call.message.chat.id,
        f"Selected user {user_id}.\n\nSend target group ID."
    )
@bot.message_handler(func=lambda m: m.from_user.id in admin_state)
def receive_group_id(message):
    if message.from_user.id != ADMIN_ID:
        return

    try:
        group_id = int(message.text)
    except:
        bot.reply_to(message, "Send valid numeric group ID.")
        return

    target_user = admin_state[message.from_user.id]["target_user"]

    conn = get_connection()
    cur = conn.cursor()

    cur.execute("SELECT COUNT(*) FROM stored_media WHERE user_id=%s", (target_user,))
    total_files = cur.fetchone()[0]

    if total_files == 0:
        bot.reply_to(message, "User has no media.")
        cur.close()
        release_connection(conn)
        return

    cur.execute("""
        INSERT INTO send_jobs (admin_id, target_user, group_id, total_files)
        VALUES (%s, %s, %s, %s)
        RETURNING id
    """, (ADMIN_ID, target_user, group_id, total_files))

    job_id = cur.fetchone()[0]

    conn.commit()
    cur.close()
    release_connection(conn)

    markup = InlineKeyboardMarkup()
    markup.add(
        InlineKeyboardButton("⏸ Pause", callback_data=f"pause_{job_id}"),
        InlineKeyboardButton("▶ Resume", callback_data=f"resume_{job_id}")
    )
    markup.add(
        InlineKeyboardButton("❌ Cancel", callback_data=f"cancel_{job_id}")
    )

    bot.reply_to(
        message,
        f"🚀 Job #{job_id} created.\nTotal files: {total_files}",
        reply_markup=markup
    )

    admin_state.pop(message.from_user.id)
@bot.callback_query_handler(func=lambda call: call.data.startswith("pause_"))
def pause_job(call):
    job_id = int(call.data.split("_")[-1])

    conn = get_connection()
    cur = conn.cursor()
    cur.execute("UPDATE send_jobs SET status='paused' WHERE id=%s", (job_id,))
    conn.commit()
    cur.close()
    release_connection(conn)

    bot.answer_callback_query(call.id, "Paused.")


@bot.callback_query_handler(func=lambda call: call.data.startswith("resume_"))
def resume_job(call):
    job_id = int(call.data.split("_")[-1])

    conn = get_connection()
    cur = conn.cursor()
    cur.execute("UPDATE send_jobs SET status='running' WHERE id=%s", (job_id,))
    conn.commit()
    cur.close()
    release_connection(conn)

    bot.answer_callback_query(call.id, "Resumed.")


@bot.callback_query_handler(func=lambda call: call.data.startswith("cancel_"))
def cancel_job(call):
    job_id = int(call.data.split("_")[-1])

    conn = get_connection()
    cur = conn.cursor()
    cur.execute("UPDATE send_jobs SET status='cancelled' WHERE id=%s", (job_id,))
    conn.commit()
    cur.close()
    release_connection(conn)

    bot.answer_callback_query(call.id, "Cancelled.")
# =====================================================
# ================= MEDIA HANDLER =====================
# =====================================================

@bot.message_handler(content_types=['photo', 'video', 'document', 'audio'])
def media_handler(message):
    register_user(message)

    user_id = message.from_user.id
    caption = message.caption
    media_group_id = message.media_group_id

    if message.content_type == "photo":
        file = message.photo[-1]
        save_media(user_id, file.file_id, "photo", caption, media_group_id, file.file_size)

    elif message.content_type == "video":
        file = message.video
        save_media(user_id, file.file_id, "video", caption, media_group_id, file.file_size)

    elif message.content_type == "document":
        file = message.document
        save_media(user_id, file.file_id, "document", caption, media_group_id, file.file_size)

    elif message.content_type == "audio":
        file = message.audio
        save_media(user_id, file.file_id, "audio", caption, media_group_id, file.file_size)

    bot.reply_to(message, "✅ Saved.")

# =====================================================
# ================= WORKER ENGINE =====================
# =====================================================

BASE_DELAY = 2.2
BATCH_SIZE = 1000


def worker_loop():
    print("Worker started...")

    while True:
        conn = get_connection()
        cur = conn.cursor()

        # Find next pending or running job (oldest first)
        cur.execute("""
            SELECT id
            FROM send_jobs
            WHERE status IN ('pending', 'running')
            ORDER BY created_at ASC
            LIMIT 1
        """)

        job = cur.fetchone()

        if not job:
            cur.close()
            release_connection(conn)
            time.sleep(5)
            continue

        job_id = job[0]

        cur.close()
        release_connection(conn)

        process_job(job_id)


def process_job(job_id):
    print(f"Processing job {job_id}")
        # ===== Admin Progress Message =====
    conn = get_connection()
    cur = conn.cursor()

    cur.execute("SELECT admin_id, total_files FROM send_jobs WHERE id=%s", (job_id,))
    admin_id, total_files = cur.fetchone()

    cur.close()
    release_connection(conn)

    progress_message = bot.send_message(
        admin_id,
        "🚀 Sending Started...\n\n0%"
    )

    progress_message_id = progress_message.message_id
        # ===== Safety Controls =====
    base_delay = BASE_DELAY
    extra_delay = 0
    rate_limit_hits = 0
    safe_break_interval = 600
    start_time = time.time()
    while True:
        conn = get_connection()
        cur = conn.cursor()

        # Reload job state from DB (DB is source of truth)
        cur.execute("""
            SELECT target_user, group_id,
                   last_sent_id, total_files,
                   sent_count, status
            FROM send_jobs
            WHERE id = %s
        """, (job_id,))

        job = cur.fetchone()

        if not job:
            cur.close()
            release_connection(conn)
            return

        target_user, group_id, last_sent_id, total_files, sent_count, status = job

        # State machine checks
        if status == "pending":
            cur.execute("""
                UPDATE send_jobs
                SET status='running', updated_at=NOW()
                WHERE id=%s
            """, (job_id,))
            conn.commit()

        if status == "paused":
            cur.close()
            release_connection(conn)
            time.sleep(5)
            continue

        if status == "cancelled":
            cur.close()
            release_connection(conn)
            print(f"Job {job_id} cancelled.")
            return

        if status not in ("running",):
            cur.close()
            release_connection(conn)
            return

        # Fetch batch
        cur.execute("""
            SELECT id, file_id, file_type, caption, media_group_id
            FROM stored_media
            WHERE user_id = %s AND id > %s
            ORDER BY id ASC
            LIMIT %s
        """, (target_user, last_sent_id, BATCH_SIZE))

        rows = cur.fetchall()

        if not rows:
            cur.execute("""
                UPDATE send_jobs
                SET status='completed', updated_at=NOW()
                WHERE id=%s
            """, (job_id,))
            conn.commit()

            cur.close()
            release_connection(conn)
            print(f"Job {job_id} completed.")
            return
        try:
            bot.edit_message_text(
                "✅ <b>Migration Completed</b>\n\n"
                f"Total Sent: {sent_count}",
                admin_id,
                progress_message_id
            )
        except:
            pass
        # Group media by media_group_id (album support)
        grouped = {}

        for media_id, file_id, file_type, caption, media_group_id in rows:
            if media_group_id:
                grouped.setdefault(media_group_id, []).append(
                    (media_id, file_id, file_type, caption)
                )
            else:
                grouped[f"single_{media_id}"] = [
                    (media_id, file_id, file_type, caption)
                ]

        for group_key, items in grouped.items():

            # Re-check cancellation before sending
            cur.execute("SELECT status FROM send_jobs WHERE id=%s", (job_id,))
            current_status = cur.fetchone()[0]

            if current_status == "cancelled":
                cur.close()
                release_connection(conn)
                return

            try:
                # ===== ALBUM =====
                if len(items) > 1 and all(i[2] in ["photo", "video"] for i in items):

                    media_list = []

                    for index, (media_id, file_id, file_type, caption) in enumerate(items):
                        if file_type == "photo":
                            if index == 0:
                                media_list.append(
                                    telebot.types.InputMediaPhoto(file_id, caption=caption)
                                )
                            else:
                                media_list.append(
                                    telebot.types.InputMediaPhoto(file_id)
                                )

                        elif file_type == "video":
                            if index == 0:
                                media_list.append(
                                    telebot.types.InputMediaVideo(file_id, caption=caption)
                                )
                            else:
                                media_list.append(
                                    telebot.types.InputMediaVideo(file_id)
                                )

                    bot.send_media_group(group_id, media_list)

                    sent_count += len(items)
                    last_sent_id = items[-1][0]

                # ===== SINGLE =====
                else:
                    media_id, file_id, file_type, caption = items[0]

                    if file_type == "photo":
                        bot.send_photo(group_id, file_id, caption=caption)
                    elif file_type == "video":
                        bot.send_video(group_id, file_id, caption=caption)
                    elif file_type == "document":
                        bot.send_document(group_id, file_id, caption=caption)
                    elif file_type == "audio":
                        bot.send_audio(group_id, file_id, caption=caption)

                    sent_count += 1
                    last_sent_id = media_id

                rate_limit_hits = 0

                # ===== SAFE BREAK =====
                if sent_count > 0 and sent_count % safe_break_interval == 0:

                    elapsed = time.time() - start_time
                    speed = sent_count / elapsed if elapsed > 0 else 0

                    pause_time = 300  # base 5 min

                    if speed > 2:
                        pause_time += 900
                    elif speed > 1:
                        pause_time += 600
                    elif speed > 0.5:
                        pause_time += 300

                    pause_time = max(300, min(pause_time, 1800))

                    print(f"Safe break activated. Sleeping {pause_time} seconds.")
                    time.sleep(pause_time)

                # ===== UPDATE PROGRESS =====
                                # ===== PROGRESS UPDATE =====
                if sent_count % 25 == 0 or sent_count == total_files:

                    percent = int((sent_count / total_files) * 100)

                    elapsed = time.time() - start_time
                    speed = round(sent_count / elapsed, 2) if elapsed > 0 else 0

                    remaining = total_files - sent_count
                    eta_seconds = int(remaining / speed) if speed > 0 else 0

                    minutes = eta_seconds // 60
                    seconds = eta_seconds % 60

                    eta_text = f"{minutes}m {seconds}s" if eta_seconds > 0 else "calculating..."

                    progress_text = (
                        "📦 <b>Migration Progress</b>\n\n"
                        f"📊 {sent_count} / {total_files}\n"
                        f"📈 {percent}%\n"
                        f"⚡ Speed: {speed} files/sec\n"
                        f"⏳ ETA: {eta_text}"
                    )

                    try:
                        bot.edit_message_text(
                            progress_text,
                            admin_id,
                            progress_message_id
                        )
                    except:
                        pass

                # ===== DB UPDATE =====
                if sent_count % 50 == 0:
                    cur.execute("""
                        UPDATE send_jobs
                        SET last_sent_id=%s,
                            sent_count=%s,
                            updated_at=NOW()
                        WHERE id=%s
                    """, (last_sent_id, sent_count, job_id))
                    conn.commit()
                    cur.execute("""
                        UPDATE send_jobs
                        SET last_sent_id=%s,
                            sent_count=%s,
                            updated_at=NOW()
                        WHERE id=%s
                    """, (last_sent_id, sent_count, job_id))
                    conn.commit()

                time.sleep(base_delay + extra_delay)

            except telebot.apihelper.ApiTelegramException as e:

                # ===== 429 HANDLING =====
                if e.error_code == 429:
                    retry_after = 5
                    try:
                        retry_after = int(
                            e.result_json.get("parameters", {}).get("retry_after", 5)
                        )
                    except:
                        pass

                    rate_limit_hits += 1
                    extra_delay += 0.3

                    print(f"Rate limited. Sleeping {retry_after}s. New delay: {base_delay + extra_delay}")

                    time.sleep(retry_after)
                    continue

                # ===== FATAL ERRORS =====
                elif e.error_code in (403, 400):
                    print("Fatal error. Marking job failed.")

                    cur.execute("""
                        UPDATE send_jobs
                        SET status='failed',
                            updated_at=NOW()
                        WHERE id=%s
                    """, (job_id,))
                    conn.commit()

                    cur.close()
                    release_connection(conn)
                    try:
                        bot.edit_message_text(
                            "❌ <b>Migration Failed</b>\n\n"
                            "Bot lost access or invalid group.",
                            admin_id,
                            progress_message_id
                        )
                    except:
                        pass
                    return

                else:
                    print("Telegram error:", e)
                    time.sleep(3)
                    continue

            except Exception as e:
                print("Unexpected error:", e)
                time.sleep(3)
                continue

        # Final batch update
        cur.execute("""
            UPDATE send_jobs
            SET last_sent_id=%s,
                sent_count=%s,
                updated_at=NOW()
            WHERE id=%s
        """, (last_sent_id, sent_count, job_id))
        conn.commit()

        cur.close()
        release_connection(conn)
# =====================================================
# ================= STARTUP ===========================
# =====================================================

if __name__ == "__main__":
    init_db()

    worker_thread = threading.Thread(target=worker_loop, daemon=True)
    worker_thread.start()

    print("Bot running...")
    bot.infinity_polling()
