import sqlite3
import uuid
import logging
from datetime import datetime
from telethon import TelegramClient
from telethon.sessions import StringSession
from telethon.tl.functions.account import GetAuthorizationsRequest

from config import API_ID, API_HASH, DATABASE_PATH

# ======================
# Logging
# ======================

logger = logging.getLogger(__name__)

# ======================
# Database Helpers
# ======================

def get_connection():
    """إنشاء اتصال بقاعدة البيانات مع تحسينات الأداء"""
    conn = sqlite3.connect(DATABASE_PATH)
    conn.execute("PRAGMA journal_mode = WAL")
    return conn


def init_sessions_table():
    """إنشاء جدول الجلسات مع تحسينات"""
    conn = get_connection()
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS sessions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            session TEXT NOT NULL UNIQUE,
            phone TEXT,
            user_id INTEGER,
            added_date TEXT DEFAULT CURRENT_TIMESTAMP,
            last_used TEXT,
            status TEXT DEFAULT 'active',  -- active, expired, error
            note TEXT
        )
    """)

    # إنشاء فهارس للبحث السريع
    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_sessions_status 
        ON sessions(status)
    """)
    
    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_sessions_phone 
        ON sessions(phone)
    """)
    
    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_sessions_user_id 
        ON sessions(user_id)
    """)

    conn.commit()
    conn.close()


# ======================
# Session Validation
# ======================

async def validate_session(session_string: str) -> dict:
    """
    التحقق من صحة الـ Session وجمع معلومات الحساب
    """
    client = None
    try:
        client = TelegramClient(
            StringSession(session_string),
            API_ID,
            API_HASH
        )
        
        await client.connect()
        
        if not await client.is_user_authorized():
            raise ValueError("Session غير صالح أو منتهي")
        
        # الحصول على معلومات الحساب
        me = await client.get_me()
        
        # التحقق من الجلسات النشطة
        try:
            authorizations = await client(GetAuthorizationsRequest())
            if not authorizations.authorizations:
                logger.warning("No active authorizations found")
        except Exception as e:
            logger.warning(f"Could not fetch authorizations: {e}")
        
        account_info = {
            "phone": me.phone if me.phone else "Unknown",
            "user_id": me.id,
            "username": me.username if me.username else None,
            "first_name": me.first_name if me.first_name else "",
            "last_name": me.last_name if me.last_name else "",
            "is_bot": me.bot,
            "is_premium": getattr(me, 'premium', False),
            "is_verified": getattr(me, 'verified', False),
            "session_valid": True
        }
        
        return account_info
        
    except Exception as e:
        logger.error(f"Session validation failed: {e}")
        raise ValueError(f"Session غير صالح: {str(e)}")
        
    finally:
        if client:
            await client.disconnect()


# ======================
# Session Operations
# ======================

async def add_session(session_string: str, custom_name: str = None):
    """
    إضافة Session String مع التحقق من صحته
    """
    init_sessions_table()
    
    # التحقق من صحة الـ Session
    account_info = await validate_session(session_string)
    
    # إنشاء اسم للحساب
    if custom_name:
        account_name = custom_name
    else:
        phone = account_info["phone"]
        if phone != "Unknown":
            # استخدام آخر 4 أرقام من الهاتف
            phone_suffix = phone[-4:] if len(phone) > 4 else phone
            account_name = f"Account-{phone_suffix}"
        else:
            account_name = f"Account-{uuid.uuid4().hex[:6]}"
    
    # إضافة معلومات إضافية للاسم
    if account_info["first_name"]:
        account_name = f"{account_info['first_name']} ({account_name})"
    
    conn = get_connection()
    cur = conn.cursor()

    try:
        # التحقق من عدم وجود نفس الـ session مسبقاً
        cur.execute("SELECT id FROM sessions WHERE session = ?", (session_string,))
        if cur.fetchone():
            raise ValueError("هذا الحساب مضاف مسبقاً")
        
        # التحقق من عدم وجود نفس رقم الهاتف
        if account_info["phone"] != "Unknown":
            cur.execute("SELECT id FROM sessions WHERE phone = ?", (account_info["phone"],))
            if cur.fetchone():
                raise ValueError("رقم الهاتف هذا مضاف مسبقاً")
        
        # إضافة الحساب
        cur.execute(
            """
            INSERT INTO sessions 
            (name, session, phone, user_id, added_date, status) 
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                account_name,
                session_string,
                account_info["phone"],
                account_info["user_id"],
                datetime.now().isoformat(),
                "active"
            )
        )
        
        conn.commit()
        
        logger.info(f"Session added successfully: {account_name}")
        return {
            "id": cur.lastrowid,
            "name": account_name,
            "phone": account_info["phone"],
            "user_id": account_info["user_id"]
        }
        
    except sqlite3.IntegrityError as e:
        logger.error(f"Database integrity error: {e}")
        raise ValueError("هذا الحساب مضاف مسبقاً")
        
    except Exception as e:
        logger.error(f"Error adding session: {e}")
        raise
        
    finally:
        conn.close()


async def update_session_status(session_id: int, status: str, note: str = None):
    """
    تحديث حالة الجلسة
    """
    try:
        conn = get_connection()
        cur = conn.cursor()
        
        update_data = {
            "status": status,
            "last_used": datetime.now().isoformat()
        }
        
        if note:
            update_data["note"] = note
        
        query = """
            UPDATE sessions 
            SET status = ?, last_used = ?, note = ?
            WHERE id = ?
        """
        
        cur.execute(query, (status, update_data["last_used"], note, session_id))
        conn.commit()
        conn.close()
        
        logger.info(f"Session {session_id} status updated to {status}")
        
    except Exception as e:
        logger.error(f"Error updating session status: {e}")


def get_all_sessions():
    """
    إرجاع كل الحسابات المضافة
    """
    init_sessions_table()

    conn = get_connection()
    cur = conn.cursor()

    cur.execute("""
        SELECT id, name, session, phone, user_id, added_date, last_used, status, note 
        FROM sessions 
        ORDER BY added_date DESC
    """)
    
    rows = cur.fetchall()

    conn.close()

    return [
        {
            "id": row[0],
            "name": row[1],
            "session": row[2],
            "phone": row[3],
            "user_id": row[4],
            "added_date": row[5],
            "last_used": row[6],
            "status": row[7],
            "note": row[8]
        }
        for row in rows
    ]


def get_active_sessions():
    """
    إرجاع الحسابات النشطة فقط
    """
    all_sessions = get_all_sessions()
    return [s for s in all_sessions if s["status"] == "active"]


async def test_session(session_id: int) -> dict:
    """
    اختبار جلسة محددة
    """
    sessions = get_all_sessions()
    session_data = next((s for s in sessions if s["id"] == session_id), None)
    
    if not session_data:
        raise ValueError("Session not found")
    
    try:
        account_info = await validate_session(session_data["session"])
        
        # تحديث حالة الجلسة
        await update_session_status(session_id, "active", "Tested successfully")
        
        return {
            "success": True,
            "message": "Session is valid",
            "account_info": account_info,
            "session_data": {
                "name": session_data["name"],
                "phone": session_data["phone"],
                "status": "active"
            }
        }
        
    except Exception as e:
        # تحديث حالة الجلسة إلى خطأ
        await update_session_status(session_id, "error", f"Test failed: {str(e)}")
        
        return {
            "success": False,
            "message": str(e),
            "session_data": {
                "name": session_data["name"],
                "phone": session_data["phone"],
                "status": "error"
            }
        }


def delete_session(session_id: int):
    """
    حذف حساب واحد
    """
    init_sessions_table()

    conn = get_connection()
    cur = conn.cursor()

    # الحصول على معلومات الحساب قبل الحذف (للـ Log)
    cur.execute("SELECT name, phone FROM sessions WHERE id = ?", (session_id,))
    session_info = cur.fetchone()
    
    if session_info:
        logger.info(f"Deleting session: {session_info[0]} ({session_info[1]})")

    cur.execute("DELETE FROM sessions WHERE id = ?", (session_id,))
    deleted_count = conn.total_changes

    conn.commit()
    conn.close()
    
    return deleted_count > 0


def delete_all_sessions():
    """
    حذف جميع الحسابات
    """
    init_sessions_table()

    conn = get_connection()
    cur = conn.cursor()

    cur.execute("SELECT COUNT(*) FROM sessions")
    count_before = cur.fetchone()[0]
    
    cur.execute("DELETE FROM sessions")
    deleted_count = conn.total_changes

    conn.commit()
    conn.close()
    
    logger.info(f"Deleted {deleted_count} sessions out of {count_before}")
    return deleted_count


def get_session_stats() -> dict:
    """
    إحصائيات الجلسات
    """
    init_sessions_table()

    conn = get_connection()
    cur = conn.cursor()

    stats = {
        "total": 0,
        "active": 0,
        "expired": 0,
        "error": 0,
        "by_month": []
    }

    # الإحصائيات الأساسية
    cur.execute("""
        SELECT status, COUNT(*) 
        FROM sessions 
        GROUP BY status
    """)
    
    for status, count in cur.fetchall():
        stats["total"] += count
        if status == "active":
            stats["active"] = count
        elif status == "expired":
            stats["expired"] = count
        elif status == "error":
            stats["error"] = count

    # الإحصائيات الشهرية
    cur.execute("""
        SELECT 
            strftime('%Y-%m', added_date) as month,
            COUNT(*) as count
        FROM sessions
        GROUP BY strftime('%Y-%m', added_date)
        ORDER BY month DESC
        LIMIT 6
    """)
    
    stats["by_month"] = [
        {"month": row[0], "count": row[1]}
        for row in cur.fetchall()
    ]

    conn.close()
    return stats


# ======================
# Session Health Check
# ======================

async def check_all_sessions_health():
    """
    التحقق من صحة جميع الجلسات
    """
    sessions = get_all_sessions()
    results = []
    
    for session in sessions:
        try:
            account_info = await validate_session(session["session"])
            
            # تحديث حالة الجلسة
            await update_session_status(session["id"], "active", "Health check passed")
            
            results.append({
                "id": session["id"],
                "name": session["name"],
                "status": "healthy",
                "phone": account_info["phone"],
                "user_id": account_info["user_id"]
            })
            
        except Exception as e:
            # تحديث حالة الجلسة
            await update_session_status(
                session["id"], 
                "error", 
                f"Health check failed: {str(e)}"
            )
            
            results.append({
                "id": session["id"],
                "name": session["name"],
                "status": "unhealthy",
                "error": str(e),
                "phone": session["phone"]
            })
    
    return results


# ======================
# Export/Import Sessions
# ======================

def export_sessions_to_file(filepath: str = "sessions_export.txt") -> bool:
    """
    تصدير الجلسات إلى ملف نصي
    """
    try:
        sessions = get_all_sessions()
        
        with open(filepath, "w", encoding="utf-8") as f:
            f.write("# Telegram Sessions Export\n")
            f.write(f"# Export Date: {datetime.now().isoformat()}\n")
            f.write(f"# Total Sessions: {len(sessions)}\n\n")
            
            for session in sessions:
                f.write(f"# ID: {session['id']}\n")
                f.write(f"# Name: {session['name']}\n")
                f.write(f"# Phone: {session['phone']}\n")
                f.write(f"# Status: {session['status']}\n")
                f.write(f"# Added: {session['added_date']}\n")
                f.write(f"{session['session']}\n")
                f.write("-" * 50 + "\n\n")
        
        logger.info(f"Sessions exported to {filepath}")
        return True
        
    except Exception as e:
        logger.error(f"Error exporting sessions: {e}")
        return False


async def import_sessions_from_file(filepath: str) -> dict:
    """
    استيراد الجلسات من ملف نصي
    """
    results = {
        "total": 0,
        "success": 0,
        "failed": 0,
        "errors": []
    }
    
    try:
        with open(filepath, "r", encoding="utf-8") as f:
            content = f.read()
        
        # استخراج الجلسات من الملف
        lines = content.split('\n')
        current_session = None
        session_lines = []
        
        for line in lines:
            line = line.strip()
            
            if line.startswith('#'):
                continue
            
            if line and not line.startswith('-'):
                session_lines.append(line)
            
            if len(session_lines) == 1:  # نحتاج فقط سطر واحد للـ session string
                session_string = session_lines[0].strip()
                if session_string:
                    results["total"] += 1
                    
                    try:
                        await add_session(session_string)
                        results["success"] += 1
                    except Exception as e:
                        results["failed"] += 1
                        results["errors"].append(str(e))
                    
                    session_lines = []
        
        logger.info(f"Sessions import completed: {results}")
        return results
        
    except Exception as e:
        logger.error(f"Error importing sessions: {e}")
        results["errors"].append(str(e))
        return results
