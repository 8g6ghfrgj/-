import sqlite3
import os
import shutil
import logging
from datetime import datetime
from typing import Dict, List, Tuple, Optional
from config import DATABASE_PATH

# ======================
# Logging
# ======================

logger = logging.getLogger(__name__)

# ======================
# Database Constants
# ======================

# مسارات النسخ الاحتياطي
BACKUP_DIR = "database_backups"
BACKUP_RETENTION_DAYS = 30  # الاحتفاظ بالنسخ الاحتياطي لمدة 30 يومًا

# ======================
# Connection
# ======================

def get_connection():
    """
    إنشاء اتصال بقاعدة البيانات
    مع زيادة حجم الـ cache وتحسين الأداء
    """
    conn = sqlite3.connect(
        DATABASE_PATH,
        check_same_thread=False,
        timeout=30  # زيادة مهلة الانتظار
    )
    
    # تحسينات الأداء
    conn.execute("PRAGMA journal_mode = WAL")  # تحسين الأداء للقراءة/كتابة متزامنة
    conn.execute("PRAGMA synchronous = NORMAL")  # توازن بين الأمان والأداء
    conn.execute(f"PRAGMA cache_size = {-50000}")  # 50MB cache
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute("PRAGMA temp_store = MEMORY")
    conn.execute("PRAGMA mmap_size = 268435456")  # 256MB memory mapping
    
    return conn


# ======================
# Backup Functions
# ======================

def create_backup():
    """
    إنشاء نسخة احتياطية من قاعدة البيانات
    """
    try:
        # إنشاء مجلد النسخ الاحتياطي إذا لم يكن موجوداً
        os.makedirs(BACKUP_DIR, exist_ok=True)
        
        # تسمية الملف بالتاريخ والوقت
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_file = os.path.join(BACKUP_DIR, f"links_backup_{timestamp}.db")
        
        # نسخ الملف
        shutil.copy2(DATABASE_PATH, backup_file)
        
        logger.info(f"Backup created: {backup_file}")
        
        # تنظيف النسخ القديمة
        cleanup_old_backups()
        
        return backup_file
    except Exception as e:
        logger.error(f"Failed to create backup: {e}")
        return None


def cleanup_old_backups():
    """
    حذف النسخ الاحتياطية القديمة
    """
    try:
        if not os.path.exists(BACKUP_DIR):
            return
        
        cutoff_date = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=BACKUP_RETENTION_DAYS)
        
        for filename in os.listdir(BACKUP_DIR):
            if filename.startswith("links_backup_") and filename.endswith(".db"):
                # استخراج التاريخ من اسم الملف
                try:
                    date_str = filename.replace("links_backup_", "").replace(".db", "")
                    file_date = datetime.strptime(date_str, "%Y%m%d_%H%M%S")
                    
                    if file_date < cutoff_date:
                        file_path = os.path.join(BACKUP_DIR, filename)
                        os.remove(file_path)
                        logger.info(f"Deleted old backup: {filename}")
                except Exception as e:
                    logger.warning(f"Could not parse backup file date {filename}: {e}")
                    
    except Exception as e:
        logger.error(f"Error cleaning up old backups: {e}")


def restore_backup(backup_file: str) -> bool:
    """
    استعادة قاعدة البيانات من نسخة احتياطية
    """
    try:
        if not os.path.exists(backup_file):
            logger.error(f"Backup file not found: {backup_file}")
            return False
        
        # إغلاق أي اتصالات حالية
        try:
            conn = get_connection()
            conn.close()
        except:
            pass
        
        # نسخ ملف النسخة الاحتياطية
        shutil.copy2(backup_file, DATABASE_PATH)
        
        logger.info(f"Database restored from backup: {backup_file}")
        return True
    except Exception as e:
        logger.error(f"Failed to restore backup: {e}")
        return False


def list_backups() -> List[Dict[str, str]]:
    """
    عرض قائمة النسخ الاحتياطية المتاحة
    """
    backups = []
    
    try:
        if not os.path.exists(BACKUP_DIR):
            return backups
        
        for filename in sorted(os.listdir(BACKUP_DIR), reverse=True):
            if filename.startswith("links_backup_") and filename.endswith(".db"):
                file_path = os.path.join(BACKUP_DIR, filename)
                size = os.path.getsize(file_path)
                size_mb = size / (1024 * 1024)
                
                # استخراج التاريخ
                date_str = filename.replace("links_backup_", "").replace(".db", "")
                try:
                    file_date = datetime.strptime(date_str, "%Y%m%d_%H%M%S")
                    formatted_date = file_date.strftime("%Y-%m-%d %H:%M:%S")
                except:
                    formatted_date = "Unknown"
                
                backups.append({
                    "filename": filename,
                    "path": file_path,
                    "date": formatted_date,
                    "size_mb": f"{size_mb:.2f}",
                    "size_bytes": size
                })
    except Exception as e:
        logger.error(f"Error listing backups: {e}")
    
    return backups


# ======================
# Init
# ======================

def init_db():
    """
    إنشاء جداول قاعدة البيانات
    مع تحسين المساحة والأداء
    """
    try:
        dir_name = os.path.dirname(DATABASE_PATH)
        if dir_name:
            os.makedirs(dir_name, exist_ok=True)
        
        conn = get_connection()
        cur = conn.cursor()
        
        # جدول الروابط الرئيسي
        cur.execute("""
            CREATE TABLE IF NOT EXISTS links (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                url TEXT NOT NULL UNIQUE,
                platform TEXT NOT NULL,
                source_account TEXT,
                chat_type TEXT,
                chat_id TEXT,
                message_date TEXT,
                collected_date TEXT DEFAULT CURRENT_TIMESTAMP,
                source_type TEXT  -- text, button, comment, file
            )
        """)
        
        # جدول للإحصائيات اليومية
        cur.execute("""
            CREATE TABLE IF NOT EXISTS daily_stats (
                date TEXT PRIMARY KEY,
                telegram_count INTEGER DEFAULT 0,
                whatsapp_count INTEGER DEFAULT 0,
                total_count INTEGER DEFAULT 0
            )
        """)
        
        # جدول لأنواع روابط تليجرام
        cur.execute("""
            CREATE TABLE IF NOT EXISTS telegram_types (
                link_id INTEGER,
                telegram_type TEXT,  -- invite_with_plus, invite_without_plus
                FOREIGN KEY (link_id) REFERENCES links(id)
            )
        """)
        
        # إنشاء الفهارس الأساسية
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_links_platform_type 
            ON links (platform, chat_type)
        """)
        
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_links_date 
            ON links (message_date DESC)
        """)
        
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_links_source_type 
            ON links (source_type)
        """)
        
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_links_platform_date 
            ON links (platform, message_date DESC)
        """)
        
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_links_url 
            ON links (url)
        """)
        
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_links_collected_date 
            ON links (collected_date DESC)
        """)
        
        conn.commit()
        conn.close()
        
        # إنشاء أول نسخة احتياطية
        create_backup()
        
        logger.info("Database initialized successfully with enhanced structure")
        
    except Exception as e:
        logger.error(f"Failed to initialize database: {e}")
        raise


# ======================
# Vacuum and Optimize
# ======================

def optimize_database():
    """
    تحسين قاعدة البيانات وتقليص حجمها
    """
    try:
        conn = get_connection()
        conn.execute("VACUUM")
        conn.execute("ANALYZE")
        conn.commit()
        conn.close()
        
        logger.info("Database optimized successfully")
        
        # تحديث الإحصائيات
        update_daily_stats()
        
    except Exception as e:
        logger.error(f"Failed to optimize database: {e}")


def update_daily_stats():
    """
    تحديث الإحصائيات اليومية
    """
    try:
        today = datetime.now().strftime("%Y-%m-%d")
        
        conn = get_connection()
        cur = conn.cursor()
        
        # حساب الإحصائيات
        cur.execute("""
            SELECT 
                COUNT(CASE WHEN platform LIKE 'telegram%' THEN 1 END) as telegram_count,
                COUNT(CASE WHEN platform = 'whatsapp' THEN 1 END) as whatsapp_count,
                COUNT(*) as total_count
            FROM links 
            WHERE date(collected_date) = ?
        """, (today,))
        
        stats = cur.fetchone()
        
        if stats:
            cur.execute("""
                INSERT OR REPLACE INTO daily_stats 
                (date, telegram_count, whatsapp_count, total_count)
                VALUES (?, ?, ?, ?)
            """, (today, stats[0], stats[1], stats[2]))
        
        conn.commit()
        conn.close()
        
    except Exception as e:
        logger.error(f"Failed to update daily stats: {e}")


# ======================
# Save Link
# ======================

def save_link(
    url: str,
    platform: str,
    source_account: str,
    chat_type: str,
    chat_id: str,
    message_date,
    source_type: Optional[str] = None
):
    """
    حفظ الرابط مرة واحدة فقط مع تحديث الإحصائيات
    """
    if not url:
        return
    
    try:
        conn = get_connection()
        cur = conn.cursor()
        
        # التحقق أولاً إذا كان الرابط موجوداً
        cur.execute("SELECT id FROM links WHERE url = ?", (url,))
        existing = cur.fetchone()
        
        if existing:
            conn.close()
            return
        
        # إدراج الرابط الجديد
        cur.execute(
            """
            INSERT OR IGNORE INTO links
            (url, platform, source_account, chat_type, chat_id, message_date, source_type)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                url,
                platform,
                source_account,
                chat_type,
                chat_id,
                message_date.isoformat() if message_date else None,
                source_type
            )
        )
        
        # إذا كان الرابط من تليجرام، حفظ نوعه
        if platform.startswith('telegram_'):
            link_id = cur.lastrowid
            telegram_type = platform.replace('telegram_', '')
            
            cur.execute(
                """
                INSERT INTO telegram_types (link_id, telegram_type)
                VALUES (?, ?)
                """,
                (link_id, telegram_type)
            )
        
        conn.commit()
        conn.close()
        
        # تحديث الإحصائيات بعد كل 100 رابط
        cur.execute("SELECT COUNT(*) FROM links")
        total_count = cur.fetchone()[0]
        if total_count % 100 == 0:
            update_daily_stats()
        
    except sqlite3.IntegrityError:
        # تجاهل الأخطاء المتعلقة بتكرار الروابط
        pass
    except Exception as e:
        logger.error(f"Failed to save link {url}: {e}")
    finally:
        try:
            conn.close()
        except:
            pass


# ======================
# Stats Functions
# ======================

def count_links_by_platform() -> Dict[str, int]:
    """
    إحصائيات الروابط حسب المنصة
    """
    try:
        conn = get_connection()
        cur = conn.cursor()
        
        cur.execute("""
            SELECT 
                CASE 
                    WHEN platform LIKE 'telegram%' THEN 'telegram'
                    ELSE platform
                END as platform_group,
                COUNT(*) as count
            FROM links
            GROUP BY platform_group
            ORDER BY count DESC
        """)
        
        rows = cur.fetchall()
        conn.close()
        
        return {platform: count for platform, count in rows}
    except Exception as e:
        logger.error(f"Error counting links: {e}")
        return {}


def get_detailed_stats() -> Dict[str, any]:
    """
    إحصائيات مفصلة
    """
    stats = {
        "total": 0,
        "platforms": {},
        "telegram_types": {},
        "chat_types": {},
        "source_types": {},
        "daily_stats": []
    }
    
    try:
        conn = get_connection()
        cur = conn.cursor()
        
        # إجمالي الروابط
        cur.execute("SELECT COUNT(*) FROM links")
        stats["total"] = cur.fetchone()[0]
        
        # حسب المنصة
        cur.execute("""
            SELECT platform, COUNT(*) 
            FROM links 
            GROUP BY platform 
            ORDER BY COUNT(*) DESC
        """)
        stats["platforms"] = dict(cur.fetchall())
        
        # حسب نوع تليجرام
        cur.execute("""
            SELECT tt.telegram_type, COUNT(*)
            FROM telegram_types tt
            JOIN links l ON tt.link_id = l.id
            GROUP BY tt.telegram_type
            ORDER BY COUNT(*) DESC
        """)
        stats["telegram_types"] = dict(cur.fetchall())
        
        # حسب نوع المحادثة
        cur.execute("""
            SELECT chat_type, COUNT(*) 
            FROM links 
            WHERE chat_type IS NOT NULL 
            GROUP BY chat_type 
            ORDER BY COUNT(*) DESC
        """)
        stats["chat_types"] = dict(cur.fetchall())
        
        # حسب مصدر الرابط
        cur.execute("""
            SELECT source_type, COUNT(*) 
            FROM links 
            WHERE source_type IS NOT NULL 
            GROUP BY source_type 
            ORDER BY COUNT(*) DESC
        """)
        stats["source_types"] = dict(cur.fetchall())
        
        # الإحصائيات اليومية (آخر 7 أيام)
        cur.execute("""
            SELECT date, telegram_count, whatsapp_count, total_count
            FROM daily_stats
            ORDER BY date DESC
            LIMIT 7
        """)
        stats["daily_stats"] = [
            {"date": row[0], "telegram": row[1], "whatsapp": row[2], "total": row[3]}
            for row in cur.fetchall()
        ]
        
        conn.close()
        
    except Exception as e:
        logger.error(f"Error getting detailed stats: {e}")
    
    return stats


# ======================
# Pagination Functions
# ======================

def get_links_by_platform_and_type(
    platform: str,
    chat_type: str,
    limit: int = 50,
    offset: int = 0
) -> List[Tuple[str, str, str, str]]:
    """
    جلب الروابط حسب المنصة والنوع
    """
    try:
        conn = get_connection()
        cur = conn.cursor()
        
        query = """
            SELECT url, platform, chat_type, message_date
            FROM links
            WHERE platform = ? AND chat_type = ?
            ORDER BY message_date DESC
            LIMIT ? OFFSET ?
        """
        
        cur.execute(query, (platform, chat_type, limit, offset))
        rows = cur.fetchall()
        conn.close()
        
        return rows
    except Exception as e:
        logger.error(f"Error getting links: {e}")
        return []


def get_links_by_telegram_type(
    telegram_type: str,
    limit: int = 50,
    offset: int = 0
) -> List[Tuple[str, str, str]]:
    """
    جلب روابط تليجرام حسب النوع
    """
    try:
        conn = get_connection()
        cur = conn.cursor()
        
        query = """
            SELECT l.url, l.chat_type, l.message_date
            FROM links l
            JOIN telegram_types tt ON l.id = tt.link_id
            WHERE tt.telegram_type = ?
            ORDER BY l.message_date DESC
            LIMIT ? OFFSET ?
        """
        
        cur.execute(query, (telegram_type, limit, offset))
        rows = cur.fetchall()
        conn.close()
        
        return rows
    except Exception as e:
        logger.error(f"Error getting telegram links: {e}")
        return []


# ======================
# Export Functions
# ======================

def export_links(platform: str = "all", telegram_type: str = None) -> Optional[str]:
    """
    تصدير الروابط إلى ملف TXT
    """
    try:
        conn = get_connection()
        cur = conn.cursor()
        
        if platform == "all" and not telegram_type:
            cur.execute("""
                SELECT url FROM links
                ORDER BY message_date ASC
            """)
            filename = "links_all.txt"
        elif telegram_type:
            cur.execute("""
                SELECT l.url
                FROM links l
                JOIN telegram_types tt ON l.id = tt.link_id
                WHERE tt.telegram_type = ?
                ORDER BY l.message_date ASC
            """, (telegram_type,))
            filename = f"links_telegram_{telegram_type}.txt"
        else:
            cur.execute("""
                SELECT url FROM links
                WHERE platform = ?
                ORDER BY message_date ASC
            """, (platform,))
            filename = f"links_{platform}.txt"
        
        rows = cur.fetchall()
        conn.close()
        
        if not rows:
            return None
        
        # إنشاء مجلد التصدير
        export_dir = "exports"
        os.makedirs(export_dir, exist_ok=True)
        
        # تجنب تكرار أسماء الملفات
        base_name = filename.replace(".txt", "")
        counter = 1
        final_filename = filename
        
        while os.path.exists(os.path.join(export_dir, final_filename)):
            final_filename = f"{base_name}_{counter}.txt"
            counter += 1
        
        path = os.path.join(export_dir, final_filename)
        
        # كتابة الملف
        with open(path, "w", encoding="utf-8") as f:
            for (url,) in rows:
                f.write(url + "\n")
        
        logger.info(f"Exported {len(rows)} links to {path}")
        return path
        
    except Exception as e:
        logger.error(f"Error exporting links: {e}")
        return None


# ======================
# Maintenance Functions
# ======================

def get_database_size() -> Dict[str, any]:
    """
    الحصول على معلومات حجم قاعدة البيانات
    """
    try:
        size_bytes = os.path.getsize(DATABASE_PATH)
        size_mb = size_bytes / (1024 * 1024)
        
        conn = get_connection()
        cur = conn.cursor()
        
        # عدد السجلات
        cur.execute("SELECT COUNT(*) FROM links")
        total_links = cur.fetchone()[0]
        
        # مساحة الجداول
        cur.execute("""
            SELECT name, (pgsize * page_count) as size_bytes
            FROM dbstat
            ORDER BY size_bytes DESC
        """)
        
        table_sizes = [
            {"table": row[0], "size_mb": row[1] / (1024 * 1024)}
            for row in cur.fetchall()
        ]
        
        conn.close()
        
        return {
            "total_size_mb": f"{size_mb:.2f}",
            "total_size_bytes": size_bytes,
            "total_links": total_links,
            "table_sizes": table_sizes
        }
        
    except Exception as e:
        logger.error(f"Error getting database size: {e}")
        return {
            "total_size_mb": "Unknown",
            "total_size_bytes": 0,
            "total_links": 0,
            "table_sizes": []
        }


def clean_database():
    """
    تنظيف قاعدة البيانات مع الاحتفاظ بالنسخ الاحتياطية
    """
    try:
        # إنشاء نسخة احتياطية قبل التنظيف
        backup_file = create_backup()
        
        if backup_file:
            logger.info(f"Backup created before cleanup: {backup_file}")
            
            # حذف الروابط المكررة (في حال وجود أي مشكلة)
            conn = get_connection()
            cur = conn.cursor()
            
            cur.execute("""
                DELETE FROM links
                WHERE id NOT IN (
                    SELECT MIN(id)
                    FROM links
                    GROUP BY url
                )
            """)
            
            deleted_count = conn.total_changes
            conn.commit()
            conn.close()
            
            # تحسين قاعدة البيانات
            optimize_database()
            
            logger.info(f"Database cleaned. Removed {deleted_count} duplicate entries.")
            return True
        else:
            logger.error("Cannot clean database without backup")
            return False
            
    except Exception as e:
        logger.error(f"Error cleaning database: {e}")
        return False
