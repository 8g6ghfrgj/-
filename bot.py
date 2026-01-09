import asyncio
import logging
import os
import shutil
from datetime import datetime, timedelta

from telegram import (
    Update,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    InputFile
)
from telegram.ext import (
    ApplicationBuilder,
    CommandHandler,
    CallbackQueryHandler,
    MessageHandler,
    ContextTypes,
    filters,
)

from config import BOT_TOKEN
from session_manager import (
    add_session,
    get_all_sessions,
    get_active_sessions,
    delete_session,
    delete_all_sessions,
    test_session,
    get_session_stats,
    check_all_sessions_health,
    export_sessions_to_file
)
from collector import (
    start_collection,
    stop_collection,
    is_collecting,
)
from database import (
    init_db,
    export_links,
    get_links_by_platform_and_type,
    get_links_by_telegram_type,
    count_links_by_platform,
    get_detailed_stats,
    create_backup,
    restore_backup,
    list_backups,
    optimize_database,
    get_database_size,
    clean_database
)
from file_extractors import get_file_processing_stats, clear_file_cache

# ======================
# Logging
# ======================

logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# ======================
# Constants
# ======================

PAGE_SIZE = 20
ADMIN_IDS = []  # أضف هنا أي دي الأدمن الخاص بك: [123456789, 987654321]

# ======================
# Admin Check
# ======================

def is_admin(user_id: int) -> bool:
    """التحقق إذا كان المستخدم أدمن"""
    return user_id in ADMIN_IDS or len(ADMIN_IDS) == 0  # إذا لم تحدد أدمن، الجميع أدمن

# ======================
# Keyboards
# ======================

def main_keyboard(user_id: int = None):
    """لوحة المفاتيح الرئيسية مع أزرار إضافية للأدمن"""
    buttons = [
        [InlineKeyboardButton("➕ إضافة حساب", callback_data="add_account")],
        [InlineKeyboardButton("👤 عرض الحسابات", callback_data="list_accounts")],
        [InlineKeyboardButton("▶️ بدء الجمع", callback_data="start_collect")],
        [InlineKeyboardButton("⏹ إيقاف الجمع", callback_data="stop_collect")],
        [InlineKeyboardButton("📊 عرض الروابط", callback_data="view_links")],
        [InlineKeyboardButton("📤 تصدير الروابط", callback_data="export_links")],
    ]
    
    # أزرار الأدمن
    if user_id and is_admin(user_id):
        admin_buttons = [
            [InlineKeyboardButton("🔧 إدارة النظام", callback_data="admin_panel")],
            [InlineKeyboardButton("📈 إحصائيات مفصلة", callback_data="detailed_stats")],
            [InlineKeyboardButton("💾 النسخ الاحتياطي", callback_data="backup_menu")],
        ]
        buttons.extend(admin_buttons)
    
    return InlineKeyboardMarkup(buttons)


def admin_panel_keyboard():
    """لوحة إدارة النظام للأدمن"""
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("📊 إحصائيات قاعدة البيانات", callback_data="db_stats")],
        [InlineKeyboardButton("⚙️ تحسين قاعدة البيانات", callback_data="optimize_db")],
        [InlineKeyboardButton("🧹 تنظيف قاعدة البيانات", callback_data="clean_db")],
        [InlineKeyboardButton("📂 إحصائيات الملفات", callback_data="file_stats")],
        [InlineKeyboardButton("🧪 اختبار جميع الجلسات", callback_data="test_all_sessions")],
        [InlineKeyboardButton("🗑 حذف جميع الجلسات", callback_data="delete_all_sessions")],
        [InlineKeyboardButton("↩️ العودة", callback_data="back_to_main")],
    ])


def backup_menu_keyboard():
    """قائمة النسخ الاحتياطي"""
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("💾 إنشاء نسخة احتياطية", callback_data="create_backup")],
        [InlineKeyboardButton("📋 قائمة النسخ الاحتياطية", callback_data="list_backups")],
        [InlineKeyboardButton("↩️ العودة", callback_data="back_to_main")],
    ])


def platforms_keyboard():
    """لوحة اختيار المنصات"""
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("📨 تيليجرام", callback_data="choose:telegram")],
        [InlineKeyboardButton("📞 واتساب", callback_data="choose:whatsapp")],
        [InlineKeyboardButton("📈 الإحصائيات", callback_data="stats_summary")],
    ])


def telegram_types_keyboard():
    """لوحة أنواع روابط تليجرام"""
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("🔗 مع +", callback_data="links:telegram_invite_with_plus:group:0"),
            InlineKeyboardButton("🔗 بدون +", callback_data="links:telegram_invite_without_plus:group:0"),
        ],
        [
            InlineKeyboardButton("📢 قنوات", callback_data="links:telegram_invite_without_plus:channel:0"),
            InlineKeyboardButton("📊 إحصائيات تليجرام", callback_data="telegram_stats"),
        ]
    ])


def whatsapp_types_keyboard():
    """لوحة روابط واتساب"""
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("👥 مجموعات واتساب", callback_data="links:whatsapp:group:0")],
        [InlineKeyboardButton("📊 إحصائيات واتساب", callback_data="whatsapp_stats")],
    ])


def pagination_keyboard(platform, chat_type, page):
    """أزرار التصفح"""
    buttons = []

    if page > 0:
        buttons.append(
            InlineKeyboardButton(
                "⬅️ السابق",
                callback_data=f"links:{platform}:{chat_type}:{page - 1}"
            )
        )

    buttons.append(
        InlineKeyboardButton(
            "➡️ التالي",
            callback_data=f"links:{platform}:{chat_type}:{page + 1}"
        )
    )

    return InlineKeyboardMarkup([buttons])


def export_keyboard():
    """أزرار التصدير"""
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("📄 تصدير الكل", callback_data="export:all")],
        [InlineKeyboardButton("📄 تيليجرام (+)", callback_data="export_telegram:invite_with_plus")],
        [InlineKeyboardButton("📄 تيليجرام (-)", callback_data="export_telegram:invite_without_plus")],
        [InlineKeyboardButton("📄 واتساب", callback_data="export:whatsapp")],
        [InlineKeyboardButton("📄 جميع تيليجرام", callback_data="export:telegram_all")],
    ])


# ======================
# Helper Functions
# ======================

def format_number(number):
    """تنسيق الأرقام بفواصل"""
    return f"{number:,}"

def format_size(size_bytes):
    """تنسيق حجم الملف"""
    for unit in ['B', 'KB', 'MB', 'GB']:
        if size_bytes < 1024.0:
            return f"{size_bytes:.2f} {unit}"
        size_bytes /= 1024.0
    return f"{size_bytes:.2f} TB"

def format_time(seconds):
    """تنسيق الوقت"""
    if seconds < 60:
        return f"{seconds:.1f} ثانية"
    elif seconds < 3600:
        return f"{seconds/60:.1f} دقيقة"
    else:
        return f"{seconds/3600:.1f} ساعة"

# ======================
# Commands
# ======================

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """بدء البوت"""
    user_id = update.effective_user.id
    await update.message.reply_text(
        "🤖 *Telegram Multi-Account Link Collector Bot*\n\n"
        "📊 **ميزات جديدة:**\n"
        "• جمع روابط تليجرام (مع + وبدون +)\n"
        "• جمع روابط واتساب من آخر 60 يوم\n"
        "• استخراج الروابط من الملفات\n"
        "• استخراج الروابط من التعليقات\n"
        "• نظام نسخ احتياطي كامل\n"
        "• إحصائيات مفصلة\n\n"
        "اختر أمراً من القائمة:",
        reply_markup=main_keyboard(user_id),
        parse_mode="Markdown"
    )


async def stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """إحصائيات سريعة"""
    stats_data = get_detailed_stats()
    
    message = "📊 *إحصائيات الروابط*\n\n"
    message += f"• إجمالي الروابط: {format_number(stats_data['total'])}\n"
    
    for platform, count in stats_data['platforms'].items():
        if platform.startswith('telegram'):
            platform_name = platform.replace('telegram_', 'تيليجرام ').replace('_', ' ')
        elif platform == 'whatsapp':
            platform_name = 'واتساب'
        else:
            platform_name = platform
        
        message += f"• {platform_name}: {format_number(count)}\n"
    
    await update.message.reply_text(message, parse_mode="Markdown")


async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """مساعدة"""
    help_text = """
📚 *دليل الاستخدام*

*أوامر أساسية:*
/start - بدء البوت
/stats - عرض الإحصائيات
/help - هذه الرسالة

*جمع الروابط:*
• البوت يجمع روابط تليجرام فقط (مع + وبدون +)
• يجمع روابط واتساب من آخر 60 يوم فقط
• يتجاهل روابط البوتات والتكرارات
• يستخرج الروابط من الملفات والتعليقات

*مصادر الروابط:*
1. نص الرسائل
2. أزرار الرسائل
3. التعليقات على الرسائل
4. الملفات (PDF, DOCX, TXT)

*ملاحظة:* البوت لا يجمع روابط بوتات تليجرام
    """
    await update.message.reply_text(help_text, parse_mode="Markdown")


# ======================
# Callbacks
# ======================

async def callbacks(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """معالجة Callback Queries"""
    query = update.callback_query
    user_id = update.effective_user.id
    
    if not is_admin(user_id) and query.data in ["admin_panel", "detailed_stats", "backup_menu", 
                                               "db_stats", "optimize_db", "clean_db", "file_stats",
                                               "test_all_sessions", "delete_all_sessions", "create_backup",
                                               "list_backups"]:
        await query.answer("⛔ هذا الأمر للأدمن فقط!", show_alert=True)
        return
    
    await query.answer()
    data = query.data

    # ➕ إضافة حساب
    if data == "add_account":
        context.user_data["awaiting_session"] = True
        await query.edit_message_text("📥 أرسل Session String الآن:\n\n"
                                     "*ملاحظة:* يجب أن يكون الحساب نشط وليس بوت")

    # 👤 عرض الحسابات
    elif data == "list_accounts":
        sessions = get_all_sessions()
        if not sessions:
            await query.edit_message_text("❌ لا يوجد حسابات مضافة.")
            return

        text = "👤 *الحسابات المضافة:*\n\n"
        buttons = []
        
        for s in sessions:
            status_emoji = "🟢" if s['status'] == 'active' else "🔴" if s['status'] == 'error' else "🟡"
            text += f"{status_emoji} {s['name']}\n"
            text += f"   📱 {s['phone'] or 'غير معروف'}\n"
            text += f"   📅 {s['added_date'][:10] if s['added_date'] else 'غير معروف'}\n\n"
            
            buttons.append([
                InlineKeyboardButton(
                    f"🗑 حذف {s['name'][:15]}",
                    callback_data=f"delete_account:{s['id']}"
                ),
                InlineKeyboardButton(
                    f"🧪 اختبار",
                    callback_data=f"test_session:{s['id']}"
                )
            ])

        buttons.append([InlineKeyboardButton("↩️ العودة", callback_data="back_to_main")])
        
        await query.edit_message_text(
            text[:4000],
            reply_markup=InlineKeyboardMarkup(buttons),
            parse_mode="Markdown"
        )

    elif data.startswith("delete_account:"):
        session_id = int(data.split(":")[1])
        delete_session(session_id)
        await query.edit_message_text("✅ تم حذف الحساب.")
        await asyncio.sleep(1)
        await start(update, context)

    elif data.startswith("test_session:"):
        session_id = int(data.split(":")[1])
        result = await test_session(session_id)
        
        if result["success"]:
            await query.answer("✅ الجلسة صالحة!", show_alert=True)
        else:
            await query.answer(f"❌ {result['message']}", show_alert=True)

    # ▶️ بدء الجمع
    elif data == "start_collect":
        if is_collecting():
            await query.answer("⏳ الجمع يعمل بالفعل!", show_alert=True)
            return

        await query.edit_message_text("⏳ جاري بدء عملية الجمع...")
        asyncio.create_task(start_collection())
        await query.edit_message_text("✅ بدأ الجمع بنجاح!\n\n"
                                     "📊 *الميزات النشطة:*\n"
                                     "• جمع روابط تليجرام\n"
                                     "• جمع روابط واتساب (60 يوم)\n"
                                     "• استخراج من الملفات\n"
                                     "• استخراج من التعليقات")

    # ⏹ إيقاف الجمع
    elif data == "stop_collect":
        if not is_collecting():
            await query.answer("⏸ الجمع متوقف بالفعل!", show_alert=True)
            return

        stop_collection()
        await query.edit_message_text("⏹ تم إيقاف الجمع.\n\n"
                                     "*ملاحظة:* تم حفظ جميع الروابط المجمعة")

    # 📊 عرض الروابط
    elif data == "view_links":
        await query.edit_message_text(
            "📊 اختر المنصة:",
            reply_markup=platforms_keyboard()
        )

    # 📈 الإحصائيات
    elif data == "stats_summary":
        stats_data = get_detailed_stats()
        
        message = "📊 *إحصائيات مفصلة*\n\n"
        message += f"• إجمالي الروابط: {format_number(stats_data['total'])}\n\n"
        
        message += "*المنصات:*\n"
        for platform, count in stats_data['platforms'].items():
            if platform.startswith('telegram'):
                platform_name = platform.replace('telegram_', 'تيليجرام ').replace('_', ' ')
            elif platform == 'whatsapp':
                platform_name = 'واتساب'
            else:
                platform_name = platform
            
            message += f"• {platform_name}: {format_number(count)}\n"
        
        message += "\n*أنواع المحادثات:*\n"
        for chat_type, count in stats_data['chat_types'].items():
            chat_name = "مجموعات" if chat_type == "group" else "قنوات" if chat_type == "channel" else chat_type
            message += f"• {chat_name}: {format_number(count)}\n"
        
        await query.edit_message_text(message[:4000], parse_mode="Markdown")

    elif data == "telegram_stats":
        stats_data = get_detailed_stats()
        message = "📨 *إحصائيات تليجرام*\n\n"
        
        telegram_count = sum(count for platform, count in stats_data['platforms'].items() 
                           if platform.startswith('telegram'))
        message += f"• إجمالي روابط تليجرام: {format_number(telegram_count)}\n\n"
        
        message += "*حسب النوع:*\n"
        for t_type, count in stats_data.get('telegram_types', {}).items():
            type_name = "مع +" if "with_plus" in t_type else "بدون +"
            message += f"• {type_name}: {format_number(count)}\n"
        
        await query.edit_message_text(message, parse_mode="Markdown")

    elif data == "whatsapp_stats":
        stats_data = get_detailed_stats()
        whatsapp_count = stats_data['platforms'].get('whatsapp', 0)
        
        message = "📞 *إحصائيات واتساب*\n\n"
        message += f"• إجمالي روابط واتساب: {format_number(whatsapp_count)}\n\n"
        message += "*ملاحظة:* يتم جمع رواق واتساب من آخر 60 يوم فقط"
        
        await query.edit_message_text(message, parse_mode="Markdown")

    # اختيار منصة
    elif data == "choose:telegram":
        await query.edit_message_text(
            "📨 روابط تيليجرام:",
            reply_markup=telegram_types_keyboard()
        )

    elif data == "choose:whatsapp":
        await query.edit_message_text(
            "📞 روابط واتساب:",
            reply_markup=whatsapp_types_keyboard()
        )

    # عرض روابط
    elif data.startswith("links:"):
        _, platform, chat_type, page = data.split(":")
        page = int(page)

        if platform.startswith('telegram_'):
            # روابط تليجرام حسب النوع
            links = get_links_by_telegram_type(
                telegram_type=platform.replace('telegram_', ''),
                limit=PAGE_SIZE,
                offset=page * PAGE_SIZE
            )
        else:
            links = get_links_by_platform_and_type(
                platform=platform,
                chat_type=chat_type,
                limit=PAGE_SIZE,
                offset=page * PAGE_SIZE
            )

        if not links and page == 0:
            await query.answer("❌ لا توجد روابط!", show_alert=True)
            return

        # إنشاء العنوان
        if platform.startswith('telegram_'):
            type_name = "مع +" if "with_plus" in platform else "بدون +"
            title = f"تيليجرام ({type_name})"
        else:
            platform_names = {
                'whatsapp': 'واتساب',
                'telegram': 'تيليجرام'
            }
            chat_names = {
                'group': 'مجموعات',
                'channel': 'قنوات'
            }
            title = f"{platform_names.get(platform, platform)} / {chat_names.get(chat_type, chat_type)}"

        text = f"🔗 *روابط {title} – صفحة {page + 1}*\n\n"

        for item in links:
            if len(item) == 4:  # رابط مع معلومات إضافية
                url, platform_info, chat_type_info, date = item
            else:  # رابط مع التاريخ فقط
                url, date = item[0], item[-1]
            
            year = date[:4] if date else "----"
            text += f"[{year}] {url}\n"

        await query.edit_message_text(
            text[:4000],
            reply_markup=pagination_keyboard(platform, chat_type, page),
            parse_mode="Markdown"
        )

    # 📤 تصدير الروابط
    elif data == "export_links":
        await query.edit_message_text(
            "📤 اختر نوع التصدير:",
            reply_markup=export_keyboard()
        )

    elif data.startswith("export:"):
        platform = data.split(":")[1]
        
        await query.edit_message_text("⏳ جاري تصدير الروابط...")
        
        if platform == "telegram_all":
            # تصدير جميع روابط تليجرام
            path = export_links("telegram_invite_with_plus")
            path2 = export_links("telegram_invite_without_plus")
            
            if path and path2:
                # دمج الملفات
                merged_path = "exports/telegram_all.txt"
                with open(merged_path, 'w', encoding='utf-8') as outfile:
                    for fname in [path, path2]:
                        if os.path.exists(fname):
                            with open(fname, 'r', encoding='utf-8') as infile:
                                outfile.write(infile.read())
                
                with open(merged_path, "rb") as f:
                    await query.message.reply_document(
                        document=InputFile(f, filename="telegram_all.txt"),
                        caption="📨 جميع روابط تليجرام"
                    )
            else:
                await query.answer("❌ لا توجد روابط تليجرام!", show_alert=True)
        else:
            path = export_links(platform)
            
            if not path or not os.path.exists(path):
                await query.answer("❌ لا توجد روابط!", show_alert=True)
                return

            with open(path, "rb") as f:
                await query.message.reply_document(
                    document=InputFile(f, filename=os.path.basename(path)),
                    caption=f"📤 روابط {platform}"
                )

    elif data.startswith("export_telegram:"):
        telegram_type = data.split(":")[1]
        await query.edit_message_text("⏳ جاري تصدير الروابط...")
        
        path = export_links("telegram", telegram_type)
        
        if not path or not os.path.exists(path):
            await query.answer("❌ لا توجد روابط!", show_alert=True)
            return

        type_name = "مع +" if telegram_type == "invite_with_plus" else "بدون +"
        with open(path, "rb") as f:
            await query.message.reply_document(
                document=InputFile(f, filename=f"telegram_{telegram_type}.txt"),
                caption=f"📨 روابط تليجرام ({type_name})"
            )

    # 🔧 لوحة الأدمن
    elif data == "admin_panel":
        await query.edit_message_text(
            "🔧 *لوحة إدارة النظام*\n\n"
            "اختر الأمر المطلوب:",
            reply_markup=admin_panel_keyboard(),
            parse_mode="Markdown"
        )

    elif data == "detailed_stats":
        stats_data = get_detailed_stats()
        
        message = "📈 *إحصائيات مفصلة*\n\n"
        
        message += "*إجمالي الروابط:*\n"
        message += f"• الكل: {format_number(stats_data['total'])}\n\n"
        
        message += "*حسب المنصة:*\n"
        for platform, count in stats_data['platforms'].items():
            platform_name = platform.replace('telegram_', 'تيليجرام ').replace('_', ' ')
            platform_name = platform_name.replace('whatsapp', 'واتساب')
            message += f"• {platform_name}: {format_number(count)}\n"
        
        message += "\n*حسب نوع المحادثة:*\n"
        for chat_type, count in stats_data['chat_types'].items():
            chat_name = "مجموعات" if chat_type == "group" else "قنوات" if chat_type == "channel" else chat_type
            message += f"• {chat_name}: {format_number(count)}\n"
        
        message += "\n*حسب المصدر:*\n"
        for source_type, count in stats_data.get('source_types', {}).items():
            source_name = {
                'text': 'نص',
                'button': 'أزرار',
                'comment': 'تعليقات',
                'file': 'ملفات'
            }.get(source_type, source_type)
            message += f"• {source_name}: {format_number(count)}\n"
        
        await query.edit_message_text(message[:4000], parse_mode="Markdown")

    elif data == "backup_menu":
        await query.edit_message_text(
            "💾 *قائمة النسخ الاحتياطي*\n\n"
            "اختر الأمر المطلوب:",
            reply_markup=backup_menu_keyboard(),
            parse_mode="Markdown"
        )

    elif data == "create_backup":
        await query.edit_message_text("⏳ جاري إنشاء نسخة احتياطية...")
        backup_file = create_backup()
        
        if backup_file:
            file_size = os.path.getsize(backup_file)
            await query.edit_message_text(
                f"✅ تم إنشاء النسخة الاحتياطية\n\n"
                f"📁 الملف: `{os.path.basename(backup_file)}`\n"
                f"📊 الحجم: {format_size(file_size)}\n"
                f"📅 الوقت: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
                parse_mode="Markdown"
            )
        else:
            await query.edit_message_text("❌ فشل إنشاء النسخة الاحتياطية")

    elif data == "list_backups":
        backups = list_backups()
        
        if not backups:
            await query.edit_message_text("❌ لا توجد نسخ احتياطية")
            return
        
        message = "📋 *قائمة النسخ الاحتياطية*\n\n"
        
        for backup in backups[:10]:  # عرض أول 10 نسخ فقط
            message += f"📁 *{backup['filename']}*\n"
            message += f"   📅 {backup['date']}\n"
            message += f"   📊 {backup['size_mb']} MB\n\n"
        
        if len(backups) > 10:
            message += f"*ومزيد {len(backups) - 10} نسخة...*\n"
        
        await query.edit_message_text(message, parse_mode="Markdown")

    elif data == "db_stats":
        db_size = get_database_size()
        session_stats = get_session_stats()
        
        message = "🗄️ *إحصائيات قاعدة البيانات*\n\n"
        
        message += "*الحجم:*\n"
        message += f"• الحجم الكلي: {db_size['total_size_mb']} MB\n"
        message += f"• عدد الروابط: {format_number(db_size['total_links'])}\n\n"
        
        message += "*الجلسات:*\n"
        message += f"• النشطة: {session_stats['active']}\n"
        message += f"• المعطلة: {session_stats['error']}\n"
        message += f"• الكل: {session_stats['total']}\n\n"
        
        message += "*أحجام الجداول:*\n"
        for table in db_size.get('table_sizes', []):
            message += f"• {table['table']}: {table['size_mb']:.2f} MB\n"
        
        await query.edit_message_text(message, parse_mode="Markdown")

    elif data == "optimize_db":
        await query.edit_message_text("⏳ جاري تحسين قاعدة البيانات...")
        optimize_database()
        await query.edit_message_text("✅ تم تحسين قاعدة البيانات بنجاح")

    elif data == "clean_db":
        await query.edit_message_text("⚠️ *تنبيه:*\n\n"
                                     "سيتم تنظيف قاعدة البيانات من التكرارات\n"
                                     "وسيتم إنشاء نسخة احتياطية تلقائياً\n\n"
                                     "هل أنت متأكد؟",
                                     reply_markup=InlineKeyboardMarkup([
                                         [InlineKeyboardButton("✅ نعم، تابع", callback_data="confirm_clean_db")],
                                         [InlineKeyboardButton("❌ لا، إلغاء", callback_data="admin_panel")]
                                     ]),
                                     parse_mode="Markdown")

    elif data == "confirm_clean_db":
        await query.edit_message_text("⏳ جاري التنظيف مع النسخ الاحتياطي...")
        success = clean_database()
        
        if success:
            await query.edit_message_text("✅ تم تنظيف قاعدة البيانات بنجاح")
        else:
            await query.edit_message_text("❌ فشل تنظيف قاعدة البيانات")

    elif data == "file_stats":
        file_stats = get_file_processing_stats()
        
        message = "📂 *إحصائيات معالجة الملفات*\n\n"
        
        message += f"• الملفات في الكاش: {file_stats['cache_size']}\n"
        message += f"• الروابط المستخرجة: {format_number(file_stats['total_links_extracted'])}\n\n"
        
        if file_stats.get('file_types'):
            message += "*حسب نوع الملف:*\n"
            for file_type, count in file_stats['file_types'].items():
                message += f"• {file_type.upper()}: {count}\n"
        
        await query.edit_message_text(message, parse_mode="Markdown")

    elif data == "test_all_sessions":
        await query.edit_message_text("⏳ جاري اختبار جميع الجلسات...")
        results = await check_all_sessions_health()
        
        healthy = sum(1 for r in results if r['status'] == 'healthy')
        unhealthy = sum(1 for r in results if r['status'] == 'unhealthy')
        
        message = f"🧪 *نتائج اختبار الجلسات*\n\n"
        message += f"✅ النشطة: {healthy}\n"
        message += f"❌ المعطلة: {unhealthy}\n"
        message += f"📊 المجموع: {len(results)}\n\n"
        
        if unhealthy > 0:
            message += "*الجلسات المعطلة:*\n"
            for result in results:
                if result['status'] == 'unhealthy':
                    message += f"• {result['name']}: {result.get('error', 'خطأ غير معروف')}\n"
        
        await query.edit_message_text(message[:4000], parse_mode="Markdown")

    elif data == "delete_all_sessions":
        await query.edit_message_text("⚠️ *تحذير شديد:*\n\n"
                                     "سيتم حذف جميع الحسابات المضافة!\n"
                                     "لا يمكن التراجع عن هذا الإجراء\n\n"
                                     "هل أنت متأكد تماماً؟",
                                     reply_markup=InlineKeyboardMarkup([
                                         [InlineKeyboardButton("🔥 نعم، احذف الكل", callback_data="confirm_delete_all")],
                                         [InlineKeyboardButton("❌ إلغاء", callback_data="admin_panel")]
                                     ]),
                                     parse_mode="Markdown")

    elif data == "confirm_delete_all":
        deleted_count = delete_all_sessions()
        await query.edit_message_text(f"✅ تم حذف جميع الحسابات ({deleted_count})")

    elif data == "back_to_main":
        await start(update, context)

    else:
        await query.edit_message_text("❌ أمر غير معروف")


# ======================
# Messages
# ======================

async def messages(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """معالجة الرسائل النصية"""
    user_id = update.effective_user.id
    
    if context.user_data.get("awaiting_session"):
        try:
            session_text = update.message.text.strip()
            
            # التحقق من أن النص يبدو كـ session string
            if len(session_text) < 100 or ":" not in session_text:
                await update.message.reply_text("❌ هذا لا يبدو كـ Session String صالح")
                return
            
            await update.message.reply_text("⏳ جاري التحقق من الحساب...")
            await add_session(session_text)
            await update.message.reply_text("✅ تم إضافة الحساب بنجاح!")
            
        except ValueError as e:
            await update.message.reply_text(f"❌ {str(e)}")
        except Exception as e:
            logger.error(f"Error adding session: {e}")
            await update.message.reply_text("❌ حدث خطأ غير متوقع. حاول مرة أخرى.")
        finally:
            context.user_data["awaiting_session"] = False
    
    else:
        # إذا كان المستخدم أدمن ويرسل ملف backup
        if is_admin(user_id) and update.message.document:
            file = update.message.document
            if file.file_name and file.file_name.endswith('.db'):
                await update.message.reply_text("⏳ جاري استعادة النسخة الاحتياطية...")
                
                # تحميل الملف
                file_path = f"temp_backup_{file.file_name}"
                file_obj = await file.get_file()
                await file_obj.download_to_drive(file_path)
                
                # استعادة النسخة
                success = restore_backup(file_path)
                
                # حذف الملف المؤقت
                try:
                    os.remove(file_path)
                except:
                    pass
                
                if success:
                    await update.message.reply_text("✅ تم استعادة النسخة الاحتياطية بنجاح")
                else:
                    await update.message.reply_text("❌ فشل استعادة النسخة الاحتياطية")
            else:
                await update.message.reply_text("❌ يرجى إرسال ملف قاعدة بيانات (.db)")
        else:
            await update.message.reply_text(
                "📝 أرسل Session String لإضافة حساب جديد\n"
                "أو استخدم الأزرار أدناه:",
                reply_markup=main_keyboard(user_id)
            )


# ======================
# Error Handler
# ======================

async def error_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """معالجة الأخطاء"""
    logger.error(f"Update {update} caused error {context.error}")
    
    if update and update.effective_message:
        try:
            await update.effective_message.reply_text(
                "❌ حدث خطأ غير متوقع. حاول مرة أخرى."
            )
        except:
            pass


# ======================
# Main
# ======================

def main():
    """الدالة الرئيسية"""
    # تهيئة قاعدة البيانات
    init_db()
    
    # إنشاء تطبيق البوت
    app = ApplicationBuilder().token(BOT_TOKEN).build()
    
    # إضافة Handlers
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("stats", stats))
    app.add_handler(CommandHandler("help", help_command))
    app.add_handler(CallbackQueryHandler(callbacks))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, messages))
    app.add_handler(MessageHandler(filters.Document.ALL, messages))
    
    # إضافة معالج الأخطاء
    app.add_error_handler(error_handler)
    
    logger.info("🤖 Bot is starting...")
    print("\n" + "="*50)
    print("📱 Telegram Link Collector Bot")
    print("📊 Version: 2.0 (Enhanced)")
    print("🎯 Features:")
    print("  • Telegram links collection (+ and without +)")
    print("  • WhatsApp links (last 60 days only)")
    print("  • File extraction (PDF, DOCX, TXT)")
    print("  • Comments extraction")
    print("  • Full backup system")
    print("  • Advanced statistics")
    print("="*50 + "\n")
    
    # تشغيل البوت
    app.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    main()
