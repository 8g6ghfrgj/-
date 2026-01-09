import asyncio
import logging
from datetime import datetime, timedelta
from typing import List, Optional, Tuple

from telethon import TelegramClient, events
from telethon.sessions import StringSession
from telethon.tl.types import Message, MessageReplies

from config import API_ID, API_HASH
from session_manager import get_all_sessions
from database import save_link
from link_utils import (
    extract_links_from_message,
    filter_and_classify_link,
    extract_buttons_links
)
from file_extractors import extract_links_from_file
from comment_extractor import extract_comments_links  # سأقوم بإنشاء هذا الملف لاحقاً

# ======================
# Logging
# ======================

logger = logging.getLogger(__name__)

# ======================
# Global State
# ======================

_clients: List[TelegramClient] = []
_collecting: bool = False
_stop_event = asyncio.Event()
_processed_files = set()  # لتجنب معالجة نفس الملف مرتين

# ======================
# Public API
# ======================

def is_collecting() -> bool:
    return _collecting


def stop_collection():
    """
    يوقف الاستماع للرسائل الجديدة فقط
    لا يحذف أي بيانات
    """
    global _collecting
    _collecting = False
    _stop_event.set()
    logger.info("Collection stopped (listening disabled).")


async def start_collection():
    """
    تشغيل كل Sessions
    وبدء جمع التاريخ + الاستماع للجديد
    """
    global _collecting, _clients, _processed_files

    if _collecting:
        logger.info("Collection already running.")
        return

    sessions = get_all_sessions()
    if not sessions:
        logger.warning("No sessions found.")
        return

    _collecting = True
    _stop_event.clear()
    _clients = []
    _processed_files.clear()

    tasks = []
    for session in sessions:
        tasks.append(run_client(session))

    # تشغيل كل الحسابات معاً
    await asyncio.gather(*tasks)

    logger.info("Finished collecting old history.")


# ======================
# Client Runner
# ======================

async def run_client(session_data: dict):
    """
    تشغيل حساب واحد:
    - قراءة كل التاريخ
    - ثم الاستماع للجديد
    """
    session_string = session_data["session"]
    account_name = session_data["name"]

    client = TelegramClient(
        StringSession(session_string),
        API_ID,
        API_HASH
    )

    await client.connect()
    _clients.append(client)

    logger.info(f"Client started: {account_name}")

    # ======================
    # Listener (New Messages)
    # ======================

    @client.on(events.NewMessage)
    async def new_message_handler(event):
        if not _collecting:
            return

        await process_message(
            message=event.message,
            account_name=account_name,
            client=client
        )

    # ======================
    # Read Old History
    # ======================

    await collect_old_messages(client, account_name)

    # بعد الانتهاء من التاريخ نبقى فقط على الاستماع
    await _stop_event.wait()

    await client.disconnect()
    logger.info(f"Client stopped: {account_name}")


# ======================
# Collect History
# ======================

async def collect_old_messages(client: TelegramClient, account_name: str):
    """
    المرور على:
    - كل القنوات
    - كل الجروبات
    - كل المحادثات الخاصة
    وقراءة كل الرسائل من أول رسالة
    """
    async for dialog in client.iter_dialogs():
        entity = dialog.entity

        try:
            # قراءة الرسائل بشكل عكسي (من الأقدم إلى الأحدث)
            async for message in client.iter_messages(entity, reverse=True):
                if not _collecting:
                    return

                await process_message(
                    message=message,
                    account_name=account_name,
                    client=client
                )

        except Exception as e:
            logger.error(f"Error reading dialog {dialog.name}: {e}")


# ======================
# WhatsApp Time Filter
# ======================

def is_within_last_60_days(message_date: datetime) -> bool:
    """تحقق إذا كانت الرسالة ضمن آخر 60 يومًا"""
    cutoff_date = datetime.now() - timedelta(days=60)
    return message_date > cutoff_date


# ======================
# Telegram Link Validator
# ======================

def is_valid_telegram_link(link: str) -> bool:
    """
    تحقق إذا كان الرابط رابط تليجرام صالح للتجميع
    
    يسمح فقط بـ:
    1. t.me/+xxxxxxx (بدون كلمة invite/)
    2. t.me/xxxxxxx (بدون كلمة invite/)
    
    لا يجمع:
    - روابط البوتات (bot/)
    - روابط الدعوة مع invite/
    - روابط أخرى غير المطلوبة
    """
    link = link.lower().strip()
    
    # تجاهل الروابط التي تحتوي على bot/ أو invite/
    if 't.me/' in link:
        if '/bot/' in link or 't.me/bot/' in link or '/invite/' in link:
            return False
        
        # استخراج الجزء بعد t.me/
        parts = link.split('t.me/')
        if len(parts) > 1:
            path = parts[1].split('?')[0].split('/')[0]  # الحصول على الجزء الأول فقط
            
            # قبول فقط إذا كان يبدأ بـ + أو أرقام/حروف فقط
            if path.startswith('+') and len(path) > 1:
                return True
            elif path.replace('_', '').replace('-', '').isalnum():
                return True
    
    return False


# ======================
# Comment Extractor
# ======================

async def extract_comment_links(client: TelegramClient, message: Message, account_name: str) -> List[str]:
    """
    استخراج الروابط من التعليقات على الرسالة
    """
    links = []
    
    try:
        # التحقق إذا كانت الرسالة تحتوي على تعليقات
        if hasattr(message, 'replies') and isinstance(message.replies, MessageReplies):
            if message.replies.comments and message.replies.replies > 0:
                # جلب التعليقات
                async for reply in client.iter_messages(
                    message.chat_id,
                    reply_to=message.id
                ):
                    # استخراج الروابط من كل تعليق
                    reply_links = extract_links_from_message(reply)
                    links.extend(reply_links)
                    
    except Exception as e:
        logger.error(f"Error extracting comments: {e}")
    
    return links


# ======================
# File Processing
# ======================

async def process_files(client: TelegramClient, message: Message, account_name: str, message_date: datetime):
    """
    معالجة الملفات في الرسالة واستخراج الروابط منها
    تجنب معالجة نفس الملف مرتين
    """
    if not message.file:
        return
    
    try:
        # إنشاء معرف فريد للملف (اسم الملف + حجمه)
        file_name = getattr(message.file, 'name', 'unknown')
        file_size = getattr(message.file, 'size', 0)
        file_id = f"{file_name}_{file_size}"
        
        # التحقق إذا كان الملف قد تم معالجته مسبقاً
        if file_id in _processed_files:
            logger.info(f"File already processed: {file_name}")
            return
        
        # إضافة الملف إلى القائمة المعالجة
        _processed_files.add(file_id)
        
        # استخراج الروابط من الملف
        file_links = await extract_links_from_file(
            client=client,
            message=message
        )
        
        # معالجة الروابط المستخرجة
        for link in file_links:
            # تصنيف الرابط
            classified = filter_and_classify_link(link)
            if not classified:
                continue
            
            platform, link_chat_type = classified
            
            # تطبيق الفلاتر الخاصة بنا
            if platform == 'whatsapp':
                if not is_within_last_60_days(message_date):
                    continue
                save_link(
                    url=link,
                    platform=platform,
                    source_account=account_name,
                    chat_type=link_chat_type,
                    chat_id=str(message.chat_id),
                    message_date=message_date,
                    source_type='file'
                )
                
            elif platform == 'telegram':
                if is_valid_telegram_link(link):
                    # تحديد نوع رابط تليجرام
                    if link.startswith('t.me/+'):
                        telegram_type = 'invite_with_plus'
                    else:
                        telegram_type = 'invite_without_plus'
                    
                    save_link(
                        url=link,
                        platform=f'telegram_{telegram_type}',
                        source_account=account_name,
                        chat_type=link_chat_type,
                        chat_id=str(message.chat_id),
                        message_date=message_date,
                        source_type='file'
                    )
                    
    except Exception as e:
        logger.error(f"File processing error: {e}")


# ======================
# Message Processing
# ======================

async def process_message(
    message: Message,
    account_name: str,
    client: TelegramClient
):
    """
    استخراج كل الروابط من الرسالة:
    - النص
    - الروابط المخفية
    - الأزرار
    - التعليقات
    - الملفات (PDF / DOCX)
    ثم حفظها بدون تكرار
    """
    if not message or not message.text:
        return

    # ======================
    # 1️⃣ روابط النص
    # ======================
    text_links = extract_links_from_message(message)
    await process_links_list(text_links, message, account_name, 'text')

    # ======================
    # 2️⃣ روابط الأزرار
    # ======================
    button_links = extract_buttons_links(message)
    await process_links_list(button_links, message, account_name, 'button')

    # ======================
    # 3️⃣ روابط التعليقات
    # ======================
    comment_links = await extract_comment_links(client, message, account_name)
    await process_links_list(comment_links, message, account_name, 'comment')

    # ======================
    # 4️⃣ روابط الملفات
    # ======================
    await process_files(client, message, account_name, message.date)


# ======================
# Links List Processing
# ======================

async def process_links_list(links: List[str], message: Message, account_name: str, source_type: str):
    """
    معالجة قائمة من الروابط
    """
    for link in links:
        # تصنيف الرابط
        classified = filter_and_classify_link(link)
        if not classified:
            continue
        
        platform, link_chat_type = classified
        
        # تطبيق الفلاتر الخاصة بنا
        if platform == 'whatsapp':
            if not is_within_last_60_days(message.date):
                continue
            save_link(
                url=link,
                platform=platform,
                source_account=account_name,
                chat_type=link_chat_type,
                chat_id=str(message.chat_id),
                message_date=message.date,
                source_type=source_type
            )
            
        elif platform == 'telegram':
            if is_valid_telegram_link(link):
                # تحديد نوع رابط تليجرام
                if link.startswith('t.me/+'):
                    telegram_type = 'invite_with_plus'
                else:
                    telegram_type = 'invite_without_plus'
                
                save_link(
                    url=link,
                    platform=f'telegram_{telegram_type}',
                    source_account=account_name,
                    chat_type=link_chat_type,
                    chat_id=str(message.chat_id),
                    message_date=message.date,
                    source_type=source_type
                )


# ======================
# Helpers
# ======================

def get_chat_type(entity) -> str:
    """
    تحديد نوع المحادثة:
    channel / group / private
    """
    cls = entity.__class__.__name__.lower()

    if "channel" in cls:
        return "channel"
    if "chat" in cls:
        return "group"
    return "private"
