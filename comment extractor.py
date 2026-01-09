import logging
from typing import List, Set
from datetime import datetime, timedelta

from telethon import TelegramClient
from telethon.tl.types import Message, MessageReplies

from link_utils import extract_links_from_message, is_valid_link_for_extraction

logger = logging.getLogger(__name__)


async def extract_comments_links(
    client: TelegramClient,
    message: Message,
    account_name: str,
    message_date: datetime
) -> List[str]:
    """
    استخراج الروابط من التعليقات على الرسالة
    
    Args:
        client: عميل التليجرام
        message: الرسالة الأصلية
        account_name: اسم الحساب
        message_date: تاريخ الرسالة الأصلية
        
    Returns:
        List[str]: قائمة الروابط المستخرجة
    """
    links: Set[str] = set()
    
    try:
        # التحقق إذا كانت الرسالة تحتوي على تعليقات
        if hasattr(message, 'replies') and message.replies:
            replies_info: MessageReplies = message.replies
            
            # التحقق من وجود تعليقات
            if replies_info.comments and replies_info.replies > 0:
                logger.info(f"Found {replies_info.replies} comments on message {message.id} in chat {message.chat_id}")
                
                # الحصول على معرف الدردشة للتعليقات
                comments_peer = replies_info.comments_peer
                if not comments_peer:
                    logger.debug(f"No comments peer found for message {message.id}")
                    return list(links)
                
                # جلب جميع التعليقات
                comment_count = 0
                try:
                    async for reply in client.iter_messages(
                        comments_peer,
                        reply_to=message.id,
                        limit=100  # حد أقصى 100 تعليق لتجنب الحمل الزائد
                    ):
                        comment_count += 1
                        
                        # استخراج الروابط من التعليق
                        reply_links = extract_links_from_message(reply)
                        
                        # تصفية الروابط
                        for link in reply_links:
                            if is_valid_link_for_extraction(link):
                                links.add(link.strip())
                        
                        # التحقق من وجود ردود على التعليق نفسه
                        if hasattr(reply, 'replies') and reply.replies:
                            if hasattr(reply.replies, 'replies') and reply.replies.replies > 0:
                                # استخراج الروابط من الردود على التعليق
                                async for sub_reply in client.iter_messages(
                                    comments_peer,
                                    reply_to=reply.id,
                                    limit=50  # حد أقصى 50 رد لكل تعليق
                                ):
                                    sub_links = extract_links_from_message(sub_reply)
                                    for link in sub_links:
                                        if is_valid_link_for_extraction(link):
                                            links.add(link.strip())
                    
                    logger.info(f"Processed {comment_count} comments, found {len(links)} links")
                    
                except Exception as e:
                    logger.error(f"Error fetching comments for message {message.id}: {e}")
        
    except Exception as e:
        logger.error(f"Error extracting comments from message {message.id}: {e}")
    
    return list(links)


async def extract_thread_links(
    client: TelegramClient,
    message: Message
) -> List[str]:
    """
    استخراج الروابط من سلسلة الرسائل (Threads)
    
    Args:
        client: عميل التليجرام
        message: الرسالة الأصلية
        
    Returns:
        List[str]: قائمة الروابط المستخرجة
    """
    links: Set[str] = set()
    
    try:
        # التحقق إذا كانت الرسالة جزء من thread
        if hasattr(message, 'grouped_id') and message.grouped_id:
            logger.info(f"Message {message.id} is part of thread {message.grouped_id}")
            
            # جلب باقي رسائل الـ thread
            async for thread_msg in client.iter_messages(
                message.chat_id,
                search="",  # بحث فارغ للحصول على جميع الرسائل
                limit=20  # حد معقول للـ thread
            ):
                if hasattr(thread_msg, 'grouped_id') and thread_msg.grouped_id == message.grouped_id:
                    thread_links = extract_links_from_message(thread_msg)
                    for link in thread_links:
                        if is_valid_link_for_extraction(link):
                            links.add(link.strip())
    
    except Exception as e:
        logger.error(f"Error extracting thread links: {e}")
    
    return list(links)


def is_recent_comment(comment_date: datetime, original_date: datetime) -> bool:
    """
    التحقق إذا كان التعليق حديث (ضمن 60 يوم للواتساب)
    
    Args:
        comment_date: تاريخ التعليق
        original_date: تاريخ الرسالة الأصلية
        
    Returns:
        bool: True إذا كان التعليق حديث
    """
    # إذا كان التعليق أقدم من الرسالة الأصلية
    if comment_date < original_date:
        return False
    
    # للواتساب: فقط التعليقات ضمن 60 يوم
    cutoff_date = datetime.now() - timedelta(days=60)
    return comment_date > cutoff_date


async def process_comments_for_links(
    client: TelegramClient,
    message: Message,
    account_name: str,
    message_date: datetime,
    platform: str
) -> List[str]:
    """
    معالجة التعليقات واستخراج الروابط مع الفلاتر المناسبة
    
    Args:
        client: عميل التليجرام
        message: الرسالة
        account_name: اسم الحساب
        message_date: تاريخ الرسالة
        platform: المنصة (telegram/whatsapp)
        
    Returns:
        List[str]: الروابط المفلترة
    """
    all_links = await extract_comments_links(client, message, account_name, message_date)
    filtered_links = []
    
    for link in all_links:
        # تطبيق نفس الفلاتر من collector
        if "t.me" in link.lower() or "telegram.me" in link.lower():
            # رابط تليجرام
            from link_utils import is_valid_telegram_link
            if is_valid_telegram_link(link):
                filtered_links.append(link)
        
        elif "whatsapp.com" in link.lower() or "wa.me" in link.lower():
            # رابط واتساب - نتحقق من التاريخ
            # نحتاج للتحقق من تاريخ التعليق نفسه
            # سنفترض أن تعليقات واتساب كلها حديثة إذا كانت الرسالة حديثة
            if platform == "whatsapp":
                filtered_links.append(link)
    
    return filtered_links


# وظيفة مساعدة للاستخدام في collector
async def get_all_comments_links(
    client: TelegramClient,
    message: Message,
    account_name: str
) -> List[str]:
    """
    دالة مبسطة للحصول على جميع روابط التعليقات
    
    Args:
        client: عميل التليجرام
        message: الرسالة
        account_name: اسم الحساب
        
    Returns:
        List[str]: جميع الروابط من التعليقات
    """
    try:
        return await extract_comments_links(client, message, account_name, message.date)
    except Exception as e:
        logger.error(f"Error in get_all_comments_links: {e}")
        return []
