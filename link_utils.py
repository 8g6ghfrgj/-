import re
from typing import List, Set, Optional, Tuple
from telethon.tl.types import Message


# ======================
# Regex عام لأي رابط
# ======================

URL_REGEX = re.compile(
    r"(https?://[^\s<>\"]+)",
    re.IGNORECASE
)


# ======================
# أنماط المنصات (معدلة - فقط تليجرام وواتساب)
# ======================

PLATFORM_PATTERNS = {
    "telegram": re.compile(r"(t\.me|telegram\.me)", re.IGNORECASE),
    "whatsapp": re.compile(r"(wa\.me|chat\.whatsapp\.com)", re.IGNORECASE),
    # تم إزالة باقي المنصات
}


# ======================
# أنماط روابط تليجرام المطلوبة
# ======================

# النوع الأول: يبدأ بعلامة +
TG_PLUS_INVITE_REGEX = re.compile(
    r"https?://t\.me/\+[A-Za-z0-9_-]+",
    re.IGNORECASE
)

# النوع الثاني: بدون علامة + (فقط أرقام وحروف)
TG_WITHOUT_PLUS_REGEX = re.compile(
    r"https?://t\.me/[A-Za-z0-9_-]+(?!/)",
    re.IGNORECASE
)

# روابط تليجرام المرفوضة
TG_REJECTED_PATTERNS = [
    re.compile(r"/bot/", re.IGNORECASE),
    re.compile(r"t\.me/bot", re.IGNORECASE),
    re.compile(r"/invite/", re.IGNORECASE),
    re.compile(r"t\.me/invite", re.IGNORECASE),
]


# ======================
# أنماط روابط واتساب
# ======================

WA_GROUP_REGEX = re.compile(r"https?://chat\.whatsapp\.com/[A-Za-z0-9]+", re.I)
WA_PHONE_REGEX = re.compile(r"https?://wa\.me/\d+", re.I)


# ======================
# استخراج الروابط من الرسالة (معدل)
# ======================

def extract_links_from_message(message: Message) -> List[str]:
    """
    استخراج الروابط من:
    - نص الرسالة
    - الكابتشن
    - الروابط المخفية
    """
    links: Set[str] = set()

    text = message.text or message.message or ""
    if text:
        found_links = URL_REGEX.findall(text)
        for link in found_links:
            # تصفية الروابط على مستوى الاستخراج
            if is_valid_link_for_extraction(link):
                links.add(link.strip())

    return list(links)


# ======================
# استخراج روابط الأزرار
# ======================

def extract_buttons_links(message: Message) -> List[str]:
    """
    استخراج الروابط من أزرار الرسالة
    """
    links: Set[str] = set()
    
    if message.reply_markup:
        for row in message.reply_markup.rows:
            for button in row.buttons:
                if hasattr(button, "url") and button.url:
                    link = button.url
                    # تصفية الروابط على مستوى الاستخراج
                    if is_valid_link_for_extraction(link):
                        links.add(link.strip())
    
    return list(links)


# ======================
# فحص أولي للروابط
# ======================

def is_valid_link_for_extraction(link: str) -> bool:
    """
    فحص أولي للروابط قبل التصنيف
    تقليل الحمل عن طريق رفض الروابط غير المرغوبة مبكراً
    """
    if not link:
        return False
    
    link_lower = link.lower()
    
    # رفض الروابط غير المرغوبة مبكراً
    if any(pattern.search(link_lower) for pattern in TG_REJECTED_PATTERNS):
        return False
    
    # قبول فقط روابط تليجرام وواتساب
    if "t.me" in link_lower or "telegram.me" in link_lower:
        return True
    
    if "whatsapp.com" in link_lower or "wa.me" in link_lower:
        return True
    
    # رفض كل الروابط الأخرى
    return False


# ======================
# تصنيف المنصة (معدل)
# ======================

def classify_platform(url: str) -> str:
    """
    تصنيف المنصة - يعيد فقط 'telegram' أو 'whatsapp' أو 'other'
    """
    url_lower = url.lower()
    
    if "t.me" in url_lower or "telegram.me" in url_lower:
        return "telegram"
    
    if "whatsapp.com" in url_lower or "wa.me" in url_lower:
        return "whatsapp"
    
    return "other"


# ======================
# تصنيف روابط تليجرام
# ======================

def classify_telegram_link(url: str) -> Tuple[str, str]:
    """
    تصنيف رابط تليجرام إلى نوعين
    
    Returns:
        (telegram_type, chat_type)
        telegram_type: 'invite_with_plus' أو 'invite_without_plus'
        chat_type: 'group' أو 'channel'
    """
    url_lower = url.lower()
    
    # تحديد نوع الرابط
    if TG_PLUS_INVITE_REGEX.match(url):
        telegram_type = "invite_with_plus"
        chat_type = "group"  # روابط + عادة تكون لجروبات
    elif TG_WITHOUT_PLUS_REGEX.match(url):
        telegram_type = "invite_without_plus"
        # محاولة التخمين إذا كان قناة أو مجموعة
        if any(pattern in url_lower for pattern in ["c/", "channel/"]):
            chat_type = "channel"
        else:
            chat_type = "group"
    else:
        # هذا لا يجب أن يحدث إذا استخدمنا is_valid_telegram_link أولاً
        telegram_type = "unknown"
        chat_type = "unknown"
    
    return telegram_type, chat_type


# ======================
# تصنيف روابط واتساب
# ======================

def classify_whatsapp_link(url: str) -> str:
    """
    تصنيف رابط واتساب
    
    Returns:
        chat_type: 'group' أو 'private'
    """
    url_lower = url.lower()
    
    if WA_GROUP_REGEX.match(url):
        return "group"
    elif WA_PHONE_REGEX.match(url):
        return "private"
    else:
        return "unknown"


# ======================
# الفلترة والتصنيف النهائي
# ======================

def filter_and_classify_link(url: str) -> Optional[Tuple[str, str]]:
    """
    فلترة الرابط قبل الحفظ
    
    Returns:
        (platform_type, chat_type)
        أو None إذا الرابط مرفوض
        
    platform_type:
        - 'whatsapp' لروابط واتساب
        - 'telegram_invite_with_plus' لروابط تليجرام مع +
        - 'telegram_invite_without_plus' لروابط تليجرام بدون +
    """
    
    # فحص سريع أولي
    if not is_valid_link_for_extraction(url):
        return None
    
    # ===== Telegram =====
    if "t.me" in url.lower() or "telegram.me" in url.lower():
        # التحقق من صحة رابط تليجرام
        if not is_valid_telegram_link(url):
            return None
        
        # تصنيف رابط تليجرام
        telegram_type, chat_type = classify_telegram_link(url)
        
        # إعادة النتيجة مع platform_type المناسب
        platform_type = f"telegram_{telegram_type}"
        return (platform_type, chat_type)
    
    # ===== WhatsApp =====
    elif "whatsapp.com" in url.lower() or "wa.me" in url.lower():
        chat_type = classify_whatsapp_link(url)
        
        # رفض الروابط الخاصة (أرقام هواتف)
        if chat_type == "private":
            return None
        
        # قبول فقط روابط المجموعات
        if chat_type == "group":
            return ("whatsapp", "group")
        
        return None
    
    # ===== رفض كل الروابط الأخرى =====
    return None


# ======================
# التحقق من صحة رابط تليجرام (للاستخدام في collector)
# ======================

def is_valid_telegram_link(link: str) -> bool:
    """
    تحقق إذا كان الرابط رابط تليجرام صالح للتجميع
    
    يسمح فقط بـ:
    1. t.me/+xxxxxxx
    2. t.me/xxxxxxx (بدون مسارات إضافية)
    
    لا يجمع:
    - روابط البوتات (bot/)
    - روابط الدعوة مع invite/
    - روابط الرسائل الفردية
    - t.me/+1-9 (الهواتف - مرفوضة)
    - روابط بمسارات متعددة
    """
    link_lower = link.lower().strip()
    
    # تجاهل الروابط التي تحتوي على أنماط مرفوضة
    if any(pattern.search(link_lower) for pattern in TG_REJECTED_PATTERNS):
        return False
    
    # التحقق من وجود t.me
    if 't.me/' not in link_lower:
        return False
    
    # استخراج المسار
    parts = link_lower.split('t.me/')
    if len(parts) < 2:
        return False
    
    path_part = parts[1].split('?')[0]  # إزالة query parameters
    
    # تقسيم المسار إلى أجزاء
    path_segments = path_part.split('/')
    first_segment = path_segments[0]
    
    # رفض إذا كان هناك أكثر من جزء (مثال: t.me/username/123)
    if len(path_segments) > 1:
        return False
    
    # رفض الروابط التي تبدو كأرقام هواتف (مثل t.me/+123456789)
    if first_segment.startswith('+') and len(first_segment) <= 12:
        # إذا كان الرقم قصير (مثل +123456789) فهو رقم هاتف
        # أرقام الهواتف عادة بين 7-12 رقم مع +
        remaining = first_segment[1:]
        if remaining.isdigit() and 7 <= len(remaining) <= 12:
            return False
    
    # قبول فقط إذا كان:
    # 1. يبدأ بـ + ويتبعه أحرف/أرقام
    # 2. أو يتكون فقط من أحرف وأرقام وشرطة سفلية وشرطة
    if first_segment.startswith('+'):
        # التحقق من أن ما بعد + يحتوي على أحرف صالحة
        remaining = first_segment[1:]
        if not remaining.replace('_', '').replace('-', '').isalnum():
            return False
        return True
    else:
        # رابط بدون +
        if not first_segment.replace('_', '').replace('-', '').isalnum():
            return False
        return True
