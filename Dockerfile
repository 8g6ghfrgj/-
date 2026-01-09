FROM python:3.11-slim

WORKDIR /app

# تحسين الأداء + منع مشاكل البايت
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=/app

# تثبيت المتطلبات
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# نسخ المشروع
COPY . .

# إنشاء المجلدات المطلوبة
RUN mkdir -p data exports

# تشغيل البوت
CMD ["python", "bot.py"]
