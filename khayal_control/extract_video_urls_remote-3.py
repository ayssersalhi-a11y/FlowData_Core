"""
=====================================================================
عامل استخراج روابط الفيديو (Extract Worker) - نسخة WebSocket عبر Render
يتصل بخادم Render على القناة "kaggle_extract"، ويستقبل أوامر start/stop
فوريًا (بلا انتظار دورة فحص كما كان الحال مع GitHub)، وينفذ الاستخراج،
ثم يرسل النتائج فور جهوزيتها لنفس القناة.

استبدال كامل لآلية khayal_control/commands.json و results.json على
GitHub. تخزين بيانات الأفلام/المسلسلات نفسها يبقى على GitHub كما هو
(Github_Manager.gd) - لا علاقة له بهذا الملف.

مناسب للتشغيل كـ "Save & Run All" (Batch/Commit) في Kaggle، لأنه لا
ينتظر أي تفاعل بشري مباشر، ويعيد الاتصال تلقائيًا عند أي انقطاع.

جديد: بعد استخراج روابط الحلقات النهائية فقط (وليس كل الروابط
الخام المكتشفة)، يُنزَّل كل رابط تلقائيًا داخل /kaggle/working عبر
FFmpeg باسم مُعاد تسميته حسب ترتيب الحلقة (episode_01.mp4، episode_02.mp4...).

جديد أيضًا: وضع "تصفح يدوي" (manual_start/manual_click/manual_finish/
manual_cancel) للمواقع التي لا تعرض حلقاتها كأزرار أرقام. يبقي جلسة
متصفح واحدة مفتوحة، يرسل لقطة شاشة WebP بعد كل خطوة عبر نفس قناة
WebSocket، وينتظر أمرك بالزر التالي الذي تريد الضغط عليه (بالاسم/النص
الظاهر في اللقطة) حتى تصل لصفحة الفيديو ثم تطلب الاستخراج النهائي.

جديد أيضًا: تثبيت تلقائي ذاتي للمتطلبات عند تشغيل الخلية — لا حاجة
لأي خلية !pip install منفصلة بعد الآن. السكربت يتحقق من كل حزمة
(playwright, websockets, pillow) ويثبتها فقط إن كانت ناقصة، ويتجاوز
التثبيت تمامًا إن كانت موجودة مسبقًا (يوفر وقتًا في كل تشغيلة جديدة).
=====================================================================

خطوات الإعداد:
1) عدّل RENDER_WS_URL أدناه ليطابق خادمك على Render (نفس المسار
   المستخدم في realtime_bridge.py: /ws/kaggle_extract).
2) لا حاجة لأي تثبيت يدوي — شغّل الخلية مباشرة والسكربت سيتحقق من
   المتطلبات (playwright, websockets, pillow, متصفح Chromium) ويثبت
   الناقص منها تلقائيًا في كل مرة (ويتجاوز الموجود بسرعة).
3) شغّل هذا الكود في خلية Kaggle - سيتصل بالخادم فورًا وينتظر الأوامر.

=====================================================================
تنسيق الأمر القادم من Godot (عبر قناة "extract"):

{
  "channel": "extract",
  "command": "start",
  "url": "https://example.com/watch/episode-1",
  "start_episode": 1,
  "max_episodes": 5,
  "request_id": "..."   <-- أي قيمة فريدة، غير مستخدمة الآن للمقارنة
                             (كل أمر يصل فوريًا عبر القناة المفتوحة)
}

لإرسال أمر "إيقاف" أثناء التنفيذ:
{ "channel": "extract", "command": "stop", "request_id": "..." }
=====================================================================
استمرارية التشغيل (حل قيد الجلسات 9-12 ساعة):

كل جلسة Kaggle (CPU) تُقتل قسرًا بعد ~12 ساعة كحد أقصى. استخدم ميزة
Kaggle الرسمية "Schedule" لجدولة تشغيله تلقائيًا كل 13-14 ساعة.

عند كل إقلاع، يُرسل هذا السكربت "نبضة" فورية بحقل "session_started_at"
عبر نفس قناة WebSocket فور نجاح الاتصال - راقبها من Godot للتأكد أن
الجدولة تعمل فعليًا دون فتح المتصفح يدويًا.
=====================================================================
"""

import os
import re
import io
import sys
import json
import time
import base64
import asyncio
import random
import subprocess
import importlib
import traceback

# ===================== تثبيت تلقائي ذاتي للمتطلبات =====================
# يفحص كل حزمة قبل استيرادها: إن كانت مثبتة مسبقًا يتجاوزها فورًا،
# وإن كانت ناقصة يثبتها عبر pip. لا حاجة لأي خلية !pip install منفصلة.

def _ensure_package(pip_name: str, import_name: str = None):
    import_name = import_name or pip_name
    try:
        importlib.import_module(import_name)
        print(f"✅ الحزمة '{pip_name}' مثبتة مسبقًا — تم التجاوز.")
    except ImportError:
        print(f"📦 الحزمة '{pip_name}' غير موجودة — جارٍ التثبيت...")
        subprocess.run(
            [sys.executable, "-m", "pip", "install", pip_name, "--quiet"],
            check=True,
        )
        print(f"✅ تم تثبيت '{pip_name}' بنجاح.")


for _pip_name, _import_name in [
    ("playwright", "playwright"),
    ("websockets", "websockets"),
    ("pillow", "PIL"),
]:
    _ensure_package(_pip_name, _import_name)


def _ensure_playwright_browser():
    """
    أمر 'playwright install' يتحقق داخليًا من هاش المتصفح المثبت ويتجاوز
    التنزيل تلقائيًا إن كانت نفس النسخة موجودة مسبقًا — لذا تشغيله في كل
    مرة آمن، وسريع جدًا عندما لا يوجد شيء جديد لتنزيله.
    """
    try:
        result = subprocess.run(
            [sys.executable, "-m", "playwright", "install", "chromium", "--with-deps"],
            capture_output=True, text=True, timeout=300,
        )
        if result.returncode == 0:
            print("✅ متصفح Chromium جاهز (تم التأكد من وجوده أو تثبيته الآن).")
        else:
            print(f"⚠️ تحذير أثناء تجهيز متصفح Chromium:\n{result.stderr[-500:]}")
    except Exception as e:
        print(f"⚠️ تعذر تشغيل أمر تثبيت متصفح Chromium: {e}")


_ensure_playwright_browser()
# ========================================================================

import websockets
from PIL import Image as PILImage
from playwright.async_api import async_playwright
from IPython.display import Image, display

# ============== إعدادات Render - عدّلها حسب خادمك ==============
RENDER_WS_URL = "wss://render-khayal.onrender.com/ws/kaggle_extract"
RECONNECT_DELAY_SECONDS = 5
# ===================================================================

VIDEO_PATTERN = re.compile(
    r"\.(mp4|m3u8|ts|webm|mov|mkv|mpd|aac)(\?|$)|init-stream", re.IGNORECASE
)

WAIT_SECONDS = 15
MIN_SEGMENTS_FOR_FULL_EPISODE = 5
DELAY_MIN = 6
DELAY_MAX = 12

DOWNLOAD_DIR = "/kaggle/working/downloaded_episodes"
SCREENSHOT_WEBP_QUALITY = 70  # جودة WebP عند إرسال لقطات التصفح اليدوي (0-100)

# حالة مشتركة بين مهمة الاستماع (listen_forever) ومهمة التنفيذ (worker_loop)
command_queue: asyncio.Queue = asyncio.Queue()
stop_event = asyncio.Event()
_active_ws = None  # الاتصال الحالي المفتوح - يُستخدم للإرسال الفوري

# حالة وضع "التصفح اليدوي" (منفصلة تمامًا عن الوضع التلقائي أعلاه)
manual_command_queue: asyncio.Queue = asyncio.Queue()
_manual_playwright = None
_manual_browser = None
_manual_context = None
_manual_page = None
_manual_seen_urls: set = set()
_manual_ordered_urls: list = []


# ===================== أدوات الاتصال بخادم Render =====================

async def send_result(payload: dict):
    """يرسل نتيجة فورًا عبر القناة المفتوحة حاليًا (إن وُجدت)."""
    global _active_ws
    payload.setdefault("channel", "extract")
    if _active_ws is not None:
        try:
            await _active_ws.send(json.dumps(payload, ensure_ascii=False))
            if payload.get("type") != "log":
                print(f"   📤 أُرسلت نتيجة فورًا: {payload.get('status')}")
            return
        except Exception as e:
            if payload.get("type") != "log":
                print(f"   ⚠️ تعذر إرسال النتيجة (الاتصال مقطوع؟): {e}")
    else:
        if payload.get("type") != "log":
            print("   ⚠️ لا يوجد اتصال نشط حاليًا - سيُعاد الاتصال تلقائيًا قريبًا")


def log_print(text: str):
    """تطبع النص في Kaggle وترسله فوراً إلى Godot كسجل مباشر."""
    print(text)
    try:
        loop = asyncio.get_running_loop()
        loop.create_task(send_result({
            "channel": "extract",
            "type": "log",
            "message": text
        }))
    except RuntimeError:
        pass


async def listen_forever():
    """يحافظ على اتصال WebSocket مفتوحًا، ويعيد الاتصال تلقائيًا عند الانقطاع."""
    global _active_ws
    while True:
        try:
            log_print(f"🔌 محاولة الاتصال بخادم Render: {RENDER_WS_URL}")
            async with websockets.connect(RENDER_WS_URL, ping_interval=20, ping_timeout=20) as ws:
                _active_ws = ws
                log_print("✅ متصل بخادم Render (قناة الاستخراج)")

                session_start_time = time.strftime("%Y-%m-%d %H:%M:%S")
                await send_result({
                    "status": "session_started",
                    "session_started_at": session_start_time,
                })

                async for raw in ws:
                    try:
                        data = json.loads(raw)
                    except json.JSONDecodeError:
                        continue

                    cmd = data.get("command")
                    if cmd == "start":
                        log_print(f"\n📥 تم استلام أمر بدء: {json.dumps(data, ensure_ascii=False)}")
                        await command_queue.put(data)
                    elif cmd == "stop":
                        log_print("\n🛑 تم استلام أمر إيقاف")
                        stop_event.set()
                    elif cmd in ("manual_start", "manual_click", "manual_finish", "manual_cancel"):
                        log_print(f"\n📥 [تصفح يدوي] أمر مستلم: {cmd}")
                        await manual_command_queue.put(data)

        except Exception as e:
            # نلتقط أي نوع فشل اتصال (رفض المصافحة، رابط خاطئ، DNS، انقطاع
            # الشبكة...) وليس فقط ConnectionClosed/OSError - أي استثناء غير
            # ملتقط هنا كان سيُسقط الخلية بالكامل في Kaggle بدل إعادة المحاولة.
            log_print(f"⚠️ فشل الاتصال ({type(e).__name__}: {e}) - إعادة المحاولة خلال {RECONNECT_DELAY_SECONDS} ثوانٍ...")
        finally:
            _active_ws = None
        await asyncio.sleep(RECONNECT_DELAY_SECONDS)


# ===================== منطق الفلترة (بلا تغيير) =====================

def filter_full_episodes_only(urls: list) -> list:
    groups = {}
    for u in urls:
        prefix = u.rsplit("/", 1)[0]
        groups.setdefault(prefix, []).append(u)

    kept = []
    for prefix, group_urls in groups.items():
        ts_count = sum(1 for u in group_urls if u.lower().endswith(".ts"))
        m3u8_links = [u for u in group_urls if ".m3u8" in u.lower()]
        if ts_count >= MIN_SEGMENTS_FOR_FULL_EPISODE and m3u8_links:
            kept.extend(m3u8_links)
    return kept


async def human_delay(label: str = ""):
    seconds = random.uniform(DELAY_MIN, DELAY_MAX)
    log_print(f"   ⏳ تأخير {seconds:.1f} ثانية {label}...")
    await asyncio.sleep(seconds)


# ===================== التقاط لقطة شاشة وتحويلها WebP (للتصفح اليدوي) =====================

async def capture_screenshot_webp_base64(page, quality: int = SCREENSHOT_WEBP_QUALITY) -> str:
    """
    يلتقط لقطة شاشة للصفحة الحالية، يحولها إلى WebP (أصغر حجمًا بكثير من
    PNG بجودة مقاربة)، ويعيدها كنص Base64 جاهز للإرسال عبر WebSocket.
    """
    png_bytes = await page.screenshot(type="png")
    img = PILImage.open(io.BytesIO(png_bytes)).convert("RGB")
    buf = io.BytesIO()
    img.save(buf, format="WEBP", quality=quality)
    webp_bytes = buf.getvalue()
    log_print(f"   🖼️ لقطة شاشة: PNG {len(png_bytes)//1024}KB → WebP {len(webp_bytes)//1024}KB")
    return base64.b64encode(webp_bytes).decode("ascii")


# ===================== تنزيل الروابط النهائية فقط (خطوة جديدة) =====================

def download_final_link(url: str, filename: str) -> str:
    """
    يُنزّل رابط m3u8/ts نهائي واحد فقط عبر FFmpeg إلى DOWNLOAD_DIR
    باسم مُعاد تسميته (وليس اسم الملف الأصلي من الرابط).
    يُعيد المسار المحلي عند النجاح، أو "" عند الفشل.
    """
    os.makedirs(DOWNLOAD_DIR, exist_ok=True)
    dest_path = os.path.join(DOWNLOAD_DIR, filename)

    log_print(f"   ⬇️ بدء تنزيل: {filename}")
    cmd = ["ffmpeg", "-y", "-i", url, "-c", "copy", dest_path]
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=900)
        if result.returncode != 0:
            log_print(f"   ❌ فشل تنزيل {filename}: {result.stderr[-800:]}")
            return ""
        size_mb = os.path.getsize(dest_path) / (1024 * 1024)
        log_print(f"   ✅ اكتمل تنزيل {filename} ({size_mb:.1f} MB)")
        return dest_path
    except Exception as e:
        log_print(f"   ❌ استثناء أثناء تنزيل {filename}: {e}")
        return ""


def download_all_final_links(episode_links: list) -> list:
    """
    يُنزّل فقط الروابط النهائية المُفلترة (episode_links) وليس كل
    الروابط الخام. يُعيد قائمة من القواميس: {"url", "local_path", "renamed_to", "success"}.
    """
    if not episode_links:
        log_print("   ℹ️ لا توجد روابط نهائية لتنزيلها.")
        return []

    log_print(f"\n📦 بدء تنزيل {len(episode_links)} رابط نهائي إلى مخزن Kaggle...")
    downloaded = []
    for idx, url in enumerate(episode_links, start=1):
        filename = f"episode_{idx:02d}.mp4"
        local_path = download_final_link(url, filename)
        downloaded.append({
            "url": url,
            "renamed_to": filename,
            "local_path": local_path,
            "success": bool(local_path),
        })
    ok_count = sum(1 for d in downloaded if d["success"])
    log_print(f"📦 انتهى التنزيل: {ok_count}/{len(episode_links)} بنجاح.")
    return downloaded


# ===================== منطق الضغط على أزرار الحلقات (بلا تغيير) =====================

async def _scroll_then_click(locator):
    await locator.scroll_into_view_if_needed(timeout=5000)
    await locator.click(timeout=5000)


async def try_click_episode(page, num: int) -> bool:
    locator = page.locator(f"text=/^{num}$/").first
    strategies = [
        ("ضغط عادي", lambda: locator.click(timeout=5000)),
        ("تمرير ثم ضغط", lambda: _scroll_then_click(locator)),
        ("ضغط إجباري (force)", lambda: locator.click(timeout=5000, force=True)),
        ("ضغط عبر JavaScript مباشرة", lambda: locator.dispatch_event("click")),
    ]
    for name, action in strategies:
        success = False
        try:
            log_print(f"   🔸 محاولة: {name}...")
            await action()
            log_print(f"   ✅ نجحت المحاولة: {name}")
            success = True
        except Exception as e:
            log_print(f"   ❌ فشلت المحاولة ({name}): {e}")

        await page.wait_for_timeout(1500)
        shot_path = f"/kaggle/working/episode_{num}_{name.replace(' ', '_')}.png"
        await page.screenshot(path=shot_path)
        display(Image(filename=shot_path))

        if success:
            return True

        for close_selector in ["button[aria-label*='close' i]", ".modal-close", "svg[class*='close']"]:
            try:
                close_btn = page.locator(close_selector).first
                if await close_btn.is_visible(timeout=1000):
                    await close_btn.click(timeout=1000)
            except Exception:
                pass
        try:
            await page.keyboard.press("Escape")
        except Exception:
            pass

    return False


# ===================== التصفح اليدوي (للمواقع بلا أزرار أرقام) =====================
# جلسة متصفح واحدة تبقى مفتوحة بين كل خطوة والتي تليها. بعد كل خطوة
# (فتح صفحة أو ضغط زر) تُرسَل لقطة شاشة فورًا، وينتظر السكربت أمرك
# التالي. كل طلبات/استجابات الفيديو تُلتقط في الخلفية طوال الجلسة
# بنفس آلية VIDEO_PATTERN المستخدمة في الوضع التلقائي.

def _manual_add_url(url: str, source: str):
    if url not in _manual_seen_urls:
        _manual_seen_urls.add(url)
        _manual_ordered_urls.append(url)
        log_print(f"   [+ جديد] {source}: {url}")


async def _manual_on_response(response):
    if VIDEO_PATTERN.search(response.url):
        _manual_add_url(response.url, "استجابة")


async def _manual_try_click_text(text: str) -> bool:
    """
    يحاول العثور على عنصر يحتوي هذا النص بالضبط أولًا، ثم بشكل جزئي إن
    لم يُعثر عليه، ثم يضغط عليه بعدة استراتيجيات (بنفس أسلوب try_click_episode).
    """
    locators = [
        _manual_page.locator(f"text=/^{re.escape(text)}$/").first,
        _manual_page.get_by_text(text, exact=False).first,
    ]
    for locator in locators:
        try:
            if await locator.count() == 0:
                continue
        except Exception:
            continue

        strategies = [
            ("ضغط عادي", lambda l=locator: l.click(timeout=5000)),
            ("تمرير ثم ضغط", lambda l=locator: _scroll_then_click(l)),
            ("ضغط إجباري (force)", lambda l=locator: l.click(timeout=5000, force=True)),
            ("ضغط عبر JavaScript مباشرة", lambda l=locator: l.dispatch_event("click")),
        ]
        for name, action in strategies:
            try:
                log_print(f"   🔸 محاولة: {name}...")
                await action()
                log_print(f"   ✅ نجحت المحاولة: {name}")
                return True
            except Exception as e:
                log_print(f"   ❌ فشلت المحاولة ({name}): {e}")
    return False


async def _manual_send_screenshot(request_id):
    if _manual_page is None:
        return
    try:
        b64 = await capture_screenshot_webp_base64(_manual_page)
        await send_result({
            "type": "screenshot",
            "image_format": "webp",
            "image_base64": b64,
            "current_url": _manual_page.url,
            "request_id": request_id,
        })
    except Exception as e:
        log_print(f"⚠️ فشل التقاط/إرسال لقطة الشاشة: {e}")


async def _manual_close_browser():
    global _manual_playwright, _manual_browser, _manual_context, _manual_page
    try:
        if _manual_browser is not None:
            await _manual_browser.close()
    except Exception:
        pass
    try:
        if _manual_playwright is not None:
            await _manual_playwright.stop()
    except Exception:
        pass
    _manual_playwright = None
    _manual_browser = None
    _manual_context = None
    _manual_page = None


async def manual_worker_loop():
    global _manual_playwright, _manual_browser, _manual_context, _manual_page
    while True:
        cmd = await manual_command_queue.get()
        action = cmd.get("command")
        request_id = cmd.get("request_id")

        if action == "manual_start":
            start_url = cmd.get("url")
            if not start_url:
                log_print("⚠️ [تصفح يدوي] لا يوجد رابط في أمر manual_start. تجاهل.")
                continue

            await _manual_close_browser()
            _manual_seen_urls.clear()
            _manual_ordered_urls.clear()

            log_print(f"\n🖱️ [تصفح يدوي] بدء جلسة جديدة: {start_url}")
            try:
                _manual_playwright = await async_playwright().start()
                _manual_browser = await _manual_playwright.chromium.launch(headless=True)
                _manual_context = await _manual_browser.new_context(
                    user_agent=(
                        "Mozilla/5.0 (Linux; Android 14; SM-G991B) "
                        "AppleWebKit/537.36 (KHTML, like Gecko) "
                        "Chrome/124.0.0.0 Mobile Safari/537.36"
                    ),
                    viewport={"width": 412, "height": 915},
                    is_mobile=True,
                    has_touch=True,
                )
                _manual_page = await _manual_context.new_page()
                _manual_page.on(
                    "request",
                    lambda r: _manual_add_url(r.url, "طلب") if VIDEO_PATTERN.search(r.url) else None,
                )
                _manual_page.on("response", lambda r: asyncio.create_task(_manual_on_response(r)))
                await _manual_page.goto(start_url, timeout=30000, wait_until="load")
                await asyncio.sleep(2)
            except Exception:
                err = traceback.format_exc()
                log_print(f"❌ [تصفح يدوي] فشل فتح الصفحة:\n{err}")
                await _manual_close_browser()
                continue

            await _manual_send_screenshot(request_id)

        elif action == "manual_finish":
            log_print("\n🏁 [تصفح يدوي] إنهاء الجلسة — استخراج الروابط النهائية وتنزيلها...")
            full_links = filter_full_episodes_only(_manual_ordered_urls)
            downloaded_files = download_all_final_links(full_links)
            result_payload = {
                "status": "done",
                "request_id": request_id,
                "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
                "raw_links_count": len(_manual_ordered_urls),
                "episode_links": full_links,
                "downloaded_files": downloaded_files,
            }
            log_print(f"\n{'#'*60}\nالنتيجة النهائية (تصفح يدوي):\n{json.dumps(result_payload, ensure_ascii=False, indent=2)}\n{'#'*60}")
            await send_result(result_payload)
            await _manual_close_browser()

        elif action == "manual_click":
            if _manual_page is None:
                log_print("⚠️ [تصفح يدوي] لا توجد جلسة نشطة — أرسل 'بدء تصفح يدوي' أولاً.")
                continue
            text = (cmd.get("text") or "").strip()
            if not text:
                log_print("⚠️ [تصفح يدوي] لم يُرسَل أي نص للبحث عن الزر.")
                await _manual_send_screenshot(request_id)
                continue

            log_print(f"\n🖱️ [تصفح يدوي] البحث عن: \"{text}\" والضغط عليه...")
            clicked = await _manual_try_click_text(text)
            if not clicked:
                log_print(f"   ❌ لم يُعثر على أي عنصر مطابق للنص: \"{text}\"")
            await asyncio.sleep(2)
            await _manual_send_screenshot(request_id)

        elif action == "manual_cancel":
            log_print("🛑 [تصفح يدوي] إلغاء الجلسة.")
            await _manual_close_browser()


# ===================== المنطق الرئيسي لاستخراج الروابط (الوضع التلقائي) =====================

async def find_video_links_auto_episodes(start_url: str, start_from_episode: int = None, max_episodes: int = None):
    ordered_urls = []
    seen_urls = set()

    def add_url(url: str, source: str):
        if url not in seen_urls:
            seen_urls.add(url)
            ordered_urls.append(url)
            log_print(f"   [+ جديد] {source}: {url}")

    def on_request(request):
        if VIDEO_PATTERN.search(request.url):
            add_url(request.url, "طلب")

    async def on_response(response):
        if VIDEO_PATTERN.search(response.url):
            add_url(response.url, "استجابة")

    async with async_playwright() as p:
        log_print("1) تشغيل المتصفح...")
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(
            user_agent=(
                "Mozilla/5.0 (Linux; Android 14; SM-G991B) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/124.0.0.0 Mobile Safari/537.36"
            ),
            viewport={"width": 412, "height": 915},
            is_mobile=True,
            has_touch=True,
        )
        page = await context.new_page()
        page.on("request", on_request)
        page.on("response", lambda r: asyncio.create_task(on_response(r)))

        log_print(f"2) فتح الصفحة: {start_url}")
        await page.goto(start_url, timeout=30000, wait_until="load")
        await human_delay("بعد تحميل الصفحة")
        await page.screenshot(path="/kaggle/working/page_screenshot.png")
        display(Image(filename="/kaggle/working/page_screenshot.png"))

        log_print("3) البحث عن أزرار الحلقات...")
        episode_locator = page.locator("text=/^\\d{1,3}$/")
        count = await episode_locator.count()

        episode_numbers = []
        seen_numbers = set()
        for idx in range(count):
            try:
                text = (await episode_locator.nth(idx).text_content() or "").strip()
                if text.isdigit() and text not in seen_numbers:
                    seen_numbers.add(text)
                    episode_numbers.append(int(text))
            except Exception:
                continue
        episode_numbers.sort()

        if start_from_episode:
            episode_numbers = [n for n in episode_numbers if n >= start_from_episode]
        if max_episodes:
            episode_numbers = episode_numbers[:max_episodes]

        if not episode_numbers:
            log_print("   لم يتم اكتشاف أي أزرار حلقات. سيتم فحص الصفحة كما هي فقط.")
            await human_delay("قبل الانتظار لالتقاط الطلبات")
            await page.wait_for_timeout(WAIT_SECONDS * 1000)
        else:
            log_print(f"   سيتم فحص هذه الحلقات: {episode_numbers}\n")
            for num in episode_numbers:
                if stop_event.is_set():
                    log_print("\n🛑 تم استلام أمر إيقاف عن بُعد. إنهاء العملية مبكرًا.")
                    break

                log_print(f"\n--- الانتقال إلى الحلقة {num} ---")
                await human_delay(f"قبل الضغط على الحلقة {num}")
                await try_click_episode(page, num)
                await page.wait_for_timeout(WAIT_SECONDS * 1000)

        await browser.close()

    return ordered_urls


# ===================== الحلقة الرئيسية (Worker Loop) =====================

async def worker_loop():
    while True:
        cmd = await command_queue.get()
        stop_event.clear()

        request_id = cmd.get("request_id")
        start_url = cmd.get("url")
        start_from = cmd.get("start_episode")
        max_episodes = cmd.get("max_episodes")

        if not start_url:
            log_print("⚠️ الأمر المستلم لا يحتوي على رابط. تجاهل.")
            continue

        await send_result({"status": "running", "request_id": request_id, "url": start_url})

        try:
            urls = await find_video_links_auto_episodes(start_url, start_from, max_episodes)
            full_episode_links = filter_full_episodes_only(urls)

            # === خطوة جديدة: تنزيل الروابط النهائية فقط تلقائيًا، بأسماء مُعاد تسميتها ===
            downloaded_files = download_all_final_links(full_episode_links)

            result_payload = {
                "status": "done",
                "request_id": request_id,
                "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
                "source_url": start_url,
                "raw_links_count": len(urls),
                "episode_links": full_episode_links,
                "downloaded_files": downloaded_files,
            }
        except Exception:
            err = traceback.format_exc()
            log_print(f"\n--- حدث خطأ: ---\n{err}")
            result_payload = {
                "status": "error",
                "request_id": request_id,
                "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
                "error": err[-2000:],
            }

        log_print(f"\n{'#'*60}\nالنتيجة النهائية:\n{json.dumps(result_payload, ensure_ascii=False, indent=2)}\n{'#'*60}")
        await send_result(result_payload)

        log_print("\n🔁 العودة لوضع الانتظار لأمر جديد...\n")


# ===================== تشغيل مهمتي الاستماع والتنفيذ معًا =====================

async def main():
    await asyncio.gather(listen_forever(), worker_loop(), manual_worker_loop())


# تشغيل مباشر - انسخ كل هذا الملف في خلية واحدة بـ Kaggle وشغّله
# (Save & Run All مقبول، لا حاجة لتفاعل بشري)
if __name__ == "__main__":
    asyncio.run(main())
