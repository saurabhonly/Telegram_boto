import asyncio
import json
import os
import re
import uuid
import psycopg2
import psycopg2.extras
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
from difflib import SequenceMatcher
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    ApplicationBuilder,
    CommandHandler,
    MessageHandler,
    filters,
    ContextTypes,
    CallbackQueryHandler,
)

BOT_TOKEN       = os.environ["BOT_TOKEN"]
YOUTUBE_API_KEY = os.environ["YOUTUBE_API_KEY"]
TMDB_API_KEY    = os.environ["TMDB_API_KEY"]
DATABASE_URL    = os.environ["DATABASE_URL"]

OGADS_BASE = "https://omg10.com/4/10748956"

_DOMAINS     = os.environ.get("REPLIT_DOMAINS", "")
PUBLIC_HOST  = _DOMAINS.split(",")[0].strip() if _DOMAINS else ""
POSTBACK_URL = f"https://{PUBLIC_HOST}/api/ogads-callback" if PUBLIC_HOST else ""

VIDEO_EXTENSIONS = {".mp4", ".mkv", ".avi", ".ogv", ".mov", ".webm"}
MIN_MOVIE_MB     = 50

TMDB_IMG_BASE = "https://image.tmdb.org/t/p/w500"

SKIP_WORDS = {
    "clip", "trailer", "teaser", "scene", "short", "excerpt",
    "preview", "promo", "song", "music", "ost", "dance",
    "interview", "making", "behind the scenes", "reaction",
    "review", "highlight", "reel", "montage", "cut",
}

QUALITY_LABELS = {
    "2160": "4K (2160p) 🔥",
    "1080": "1080p HD ⭐",
    "720":  "720p HD",
    "480":  "480p",
    "360":  "360p",
    "240":  "240p",
}
SOURCE_LABELS = {
    "archive":   "🏛️ Archive.org",
    "wikimedia": "🌐 Wikimedia",
    "search":    "🔍 Archive.org Search",
}


# ══════════════════════════════════════════════
# DATABASE helpers
# ══════════════════════════════════════════════

def db_conn():
    return psycopg2.connect(DATABASE_URL)


def db_save_session(token: str, chat_id: int, files: list, title: str):
    with db_conn() as conn, conn.cursor() as cur:
        cur.execute(
            """INSERT INTO bot_sessions (token, chat_id, results_json, tmdb_json)
               VALUES (%s, %s, %s, %s)
               ON CONFLICT (token) DO NOTHING""",
            (token, chat_id, json.dumps(files), json.dumps({"title": title})),
        )


def db_get_pending_verified() -> list[dict]:
    with db_conn() as conn, conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            """SELECT token, chat_id, results_json, tmdb_json
               FROM bot_sessions
               WHERE verified = TRUE AND notified = FALSE
                 AND created_at > NOW() - INTERVAL '2 hours'"""
        )
        return [dict(r) for r in cur.fetchall()]


def db_mark_notified(token: str):
    with db_conn() as conn, conn.cursor() as cur:
        cur.execute(
            "UPDATE bot_sessions SET notified = TRUE WHERE token = %s", (token,)
        )


def db_cleanup_old():
    with db_conn() as conn, conn.cursor() as cur:
        cur.execute("DELETE FROM bot_sessions WHERE created_at < NOW() - INTERVAL '3 hours'")


# ══════════════════════════════════════════════
# RELEVANCE SCORING
# ══════════════════════════════════════════════

def title_score(result_title: str, query: str) -> float:
    r     = result_title.lower().strip()
    t     = query.lower().strip()
    seq   = SequenceMatcher(None, r, t).ratio()
    words = [w for w in re.split(r"\W+", t) if len(w) > 2]
    hits  = sum(1 for w in words if w in r) if words else 0
    word  = hits / len(words) if words else 0
    return round(0.55 * seq + 0.45 * word, 4)


# ══════════════════════════════════════════════
# SOURCE 1 — Internet Archive (no duration filter)
# ══════════════════════════════════════════════

def search_archive(query: str) -> list[dict]:
    res = _archive_api(f'"{query}"') or _archive_api(query)
    return res


def _is_short_clip(title: str, duration: float) -> bool:
    """Return True if this looks like a clip/trailer/short — not a full movie."""
    tl = title.lower()
    if any(word in tl for word in SKIP_WORDS):
        return True
    if duration and 0 < duration < 1800:   # less than 30 minutes
        return True
    return False


def _archive_api(term: str) -> list[dict]:
    q   = requests.utils.quote(term)
    url = (
        "https://archive.org/advancedsearch.php"
        f"?q={q}+AND+mediatype:(movies)"
        "&fl[]=identifier,title,duration&output=json&rows=15&sort[]=downloads+desc"
    )
    try:
        res = requests.get(url, timeout=12).json()
    except Exception:
        return []
    out = []
    for doc in res.get("response", {}).get("docs", []):
        t   = doc.get("title", "").strip()
        iid = doc.get("identifier", "").strip()
        dur = float(doc.get("duration") or 0)
        if not t or not iid:
            continue
        if _is_short_clip(t, dur):
            continue
        out.append({
            "title":        t,
            "source":       "archive",
            "identifier":   iid,
            "thumb":        f"https://archive.org/services/img/{iid}",
            "watch_url":    f"https://archive.org/details/{iid}",
            "can_download": True,
            "score":        0.0,
            "is_fallback":  False,
        })
    return out


# ══════════════════════════════════════════════
# SOURCE 2 — Wikimedia Commons
# ══════════════════════════════════════════════

def search_wikimedia(query: str) -> list[dict]:
    API = "https://commons.wikimedia.org/w/api.php"
    VIDEO_EXTS = {".ogv", ".webm", ".mp4", ".ogg"}
    try:
        s = requests.get(API, params={
            "action": "query", "list": "search",
            "srsearch": f"{query} film", "srnamespace": 6,
            "srlimit": 10, "srprop": "title", "format": "json",
        }, timeout=12).json()
        items = s.get("query", {}).get("search", [])
    except Exception:
        return []

    file_titles = [
        it["title"] for it in items
        if os.path.splitext(it.get("title", ""))[1].lower() in VIDEO_EXTS
    ]
    if not file_titles:
        return []

    try:
        info = requests.get(API, params={
            "action": "query", "titles": "|".join(file_titles[:5]),
            "prop": "imageinfo", "iiprop": "url|thumburl|size",
            "iiurlwidth": 500, "format": "json",
        }, timeout=12).json()
        pages = info.get("query", {}).get("pages", {})
    except Exception:
        return []

    out = []
    for page in pages.values():
        fname  = page.get("title", "")
        iinfo  = (page.get("imageinfo") or [{}])[0]
        direct = iinfo.get("url", "")
        thumb  = iinfo.get("thumburl", "") or direct
        if not direct:
            continue
        display = re.sub(r"^File:", "", fname)
        display = os.path.splitext(display)[0].replace("_", " ").strip()
        out.append({
            "title":        display,
            "source":       "wikimedia",
            "identifier":   fname,
            "thumb":        thumb,
            "watch_url":    f"https://commons.wikimedia.org/wiki/{fname.replace(' ', '_')}",
            "direct_url":   direct,
            "can_download": True,
            "score":        0.0,
            "is_fallback":  False,
        })
    return out


# ══════════════════════════════════════════════
# FALLBACK — Archive.org search page
# ══════════════════════════════════════════════

def make_search_fallback(query: str, poster_url: str | None) -> dict:
    """Always-available result: points to Archive.org search results for the query."""
    search_url = (
        "https://archive.org/search"
        f"?query={requests.utils.quote(query)}"
        "&and[]=mediatype%3A%22movies%22"
    )
    return {
        "title":        query,
        "source":       "search",
        "identifier":   None,
        "thumb":        poster_url or "",
        "watch_url":    search_url,
        "can_download": True,
        "score":        1.0,
        "is_fallback":  True,
        "search_url":   search_url,
    }


# ══════════════════════════════════════════════
# TMDB — last, only for metadata
# ══════════════════════════════════════════════

def tmdb_enrich(query: str) -> dict:
    params = {"api_key": TMDB_API_KEY, "query": query, "page": 1}
    try:
        data = requests.get("https://api.themoviedb.org/3/search/multi",
                            params=params, timeout=10).json()
    except Exception:
        return {}
    for item in data.get("results", []):
        if item.get("media_type") not in ("movie", "tv"):
            continue
        title  = item.get("title") or item.get("name") or ""
        year   = (item.get("release_date") or item.get("first_air_date") or "")[:4]
        poster = item.get("poster_path")
        if not title:
            continue
        return {
            "title":      title,
            "year":       year,
            "poster_url": (TMDB_IMG_BASE + poster) if poster else None,
            "language":   item.get("original_language", "").upper(),
            "overview":   (item.get("overview") or "")[:200],
        }
    return {}


# ══════════════════════════════════════════════
# MULTI-SOURCE SEARCH
# ══════════════════════════════════════════════

def run_all_sources(query: str, poster_url: str | None = None) -> list[dict]:
    all_results: list[dict] = []
    with ThreadPoolExecutor(max_workers=2) as ex:
        futures = {
            ex.submit(search_archive,   query): "archive",
            ex.submit(search_wikimedia, query): "wikimedia",
        }
        for future in as_completed(futures):
            try:
                all_results.extend(future.result())
            except Exception:
                pass

    for r in all_results:
        r["score"] = title_score(r["title"], query)

    all_results.sort(key=lambda x: (-x["score"], {"archive": 0, "wikimedia": 1}.get(x["source"], 9)))

    seen: set[str] = set()
    ranked: list[dict] = []
    for r in all_results:
        if r["watch_url"] not in seen:
            seen.add(r["watch_url"])
            ranked.append(r)

    top = ranked[:3]

    # Always guarantee at least one result via Archive.org search fallback
    if not top:
        top = [make_search_fallback(query, poster_url)]
    elif len(top) < 3:
        # Add search fallback as last option so user can explore more
        top.append(make_search_fallback(query, poster_url))

    return top


# ══════════════════════════════════════════════
# ARCHIVE.ORG file list
# ══════════════════════════════════════════════

def get_archive_files(identifier: str) -> list[dict]:
    try:
        meta = requests.get(f"https://archive.org/metadata/{identifier}", timeout=12).json()
    except Exception:
        return []
    files = []
    for f in meta.get("files", []):
        name: str = f.get("name", "")
        ext = os.path.splitext(name)[1].lower()
        if ext not in VIDEO_EXTENSIONS:
            continue
        size_mb = round(int(f.get("size", 0) or 0) / (1024 * 1024), 1)
        if 0 < size_mb < MIN_MOVIE_MB:
            continue
        quality = _detect_quality(name, f, size_mb)
        dl_url  = (f"https://archive.org/download/{identifier}/"
                   + requests.utils.quote(name))
        files.append({"name": name, "quality": quality,
                      "size_mb": size_mb, "url": dl_url, "ext": ext})
    order = ["2160", "1080", "720", "480", "360", "240", "other"]
    files.sort(key=lambda x: order.index(x["quality"]) if x["quality"] in order else len(order))
    return files[:8]


def _detect_quality(filename: str, meta: dict, size_mb: float) -> str:
    combined = filename + " " + str(meta.get("height", ""))
    for key in ["2160", "1080", "720", "480", "360", "240"]:
        if key in combined:
            return key
    if size_mb > 3000: return "1080"
    if size_mb > 1000: return "720"
    if size_mb > 400:  return "480"
    if size_mb > 100:  return "360"
    return "other"


# ══════════════════════════════════════════════
# SEND RESULT CARD
# ══════════════════════════════════════════════

async def send_result_card(chat_id: int, bot, idx: int, result: dict, tmdb: dict):
    src_lbl   = SOURCE_LABELS.get(result["source"], result["source"])
    score_pct = int(result["score"] * 100)
    title     = result["title"]
    is_fb     = result.get("is_fallback", False)

    photo    = tmdb.get("poster_url") if idx == 0 and tmdb.get("poster_url") else result.get("thumb", "")
    year_tag = f" ({tmdb['year']})" if idx == 0 and tmdb.get("year") else ""
    overview = f"\n_{tmdb['overview']}_" if idx == 0 and tmdb.get("overview") else ""

    if is_fb:
        caption = (
            f"🔍 *{title}* — Archive.org Search\n\n"
            "Yeh movie directly indexed nahi mili, lekin\n"
            "Archive.org pe search karke dekho 👇"
            + overview
        )
    else:
        caption = (
            f"{src_lbl}  |  🎯 {score_pct}% match\n"
            f"🎬 *{title}*{year_tag}" + overview
        )

    source = result["source"]

    if source == "archive":
        buttons = [[
            InlineKeyboardButton("📺 Watch on Archive.org", url=result["watch_url"]),
            InlineKeyboardButton("📥 Download 🔒", callback_data=f"dl_lock_{idx}"),
        ]]
    elif source == "wikimedia":
        buttons = [[
            InlineKeyboardButton("📺 Watch", url=result["watch_url"]),
            InlineKeyboardButton("📥 Download 🔒", callback_data=f"dl_lock_{idx}"),
        ]]
    else:
        # search fallback
        buttons = [[
            InlineKeyboardButton("🔍 Browse on Archive.org", url=result["watch_url"]),
            InlineKeyboardButton("📥 Download 🔒", callback_data=f"dl_lock_{idx}"),
        ]]

    markup = InlineKeyboardMarkup(buttons)
    try:
        if photo:
            await bot.send_photo(chat_id=chat_id, photo=photo,
                                 caption=caption, parse_mode="Markdown",
                                 reply_markup=markup)
            return
    except Exception:
        pass
    await bot.send_message(chat_id=chat_id, text=caption,
                           parse_mode="Markdown", reply_markup=markup)


# ══════════════════════════════════════════════
# BACKGROUND POLLER
# ══════════════════════════════════════════════

async def poll_verified_sessions(app):
    cleanup_counter = 0
    while True:
        await asyncio.sleep(5)
        try:
            pending = db_get_pending_verified()
        except Exception:
            continue

        for session in pending:
            token   = session["token"]
            chat_id = session["chat_id"]
            files   = json.loads(session["results_json"])
            meta    = json.loads(session["tmdb_json"])
            title   = meta.get("title", "Movie")

            try:
                await app.bot.send_message(
                    chat_id=chat_id,
                    text=f"✅ *Task complete! Download links ready hain* 🎉\n\n🎬 *{title}*",
                    parse_mode="Markdown",
                )
                await _send_download_links(app.bot, chat_id, files, title)
                db_mark_notified(token)
            except Exception as e:
                print(f"[Poller] Error for {chat_id}: {e}")

        cleanup_counter += 1
        if cleanup_counter >= 360:
            cleanup_counter = 0
            try:
                db_cleanup_old()
            except Exception:
                pass


async def _send_download_links(bot, chat_id: int, files: list[dict], title: str):
    if not files:
        await bot.send_message(
            chat_id=chat_id,
            text=f"⚠️ Direct download files nahi mili.\n"
                 f"🌐 Archive.org pe search karo:\n"
                 f"https://archive.org/search?query={requests.utils.quote(title)}&and[]=mediatype%3A%22movies%22"
        )
        return

    # Check if it's a search-fallback (URL-only, no quality files)
    if files and files[0].get("is_search"):
        await bot.send_message(
            chat_id=chat_id,
            text=f"🔍 *{title}* ke liye Archive.org search:\n\n"
                 f"{files[0]['url']}\n\n"
                 "_Wahan se apni pasand ki file download karo._",
            parse_mode="Markdown",
        )
        return

    btns: list[list[InlineKeyboardButton]] = []
    for f in files:
        label = QUALITY_LABELS.get(f["quality"], f"Quality {f['quality']}")
        size  = f" ({f['size_mb']} MB)" if f.get("size_mb", 0) > 0 else ""
        btns.append([InlineKeyboardButton(f"📥 {label}{size}", url=f["url"])])

    await bot.send_message(
        chat_id=chat_id,
        text=f"📥 *{title}* — Quality chunke download karo:",
        parse_mode="Markdown",
        reply_markup=InlineKeyboardMarkup(btns),
    )


# ══════════════════════════════════════════════
# HANDLERS
# ══════════════════════════════════════════════

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_text(
        "🎬 *Movie & Web Series Bot*\n\n"
        "Kisi bhi movie ka naam type karo!\n\n"
        "📺 *Watch* → Archive.org pe free dekho\n"
        "📥 *Download* → Ek chhota task karo → Link milega\n\n"
        "Naam bhejo 👇",
        parse_mode="Markdown",
    )


async def search(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.message.text.strip()
    if not query:
        return

    msg = await update.message.reply_text(
        f"🔍 *\"{query}\"* search ho raha hai...",
        parse_mode="Markdown",
    )

    # Run sources + TMDB in parallel
    with ThreadPoolExecutor(max_workers=2) as ex:
        tmdb_future    = ex.submit(tmdb_enrich, query)
        sources_future = ex.submit(run_all_sources, query, None)
        tmdb    = tmdb_future.result()
        results = sources_future.result()

    # Attach poster to any fallback card
    for r in results:
        if r.get("is_fallback") and not r.get("thumb"):
            r["thumb"] = tmdb.get("poster_url", "")

    context.user_data["results"] = results
    context.user_data["tmdb"]    = tmdb

    await msg.delete()

    header = (
        f"🎬 *{tmdb.get('title', query)}*"
        + (f" ({tmdb['year']})" if tmdb.get("year") else "")
        + (f"  🌐 {tmdb['language']}" if tmdb.get("language") else "")
        + "\n\n"
        + f"📺 Watch: *Free*  |  📥 Download: *Task karo*\n"
        + "─" * 20
    )
    await update.message.reply_text(header, parse_mode="Markdown")

    for i, result in enumerate(results):
        await send_result_card(
            chat_id=update.effective_chat.id,
            bot=context.bot,
            idx=i,
            result=result,
            tmdb=tmdb,
        )


# ── Download Lock ─────────────────────────────

async def download_lock(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    q = update.callback_query
    await q.answer()

    idx     = int(q.data.split("_")[2])
    results = context.user_data.get("results", [])
    tmdb    = context.user_data.get("tmdb", {})

    if idx >= len(results):
        await q.message.reply_text("⚠️ Session expire ho gayi. Dobara search karo.")
        return

    result  = results[idx]
    title   = tmdb.get("title") or result["title"]
    source  = result["source"]
    chat_id = q.message.chat_id

    # Prepare download files
    if source == "archive" and result.get("identifier"):
        wait  = await q.message.reply_text("⏳ Files check ho rahi hain...")
        files = get_archive_files(result["identifier"])
        await wait.delete()
        if not files:
            # Fallback to search page
            search_url = (
                f"https://archive.org/search?query={requests.utils.quote(title)}"
                "&and[]=mediatype%3A%22movies%22"
            )
            files = [{"is_search": True, "url": search_url, "quality": "other", "size_mb": 0}]
    elif source == "wikimedia":
        direct = result.get("direct_url", "")
        if not direct:
            await q.message.reply_text("⚠️ Download link nahi mila.")
            return
        files = [{"quality": "other", "size_mb": 0, "url": direct, "ext": ""}]
    else:
        # Search fallback — give archive.org search as download
        search_url = result.get("search_url") or result.get("watch_url")
        files = [{"is_search": True, "url": search_url, "quality": "other", "size_mb": 0}]

    token = str(uuid.uuid4())
    db_save_session(token, chat_id, files, title)
    context.user_data[f"dl_files_{idx}"] = files
    context.user_data[f"dl_token_{idx}"] = token

    ogads_url = f"{OGADS_BASE}?sub1={token}"

    caption = (
        f"🔒 *Download Lock*\n\n"
        f"🎬 *{title}*\n\n"
        "📺 *Watch karna hai?* — Upar *Watch* button dabao (free!)\n\n"
        "📥 *Download karna hai?*\n"
        "1️⃣ *Task Karo* button dabao\n"
        "2️⃣ Chhota sa task complete karo\n"
        "3️⃣ Download link *automatic* aa jaayega! ✅"
    )

    btns = [
        [InlineKeyboardButton("✅ Task Karo – Download Pao", url=ogads_url)],
        [InlineKeyboardButton("🔄 Ho Gaya (Manual)", callback_data=f"dl_manual_{idx}")],
    ]
    await q.message.reply_text(
        caption, parse_mode="Markdown",
        reply_markup=InlineKeyboardMarkup(btns),
    )


# ── Manual fallback ───────────────────────────

async def download_manual(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    q = update.callback_query
    await q.answer()

    idx     = int(q.data.split("_")[2])
    files   = context.user_data.get(f"dl_files_{idx}", [])
    token   = context.user_data.get(f"dl_token_{idx}", "")
    results = context.user_data.get("results", [])
    tmdb    = context.user_data.get("tmdb", {})
    title   = tmdb.get("title") or (results[idx]["title"] if idx < len(results) else "Movie")

    if not files:
        await q.message.reply_text("⚠️ Session expire ho gayi. Dobara search karo.")
        return

    await q.message.reply_text("✅ *Download links aa gayi!* 🎉", parse_mode="Markdown")
    await _send_download_links(context.bot, q.message.chat_id, files, title)

    if token:
        try:
            db_mark_notified(token)
        except Exception:
            pass


# ══════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════

def main() -> None:
    if POSTBACK_URL:
        print(f"✅ OGAds postback URL: {POSTBACK_URL}?token={{sub1}}")
    else:
        print("⚠️  REPLIT_DOMAINS not set — postback auto-unlock may not work in dev mode")

    app = ApplicationBuilder().token(BOT_TOKEN).build()

    app.add_handler(CommandHandler("start", start))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, search))
    app.add_handler(CallbackQueryHandler(download_lock,   pattern=r"^dl_lock_\d+$"))
    app.add_handler(CallbackQueryHandler(download_manual, pattern=r"^dl_manual_\d+$"))

    loop = asyncio.get_event_loop()
    loop.create_task(poll_verified_sessions(app))

    print("✅ Movie Bot chal raha hai — Koi bhi movie, hamesha Watch + Download milega")
    app.run_polling(drop_pending_updates=True)


if __name__ == "__main__":
    main()
