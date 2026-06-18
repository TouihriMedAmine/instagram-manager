"""
Instagram Followers & Followings Analyzer
==========================================
Logs into your Instagram account, fetches your followers and followings
with their follower/following counts, and displays real-time progress
on a local web page.

Uses instagrapi (private API) with session reuse and challenge handling.

Requirements: instagrapi, flask
Credentials: entered at runtime (username + password)

Fixes applied:
  1. Session is fully cleared before login so switching accounts works correctly.
  2. Pending outgoing follow requests are fetched live from the API (with
     pagination) instead of relying on a stale local JSON export file.
"""

import os
import json
import time
import threading
import requests
from datetime import datetime
from pathlib import Path

from instagrapi import Client
from instagrapi.mixins.challenge import ChallengeResolveMixin
from flask import Flask, render_template, jsonify, request as flask_request

# ── Monkey-patch: fix crash on empty challenge response ──────────────────────
_original_challenge_resolve = ChallengeResolveMixin.challenge_resolve_contact_form

def _patched_challenge_resolve(self, challenge_url):
    try:
        return _original_challenge_resolve(self, challenge_url)
    except requests.exceptions.JSONDecodeError:
        raise Exception(
            "\n>>> Instagram returned an empty challenge response.\n"
            "Please log in manually via the Instagram app/browser,\n"
            "complete any verification, wait a few minutes, then retry.\n"
        )

ChallengeResolveMixin.challenge_resolve_contact_form = _patched_challenge_resolve

# ── Configuration ────────────────────────────────────────────────────────────
USERNAME = ""
PASSWORD = ""
_scrape_worker: threading.Thread | None = None
_login_lock = threading.Lock()
_challenge_lock = threading.Lock()
_challenge_event: threading.Event | None = None
_pending_challenge: dict | None = None
DEV_RELOAD = os.getenv("DEV_RELOAD", "1") != "0"
SESSION_FILE = Path(__file__).parent / "instagram_session.json"

# Delay (seconds) between each user_info lookup – increase if rate-limited
REQUEST_DELAY = float(os.getenv("REQUEST_DELAY", "2"))
MESSAGE_THREADS_LIMIT = int(os.getenv("MESSAGE_THREADS_LIMIT", "200"))


def _reset_state_for_login():
    """Reset scraped data before starting a new login session."""
    state.update({
        "status": "idle",
        "message": "",
        "profile": {},
        "followers": [],
        "followings": [],
        "pending_requests": [],
        "message_threads": [],
        "message_thread_messages": [],
        "messages_total": 0,
        "messages_fetched": 0,
        "active_thread_id": None,
        "followers_total": 0,
        "followings_total": 0,
        "followers_fetched": 0,
        "followings_fetched": 0,
        "timestamp": None,
        "error": None,
        "api_available": False,
    })


def _load_saved_session() -> dict | None:
    if not SESSION_FILE.exists():
        return None
    try:
        with open(SESSION_FILE, "r", encoding="utf-8") as handle:
            data = json.load(handle)
        if isinstance(data, dict) and data.get("sessionid"):
            return data
    except Exception as exc:
        print(f"  ⚠ Could not load saved session: {exc}")
    return None


def _save_session(cl: Client, username: str):
    sessionid = getattr(cl, "sessionid", None)
    if not sessionid:
        return
    payload = {
        "username": username,
        "sessionid": sessionid,
        "saved_at": datetime.now().isoformat(),
    }
    try:
        with open(SESSION_FILE, "w", encoding="utf-8") as handle:
            json.dump(payload, handle, ensure_ascii=False, indent=2)
    except Exception as exc:
        print(f"  ⚠ Could not save session: {exc}")


def _start_scrape_worker():
    global _scrape_worker
    with _login_lock:
        if _scrape_worker and _scrape_worker.is_alive():
            return False
        _scrape_worker = threading.Thread(target=scrape, daemon=True)
        _scrape_worker.start()
        return True


def _start_scrape_worker_with_client(cl: Client):
    global _scrape_worker
    with _login_lock:
        if _scrape_worker and _scrape_worker.is_alive():
            return False
        _scrape_worker = threading.Thread(target=scrape, args=(cl,), daemon=True)
        _scrape_worker.start()
        return True


# ── Shared state between scraper thread and Flask ────────────────────────────
state = {
    "status": "idle",           # idle | logging_in | fetching_followers | fetching_followings | fetching_pending | enriching | done | error
    "message": "",
    "profile": {},              # logged-in user's stats
    "followers": [],            # list of dicts
    "followings": [],           # list of dicts
    "pending_requests": [],     # follow requests you sent, not yet accepted
    "message_threads": [],      # inbox threads
    "message_thread_messages": [],
    "messages_total": 0,
    "messages_fetched": 0,
    "active_thread_id": None,
    "followers_total": 0,
    "followings_total": 0,
    "followers_fetched": 0,
    "followings_fetched": 0,
    "timestamp": None,
    "error": None,
    "api_available": False,
}

# ── Rate-limit cooldown tracker ──────────────────────────────────────────────
_rate_limit_lock = threading.Lock()
_rate_limit_until = 0.0          # epoch time – requests blocked until this moment
_unfollow_delay = float(os.getenv("UNFOLLOW_DELAY", "8"))  # seconds between unfollows

def _is_rate_limited() -> float:
    """Return seconds remaining on cooldown, or 0 if clear."""
    with _rate_limit_lock:
        remaining = _rate_limit_until - time.time()
        return max(0.0, remaining)

def _set_cooldown(seconds: float):
    """Set a global cooldown after hitting a 429."""
    with _rate_limit_lock:
        global _rate_limit_until
        _rate_limit_until = max(_rate_limit_until, time.time() + seconds)
        print(f"  ⏳ Global cooldown set: {seconds:.0f}s (until {datetime.fromtimestamp(_rate_limit_until).strftime('%H:%M:%S')})")

def _is_rate_limit_error(exc: Exception) -> bool:
    """Detect whether an exception is a rate-limit / auth error from Instagram."""
    msg = str(exc).lower()
    return any(kw in msg for kw in ("429", "rate", "login_required", "please wait", "challenge_required", "too many"))


def _enrich_user_list(cl: Client, users: list[dict]) -> list[dict]:
    """Fetch follower/following counts for a list of user dicts."""
    users_to_enrich: dict[str, int] = {}
    for user in users:
        username = user.get("username")
        pk = user.get("pk")
        if username and pk and username not in users_to_enrich:
            users_to_enrich[username] = int(pk)

    enriched_cache: dict[str, dict] = {}
    consecutive_failures = 0
    for username, pk in users_to_enrich.items():
        enriched = _enrich_user(cl, pk, username)
        if enriched:
            enriched_cache[username] = enriched
            consecutive_failures = 0
        else:
            consecutive_failures += 1
            if consecutive_failures >= 5:
                print("  ⚠ Too many consecutive failures, skipping remaining enrichment.")
                break

        time.sleep(REQUEST_DELAY)

    for index, entry in enumerate(users):
        username = entry.get("username")
        if username in enriched_cache:
            users[index] = enriched_cache[username]

    return users


def _refresh_followers(cl: Client, user_id: int) -> list[dict]:
    followers_dict = cl.user_followers(user_id)
    followers = [_user_short_to_dict(user) for user in followers_dict.values()]
    return _enrich_user_list(cl, followers)


def _refresh_followings(cl: Client, user_id: int) -> list[dict]:
    following_dict = cl.user_following(user_id)
    followings = [_user_short_to_dict(user) for user in following_dict.values()]
    return _enrich_user_list(cl, followings)


def _current_user_id(cl: Client) -> int:
    """Return the current logged-in Instagram user id."""
    if getattr(cl, "user_id", None):
        return int(cl.user_id)
    return int(cl.user_id_from_username(USERNAME))


def _refresh_state_section(cl: Client, section: str) -> list[str]:
    """Refresh one or more state sections from Instagram."""
    refreshed: list[str] = []
    user_id = _current_user_id(cl)

    if section in ("followers", "not_following_back", "fans"):
        followers = _refresh_followers(cl, user_id)
        state["followers"] = followers
        state["followers_total"] = len(followers)
        state["followers_fetched"] = len(followers)
        if state.get("profile") and isinstance(state["profile"].get("followers"), int):
            state["profile"]["followers"] = len(followers)
        refreshed.append("followers")

    if section in ("followings", "not_following_back", "fans"):
        followings = _refresh_followings(cl, user_id)
        state["followings"] = followings
        state["followings_total"] = len(followings)
        state["followings_fetched"] = len(followings)
        if state.get("profile") and isinstance(state["profile"].get("followees"), int):
            state["profile"]["followees"] = len(followings)
        refreshed.append("followings")

    if section == "pending":
        pending = fetch_pending_outgoing_requests(cl)
        state["pending_requests"] = pending
        refreshed.append("pending_requests")

    if section == "messages":
        message_threads = fetch_message_threads(cl)
        state["message_threads"] = message_threads
        state["messages_total"] = len(message_threads)
        state["messages_fetched"] = len(message_threads)
        if state.get("active_thread_id") and not any(str(thread.get("thread_id")) == str(state["active_thread_id"]) for thread in message_threads):
            state["active_thread_id"] = None
            state["message_thread_messages"] = []
        refreshed.append("message_threads")

    state["timestamp"] = datetime.now().isoformat()
    return refreshed


# ── Flask app ────────────────────────────────────────────────────────────────
app = Flask(__name__, template_folder="templates")
app.config["TEMPLATES_AUTO_RELOAD"] = True
app.jinja_env.auto_reload = True


@app.route("/api/login", methods=["POST", "OPTIONS"])
def api_login():
    """Start a scrape session from browser-submitted credentials."""
    if flask_request.method == "OPTIONS":
        return ("", 204)

    data = flask_request.get_json(silent=True) or {}
    username = data.get("username", "").strip()
    password = data.get("password", "").strip()

    if not username or not password:
        return jsonify({"ok": False, "error": "Username and password are required."}), 400

    if _scrape_worker and _scrape_worker.is_alive():
        return jsonify({"ok": False, "error": "A login or scrape session is already running."}), 409

    global USERNAME, PASSWORD
    USERNAME = username
    PASSWORD = password
    _reset_state_for_login()
    state["status"] = "logging_in"
    state["message"] = f"Logging in as {USERNAME}..."

    started = _start_scrape_worker()
    if not started:
        return jsonify({"ok": False, "error": "Could not start login worker."}), 500

    return jsonify({"ok": True, "message": "Login started."})


@app.route("/api/challenge", methods=["POST", "OPTIONS"])
def api_challenge():
    """Submit an Instagram verification code from the browser."""
    if flask_request.method == "OPTIONS":
        return ("", 204)

    data = flask_request.get_json(silent=True) or {}
    code = data.get("code", "").strip()
    if not code:
        return jsonify({"ok": False, "error": "Missing verification code."}), 400

    with _challenge_lock:
        if not _pending_challenge or not _challenge_event:
            return jsonify({"ok": False, "error": "No pending verification challenge."}), 400
        _pending_challenge["code"] = code
        _challenge_event.set()

    state["message"] = "Verification code submitted. Finishing login..."
    state["status"] = "logging_in"
    return jsonify({"ok": True})


@app.after_request
def add_cors_headers(response):
    """Allow local browser extension/UI to call API endpoints."""
    if flask_request.path.startswith("/api/"):
        response.headers["Access-Control-Allow-Origin"] = "*"
        response.headers["Access-Control-Allow-Headers"] = "Content-Type"
        response.headers["Access-Control-Allow-Methods"] = "GET,POST,OPTIONS"
    return response

# Shared reference to the logged-in Client (set by scrape thread after login)
_client: Client | None = None


# ── Live pending outgoing follow requests ─────────────────────────────────────
def _is_http_404(exc: Exception) -> bool:
    """Return True if the exception represents a 404 HTTP error."""
    if isinstance(exc, requests.HTTPError) and getattr(exc, "response", None) is not None:
        return getattr(exc.response, "status_code", None) == 404
    return "404" in str(exc)


def _collect_outgoing_page(result: dict, pending: list[dict]):
    """Append users from the API page into pending list."""
    for u in result.get("users", []):
        # Keep only requests YOU sent (outgoing), skip incoming requests to you
        fs = u.get("friendship_status") or {}
        is_outgoing = bool(fs.get("outgoing_request") or u.get("outgoing_request"))
        is_incoming = fs.get("incoming_request") or u.get("incoming_request")
        if not is_outgoing:
            continue
        if is_incoming:
            continue
        pending.append({
            "pk": str(u.get("pk", "")),
            "username": u.get("username", ""),
            "full_name": u.get("full_name", ""),
            "profile_pic_url": u.get("profile_pic_url", ""),
            "is_private": u.get("is_private", True),
            "followers": None,
            "followees": None,
            "href": f"https://www.instagram.com/{u.get('username', '')}/",
            "timestamp": 0,
        })


def fetch_pending_outgoing_requests(cl: Client) -> list[dict]:
    """
    Fetch follow requests you've sent that haven't been accepted yet.

    Instagram occasionally shuffles this endpoint; we try a small
    compatibility chain and gracefully skip if all variants 404.
    """

    pending: list[dict] = []

    # Each tuple: (endpoint, base params)
    endpoint_variants = [
        ("friendships/outgoing_requests/", {}),  # historical endpoint
        ("friendships/pending_outgoing/", {"search_surface": "follow_list_page"}),
        # Fallback that some builds expose via the general pending route
        ("friendships/pending/", {"is_pending_outgoing": True, "search_surface": "follow_list_page"}),
    ]

    last_exc: Exception | None = None

    for endpoint, base_params in endpoint_variants:
        last_exc = None  # reset per variant
        pending.clear()
        next_max_id = None

        while True:
            params = dict(base_params)
            if next_max_id:
                params["max_id"] = next_max_id

            try:
                result = cl.private_request(endpoint, params=params)
            except Exception as exc:  # 404s should try the next variant
                last_exc = exc
                if _is_http_404(exc):
                    print(f"  ↪ Endpoint {endpoint} returned 404, trying fallback...")
                    break
                print(f"  ⚠ Error fetching outgoing requests from {endpoint}: {exc}")
                return pending

            _collect_outgoing_page(result, pending)

            next_max_id = result.get("next_max_id")
            if not next_max_id:
                break

            time.sleep(2)  # avoid rate limiting between pages

        # If we fetched anything (or got an empty but valid response), stop
        if pending or last_exc is None or not _is_http_404(last_exc):
            break

    if last_exc and not pending:
        print(f"  ⚠ Error fetching outgoing requests: {last_exc}")

    return pending


# ── Flask routes ─────────────────────────────────────────────────────────────
@app.route("/")
def index():
    return render_template("index.html")


@app.route("/api/state")
def api_state():
    return jsonify(state)


@app.route("/api/refresh/<section>", methods=["POST", "OPTIONS"])
def api_refresh_section(section):
    """Refresh a single list or a derived view's source lists."""
    if flask_request.method == "OPTIONS":
        return ("", 204)

    if _client is None:
        return jsonify({"ok": False, "error": "Not logged in yet."}), 503

    remaining = _is_rate_limited()
    if remaining > 0:
        return jsonify({"ok": False, "error": f"Rate-limit cooldown active. Retry in {int(remaining)}s.", "retry_after": int(remaining)}), 429

    section = (section or "").strip().lower()
    valid_sections = {"followers", "followings", "pending", "messages", "not_following_back", "fans"}
    if section not in valid_sections:
        return jsonify({"ok": False, "error": f"Unknown refresh target '{section}'."}), 400

    try:
        state["message"] = f"Refreshing {section.replace('_', ' ')}..."
        refreshed = _refresh_state_section(_client, section)
        label = section.replace("_", " ")
        state["message"] = f"Refreshed {label}."
        state["status"] = "done"
        return jsonify({"ok": True, "section": section, "refreshed": refreshed, "state": state})
    except Exception as exc:
        if _is_rate_limit_error(exc):
            _set_cooldown(180)
            return jsonify({"ok": False, "error": "Rate limited by Instagram.", "retry_after": 180}), 429
        return jsonify({"ok": False, "error": str(exc)}), 500


def _get_attr(obj, name: str, default=None):
    """Read an attribute or dictionary key from an instagrapi model."""
    if isinstance(obj, dict):
        return obj.get(name, default)
    return getattr(obj, name, default)


def _as_int(value, default: int = 0) -> int:
    try:
        if value is None or value == "":
            return default
        return int(value)
    except Exception:
        return default


def _normalize_direct_message(message, thread_id: int | str | None = None) -> dict:
    """Convert a direct message object into a plain dict for the UI."""
    user = _get_attr(message, "user", {}) or {}
    message_id = _get_attr(message, "id") or _get_attr(message, "pk") or _get_attr(message, "message_id")
    text = _get_attr(message, "text") or _get_attr(message, "item_text") or ""
    return {
        "id": str(message_id or ""),
        "thread_id": str(thread_id or _get_attr(message, "thread_id") or ""),
        "user_id": str(_get_attr(message, "user_id") or _get_attr(user, "pk") or _get_attr(user, "id") or ""),
        "username": _get_attr(user, "username") or _get_attr(message, "username") or "",
        "full_name": _get_attr(user, "full_name") or "",
        "text": text,
        "item_type": _get_attr(message, "item_type") or _get_attr(message, "type") or "text",
        "timestamp": _as_int(_get_attr(message, "timestamp") or _get_attr(message, "taken_at") or _get_attr(message, "created_at"), 0),
        "is_sent_by_me": bool(_get_attr(message, "is_sent_by_viewer") or _get_attr(message, "is_sent_by_me")),
        "profile_pic_url": str(_get_attr(user, "profile_pic_url") or ""),
    }


def _normalize_direct_message_raw(message: dict, thread_id: int | str | None = None) -> dict:
    """Convert a raw direct message dict into a plain dict for the UI."""
    user = message.get("user") or {}
    text = message.get("text") or message.get("item_text") or message.get("message") or ""
    return {
        "id": str(message.get("item_id") or message.get("id") or message.get("pk") or ""),
        "thread_id": str(thread_id or message.get("thread_id") or message.get("thread_v2_id") or ""),
        "user_id": str(message.get("user_id") or user.get("pk") or user.get("id") or ""),
        "username": user.get("username") or message.get("username") or "",
        "full_name": user.get("full_name") or "",
        "text": text,
        "item_type": message.get("item_type") or message.get("type") or "text",
        "timestamp": _as_int(message.get("timestamp") or message.get("taken_at") or message.get("created_at"), 0),
        "is_sent_by_me": bool(message.get("is_sent_by_viewer") or message.get("is_sent_by_me")),
        "profile_pic_url": str(user.get("profile_pic_url") or message.get("profile_pic_url") or ""),
    }


def _normalize_direct_thread_raw(thread: dict, box: str = "") -> dict:
    """Convert a raw direct thread dict into a plain dict for the UI."""
    participants = []
    for user in thread.get("users") or thread.get("participants") or []:
        participants.append({
            "pk": str(user.get("pk") or user.get("id") or ""),
            "username": user.get("username") or "",
            "full_name": user.get("full_name") or "",
            "profile_pic_url": str(user.get("profile_pic_url") or ""),
        })

    items = thread.get("items") or []
    preview = _normalize_direct_message_raw(items[0], thread.get("id") or thread.get("thread_id")) if items else None

    title = thread.get("thread_title") or thread.get("title") or ""
    if not title and participants:
        title = ", ".join(p["username"] for p in participants if p["username"])

    thread_ts = thread.get("last_activity_at") or thread.get("updated_at") or (preview["timestamp"] if preview else 0)
    return {
        "thread_id": str(thread.get("id") or thread.get("thread_id") or ""),
        "thread_v2_id": str(thread.get("thread_v2_id") or ""),
        "box": box,
        "title": title,
        "participants": participants,
        "profile_pic_url": _thread_profile_pic_url(thread, participants, preview),
        "item_count": _as_int(thread.get("item_count") or len(items), len(items)),
        "last_activity_at": _as_int(thread_ts, 0),
        "timestamp": _as_int(thread_ts, 0),
        "last_message": preview,
        "is_pending": bool(thread.get("is_pending", False)),
        "muted": bool(thread.get("muted", False)),
        "is_group": len(participants) > 2,
    }


def _thread_profile_pic_url(thread, participants: list[dict], preview: dict | None = None) -> str:
    """Pick the best representative avatar for a direct thread."""
    for participant in participants:
        username = participant.get("username")
        if username and username != USERNAME and participant.get("profile_pic_url"):
            return str(participant.get("profile_pic_url") or "")

    for participant in participants:
        if participant.get("profile_pic_url"):
            return str(participant.get("profile_pic_url") or "")

    if preview and preview.get("profile_pic_url"):
        return str(preview.get("profile_pic_url") or "")

    return str(_get_attr(thread, "profile_pic_url") or _get_attr(thread, "thread_image", {}).get("url", "") or "")


def _normalize_direct_thread(thread, box: str = "") -> dict:
    """Convert a direct thread object into a plain dict for the UI."""
    participants = []
    for user in _get_attr(thread, "users", []) or _get_attr(thread, "participants", []) or []:
        participants.append({
            "pk": str(_get_attr(user, "pk") or _get_attr(user, "id") or ""),
            "username": _get_attr(user, "username") or "",
            "full_name": _get_attr(user, "full_name") or "",
            "profile_pic_url": str(_get_attr(user, "profile_pic_url") or ""),
        })

    items = _get_attr(thread, "items", []) or []
    preview = _normalize_direct_message(items[0], _get_attr(thread, "id") or _get_attr(thread, "thread_id")) if items else None

    title = _get_attr(thread, "thread_title") or _get_attr(thread, "title") or ""
    if not title and participants:
        title = ", ".join(p["username"] for p in participants if p["username"])

    return {
        "thread_id": str(_get_attr(thread, "id") or _get_attr(thread, "thread_id") or ""),
        "thread_v2_id": str(_get_attr(thread, "thread_v2_id") or ""),
        "box": box,
        "title": title,
        "participants": participants,
        "profile_pic_url": _thread_profile_pic_url(thread, participants, preview),
        "item_count": _as_int(_get_attr(thread, "item_count") or len(items), len(items)),
        "last_activity_at": _as_int(_get_attr(thread, "last_activity_at") or _get_attr(thread, "updated_at") or (preview["timestamp"] if preview else 0), 0),
        "timestamp": _as_int(_get_attr(thread, "last_activity_at") or _get_attr(thread, "updated_at") or (preview["timestamp"] if preview else 0), 0),
        "last_message": preview,
        "is_pending": bool(_get_attr(thread, "is_pending", False)),
        "muted": bool(_get_attr(thread, "muted", False)),
        "is_group": len(participants) > 2,
    }


def fetch_message_threads(cl: Client) -> list[dict]:
    """Fetch inbox threads with fallbacks across supported inbox selectors."""
    threads_by_id: dict[str, dict] = {}
    fetch_errors: list[str] = []

    # Some account types do not support explicit box filters. Start with the
    # default selector, then fall back to specific boxes.
    box_variants = ["", "primary", "general"]
    for box in box_variants:
        try:
            params = {
                "visual_message_return_type": "unseen",
                "persistentBadging": "true",
                "limit": str(MESSAGE_THREADS_LIMIT),
                "is_prefetching": "false",
                "thread_message_limit": "1",
            }
            if box:
                params["folder"] = "1" if box == "general" else "0"

            raw_result = cl.private_request("direct_v2/inbox/", params=params)
        except Exception as exc:
            label = box or "default"
            msg = f"{label}: {exc}"
            print(f"  ⚠ Could not fetch {label} message threads: {exc}")
            fetch_errors.append(msg)
            continue

        inbox = raw_result.get("inbox", {}) if isinstance(raw_result, dict) else {}
        raw_threads = inbox.get("threads", []) or []

        for thread in raw_threads:
            normalized = _normalize_direct_thread_raw(thread, box=box)
            thread_id = normalized.get("thread_id")
            if not thread_id:
                continue
            existing = threads_by_id.get(thread_id)
            if not existing or normalized.get("last_activity_at", 0) >= existing.get("last_activity_at", 0):
                threads_by_id[thread_id] = normalized

        # If default selector already returned data, do not spend extra API
        # calls querying additional boxes.
        if box == "" and threads_by_id:
            break

    threads = list(threads_by_id.values())
    threads.sort(key=lambda item: item.get("last_activity_at", 0), reverse=True)

    if not threads and fetch_errors:
        raise Exception("Could not fetch message threads. " + " | ".join(fetch_errors))

    return threads


def fetch_thread_messages(cl: Client, thread_id: int) -> list[dict]:
    """Fetch messages for a single direct thread."""
    params = {
        "visual_message_return_type": "unseen",
        "direction": "older",
        "seq_id": "40065",
        "limit": "50",
    }
    items: list[dict] = []
    cursor = None

    while True:
        if cursor:
            params["cursor"] = cursor

        result = cl.private_request(f"direct_v2/threads/{thread_id}/", params=params)
        thread = result.get("thread", {}) if isinstance(result, dict) else {}
        for message in thread.get("items", []) or []:
            items.append(message)

        cursor = thread.get("oldest_cursor")
        if not cursor:
            break

    normalized = [_normalize_direct_message_raw(message, thread_id) for message in items]
    normalized.sort(key=lambda item: item.get("timestamp", 0))
    return normalized


def _is_not_following_error(exc: Exception) -> bool:
    """Return True if an unfollow attempt failed because the account is not followed."""
    msg = str(exc).lower()
    return any(kw in msg for kw in ("not following", "no relationship", "relationship", "cannot unfollow"))


def _resolve_user_pk(username: str, list_name: str | None = None) -> str:
    """Resolve a user PK from cached state first, then Instagram."""
    if list_name and list_name in state:
        for u in state[list_name]:
            if u.get("username") == username and u.get("pk"):
                return str(u.get("pk"))

    for list_name in ("followers", "followings", "pending_requests"):
        for u in state[list_name]:
            if u.get("username") == username and u.get("pk"):
                return str(u.get("pk"))

    return str(_client.user_id_from_username(username))


def _remove_from_state(list_name: str, username: str, profile_field: str | None = None):
    """Remove a user from a local list and keep counters in sync."""
    before = len(state[list_name])
    state[list_name] = [u for u in state[list_name] if u.get("username") != username]
    removed = before - len(state[list_name])

    total_key = f"{list_name}_total"
    if total_key in state:
        state[total_key] = len(state[list_name])

    if removed and profile_field and state.get("profile"):
        current = state["profile"].get(profile_field)
        if current is not None:
            state["profile"][profile_field] = max(0, int(current) - removed)

    return removed


def _remove_threads_from_state(thread_ids: list[str]):
    """Remove deleted DM threads from local state."""
    thread_set = {str(thread_id) for thread_id in thread_ids}
    state["message_threads"] = [thread for thread in state["message_threads"] if thread.get("thread_id") not in thread_set]
    if state.get("active_thread_id") in thread_set:
        state["active_thread_id"] = None
        state["message_thread_messages"] = []
    state["messages_total"] = len(state["message_threads"])


@app.route("/api/cancel_request", methods=["POST", "OPTIONS"])
def api_cancel_request():
    """Cancel a pending follow request by username."""
    if flask_request.method == "OPTIONS":
        return ("", 204)

    if _client is None:
        return jsonify({"ok": False, "error": "Not logged in yet."}), 503

    remaining = _is_rate_limited()
    if remaining > 0:
        return jsonify({"ok": False, "error": f"Rate-limit cooldown. Retry in {int(remaining)}s.", "retry_after": int(remaining)}), 429

    data = flask_request.get_json(silent=True) or {}
    username = data.get("username", "").strip()
    if not username:
        return jsonify({"ok": False, "error": "Missing username."}), 400

    # Resolve PK
    pk = data.get("pk", "").strip()
    if not pk:
        for u in state["pending_requests"]:
            if u["username"] == username:
                pk = u.get("pk", "")
                break
    if not pk:
        try:
            pk = str(_client.user_id_from_username(username))
        except Exception as exc:
            if _is_rate_limit_error(exc):
                return jsonify({"ok": False, "error": "Rate limited. Try again in ~60s.", "retry_after": 60}), 429
            return jsonify({"ok": False, "error": f"Could not resolve '{username}': {exc}"}), 404

    try:
        _client.private_request(f"friendships/destroy/{pk}/", data={"user_id": pk})
        state["pending_requests"] = [u for u in state["pending_requests"] if u["username"] != username]
        print(f"  ✓ Cancelled request to {username}")
        time.sleep(3)
        return jsonify({"ok": True, "username": username})
    except Exception as exc:
        if _is_rate_limit_error(exc):
            _set_cooldown(180)
            return jsonify({"ok": False, "error": "Rate limited by Instagram.", "retry_after": 180}), 429
        return jsonify({"ok": False, "error": str(exc)}), 500


@app.route("/api/remove_follower", methods=["POST", "OPTIONS"])
def api_remove_follower():
    """Remove a follower from your followers list without unfollowing them."""
    if flask_request.method == "OPTIONS":
        return ("", 204)

    if _client is None:
        return jsonify({"ok": False, "error": "Not logged in yet. Please wait for login to complete."}), 503

    remaining = _is_rate_limited()
    if remaining > 0:
        return jsonify({"ok": False, "error": f"Rate-limit cooldown active. Retry in {int(remaining)}s.", "retry_after": int(remaining)}), 429

    data = flask_request.get_json(silent=True) or {}
    username = data.get("username", "").strip()
    pk = data.get("pk", "").strip()
    if not username:
        return jsonify({"ok": False, "error": "Missing username."}), 400

    if username == USERNAME:
        return jsonify({"ok": False, "error": "Cannot remove yourself."}), 400

    if not pk:
        try:
            pk = _resolve_user_pk(username, "followers")
        except Exception as exc:
            if _is_rate_limit_error(exc):
                return jsonify({"ok": False, "error": "Rate limited looking up user. Try again in ~60s.", "retry_after": 60}), 429
            return jsonify({"ok": False, "error": f"Could not resolve username '{username}': {exc}"}), 404

    try:
        _client.user_remove_follower(int(pk))
        _remove_from_state("followers", username, "followers")
        print(f"  ✓ Removed follower {username}")
        return jsonify({"ok": True, "username": username, "removed_follower": True})
    except Exception as exc:
        if _is_rate_limit_error(exc):
            _set_cooldown(180)
            return jsonify({"ok": False, "error": "Rate limited by Instagram.", "retry_after": 180}), 429
        return jsonify({"ok": False, "error": str(exc)}), 500


@app.route("/api/remove_follower_and_unfollow", methods=["POST", "OPTIONS"])
def api_remove_follower_and_unfollow():
    """Remove a follower and unfollow them if they are in your following list."""
    if flask_request.method == "OPTIONS":
        return ("", 204)

    if _client is None:
        return jsonify({"ok": False, "error": "Not logged in yet. Please wait for login to complete."}), 503

    remaining = _is_rate_limited()
    if remaining > 0:
        return jsonify({"ok": False, "error": f"Rate-limit cooldown active. Retry in {int(remaining)}s.", "retry_after": int(remaining)}), 429

    data = flask_request.get_json(silent=True) or {}
    username = data.get("username", "").strip()
    pk = data.get("pk", "").strip()
    if not username:
        return jsonify({"ok": False, "error": "Missing username."}), 400

    if username == USERNAME:
        return jsonify({"ok": False, "error": "Cannot remove yourself."}), 400

    if not pk:
        try:
            pk = _resolve_user_pk(username, "followers")
        except Exception as exc:
            if _is_rate_limit_error(exc):
                return jsonify({"ok": False, "error": "Rate limited looking up user. Try again in ~60s.", "retry_after": 60}), 429
            return jsonify({"ok": False, "error": f"Could not resolve username '{username}': {exc}"}), 404

    should_unfollow = any(u.get("username") == username for u in state["followings"])

    try:
        _client.user_remove_follower(int(pk))
        _remove_from_state("followers", username, "followers")
        print(f"  ✓ Removed follower {username}")
    except Exception as exc:
        if _is_rate_limit_error(exc):
            _set_cooldown(180)
            return jsonify({"ok": False, "error": "Rate limited by Instagram.", "retry_after": 180}), 429
        return jsonify({"ok": False, "error": str(exc)}), 500

    warning = None
    if should_unfollow:
        try:
            _client.user_unfollow(int(pk))
            _remove_from_state("followings", username, "followees")
            print(f"  ✓ Unfollowed {username}")
        except Exception as exc:
            if _is_rate_limit_error(exc):
                _set_cooldown(180)
                warning = "Removed follower, but unfollow hit a rate limit."
            elif _is_not_following_error(exc):
                warning = "Removed follower. You were not following this account."
            else:
                warning = f"Removed follower, but could not unfollow: {exc}"

    response = {"ok": True, "username": username, "removed_follower": True, "unfollowed": should_unfollow and warning is None}
    if warning:
        response["warning"] = warning
    return jsonify(response)


@app.route("/api/unfollow", methods=["POST", "OPTIONS"])
def api_unfollow():
    """Unfollow a user by username. Expects JSON: {"username": "...", "pk": "..."}"""
    if flask_request.method == "OPTIONS":
        return ("", 204)

    if _client is None:
        return jsonify({"ok": False, "error": "Not logged in yet. Please wait for login to complete."}), 503

    # ── Check global cooldown ────────────────────────────────────────────
    remaining = _is_rate_limited()
    if remaining > 0:
        return jsonify({
            "ok": False,
            "error": f"Rate-limit cooldown active. Retry in {int(remaining)}s.",
            "retry_after": int(remaining),
        }), 429

    data = flask_request.get_json(silent=True) or {}
    username = data.get("username", "").strip()
    pk = data.get("pk", "").strip()
    if not username:
        return jsonify({"ok": False, "error": "Missing username."}), 400

    # Prevent unfollowing yourself
    if username == USERNAME:
        return jsonify({"ok": False, "error": "Cannot unfollow yourself."}), 400

    # ── Resolve PK ───────────────────────────────────────────────────────
    if not pk:
        for u in state["followings"]:
            if u["username"] == username:
                pk = u.get("pk")
                break
    if not pk:
        try:
            pk = _resolve_user_pk(username, "followings")
        except Exception as exc:
            if _is_rate_limit_error(exc):
                return jsonify({
                    "ok": False,
                    "error": "Rate limited looking up user. Try again in ~60s.",
                    "retry_after": 60,
                }), 429
            return jsonify({"ok": False, "error": f"Could not resolve username '{username}': {exc}"}), 404

    # Cache the resolved pk back into state
    for u in state["followings"]:
        if u["username"] == username and not u.get("pk"):
            u["pk"] = pk
            break

    if not pk:
        return jsonify({"ok": False, "error": f"Could not find PK for {username}."}), 404

    # ── Unfollow with retry + exponential backoff ────────────────────────
    last_err = None
    for attempt in range(3):
        try:
            _client.user_unfollow(int(pk))

            # Remove from state so the UI updates
            _remove_from_state("followings", username, "followees")

            print(f"  ✓ Unfollowed {username}")
            time.sleep(_unfollow_delay)
            return jsonify({"ok": True, "username": username})

        except Exception as exc:
            last_err = exc
            if _is_rate_limit_error(exc):
                cooldown = (attempt + 1) * 180
                _set_cooldown(cooldown)
                if attempt < 2:
                    print(f"  ⚠ Rate limited unfollowing {username} (attempt {attempt+1}/3). Waiting {cooldown}s...")
                    time.sleep(min(cooldown, 30))
                    return jsonify({
                        "ok": False,
                        "error": f"Rate limited. Cooldown {cooldown}s.",
                        "retry_after": cooldown,
                    }), 429
            else:
                print(f"  ⚠ Error unfollowing {username} (attempt {attempt+1}/3): {exc}")
                time.sleep(5 * (attempt + 1))

    # All retries exhausted
    err_msg = str(last_err)
    if _is_rate_limit_error(last_err):
        _set_cooldown(600)
        return jsonify({
            "ok": False,
            "error": "Rate limited by Instagram. Wait 10 minutes and try again.",
            "retry_after": 600,
        }), 429
    print(f"  ✗ Failed to unfollow {username}: {last_err}")
    return jsonify({"ok": False, "error": err_msg}), 500


@app.route("/api/messages/thread/<thread_id>", methods=["GET", "POST", "OPTIONS"])
def api_message_thread(thread_id):
    """Return messages for a direct thread."""
    if flask_request.method == "OPTIONS":
        return ("", 204)

    if _client is None:
        return jsonify({"ok": False, "error": "Not logged in yet."}), 503

    try:
        messages = fetch_thread_messages(_client, int(thread_id))
        state["active_thread_id"] = str(thread_id)
        state["message_thread_messages"] = messages
        return jsonify({"ok": True, "thread_id": str(thread_id), "messages": messages})
    except Exception as exc:
        if _is_rate_limit_error(exc):
            _set_cooldown(180)
            return jsonify({"ok": False, "error": "Rate limited by Instagram.", "retry_after": 180}), 429
        return jsonify({"ok": False, "error": str(exc)}), 500


@app.route("/api/messages/delete_threads", methods=["POST", "OPTIONS"])
def api_delete_message_threads():
    """Delete/hide selected direct message threads."""
    if flask_request.method == "OPTIONS":
        return ("", 204)

    if _client is None:
        return jsonify({"ok": False, "error": "Not logged in yet."}), 503

    remaining = _is_rate_limited()
    if remaining > 0:
        return jsonify({"ok": False, "error": f"Rate-limit cooldown active. Retry in {int(remaining)}s.", "retry_after": int(remaining)}), 429

    data = flask_request.get_json(silent=True) or {}
    thread_ids = data.get("thread_ids") or []
    if not isinstance(thread_ids, list) or not thread_ids:
        return jsonify({"ok": False, "error": "Missing thread_ids."}), 400

    deleted = []
    failed = []

    for raw_thread_id in thread_ids:
        thread_id = str(raw_thread_id).strip()
        if not thread_id:
            continue
        try:
            _client.direct_thread_hide(int(thread_id), move_to_spam=False)
            deleted.append(thread_id)
            time.sleep(1)
        except Exception as exc:
            failed.append({"thread_id": thread_id, "error": str(exc)})
            if _is_rate_limit_error(exc):
                _set_cooldown(180)
                break

    if deleted:
        _remove_threads_from_state(deleted)

    return jsonify({"ok": len(deleted) > 0, "deleted": deleted, "failed": failed})


# ── Login ─────────────────────────────────────────────────────────────────────
def challenge_code_handler(username, choice):
    """Wait for a verification code submitted from the browser UI."""
    global _challenge_event, _pending_challenge

    event = threading.Event()
    with _challenge_lock:
        _challenge_event = event
        _pending_challenge = {"username": username, "choice": choice, "code": None}

    state["status"] = "challenge_required"
    state["message"] = "Instagram requires verification. Enter the code in the login page."

    if not event.wait(timeout=600):
        with _challenge_lock:
            _challenge_event = None
            _pending_challenge = None
        raise Exception("Timed out waiting for the Instagram verification code.")

    with _challenge_lock:
        code = (_pending_challenge or {}).get("code", "")
        _challenge_event = None
        _pending_challenge = None

    if not code:
        raise Exception("Verification code was not provided.")

    return code


def login_with_credentials(cl: Client, username: str, password: str):
    """
    Fresh login using provided credentials.

    FIX: Clears any cached session/device fingerprint before attempting login
    so that switching to a different account always works correctly.
    Without this, instagrapi silently reuses the previous session even when
    different credentials are supplied.
    """
    cl.challenge_code_handler = challenge_code_handler
    cl.delay_range = [1, 3]

    # ── Clear any previously cached session so new credentials take effect ──
    cl.set_settings({})
    cl.set_uuids({})

    for attempt in range(3):
        try:
            cl.login(username, password)
            print("Logged in successfully.")
            return
        except Exception as e:
            if attempt < 2:
                wait = (attempt + 1) * 30
                print(f"Login attempt {attempt + 1} failed: {e}")
                print(f"Retrying in {wait} seconds...")
                time.sleep(wait)
            else:
                raise


# ── Helpers ───────────────────────────────────────────────────────────────────
def _user_short_to_dict(user_short) -> dict:
    """
    Convert an instagrapi UserShort to a plain dict.
    UserShort does NOT have follower_count/following_count – those are None
    until we enrich later.
    """
    return {
        "pk": str(user_short.pk),
        "username": user_short.username,
        "full_name": user_short.full_name or "",
        "followers": None,
        "followees": None,
        "profile_pic_url": str(user_short.profile_pic_url) if user_short.profile_pic_url else "",
        "is_private": getattr(user_short, "is_private", False),
    }


def _enrich_user(cl: Client, user_pk: int, username: str) -> dict:
    """
    Fetch full User info by user PK using the raw API endpoint.
    Avoids instagrapi's model parsing which crashes on newer fields.
    Includes retry with exponential backoff on rate limits.
    """
    for attempt in range(3):
        try:
            result = cl.private_request(f"users/{user_pk}/info/")
            u = result.get("user", {})
            return {
                "pk": str(user_pk),
                "username": u.get("username", username),
                "full_name": u.get("full_name", ""),
                "followers": u.get("follower_count"),
                "followees": u.get("following_count"),
                "profile_pic_url": u.get("profile_pic_url", ""),
                "is_private": u.get("is_private", False),
            }
        except Exception as exc:
            if _is_rate_limit_error(exc) and attempt < 2:
                wait = (attempt + 1) * 60
                print(f"  ⚠ Rate limited enriching {username}, waiting {wait}s (attempt {attempt+1}/3)")
                time.sleep(wait)
            else:
                print(f"  ⚠ Could not enrich {username} (pk={user_pk}): {exc}")
                return None
    return None


# ── Main scraper ──────────────────────────────────────────────────────────────
def scrape():
    """Main scraping logic – runs in a daemon thread."""
    cl = Client()
    saved_session = _load_saved_session()

    # ── Login ────────────────────────────────────────────────────────────
    if saved_session and saved_session.get("username") == USERNAME and saved_session.get("sessionid"):
        state["status"] = "logging_in"
        state["message"] = f"Restoring saved session for {USERNAME}..."
        print(state["message"])
        try:
            cl.login_by_sessionid(saved_session["sessionid"])
            print("Restored session successfully.")
        except Exception as exc:
            state["status"] = "error"
            state["error"] = f"Saved session is invalid: {exc}"
            return
    else:
        state["status"] = "logging_in"
        state["message"] = f"Logging in as {USERNAME}..."
        print(state["message"])

        try:
            login_with_credentials(cl, USERNAME, PASSWORD)
        except Exception as exc:
            state["status"] = "error"
            state["error"] = f"Login failed: {exc}"
            return

    _save_session(cl, cl.username or USERNAME)

    global _client
    _client = cl
    state["api_available"] = True

    print("Logged in. Waiting 10s before making API calls...")
    state["message"] = "Logged in. Cooling down before fetching..."
    time.sleep(10)

    # ── Own profile info ─────────────────────────────────────────────────
    user_id = cl.user_id
    my_info = None

    if user_id:
        try:
            result = cl.private_request(f"users/{user_id}/info/")
            user_data = result.get("user", {})
            my_info = {
                "username": user_data.get("username", USERNAME),
                "full_name": user_data.get("full_name", ""),
                "followers": user_data.get("follower_count", 0),
                "followees": user_data.get("following_count", 0),
                "profile_pic_url": user_data.get("profile_pic_url", ""),
            }
        except Exception as exc:
            print(f"Could not fetch own profile info: {exc}")

    if not user_id:
        try:
            user_id = cl.user_id_from_username(USERNAME)
        except Exception as exc:
            print(f"Could not get user_id: {exc}")

    if my_info:
        state["profile"] = my_info
        state["followers_total"] = my_info["followers"]
        state["followings_total"] = my_info["followees"]
    else:
        state["profile"] = {"username": USERNAME, "full_name": "", "followers": 0, "followees": 0, "profile_pic_url": ""}
        print("Could not fetch profile counts – will update after fetching lists.")

    if not user_id:
        state["status"] = "error"
        state["error"] = "Could not determine user ID. Please retry login with correct credentials."
        return

    # ── Pending outgoing follow requests (live from API) ─────────────────
    state["status"] = "fetching_pending"
    state["message"] = "Fetching pending outgoing follow requests..."
    print(state["message"])

    try:
        pending = fetch_pending_outgoing_requests(cl)
        state["pending_requests"] = pending
        print(f"  Got {len(pending)} pending outgoing follow requests.")
    except Exception as exc:
        print(f"  ⚠ Could not fetch pending requests: {exc}")
        state["pending_requests"] = []

    time.sleep(2)

    # ── Messages / inbox threads ───────────────────────────────────────
    state["status"] = "fetching_messages"
    state["message"] = "Fetching message threads..."
    print(state["message"])
    time.sleep(2)

    try:
        message_threads = fetch_message_threads(cl)
        state["message_threads"] = message_threads
        state["messages_total"] = len(message_threads)
        state["messages_fetched"] = len(message_threads)
        print(f"  Got {len(message_threads)} message threads.")
    except Exception as exc:
        print(f"  ⚠ Could not fetch message threads: {exc}")
        state["message_threads"] = []
        state["messages_total"] = 0
        state["messages_fetched"] = 0

    time.sleep(2)

    # ── Followers ────────────────────────────────────────────────────────
    state["status"] = "fetching_followers"
    state["message"] = "Fetching followers..."
    print(state["message"])
    time.sleep(2)

    followers_dict = None
    for attempt in range(4):
        try:
            followers_dict = cl.user_followers(user_id)
            break
        except Exception as exc:
            wait = (attempt + 1) * 60
            if attempt < 3:
                print(f"  Followers fetch failed: {exc}")
                print(f"  Retrying in {wait}s (attempt {attempt+1}/4)...")
                state["message"] = f"Rate limited – retrying in {wait}s..."
                time.sleep(wait)
            else:
                state["status"] = "error"
                state["error"] = f"Could not fetch followers after 4 attempts: {exc}"
                return

    for uid, user in followers_dict.items():
        state["followers"].append(_user_short_to_dict(user))
        state["followers_fetched"] += 1

    state["followers_total"] = len(state["followers"])
    print(f"  Got {len(state['followers'])} followers.")

    time.sleep(2)

    # ── Followings ───────────────────────────────────────────────────────
    state["status"] = "fetching_followings"
    state["message"] = "Fetching followings..."
    print(state["message"])
    time.sleep(2)

    following_dict = None
    for attempt in range(4):
        try:
            following_dict = cl.user_following(user_id)
            break
        except Exception as exc:
            wait = (attempt + 1) * 60
            if attempt < 3:
                print(f"  Followings fetch failed: {exc}")
                print(f"  Retrying in {wait}s (attempt {attempt+1}/4)...")
                state["message"] = f"Rate limited – retrying in {wait}s..."
                time.sleep(wait)
            else:
                state["status"] = "error"
                state["error"] = f"Could not fetch followings after 4 attempts: {exc}"
                return

    for uid, user in following_dict.items():
        state["followings"].append(_user_short_to_dict(user))
        state["followings_fetched"] += 1

    state["followings_total"] = len(state["followings"])
    print(f"  Got {len(state['followings'])} followings.")

    if state["profile"]["followers"] == 0:
        state["profile"]["followers"] = len(state["followers"])
    if state["profile"]["followees"] == 0:
        state["profile"]["followees"] = len(state["followings"])

    # ── Enrich with follower/following counts ────────────────────────────
    users_to_enrich = {}
    for u in state["followers"] + state["followings"]:
        if u["username"] not in users_to_enrich:
            users_to_enrich[u["username"]] = int(u["pk"])

    total_to_enrich = len(users_to_enrich)
    enriched_count = 0

    state["status"] = "enriching"
    state["message"] = f"Fetching follower counts for {total_to_enrich} users..."
    print(state["message"])

    enriched_cache = {}
    consecutive_failures = 0
    for uname, pk in users_to_enrich.items():
        enriched = _enrich_user(cl, pk, uname)
        if enriched:
            enriched_cache[uname] = enriched
            consecutive_failures = 0
        else:
            consecutive_failures += 1
            if consecutive_failures >= 5:
                print("  ⚠ Too many consecutive failures, skipping remaining enrichment.")
                state["message"] = "Enrichment stopped (rate limited). Showing basic data."
                break
        enriched_count += 1
        state["message"] = f"Enriching user data... ({enriched_count}/{total_to_enrich})"
        time.sleep(REQUEST_DELAY)

    # Apply enriched data back
    for lst_key in ("followers", "followings"):
        for i, entry in enumerate(state[lst_key]):
            if entry["username"] in enriched_cache:
                state[lst_key][i] = enriched_cache[entry["username"]]

    # ── Done ─────────────────────────────────────────────────────────────
    state["status"] = "done"
    state["timestamp"] = datetime.now().isoformat()
    state["message"] = "All data fetched!"
    print(state["message"])

    out_path = Path(__file__).parent / "results.json"
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False, indent=2)
    print(f"Results saved to {out_path}")


# ── Entry point ───────────────────────────────────────────────────────────────
if __name__ == "__main__":
    saved_session = _load_saved_session()
    if saved_session and saved_session.get("username") and (not DEV_RELOAD or os.environ.get("WERKZEUG_RUN_MAIN") == "true"):
        USERNAME = saved_session["username"]
        state["message"] = f"Restoring saved session for {USERNAME}..."
        print(f"Restoring saved session for {USERNAME}...")
        _start_scrape_worker()

    print("\n🌐 Open http://localhost:5000 in your browser to log in and start fetching.\n")
    app.run(host="127.0.0.1", port=5000, debug=False, threaded=True, use_reloader=DEV_RELOAD)