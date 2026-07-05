"""
Reddit Pain-Thread Scraper Pipeline (Apify Edition) — v2 IMPROVED
AI Employee Agency — Lead Generation Engine

Actor: trudax/reddit-scraper-lite (Pay Per Result — works on free plan)

v2 improvements:
  - Persistent dedup (no duplicate leads, no duplicate GPT spend)
  - Single Apify run for all subreddits (faster, less overhead)
  - OpenAI JSON mode (no fragile regex parsing)
  - Retry with exponential backoff on all API calls
  - Tightened prefilter patterns (less false positives -> less LLM cost)
  - Filters [removed]/[deleted] posts
  - pain_intensity + buyer_intent_signals saved to CSV (tuning data)
  - Meaningful status field
  - Fail-fast on missing API keys
  - Output CSV saved to a fixed custom folder (auto-created if missing)
  - Interactive Stop / Restart menu after each run
  - Ctrl+C anytime -> gracefully stops mid-run and saves whatever has
    been scraped/processed so far (no crash, no data loss)
"""

import os
import re
import csv
import json
import time
import signal
import logging
from datetime import datetime, timezone
from dataclasses import dataclass, asdict
from typing import Optional, Callable, Any

from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env"))

from apify_client import ApifyClient
from openai import OpenAI

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("pain_scraper")

# ---------------------------------------------------------------------------
# GRACEFUL STOP (Ctrl+C) — press once to stop early and keep partial data,
# press again to force-quit immediately.
# ---------------------------------------------------------------------------

STOP_REQUESTED = False

def _handle_sigint(signum, frame):
    global STOP_REQUESTED
    if not STOP_REQUESTED:
        STOP_REQUESTED = True
        print(
            "\n⏸  Stop requested — finishing the current step and saving "
            "everything collected so far.\n"
            "   (Press Ctrl+C again to force-quit without saving.)"
        )
    else:
        print("\n✖ Force-quitting now (unsaved progress may be lost).")
        os._exit(1)

signal.signal(signal.SIGINT, _handle_sigint)

# ---------------------------------------------------------------------------
# CONFIG
# ---------------------------------------------------------------------------

APIFY_API_TOKEN = os.getenv("APIFY_API_TOKEN")
OPENAI_API_KEY  = os.getenv("OPENAI_API_KEY")

# Fail fast — cryptic mid-run errors se behtar hai startup pe hi rukna
if not APIFY_API_TOKEN:
    raise SystemExit("FATAL: APIFY_API_TOKEN missing in .env")
if not OPENAI_API_KEY:
    raise SystemExit("FATAL: OPENAI_API_KEY missing in .env")

GPT_MODEL = os.environ.get("GPT_MODEL", "gpt-4o-mini")

TARGET_SUBREDDITS = [
    "entrepreneur", "startups", "smallbusiness", "sales",
    "recruiting", "customerSuccess", "agency", "SaaS",
]

POST_LOOKBACK_HOURS          = 48
POSTS_PER_SUBREDDIT_PER_RUN  = 25
MIN_PRIORITY_FOR_COMMENT_GEN = 40
MIN_CONFIDENCE               = 0.55
MAX_API_RETRIES              = 3

APIFY_REDDIT_ACTOR = "trudax/reddit-scraper-lite"

# ---------------------------------------------------------------------------
# OUTPUT PATH — portable by default, overridable via .env
# ---------------------------------------------------------------------------
# By default, output goes to a local "output/" folder next to this script.
# On your own machine you can point it anywhere by setting OUTPUT_DIR in
# your .env file, e.g.:
#   OUTPUT_DIR=C:\Users\yourname\Desktop\leads

_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DEFAULT_OUTPUT_DIR = os.path.join(_SCRIPT_DIR, "output")
OUTPUT_DIR = os.environ.get("OUTPUT_DIR", DEFAULT_OUTPUT_DIR)

# Folder agar exist nahi karta to bana do (warna CSV likhte waqt crash hoga)
os.makedirs(OUTPUT_DIR, exist_ok=True)

CSV_OUTPUT_PATH = os.path.join(OUTPUT_DIR, "pain_threads_output.csv")

# Every post that gets scraped is saved here, regardless of whether it later
# passes the pre-filter or GPT classification. This is your safety net —
# if you Ctrl+C mid-run, or if 0 posts happen to match the pain patterns,
# you still have everything that was actually scraped.
RAW_CSV_PATH = os.path.join(OUTPUT_DIR, "raw_scraped_posts.csv")

# ---------------------------------------------------------------------------
# PAIN SIGNAL PATTERNS (tightened — broad patterns ab context maangte hain)
# ---------------------------------------------------------------------------

PAIN_SIGNALS = {
    "AI SDR": [
        r"too many leads", r"lead overload", r"can'?t keep up with leads",
        r"sales team (is )?overloaded", r"no time to follow up",
        r"leads (are )?slipping through", r"can'?t scale (our |my )?outreach",
        r"follow[- ]?ups? (are )?(falling|slipping)",
    ],
    "AI Support Agent": [
        r"support tickets? (piling up|backlog|overload)",
        r"drowning in (support )?tickets",
        r"can'?t keep up with (support|customer emails|customer messages)",
        r"(support|response) times? (are )?(too slow|terrible|killing)",
        r"support (queue|inbox) (is )?(out of control|exploding)",
    ],
    "AI Recruiter": [
        r"can'?t hire fast enough", r"hiring bottleneck", r"too many applicants",
        r"can'?t find good candidates",
        r"screening (resumes|cvs|applicants) (takes forever|is a nightmare|is killing)",
        r"drowning in (resumes|applications)",
    ],
    "AI Research Assistant": [
        r"research takes (too long|forever|hours)",
        r"spend(ing)? hours? (on|doing) (research|prospect research|competitor research)",
        r"manual(ly)? research(ing)?",
    ],
    "AI Workflow Automation": [
        # broad words ko context ke saath tighten kiya — false positives kam
        r"drowning in (manual|repetitive|admin) (work|tasks)",
        r"doing everything manually",
        r"(so|completely|totally) overwhelmed with (work|tasks|operations|admin)",
        r"repetitive (tasks?|work) (are|is) (killing|eating|draining)",
        r"manual (process|work) is (killing|slowing|eating)",
        r"wearing too many hats",
        r"burn(ed|t)? out (from|by|doing)",
    ],
}

COMPILED_SIGNALS = [
    re.compile(p, re.IGNORECASE)
    for patterns in PAIN_SIGNALS.values()
    for p in patterns
]

# ---------------------------------------------------------------------------
# DATA MODEL
# ---------------------------------------------------------------------------

@dataclass
class LeadRow:
    thread_url:              str
    author:                  str
    subreddit:               str
    title:                   str
    body:                    str
    upvotes:                 int
    comments_count:          int
    pain_category:           str   = ""
    confidence_score:        float = 0.0
    pain_intensity:          int   = 0
    buyer_intent_signals:    str   = ""   # "; " joined — tuning ke liye useful
    recommended_ai_employee: str   = ""
    generated_comment:       str   = ""
    priority_score:          float = 0.0
    post_age_hours:          float = 0.0
    status:                  str   = "new"
    scraped_at:              str   = ""

CSV_FIELDS = list(LeadRow.__dataclass_fields__.keys())

# ---------------------------------------------------------------------------
# CLIENTS
# ---------------------------------------------------------------------------

_apify  = ApifyClient(APIFY_API_TOKEN)
_openai = OpenAI(api_key=OPENAI_API_KEY)

# ---------------------------------------------------------------------------
# HELPERS
# ---------------------------------------------------------------------------

def with_retries(fn: Callable[[], Any], label: str) -> Any:
    """Exponential backoff retry wrapper for API calls."""
    for attempt in range(MAX_API_RETRIES):
        try:
            return fn()
        except Exception as e:
            if attempt == MAX_API_RETRIES - 1:
                log.warning(f"{label} failed after {MAX_API_RETRIES} attempts: {e}")
                return None
            wait = 2 ** attempt
            log.info(f"{label} error (attempt {attempt + 1}), retrying in {wait}s: {e}")
            time.sleep(wait)


def load_seen_urls(path: str = RAW_CSV_PATH) -> set:
    """Pehle se processed thread URLs — duplicate leads + duplicate GPT spend rokta hai."""
    if not os.path.isfile(path):
        return set()
    try:
        with open(path, newline="", encoding="utf-8") as f:
            return {r["thread_url"] for r in csv.DictReader(f) if r.get("thread_url")}
    except Exception as e:
        log.warning(f"Could not read existing CSV for dedup: {e}")
        return set()


def extract_subreddit(item: dict) -> str:
    """Item se subreddit nikalo (single-run mode me har item alag sub ka ho sakta hai)."""
    community = item.get("parsedCommunityName") or item.get("communityName") or ""
    if community:
        return community.lstrip("r/")
    m = re.search(r"reddit\.com/r/([^/]+)/", item.get("url", ""))
    return m.group(1) if m else "unknown"

# ---------------------------------------------------------------------------
# 1. SCRAPING LAYER — ab SINGLE Apify run (8x actor startup overhead khatam)
# ---------------------------------------------------------------------------

def scrape_subreddits() -> list[LeadRow]:
    now  = datetime.now(timezone.utc)
    seen = load_seen_urls()
    log.info(f"Loaded {len(seen)} previously seen URLs for dedup.")

    run_input = {
        "startUrls": [
            {"url": f"https://www.reddit.com/r/{sub}/new/"}
            for sub in TARGET_SUBREDDITS
        ],
        "sort": "new",
        "maxItems": POSTS_PER_SUBREDDIT_PER_RUN * len(TARGET_SUBREDDITS),
        "maxPostCount": POSTS_PER_SUBREDDIT_PER_RUN,
        "maxComments": 0,
        "maxCommunitiesCount": 0,
        "maxUserCount": 0,
        "scrollTimeout": 40,
        "proxy": {
            "useApifyProxy": True,
            # NOTE: pehle auto proxy try karo (sasta). Agar Reddit block kare
            # toh "apifyProxyGroups": ["RESIDENTIAL"] add karna.
        },
    }

    log.info(f"Starting single Apify run for {len(TARGET_SUBREDDITS)} subreddits ...")
    log.info("(Press Ctrl+C anytime to stop early — you'll keep whatever has scraped so far.)")

    # NOTE: .start() (not .call()) — non-blocking, so we can poll and abort
    # early on Ctrl+C instead of being stuck waiting for the full run.
    run = with_retries(
        lambda: _apify.actor(APIFY_REDDIT_ACTOR).start(run_input=run_input),
        "Apify actor start",
    )
    if run is None:
        return []

    # apify-client versions differ: dict vs object
    if isinstance(run, dict):
        run_id     = run.get("id")
        dataset_id = run.get("defaultDatasetId")
    else:
        run_id     = getattr(run, "id", None)
        dataset_id = getattr(run, "default_dataset_id", None)

    if not dataset_id or not run_id:
        log.warning(f"No run_id/dataset_id returned. run={run}")
        return []

    log.info(f"Dataset: https://console.apify.com/storage/datasets/{dataset_id}")

    # Poll run status until it finishes OR user hits Ctrl+C.
    # Items are pushed into the dataset progressively as Reddit is
    # crawled, so aborting early still leaves whatever was scraped so far.
    terminal_states = {"SUCCEEDED", "FAILED", "ABORTED", "TIMED-OUT"}
    while True:
        if STOP_REQUESTED:
            log.info("Stop requested — aborting Apify run early, keeping data scraped so far...")
            with_retries(lambda: _apify.run(run_id).abort(), "Abort Apify run")
            break

        status_info = with_retries(lambda: _apify.run(run_id).get(), "Poll Apify run status")
        if status_info is None:
            break
        status = status_info.get("status") if isinstance(status_info, dict) else getattr(status_info, "status", None)
        if status in terminal_states:
            log.info(f"Apify run finished with status: {status}")
            break

        time.sleep(3)

    rows: list[LeadRow] = []
    skipped = {"dup": 0, "old": 0, "removed": 0, "type": 0}

    for item in _apify.dataset(dataset_id).iterate_items():

        # Sirf posts chahiye (comments/communities/users nahi)
        if item.get("dataType") not in ("post", None):
            skipped["type"] += 1
            continue

        url = item.get("url", "")
        if not url or url in seen:
            skipped["dup"] += 1
            continue

        author = item.get("username", "[deleted]")
        body   = (item.get("body") or "")[:1500]

        # Removed/deleted posts pe GPT spend waste hai
        if body.strip() in ("[removed]", "[deleted]") or author == "[deleted]":
            skipped["removed"] += 1
            continue

        # Age check
        created_at = item.get("createdAt", "")
        try:
            post_dt = datetime.fromisoformat(created_at.replace("Z", "+00:00"))
            post_age_hours = (now - post_dt).total_seconds() / 3600
        except Exception:
            log.debug(f"Unparseable createdAt='{created_at}' for {url}")
            post_age_hours = 999.0

        if post_age_hours > POST_LOOKBACK_HOURS:
            skipped["old"] += 1
            continue

        rows.append(LeadRow(
            thread_url     = url,
            author         = author,
            subreddit      = extract_subreddit(item),
            title          = item.get("title", ""),
            body           = body,
            upvotes        = item.get("upVotes", 0) or 0,
            comments_count = item.get("numberOfComments", 0) or 0,
            post_age_hours = round(post_age_hours, 2),
            scraped_at     = now.isoformat(),
        ))
        seen.add(url)  # same run ke andar crossposts bhi dedup ho jayenge

    log.info(
        f"Scraped {len(rows)} fresh posts "
        f"(skipped: {skipped['dup']} dup, {skipped['old']} old, "
        f"{skipped['removed']} removed, {skipped['type']} non-post)."
    )
    return rows

# ---------------------------------------------------------------------------
# 2. PRE-FILTER
# ---------------------------------------------------------------------------

def prefilter(rows: list[LeadRow]) -> list[LeadRow]:
    survivors = []
    for row in rows:
        text = f"{row.title} {row.body}"
        if any(p.search(text) for p in COMPILED_SIGNALS):
            survivors.append(row)
    pct = (len(survivors) / len(rows) * 100) if rows else 0
    log.info(f"Pre-filter kept {len(survivors)}/{len(rows)} posts ({pct:.0f}%).")
    return survivors

# ---------------------------------------------------------------------------
# 3. CLASSIFICATION (GPT — JSON mode, no fragile parsing)
# ---------------------------------------------------------------------------

CLASSIFICATION_SYSTEM_PROMPT = """You are a B2B lead qualification analyst for an AI automation agency that sells five products:
1. AI SDR — automates lead follow-up, cold outreach, and pipeline management.
2. AI Support Agent — automates customer support ticket triage and resolution.
3. AI Recruiter — automates resume screening, candidate sourcing, interview scheduling.
4. AI Research Assistant — automates market/competitor/data research tasks.
5. AI Workflow Automation — automates repetitive manual operational tasks.

Given a Reddit post, decide if the author has a genuine business pain one of the 5 products solves.
Be skeptical — most posts are Not Relevant. Exclude hypotheticals, ads, vague venting, and posts where the author is selling something themselves.

Respond ONLY with valid JSON:
{
  "pain_category": "AI SDR" | "AI Support Agent" | "AI Recruiter" | "AI Research Assistant" | "AI Workflow Automation" | "Not Relevant",
  "confidence_score": <float 0.0-1.0>,
  "recommended_ai_employee": "<product name or null>",
  "pain_intensity": <1-5>,
  "buyer_intent_signals": ["<phrase>"],
  "reasoning": "<max 25 words>"
}"""


def classify_thread(row: LeadRow) -> Optional[dict]:
    prompt = (
        f"Subreddit: r/{row.subreddit}\n"
        f"Title: {row.title}\n"
        f"Body: {row.body}\n"
        f"Upvotes: {row.upvotes} | Comments: {row.comments_count}"
    )

    def call():
        resp = _openai.chat.completions.create(
            model=GPT_MODEL,
            temperature=0,
            max_tokens=400,
            response_format={"type": "json_object"},  # guaranteed valid JSON
            messages=[
                {"role": "system", "content": CLASSIFICATION_SYSTEM_PROMPT},
                {"role": "user", "content": prompt},
            ],
        )
        return json.loads(resp.choices[0].message.content)

    return with_retries(call, f"Classification [{row.thread_url}]")

# ---------------------------------------------------------------------------
# 4. PRIORITY SCORING
# ---------------------------------------------------------------------------

def score_priority(row: LeadRow, pain_intensity: int, buyer_intent_signals: list) -> float:
    engagement_score     = min(1.0, (row.upvotes + row.comments_count * 2) / 50)
    recency_score        = max(0.0, 1 - (row.post_age_hours / POST_LOOKBACK_HOURS))
    pain_intensity_score = pain_intensity / 5
    buyer_intent_score   = min(1.0, len(buyer_intent_signals) / 3)

    score = (
        0.30 * engagement_score
      + 0.25 * recency_score
      + 0.30 * pain_intensity_score
      + 0.15 * buyer_intent_score
    ) * 100
    return round(score, 1)

# ---------------------------------------------------------------------------
# 5. COMMENT GENERATION (GPT)
# ---------------------------------------------------------------------------

COMMENT_SYSTEM_PROMPT = """You write Reddit comments for a small AI automation studio employee.
Be genuinely helpful — not salesy.

Rules:
- Sound like a real person, not a brand
- Reference one concrete detail from the post
- Give one specific useful piece of advice
- No company name, no links, no "DM me"
- End with ONE soft opener like "Curious what you've tried so far"
- No emoji unless OP used them. Max 120 words.
- Never fabricate stats.

Output ONLY the comment text."""


def generate_comment(row: LeadRow, pain_category: str, recommended: str) -> str:
    prompt = (
        f"Subreddit: r/{row.subreddit}\n"
        f"Title: {row.title}\n"
        f"Body: {row.body}\n"
        f"Pain: {pain_category} | Product angle (don't name it): {recommended}"
    )

    def call():
        resp = _openai.chat.completions.create(
            model=GPT_MODEL,
            temperature=0.7,
            max_tokens=250,
            messages=[
                {"role": "system", "content": COMMENT_SYSTEM_PROMPT},
                {"role": "user", "content": prompt},
            ],
        )
        return resp.choices[0].message.content.strip()

    return with_retries(call, f"Comment gen [{row.thread_url}]") or ""

# ---------------------------------------------------------------------------
# 6. OUTPUT
# ---------------------------------------------------------------------------

def write_to_csv(rows: list[LeadRow], path: str = CSV_OUTPUT_PATH) -> None:
    if not rows:
        log.info("Nothing to write.")
        return
    # Extra safety: agar folder beech me delete ho gaya ho to dobara bana do
    os.makedirs(os.path.dirname(path), exist_ok=True)
    file_exists = os.path.isfile(path)
    with open(path, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_FIELDS)
        if not file_exists:
            writer.writeheader()
        for row in rows:
            writer.writerow(asdict(row))
    log.info(f"Wrote {len(rows)} rows to {path}")

# ---------------------------------------------------------------------------
# MAIN PIPELINE
# ---------------------------------------------------------------------------

def run_pipeline() -> list[LeadRow]:
    raw_rows      = scrape_subreddits()

    # Save everything scraped, no matter what happens next. This is the
    # data you actually paid Apify for — it should never disappear just
    # because the pain-signal pre-filter or GPT classification rejects it.
    write_to_csv(raw_rows, path=RAW_CSV_PATH)

    filtered_rows = prefilter(raw_rows)
    final_rows    = []

    stats = {"classified": 0, "not_relevant": 0, "low_confidence": 0, "failed": 0}

    for row in filtered_rows:
        if STOP_REQUESTED:
            log.info(
                f"Stop requested — skipping remaining {len(filtered_rows) - stats['classified'] - stats['not_relevant'] - stats['failed'] - stats['low_confidence']} posts, "
                f"saving {len(final_rows)} already-processed leads."
            )
            break

        result = classify_thread(row)

        if result is None:
            stats["failed"] += 1
            continue
        if result.get("pain_category") == "Not Relevant":
            stats["not_relevant"] += 1
            continue

        stats["classified"] += 1

        signals = result.get("buyer_intent_signals", []) or []

        row.pain_category           = result.get("pain_category", "")
        row.confidence_score        = float(result.get("confidence_score", 0.0))
        row.pain_intensity          = int(result.get("pain_intensity", 3))
        row.buyer_intent_signals    = "; ".join(signals)
        row.recommended_ai_employee = result.get("recommended_ai_employee") or ""

        if row.confidence_score < MIN_CONFIDENCE:
            stats["low_confidence"] += 1
            continue

        row.priority_score = score_priority(row, row.pain_intensity, signals)

        if row.priority_score >= MIN_PRIORITY_FOR_COMMENT_GEN:
            row.generated_comment = generate_comment(
                row, row.pain_category, row.recommended_ai_employee
            )
            row.status = "comment_ready" if row.generated_comment else "comment_failed"
        else:
            row.status = "low_priority"

        final_rows.append(row)

    final_rows.sort(key=lambda r: r.priority_score, reverse=True)
    write_to_csv(final_rows)

    log.info(
        f"Pipeline complete. {len(raw_rows)} posts scraped -> {RAW_CSV_PATH}. "
        f"{len(final_rows)} qualified leads written to "
        f"{CSV_OUTPUT_PATH}. "
        f"(classified: {stats['classified']}, not_relevant: {stats['not_relevant']}, "
        f"low_confidence: {stats['low_confidence']}, failed: {stats['failed']})"
    )
    return final_rows

# ---------------------------------------------------------------------------
# INTERACTIVE CONTROL MENU — Stop / Restart
# ---------------------------------------------------------------------------

def main_menu() -> None:
    """
    Pipeline ko chalata hai, phir user se poochta hai:
      1 -> Stop     (script yaha se exit ho jayegi)
      2 -> Restart  (pipeline dobara chalegi)
    Kisi bhi invalid input pe dobara poochega, Ctrl+C se bhi safe exit hoga.
    """
    global STOP_REQUESTED
    while True:
        STOP_REQUESTED = False  # fresh start each time, including on Restart
        try:
            run_pipeline()
        except KeyboardInterrupt:
            # Shouldn't normally hit this (SIGINT handler catches it first),
            # but kept as a safety net.
            print("\nInterrupted. Stopping.")
            break
        except Exception as e:
            log.error(f"Pipeline crashed: {e}")

        print(f"\nRaw scraped posts:  {RAW_CSV_PATH}")
        print(f"Qualified leads:    {CSV_OUTPUT_PATH}")
        print("\nWhat next?")
        print("  1. Stop")
        print("  2. Restart (run pipeline again)")

        while True:
            choice = input("Enter choice (1/2): ").strip()
            if choice == "1":
                print("Stopping. Goodbye!")
                return
            elif choice == "2":
                print("Restarting pipeline...\n")
                break
            else:
                print("Invalid input — please press 1 or 2.")


if __name__ == "__main__":
    main_menu()
