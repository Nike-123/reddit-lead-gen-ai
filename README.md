# Reddit Pain-Point Lead Generation Engine

**An automated pipeline that turns public Reddit conversations into qualified, prioritized sales leads — without a human reading a single thread.**

---

## The Business Problem

Sales and agency teams know that some of the best inbound signals aren't in a CRM — they're buried in public conversations. Every day, founders and operators post things like *"our sales team can't keep up with follow-ups"* or *"we're drowning in support tickets"* across subreddits like r/Entrepreneur, r/startups, and r/smallbusiness.

That's a person actively describing a problem your product solves — at the exact moment they're thinking about it. But finding these threads manually doesn't scale:

- Scanning 8+ subreddits by hand, every day, is a full-time job nobody wants.
- Most posts are noise — venting, ads, or irrelevant chatter — so keyword search alone produces too many false positives.
- By the time a human spots a good thread, it's often hours old and buried under new posts.
- Manually judging *"is this person a real buyer, and how urgent is their pain?"* is subjective and inconsistent across a team.

## The Solution

This project replaces that manual triage with a pipeline that scrapes, filters, scores, and drafts a first response automatically — so a human only steps in at the very last step: reviewing and posting a genuinely helpful reply.

```
Reddit (8 subreddits)
      │
      ▼
 Apify Scraper ── pulls fresh posts, deduped against everything seen before
      │
      ▼
 Regex Pre-filter ── cheaply discards obvious noise before spending on GPT
      │
      ▼
 GPT Classification ── decides: real pain? which product fits? how confident?
      │
      ▼
 Priority Scoring ── ranks leads by engagement + recency + pain intensity + buyer-intent signals
      │
      ▼
 GPT Comment Drafting ── writes a non-salesy, helpful reply for top leads only
      │
      ▼
   CSV output ── ready for a human to review and post
```

### Why this design

| Decision | Reasoning |
|---|---|
| **Regex pre-filter before GPT** | Filtering out obvious noise for free, before paying for an LLM call, keeps the classification cost roughly proportional to genuinely promising posts — not every post in the subreddit. |
| **JSON-mode GPT classification** | Removes fragile regex/string parsing of LLM output — the model returns structured data every time. |
| **Persistent dedup via CSV** | The pipeline is meant to run repeatedly (e.g. on a schedule). Without dedup, it would re-scrape and re-classify the same posts, wasting API spend. |
| **Priority scoring formula** | Combines four independent signals (engagement, recency, pain intensity, buyer intent) so leads are ranked, not just filtered into a flat "yes/no" list. |
| **Comment generation only above a priority threshold** | GPT-written replies are the most expensive step — reserved for the leads worth acting on. |
| **Graceful Ctrl+C stop** | Long-running scrapes shouldn't be all-or-nothing. Stopping early keeps everything processed so far instead of losing it. |

## What It Solves, In Practice

Instead of a person spending an hour a day scanning subreddits, this pipeline runs unattended and produces:

1. **`raw_scraped_posts.csv`** — every single post that was scraped, saved immediately, regardless of whether it passes any filter. This is your safety net: if the pain-signal filter or GPT classification rejects everything (or you stop the run early), you still have the raw data.
2. **`pain_threads_output.csv`** — only the qualified, ranked leads, each with:

- The original thread and author
- Which product the pain maps to (e.g. AI SDR, AI Support Agent, AI Recruiter, AI Research Assistant, AI Workflow Automation)
- A confidence score and priority score
- A ready-to-review, non-salesy comment draft

The human's job shrinks from *"find and evaluate leads"* to *"skim a short, ranked list and hit reply."*

## Tech Stack

- **Python 3.11+**
- **[Apify](https://apify.com/)** (`trudax/reddit-scraper-lite`) — Reddit scraping, pay-per-result
- **OpenAI API** (`gpt-4o-mini` by default, JSON mode) — classification + comment drafting
- **CSV** — lightweight, portable output (easy to open in Excel/Sheets or pipe into a CRM)

## Project Structure

```
.
├── main.py              # the full pipeline
├── requirements.txt
├── .env.example         # copy to .env and fill in your keys
└── output/              # generated CSV lands here (gitignored)
```

## Setup

```bash
git clone https://github.com/<your-username>/reddit-lead-gen-ai.git
cd reddit-lead-gen-ai
pip install -r requirements.txt
cp .env.example .env
# then edit .env with your APIFY_API_TOKEN and OPENAI_API_KEY
```

## Usage

```bash
python main.py
```

- The pipeline scrapes, filters, classifies, scores, and drafts comments for the top leads.
- Results are written to `output/pain_threads_output.csv`.
- **Press Ctrl+C at any point** to stop early — whatever has been scraped/processed so far is still saved.
- After each run, you'll be prompted:
  ```
  1. Stop
  2. Restart (run pipeline again)
  ```

## Configuration

All tunable knobs live at the top of `main.py`:

| Variable | Purpose |
|---|---|
| `TARGET_SUBREDDITS` | Which subreddits to monitor |
| `POST_LOOKBACK_HOURS` | Ignore posts older than this |
| `MIN_CONFIDENCE` | Minimum GPT confidence to count as a qualified lead |
| `MIN_PRIORITY_FOR_COMMENT_GEN` | Priority score threshold before spending on comment generation |
| `PAIN_SIGNALS` | Regex patterns used in the cheap pre-filter stage |

## Possible Extensions

- Push qualified leads straight into a CRM (HubSpot/Airtable) instead of CSV
- Slack/email notification when a high-priority lead appears
- Auto-posting the drafted comment via the Reddit API (with a human-approval step)
- Swap the CSV dedup store for a proper database as volume grows

## Disclaimer

This project scrapes and analyzes **publicly available** Reddit posts. It generates draft replies only — no comment is auto-posted. Always review drafts for tone and accuracy, and follow Reddit's API/content policies and each subreddit's rules before posting anything.
