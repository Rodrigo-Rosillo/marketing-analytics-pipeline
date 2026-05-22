"""
enrich_feedback.py
LLM enrichment step: turns the messy RAW.CUSTOMER_FEEDBACK rows into structured,
analyzable fields using Google Gemini, and lands them in RAW.FEEDBACK_ENRICHED.

For each review the model returns (as enforced JSON, not free text):
  - sentiment + confidence              (classification)
  - themes from a controlled taxonomy   (multi-label classification / normalization)
  - product_mentions, competitor_mentions (entity extraction)
  - language                            (detection)
  - resolved_campaign_id + confidence   (entity resolution: free-text -> canonical id)

Production concerns this script handles deliberately:
  - Cost / quota: a content-hash cache means identical/already-seen text is never
    re-sent. Reruns are effectively free; only new text hits the API.
  - Free-tier rate limits: requests are batched and throttled to a configurable
    RPM, with exponential backoff on 429 / RESOURCE_EXHAUSTED.
  - Deterministic CI: every successful enrichment is written to a committed JSONL
    fixture keyed by (content_hash, model_version). With --offline the script uses
    only the fixture and never calls the API, so CI needs no key and no network.

Usage:
    python enrichment/enrich_feedback.py                      # enrich all (live)
    python enrichment/enrich_feedback.py --limit 6            # smoke test
    python enrichment/enrich_feedback.py --offline            # fixture only (CI)
    python enrichment/enrich_feedback.py --sample 5           # print samples after
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
import time
from pathlib import Path
from typing import Literal

import duckdb
from dotenv import load_dotenv
from pydantic import BaseModel

load_dotenv()

# ── Paths & config ─────────────────────────────────────────────────────────────

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_DUCKDB_PATH = PROJECT_ROOT / "MARKETING_ANALYTICS.duckdb"
FIXTURE_PATH = Path(__file__).resolve().parent / "fixtures" / "feedback_enrichment.jsonl"

API_KEY_ENV = "GOOGLE_AI_STUDIO_API_KEY"
# gemini-2.5-flash-lite has free-tier quota and supports structured output.
# (gemini-2.0-flash is no longer free-tier eligible on this key as of 2026.)
DEFAULT_MODEL = os.environ.get("GEMINI_MODEL", "gemini-2.5-flash-lite")

# Controlled vocabularies — the enrichment is only valid if it stays inside these.
SENTIMENTS = ["positive", "negative", "neutral", "mixed"]
THEME_TAXONOMY = [
    "shipping", "price", "product_quality", "customer_service",
    "sizing_fit", "website_app", "returns", "other",
]

SentimentT = Literal["positive", "negative", "neutral", "mixed"]
ThemeT = Literal[
    "shipping", "price", "product_quality", "customer_service",
    "sizing_fit", "website_app", "returns", "other",
]


# ── Structured-output schema ───────────────────────────────────────────────────

class Enrichment(BaseModel):
    """One enriched feedback record. resolved_campaign_id is "" when no campaign
    is referenced; campaign_reference echoes the free-text snippet that triggered
    a match (for auditability)."""
    feedback_id: str
    sentiment: SentimentT
    sentiment_confidence: float
    themes: list[ThemeT]
    product_mentions: list[str]
    competitor_mentions: list[str]
    language: str
    campaign_reference: str
    resolved_campaign_id: str
    resolution_confidence: float


# ── Helpers ────────────────────────────────────────────────────────────────────

def content_hash(review_text: str, source: str) -> str:
    """Stable cache key for a feedback item. Hashing the text means identical
    reviews (or unchanged rows on rerun) are never re-enriched."""
    payload = f"{(review_text or '').strip()}||{(source or '').strip()}"
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def load_feedback(con: duckdb.DuckDBPyConnection, limit: int | None) -> list[dict]:
    # Note: true_campaign_id is intentionally NOT selected — the model must
    # resolve campaigns from the text alone; the label is for scoring only.
    sql = """
        select feedback_id, posted_at, source, rating, review_text
        from RAW.CUSTOMER_FEEDBACK
        order by feedback_id
    """
    if limit:
        sql += f" limit {int(limit)}"
    cols = ["feedback_id", "posted_at", "source", "rating", "review_text"]
    return [dict(zip(cols, row)) for row in con.execute(sql).fetchall()]


def load_campaign_catalog(con: duckdb.DuckDBPyConnection) -> list[dict]:
    """Built from the RAW ad tables (available after load, before dbt runs) so the
    model can map oblique references to canonical campaign ids."""
    sql = """
        select distinct campaign_id, campaign_name, channel, objective
        from (
            select campaign_id, campaign_name, channel, objective from RAW.META_ADS
            union all
            select campaign_id, campaign_name, channel, objective from RAW.GOOGLE_ADS
            union all
            select campaign_id, campaign_name, channel, objective from RAW.TIKTOK_ADS
        )
        order by campaign_id
    """
    cols = ["campaign_id", "campaign_name", "channel", "objective"]
    return [dict(zip(cols, row)) for row in con.execute(sql).fetchall()]


# ── Fixture cache ──────────────────────────────────────────────────────────────

def load_fixture() -> dict[tuple[str, str], dict]:
    cache: dict[tuple[str, str], dict] = {}
    if FIXTURE_PATH.exists():
        with open(FIXTURE_PATH, encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                rec = json.loads(line)
                cache[(rec["content_hash"], rec["model_version"])] = rec
    return cache


def save_fixture(cache: dict[tuple[str, str], dict]) -> None:
    FIXTURE_PATH.parent.mkdir(parents=True, exist_ok=True)
    # Sorted for stable, review-friendly diffs.
    with open(FIXTURE_PATH, "w", encoding="utf-8") as f:
        for key in sorted(cache):
            f.write(json.dumps(cache[key], ensure_ascii=False, sort_keys=True) + "\n")


# ── Validation / normalization of model output ─────────────────────────────────

def normalize(item: dict, valid_ids: set[str]) -> dict:
    """Coerce a raw model item into a clean, in-vocabulary record. This is the
    last line of defense before data-quality tests in dbt."""
    sentiment = item.get("sentiment", "neutral")
    if sentiment not in SENTIMENTS:
        sentiment = "neutral"

    themes = [t for t in item.get("themes", []) if t in THEME_TAXONOMY]
    themes = sorted(set(themes)) or ["other"]

    rid = (item.get("resolved_campaign_id") or "").strip()
    if rid not in valid_ids:
        rid = ""  # reject hallucinated ids; keeps the relationship test honest

    def clamp(x):
        try:
            return max(0.0, min(1.0, float(x)))
        except (TypeError, ValueError):
            return 0.0

    return {
        "sentiment": sentiment,
        "sentiment_confidence": clamp(item.get("sentiment_confidence")),
        "themes": themes,
        "product_mentions": [str(p) for p in item.get("product_mentions", [])][:10],
        "competitor_mentions": [str(c) for c in item.get("competitor_mentions", [])][:10],
        "language": (item.get("language") or "und").strip().lower()[:10],
        "campaign_reference": (item.get("campaign_reference") or "").strip()[:200],
        "resolved_campaign_id": rid,
        "resolution_confidence": clamp(item.get("resolution_confidence")) if rid else 0.0,
    }


# ── Gemini client ──────────────────────────────────────────────────────────────

def build_system_instruction(catalog: list[dict]) -> str:
    rows = "\n".join(
        f"  - {c['campaign_id']} | {c['campaign_name']} | {c['channel']} | {c['objective']}"
        for c in catalog
    )
    return f"""You are a data-enrichment service for a marketing analytics pipeline.
You receive customer feedback (reviews / social comments) that may be messy,
multilingual, contain emojis, typos, or slang. For each item return structured
fields. Be precise and conservative; never invent information.

SENTIMENT: one of {SENTIMENTS}. Use "mixed" only when clearly both positive and
negative. sentiment_confidence is your confidence in [0,1].

THEMES: zero or more from this controlled taxonomy ONLY:
{THEME_TAXONOMY}
Map paraphrases to the closest theme (e.g. "took forever to arrive" -> shipping;
"app crashed at checkout" -> website_app). Use "other" only if nothing fits.

ENTITIES: product_mentions = our products the customer names; competitor_mentions
= other brands named (e.g. Nike, Lululemon). Extract surface forms, do not normalize.

LANGUAGE: BCP-47-ish code of the review text (e.g. "en", "es", "fr").

CAMPAIGN RESOLUTION: the text may obliquely reference one of OUR campaigns below
(id | name | channel | objective):
{rows}
If the feedback clearly alludes to one campaign (e.g. "your summer sale ad" ->
the seasonal/promotions campaign, "your tiktok video" -> a TikTok awareness
campaign), set resolved_campaign_id to that id, campaign_reference to the exact
snippet, and resolution_confidence in [0,1]. If there is no clear reference, set
resolved_campaign_id to "" (empty), campaign_reference to "", and
resolution_confidence to 0. Do NOT guess when ambiguous.

Echo back each item's feedback_id exactly."""


def enrich_batch(client, model: str, system_instruction: str, batch: list[dict],
                 max_output_tokens: int = 32768):
    """Single API call for a batch; returns parsed list of raw item dicts."""
    from google.genai import types

    payload = [
        {"feedback_id": r["feedback_id"], "source": r["source"],
         "rating": r["rating"], "review_text": r["review_text"]}
        for r in batch
    ]
    resp = client.models.generate_content(
        model=model,
        contents="Enrich these feedback items:\n" + json.dumps(payload, ensure_ascii=False),
        config=types.GenerateContentConfig(
            system_instruction=system_instruction,
            response_mime_type="application/json",
            response_schema=list[Enrichment],
            temperature=0.0,
            max_output_tokens=max_output_tokens,
            # Gemini 2.5 models think by default, which silently eats the output
            # budget and truncates the JSON. Structured extraction needs no
            # reasoning trace, so disable it (frees the full budget for output).
            thinking_config=types.ThinkingConfig(thinking_budget=0),
        ),
    )
    # Surface truncation explicitly instead of failing on corrupt JSON later.
    finish = getattr(resp.candidates[0], "finish_reason", None) if resp.candidates else None
    if finish and str(finish).endswith("MAX_TOKENS"):
        raise ValueError(
            f"Response truncated (MAX_TOKENS) for a {len(batch)}-item batch; "
            "lower --batch-size or raise --max-output-tokens."
        )
    return json.loads(resp.text)


# ── Rate limiter ───────────────────────────────────────────────────────────────

class DailyQuotaExceeded(Exception):
    """Raised when the per-day free-tier request quota is hit — not retriable
    within the run (it resets daily), so we stop gracefully and resume later."""


class RateLimiter:
    """Enforce a minimum interval between calls to stay under free-tier RPM."""
    def __init__(self, rpm: int):
        self.min_interval = 60.0 / max(1, rpm)
        self._last = 0.0

    def wait(self):
        elapsed = time.time() - self._last
        if elapsed < self.min_interval:
            time.sleep(self.min_interval - elapsed)
        self._last = time.time()


def call_with_backoff(fn, *, max_retries=5):
    """Retry transient 429 / 503 with exponential backoff. A *daily* quota 429
    is not retriable here, so it raises DailyQuotaExceeded immediately."""
    delays = [15, 30, 60, 90, 120]
    for attempt in range(max_retries + 1):
        try:
            return fn()
        except Exception as e:  # noqa: BLE001 - SDK raises varied error types
            msg = str(e)
            if "PerDay" in msg or "GenerateRequestsPerDayPerProjectPerModel" in msg:
                raise DailyQuotaExceeded(msg) from e
            transient = "429" in msg or "RESOURCE_EXHAUSTED" in msg or "503" in msg or "overloaded" in msg.lower()
            if not transient or attempt == max_retries:
                raise
            delay = delays[min(attempt, len(delays) - 1)]
            print(f"  rate-limited/transient ({msg[:60]}...); retrying in {delay}s")
            time.sleep(delay)


# ── Output table ───────────────────────────────────────────────────────────────

CREATE_ENRICHED_DDL = """
CREATE TABLE IF NOT EXISTS RAW.FEEDBACK_ENRICHED (
    feedback_id           VARCHAR,
    sentiment             VARCHAR,
    sentiment_confidence  DOUBLE,
    themes                VARCHAR,   -- JSON array
    product_mentions      VARCHAR,   -- JSON array
    competitor_mentions   VARCHAR,   -- JSON array
    language              VARCHAR,
    campaign_reference    VARCHAR,
    resolved_campaign_id  VARCHAR,
    resolution_confidence DOUBLE,
    model_version         VARCHAR,
    content_hash          VARCHAR,
    _enriched_at          TIMESTAMP DEFAULT current_timestamp
)
"""

INSERT_COLS = [
    "feedback_id", "sentiment", "sentiment_confidence", "themes",
    "product_mentions", "competitor_mentions", "language", "campaign_reference",
    "resolved_campaign_id", "resolution_confidence", "model_version", "content_hash",
]


def write_enriched(con: duckdb.DuckDBPyConnection, rows: list[dict]) -> None:
    con.execute("CREATE SCHEMA IF NOT EXISTS RAW")
    con.execute(CREATE_ENRICHED_DDL)
    con.execute("DELETE FROM RAW.FEEDBACK_ENRICHED")
    placeholders = ", ".join(["?"] * len(INSERT_COLS))
    con.executemany(
        f"INSERT INTO RAW.FEEDBACK_ENRICHED ({', '.join(INSERT_COLS)}) VALUES ({placeholders})",
        [
            [
                r["feedback_id"], r["sentiment"], r["sentiment_confidence"],
                json.dumps(r["themes"]), json.dumps(r["product_mentions"]),
                json.dumps(r["competitor_mentions"]), r["language"],
                r["campaign_reference"], r["resolved_campaign_id"],
                r["resolution_confidence"], r["model_version"], r["content_hash"],
            ]
            for r in rows
        ],
    )


# ── Main ───────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(description="LLM-enrich customer feedback via Gemini")
    parser.add_argument("--model", default=DEFAULT_MODEL)
    parser.add_argument("--batch-size", type=int, default=100,
                        help="Items per request. Large batches matter on tiny free tiers "
                             "(few requests/day): batch 100 enriches 1,500 rows in ~15 calls.")
    parser.add_argument("--max-output-tokens", type=int, default=32768)
    parser.add_argument("--rpm", type=int, default=int(os.environ.get("GEMINI_RPM", "10")))
    parser.add_argument("--limit", type=int, default=None, help="Only enrich the first N rows")
    parser.add_argument("--offline", action="store_true", help="Fixture only; never call the API (CI)")
    parser.add_argument("--no-fixture-write", action="store_true", help="Do not update the fixture")
    parser.add_argument("--sample", type=int, default=0, help="Print N enriched samples at the end")
    parser.add_argument("--duckdb-path", default=os.environ.get("DUCKDB_PATH", str(DEFAULT_DUCKDB_PATH)))
    args = parser.parse_args()

    con = duckdb.connect(args.duckdb_path)
    feedback = load_feedback(con, args.limit)
    catalog = load_campaign_catalog(con)
    valid_ids = {c["campaign_id"] for c in catalog}
    cache = load_fixture()
    model = args.model

    print(f"\nFeedback rows : {len(feedback):,}")
    print(f"Campaigns     : {len(valid_ids)}")
    print(f"Fixture cache : {len(cache):,} entries")
    print(f"Model         : {model}   (offline={args.offline}, rpm={args.rpm}, batch={args.batch_size})\n")

    # Partition rows into cache hits and misses.
    misses = []
    for r in feedback:
        r["content_hash"] = content_hash(r["review_text"], r["source"])
        if (r["content_hash"], model) not in cache:
            misses.append(r)
    print(f"Cache hits    : {len(feedback) - len(misses):,}")
    print(f"To enrich     : {len(misses):,}\n")

    if misses and not args.offline:
        from google import genai
        api_key = os.environ.get(API_KEY_ENV)
        if not api_key:
            print(f"ERROR: {API_KEY_ENV} not set and {len(misses)} rows need enrichment.")
            sys.exit(1)
        client = genai.Client(api_key=api_key)
        system_instruction = build_system_instruction(catalog)
        limiter = RateLimiter(args.rpm)

        batches = [misses[i:i + args.batch_size] for i in range(0, len(misses), args.batch_size)]
        stopped_early = False
        for bi, batch in enumerate(batches, 1):
            limiter.wait()
            print(f"  batch {bi}/{len(batches)} ({len(batch)} items)...")
            try:
                raw_items = call_with_backoff(
                    lambda b=batch: enrich_batch(
                        client, model, system_instruction, b, args.max_output_tokens)
                )
            except DailyQuotaExceeded:
                print(f"\n  Daily free-tier request quota exhausted for {model}. "
                      "Stopping gracefully — progress is checkpointed.")
                stopped_early = True
                break
            by_id = {it.get("feedback_id"): it for it in raw_items if isinstance(it, dict)}
            for r in batch:
                raw = by_id.get(r["feedback_id"])
                if raw is None:
                    print(f"    WARN no result for {r['feedback_id']}; will retry next run")
                    continue
                clean = normalize(raw, valid_ids)
                cache[(r["content_hash"], model)] = {
                    "content_hash": r["content_hash"], "model_version": model, **clean,
                }
            # Checkpoint after EVERY batch so a mid-run quota stop loses nothing.
            if not args.no_fixture_write:
                save_fixture(cache)
        if not args.no_fixture_write:
            print(f"\nFixture: {FIXTURE_PATH}  ({len(cache):,} entries)")
        if stopped_early:
            print("Re-run later (quota resets daily) to finish; cached rows are skipped.")
    elif misses and args.offline:
        print(f"WARN: offline mode, {len(misses)} rows have no fixture entry; "
              "they will be written with a neutral fallback.")

    # Assemble output rows from cache (+ fallback for any offline misses).
    out_rows = []
    fallback = 0
    for r in feedback:
        rec = cache.get((r["content_hash"], model))
        if rec is None:
            fallback += 1
            rec = {"sentiment": "neutral", "sentiment_confidence": 0.0, "themes": ["other"],
                   "product_mentions": [], "competitor_mentions": [], "language": "und",
                   "campaign_reference": "", "resolved_campaign_id": "", "resolution_confidence": 0.0}
        out_rows.append({
            "feedback_id": r["feedback_id"], "content_hash": r["content_hash"],
            "model_version": model,
            **{k: rec[k] for k in (
                "sentiment", "sentiment_confidence", "themes", "product_mentions",
                "competitor_mentions", "language", "campaign_reference",
                "resolved_campaign_id", "resolution_confidence")},
        })

    write_enriched(con, out_rows)
    resolved = sum(1 for r in out_rows if r["resolved_campaign_id"])
    print(f"\nWrote RAW.FEEDBACK_ENRICHED: {len(out_rows):,} rows "
          f"({resolved:,} resolved to a campaign, {fallback:,} fallback).")

    if args.sample:
        asc = lambda s: str(s).encode("ascii", "replace").decode()
        print("\nSamples:")
        for r in out_rows[:args.sample]:
            print(f"  [{r['sentiment']:8} {r['sentiment_confidence']:.2f}] "
                  f"themes={r['themes']} lang={r['language']} "
                  f"campaign={r['resolved_campaign_id'] or '-'} "
                  f"({r['resolution_confidence']:.2f})")
    con.close()


if __name__ == "__main__":
    main()
