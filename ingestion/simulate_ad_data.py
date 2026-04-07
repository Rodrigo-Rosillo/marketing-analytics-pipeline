"""
simulate_ad_data.py
Generates one year of realistic multi-channel ad performance data
for Meta Ads, Google Ads, and TikTok Ads.

Output: data/raw/meta_ads_2024.csv
        data/raw/google_ads_2024.csv
        data/raw/tiktok_ads_2024.csv
"""

import csv
import math
import os
import random
from dataclasses import dataclass, field
from datetime import date, timedelta
from pathlib import Path


# ── Reproducibility ──────────────────────────────────────────────────────────

random.seed(42)


# ── Campaign definitions ──────────────────────────────────────────────────────

@dataclass
class Campaign:
    id: str
    name: str
    channel: str
    objective: str          # awareness | traffic | conversion
    base_daily_spend: float
    avg_cpc: float
    avg_ctr: float          # impressions → clicks
    avg_cvr: float          # clicks → conversions
    avg_order_value: float
    ad_sets: list[dict] = field(default_factory=list)


CAMPAIGNS = [
    # ── Meta Ads ─────────────────────────────────────────────────────────────
    Campaign(
        id="META_001", name="Brand Awareness Q1-Q4",
        channel="meta", objective="awareness",
        base_daily_spend=280, avg_cpc=0.45, avg_ctr=0.028,
        avg_cvr=0.03, avg_order_value=85,
        ad_sets=[
            {"id": "AS_M001", "name": "Lookalike 1% — Purchasers"},
            {"id": "AS_M002", "name": "Interest — Fitness & Wellness"},
            {"id": "AS_M003", "name": "Broad 25-44"},
        ],
    ),
    Campaign(
        id="META_002", name="Retargeting — Website Visitors",
        channel="meta", objective="conversion",
        base_daily_spend=180, avg_cpc=0.72, avg_ctr=0.042,
        avg_cvr=0.065, avg_order_value=110,
        ad_sets=[
            {"id": "AS_M004", "name": "Viewed Product — 7d"},
            {"id": "AS_M005", "name": "Add to Cart — 14d"},
        ],
    ),
    Campaign(
        id="META_003", name="Seasonal Promotions",
        channel="meta", objective="conversion",
        base_daily_spend=220, avg_cpc=0.58, avg_ctr=0.035,
        avg_cvr=0.045, avg_order_value=95,
        ad_sets=[
            {"id": "AS_M006", "name": "Holiday Bundle Promo"},
            {"id": "AS_M007", "name": "Back to School"},
            {"id": "AS_M008", "name": "Summer Sale"},
        ],
    ),

    # ── Google Ads ────────────────────────────────────────────────────────────
    Campaign(
        id="GOOG_001", name="Search — Brand Keywords",
        channel="google_ads", objective="conversion",
        base_daily_spend=320, avg_cpc=1.85, avg_ctr=0.062,
        avg_cvr=0.082, avg_order_value=130,
        ad_sets=[
            {"id": "AS_G001", "name": "Exact Match — Brand"},
            {"id": "AS_G002", "name": "Phrase Match — Brand + Product"},
        ],
    ),
    Campaign(
        id="GOOG_002", name="Search — Non-Brand",
        channel="google_ads", objective="traffic",
        base_daily_spend=410, avg_cpc=2.40, avg_ctr=0.038,
        avg_cvr=0.028, avg_order_value=115,
        ad_sets=[
            {"id": "AS_G003", "name": "Category Keywords"},
            {"id": "AS_G004", "name": "Competitor Keywords"},
            {"id": "AS_G005", "name": "Long-tail Informational"},
        ],
    ),
    Campaign(
        id="GOOG_003", name="Display Remarketing",
        channel="google_ads", objective="awareness",
        base_daily_spend=150, avg_cpc=0.38, avg_ctr=0.006,
        avg_cvr=0.022, avg_order_value=95,
        ad_sets=[
            {"id": "AS_G006", "name": "All Website Visitors — 30d"},
            {"id": "AS_G007", "name": "YouTube Viewers"},
        ],
    ),

    # ── TikTok Ads ────────────────────────────────────────────────────────────
    Campaign(
        id="TKTK_001", name="Top-of-Funnel Video Views",
        channel="tiktok", objective="awareness",
        base_daily_spend=190, avg_cpc=0.32, avg_ctr=0.018,
        avg_cvr=0.018, avg_order_value=75,
        ad_sets=[
            {"id": "AS_T001", "name": "Gen Z Interest Clusters"},
            {"id": "AS_T002", "name": "Lookalike — Email List"},
        ],
    ),
    Campaign(
        id="TKTK_002", name="Spark Ads — UGC Content",
        channel="tiktok", objective="traffic",
        base_daily_spend=140, avg_cpc=0.41, avg_ctr=0.024,
        avg_cvr=0.032, avg_order_value=82,
        ad_sets=[
            {"id": "AS_T003", "name": "Creator Whitelist"},
            {"id": "AS_T004", "name": "Trending Audio Boost"},
        ],
    ),
    Campaign(
        id="TKTK_003", name="Conversion — Product Catalog",
        channel="tiktok", objective="conversion",
        base_daily_spend=160, avg_cpc=0.55, avg_ctr=0.030,
        avg_cvr=0.048, avg_order_value=90,
        ad_sets=[
            {"id": "AS_T005", "name": "Dynamic Product Ads"},
        ],
    ),
]


# ── Seasonality helpers ───────────────────────────────────────────────────────

def seasonality_multiplier(d: date) -> float:
    """
    Smooth sinusoidal base with hard bumps for key retail periods.
    Returns a spend/volume multiplier centred on 1.0.
    """
    day_of_year = d.timetuple().tm_yday
    # Gentle base wave: peaks mid-year (summer) and Dec
    base = 1.0 + 0.12 * math.sin(2 * math.pi * (day_of_year - 60) / 365)

    # Promotional calendar bumps
    bumps = {
        # (month, day): (duration_days, multiplier_peak)
        (2, 10): (5,  1.25),   # Valentine's Day prep
        (3, 15): (4,  1.15),   # Spring Sale
        (5, 8):  (5,  1.20),   # Mother's Day
        (6, 16): (4,  1.10),   # Father's Day
        (7, 4):  (3,  1.30),   # 4th of July
        (9, 1):  (10, 1.20),   # Back to School
        (10, 28): (5, 1.40),   # Pre-Halloween
        (11, 25): (7, 1.80),   # Black Friday / Cyber Monday
        (12, 10): (14, 1.60),  # Holiday stretch
        (12, 26): (5,  0.70),  # Post-holiday lull
    }

    for (bm, bd), (duration, peak) in bumps.items():
        bump_start = date(d.year, bm, bd)
        delta = (d - bump_start).days
        if 0 <= delta < duration:
            progress = delta / duration
            # Triangle shape: ramp up then down
            intensity = 1 - abs(2 * progress - 1)
            base += (peak - 1.0) * intensity

    return max(0.4, base)


def weekday_multiplier(d: date) -> float:
    """B2C channels dip slightly on weekends for search, spike for social."""
    dow = d.weekday()  # 0=Mon … 6=Sun
    return {0: 1.00, 1: 1.02, 2: 1.03, 3: 1.01, 4: 0.98, 5: 0.90, 6: 0.85}[dow]


def channel_weekday_multiplier(channel: str, d: date) -> float:
    dow = d.weekday()
    if channel == "tiktok":
        # TikTok engagement peaks on weekends
        return {0: 0.90, 1: 0.92, 2: 0.95, 3: 0.96, 4: 1.05, 5: 1.15, 6: 1.20}[dow]
    if channel == "google_ads":
        # Search intent higher weekdays
        return {0: 1.05, 1: 1.06, 2: 1.05, 3: 1.04, 4: 1.02, 5: 0.90, 6: 0.85}[dow]
    # Meta: moderate weekend boost
    return {0: 0.97, 1: 0.98, 2: 1.00, 3: 0.99, 4: 1.02, 5: 1.08, 6: 1.10}[dow]


def jitter(value: float, pct: float = 0.12) -> float:
    """Add ±pct% multiplicative noise."""
    return value * (1 + random.uniform(-pct, pct))


# ── Row generator ─────────────────────────────────────────────────────────────

def generate_rows(campaign: Campaign, year: int = 2024) -> list[dict]:
    rows = []
    start = date(year, 1, 1)
    end   = date(year, 12, 31)
    delta = timedelta(days=1)
    d = start

    while d <= end:
        seas  = seasonality_multiplier(d)
        wday  = channel_weekday_multiplier(campaign.channel, d)
        mult  = seas * wday

        # Daily spend with noise
        spend = jitter(campaign.base_daily_spend * mult / len(campaign.ad_sets), 0.15)
        spend = round(max(0.01, spend), 2)

        # Impressions derived from spend and effective CPM
        effective_cpm = jitter((campaign.avg_cpc / campaign.avg_ctr) * 1000, 0.10)
        impressions   = int(max(1, (spend / effective_cpm) * 1000))

        # Clicks
        ctr    = jitter(campaign.avg_ctr, 0.10)
        clicks = int(max(0, round(impressions * ctr)))

        # Conversions
        cvr         = jitter(campaign.avg_cvr, 0.12)
        conversions = int(max(0, round(clicks * cvr)))

        # Conversion value (revenue)
        aov              = jitter(campaign.avg_order_value, 0.18)
        conversion_value = round(conversions * aov, 2)

        # CPC (actual)
        actual_cpc = round(spend / clicks, 4) if clicks > 0 else 0.0

        # Distribute evenly across ad sets
        for ad_set in campaign.ad_sets:
            rows.append({
                "date":             d.isoformat(),
                "channel":          campaign.channel,
                "campaign_id":      campaign.id,
                "campaign_name":    campaign.name,
                "objective":        campaign.objective,
                "ad_set_id":        ad_set["id"],
                "ad_set_name":      ad_set["name"],
                "impressions":      impressions,
                "clicks":           clicks,
                "spend":            spend,
                "conversions":      conversions,
                "conversion_value": conversion_value,
                "cpc":              actual_cpc,
                "currency":         "USD",
            })

        d += delta

    return rows


# ── Write CSVs ────────────────────────────────────────────────────────────────

FIELDNAMES = [
    "date", "channel", "campaign_id", "campaign_name", "objective",
    "ad_set_id", "ad_set_name", "impressions", "clicks", "spend",
    "conversions", "conversion_value", "cpc", "currency",
]


def write_csv(rows: list[dict], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDNAMES)
        writer.writeheader()
        writer.writerows(rows)
    print(f"  ✓  {path}  ({len(rows):,} rows)")


def main() -> None:
    output_dir = Path("data/raw")

    channel_rows: dict[str, list[dict]] = {"meta": [], "google_ads": [], "tiktok": []}

    for campaign in CAMPAIGNS:
        rows = generate_rows(campaign)
        channel_rows[campaign.channel].extend(rows)

    channel_files = {
        "meta":       output_dir / "meta_ads_2024.csv",
        "google_ads": output_dir / "google_ads_2024.csv",
        "tiktok":     output_dir / "tiktok_ads_2024.csv",
    }

    print("\nGenerating ad data files...\n")
    for channel, rows in channel_rows.items():
        # Sort by date then campaign
        rows.sort(key=lambda r: (r["date"], r["campaign_id"], r["ad_set_id"]))
        write_csv(rows, channel_files[channel])

    total = sum(len(r) for r in channel_rows.values())
    print(f"\nDone — {total:,} total rows across 3 channels.")
    print(f"Output directory: {output_dir.resolve()}\n")


if __name__ == "__main__":
    main()
