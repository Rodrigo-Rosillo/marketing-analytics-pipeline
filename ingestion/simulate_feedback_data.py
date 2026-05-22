"""
simulate_feedback_data.py
Generates a year of *deliberately messy* customer feedback — the unstructured
source the LLM enrichment layer (Phase 2) will resolve and classify.

Unlike the clean numeric ad data, this file mimics real-world feedback exhaust:
  - free-text reviews / social comments (the only field that really matters)
  - inconsistent platform labels       ("FB", "facebook", "tt", "Trustpilot"...)
  - dirty, multi-format timestamps      ("2024-03-05", "03/05/2024", "March 5, 2024", blanks)
  - ratings in mixed formats or missing ("5/5", "4.0", stars, null)
  - mixed languages, emojis, typos
  - oblique campaign references          ("saw your summer sale ad", "your tiktok")

Each row carries a `true_campaign_id` column: the campaign the comment was
generated from. It is a SYNTHETIC GROUND-TRUTH LABEL used only to measure LLM
resolution accuracy in Phase 3 — the enrichment step never reads it. Real
pipelines would use a small human-labeled holdout instead.

Output: data/raw/customer_feedback_2024.csv
"""

import csv
import random
from datetime import date, timedelta
from pathlib import Path

random.seed(7)  # reproducible


# ── Campaign reference phrases ─────────────────────────────────────────────────
# Free-text ways a customer might allude to each real campaign (see
# simulate_ad_data.py). The LLM must map these back to the campaign_id.

CAMPAIGN_REFERENCES: dict[str, list[str]] = {
    "META_001": ["saw your brand video on facebook", "that awareness ad", "your insta brand campaign"],
    "META_002": ["the ad that kept following me around", "your retargeting ad", "that ad after I left the site"],
    "META_003": ["your summer sale ad", "the back to school promo", "the holiday bundle deal", "that seasonal promo"],
    "GOOG_001": ["found you searching your brand on google", "your brand showed up first on google"],
    "GOOG_002": ["googled running gear and your ad popped up", "saw your search ad", "found you on google search"],
    "GOOG_003": ["that banner ad followed me", "your youtube ad", "the display ad I kept seeing"],
    "TKTK_001": ["your tiktok video", "that viral tiktok", "saw you on my fyp"],
    "TKTK_002": ["the creator's tiktok about you", "that ugc tiktok", "a tiktoker reviewed it"],
    "TKTK_003": ["your tiktok product ad", "the dynamic ad on tiktok"],
}
CAMPAIGN_IDS = list(CAMPAIGN_REFERENCES.keys())


# ── Source platform labels (intentionally inconsistent) ────────────────────────

SOURCE_LABELS = [
    "FB", "facebook", "Facebook", "Meta", "fb comment", "IG", "instagram", "Insta",
    "Google", "google reviews", "Google Play", "Play Store", "GMB",
    "TikTok", "tiktok", "tt", "TT comment",
    "Trustpilot", "App Store", "website review", "email survey", "Yotpo",
]


# ── Theme phrase bank, keyed by (theme, sentiment) ─────────────────────────────

THEMES = {
    "shipping": {
        "pos": ["arrived way faster than expected", "shipping was super quick", "got it in two days, amazing"],
        "neg": ["took almost three weeks to arrive", "tracking never updated and it showed up late", "shipping is painfully slow"],
        "neu": ["shipping was about average", "delivery was fine I guess"],
    },
    "price": {
        "pos": ["honestly great value for the price", "cheaper than I expected and worth it", "the promo price was a steal"],
        "neg": ["way overpriced for what you get", "not worth the money at all", "the price went up right after I bought"],
        "neu": ["price is about what you'd expect", "it's okay for the cost"],
    },
    "quality": {
        "pos": ["the quality is genuinely amazing", "so well made, feels premium", "even better quality than the photos"],
        "neg": ["fell apart after one wash", "cheap material, super disappointed", "broke within a week"],
        "neu": ["quality is decent, nothing special", "it's fine, does the job"],
    },
    "customer_service": {
        "pos": ["support team was so helpful and quick", "customer service sorted my issue in minutes", "great support, very responsive"],
        "neg": ["customer service ignored my emails for days", "support was useless and rude", "still waiting on a reply from support"],
        "neu": ["support was okay, took a bit", "customer service was average"],
    },
    "sizing_fit": {
        "pos": ["fit is perfect, true to size", "sizing was spot on"],
        "neg": ["runs really small, had to send it back", "sizing chart is totally off"],
        "neu": ["fit is alright, maybe size up"],
    },
    "website_app": {
        "pos": ["checkout was smooth and easy", "the app makes ordering so simple"],
        "neg": ["the app kept crashing at payment", "checkout failed three times before it worked", "website is a buggy mess"],
        "neu": ["the site works fine", "ordering was straightforward enough"],
    },
    "returns": {
        "pos": ["return was painless and refund was fast", "easy returns, no questions asked"],
        "neg": ["the return process is an absolute nightmare", "still haven't gotten my refund weeks later"],
        "neu": ["returns were okay, a bit slow"],
    },
}
THEME_KEYS = list(THEMES.keys())

OPENERS = {
    "pos": ["Love this!", "So happy with my order.", "Honestly impressed.", "Can't recommend enough.", "10/10."],
    "neg": ["Really disappointed.", "Would not buy again.", "Frustrated tbh.", "Save your money.", "Ugh."],
    "neu": ["Mixed feelings.", "It's fine.", "Not bad, not great.", "Decent overall."],
}

PRODUCTS = ["running shoes", "leggings", "protein powder", "water bottle", "yoga mat",
            "moisturizer", "backpack", "hoodie", "sneakers", "gym shorts"]
COMPETITORS = ["Nike", "Lululemon", "Gymshark", "Adidas", "Amazon Basics", "Alo"]

EMOJIS = ["", "", "", " 🙄", " 😍", " 👎", " 🔥", " 😡", " 💯", " 🤷", " ❤️"]

# Spanish-language full templates (~exercise language detection), with their
# implied sentiment for label realism.
SPANISH = {
    "pos": ["Me encanta! La calidad es increible y llego rapidisimo.",
            "Excelente compra, vale totalmente la pena.",
            "Super contento con el producto, lo recomiendo."],
    "neg": ["Pesima experiencia, el envio tardo siglos.",
            "Muy decepcionado, la calidad es malisima.",
            "El servicio al cliente no respondio nunca."],
    "neu": ["Esta bien, nada del otro mundo.", "Cumple, pero esperaba algo mejor."],
}


# ── Typo / noise helpers ───────────────────────────────────────────────────────

def maybe_typo(text: str) -> str:
    """Occasionally introduce a casual typo or lowercasing."""
    if random.random() < 0.18:
        text = text.replace("the ", "teh ", 1)
    if random.random() < 0.12:
        text = text.lower()
    if random.random() < 0.10:
        text = text.replace(",", "")
    return text


def messy_timestamp(d: date) -> str:
    """Return the date in one of several inconsistent formats, sometimes blank."""
    r = random.random()
    if r < 0.05:
        return ""  # missing
    months = ["January", "February", "March", "April", "May", "June", "July",
              "August", "September", "October", "November", "December"]
    fmt = random.choice(["iso", "iso_time", "us", "us_short", "long", "dotted", "spaced", "dmy"])
    if fmt == "iso":
        return d.isoformat()
    if fmt == "iso_time":
        return f"{d.isoformat()} {random.randint(0,23):02d}:{random.randint(0,59):02d}:{random.randint(0,59):02d}"
    if fmt == "us":
        return f"{d.month:02d}/{d.day:02d}/{d.year}"
    if fmt == "us_short":
        return f"{d.month}/{d.day}/{str(d.year)[2:]}"
    if fmt == "long":
        return f"{months[d.month-1]} {d.day}, {d.year}"
    if fmt == "dotted":
        return f"{d.year}.{d.month:02d}.{d.day:02d}"
    if fmt == "spaced":
        return f"  {d.isoformat()} "
    return f"{d.day} {months[d.month-1][:3]} {d.year}"  # dmy: "5 Mar 2024"


def messy_rating(sentiment: str) -> str:
    """Return a rating in a mixed format, or blank ~55% of the time."""
    if random.random() < 0.55:
        return ""
    score = {"pos": random.choice([5, 5, 4]), "neg": random.choice([1, 1, 2]),
             "neu": random.choice([3, 3, 4])}[sentiment]
    fmt = random.choice(["int", "frac", "float", "stars", "words"])
    if fmt == "int":
        return str(score)
    if fmt == "frac":
        return f"{score}/5"
    if fmt == "float":
        return f"{score}.0"
    if fmt == "stars":
        return "★" * score + "☆" * (5 - score)
    return f"{score} stars"


def messy_author() -> str:
    if random.random() < 0.10:
        return ""
    handles = ["@runner_jane", "mike_t", "Sarah K.", "user8842", "fitlife.amy",
               "j.doe", "GymRat99", "anon", "kayla_", "the_real_dan", "m.garcia",
               "wellness_wendy", "TrailDad", "—"]
    return random.choice(handles)


# ── Review text builder ────────────────────────────────────────────────────────

def build_review(sentiment: str, campaign_id: str | None) -> str:
    """Compose a free-text review from openers, theme phrases, and optional
    product / competitor / campaign references plus noise."""
    # ~12% of rows are Spanish
    if random.random() < 0.12:
        text = random.choice(SPANISH[sentiment])
    else:
        parts = []
        if random.random() < 0.7:
            parts.append(random.choice(OPENERS[sentiment]))
        n_themes = random.choice([1, 1, 2])
        for theme in random.sample(THEME_KEYS, n_themes):
            parts.append(random.choice(THEMES[theme][sentiment]).capitalize() + ".")
        if random.random() < 0.45:
            parts.append(f"The {random.choice(PRODUCTS)} {'is great' if sentiment=='pos' else 'was a letdown' if sentiment=='neg' else 'is ok'}.")
        if random.random() < 0.18:
            comp = random.choice(COMPETITORS)
            parts.append(f"{'Better than' if sentiment=='pos' else 'Going back to'} {comp} honestly.")
        text = " ".join(parts)

    # Campaign reference (only when this row is tied to a campaign)
    if campaign_id and random.random() < 0.85:
        ref = random.choice(CAMPAIGN_REFERENCES[campaign_id])
        text = f"{text} ({ref})" if random.random() < 0.5 else f"{ref.capitalize()} — {text}"

    text = maybe_typo(text)
    return text + random.choice(EMOJIS)


# ── Row generation ─────────────────────────────────────────────────────────────

SENTIMENTS = ["pos", "pos", "neg", "neg", "neu"]  # skew toward pos/neg


def generate_rows(n: int = 1500, year: int = 2024) -> list[dict]:
    rows = []
    start = date(year, 1, 1)
    for i in range(1, n + 1):
        d = start + timedelta(days=random.randint(0, 365))
        sentiment = random.choice(SENTIMENTS)
        # ~40% of feedback references no identifiable campaign
        campaign_id = None if random.random() < 0.40 else random.choice(CAMPAIGN_IDS)
        rows.append({
            "feedback_id":      f"FB_{i:06d}",
            "posted_at":        messy_timestamp(d),
            "source":           random.choice(SOURCE_LABELS),
            "rating":           messy_rating(sentiment),
            "review_text":      build_review(sentiment, campaign_id),
            "author":           messy_author(),
            "true_campaign_id": campaign_id or "",
        })
    random.shuffle(rows)
    return rows


# ── Write CSV ───────────────────────────────────────────────────────────────---

FIELDNAMES = ["feedback_id", "posted_at", "source", "rating",
              "review_text", "author", "true_campaign_id"]


def main() -> None:
    output_path = Path("data/raw/customer_feedback_2024.csv")
    output_path.parent.mkdir(parents=True, exist_ok=True)

    rows = generate_rows()
    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDNAMES)
        writer.writeheader()
        writer.writerows(rows)

    labeled = sum(1 for r in rows if r["true_campaign_id"])
    print("\nGenerating customer feedback...\n")
    print(f"  OK  {output_path}  ({len(rows):,} rows)")
    print(f"  {labeled:,} rows reference a campaign, {len(rows)-labeled:,} do not.")
    print(f"\nOutput: {output_path.resolve()}\n")


if __name__ == "__main__":
    main()
