#!/usr/bin/env python3
"""
Avito.ma Multi-City Apartment Scraper  v3
==========================================
URL : https://www.avito.ma/fr/maroc/appartements-à_vendre
      ?o=N&cities=13,5,8,90,12,15&price=100000-&has_price=true
Pages: o=1 → o=30

Install:
    pip install requests beautifulsoup4

Run:
    python scraper_avito.py                      # all 30 pages
    python scraper_avito.py --pages 3            # quick test
    python scraper_avito.py --date today         # only today's listings
    python scraper_avito.py --date week          # this week
    python scraper_avito.py --date month         # this month
    python scraper_avito.py --date all           # no date filter (default)
"""

import os, json, re, time, random, argparse
from datetime import datetime, timedelta, timezone
from collections import Counter
import requests
from bs4 import BeautifulSoup

# ── Config ──────────────────────────────────────────────────────────────────
#OUTPUT     = "data.json"
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
OUTPUT     = os.path.join(SCRIPT_DIR, "data.json")
BASE_URL   = "https://www.avito.ma/fr/maroc/appartements-%C3%A0_vendre"
PARAMS     = "cities=13,5,8,90,12,15&price=100000-&has_price=true"
PAGE_RANGE = range(1, 31)   # o=1 … o=30

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/121.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "fr-FR,fr;q=0.9,ar;q=0.8",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Referer": "https://www.avito.ma/",
}

SESSION = requests.Session()
SESSION.headers.update(HEADERS)

# ── Date filter boundaries ──────────────────────────────────────────────────
def date_boundary(mode):
    """Return a datetime cutoff; listings older than this are excluded."""
    now = datetime.now(timezone.utc)
    if mode == "today":
        return now.replace(hour=0, minute=0, second=0, microsecond=0)
    if mode == "week":
        return now - timedelta(days=now.weekday())          # Monday 00:00
    if mode == "month":
        return now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    return None   # "all" → no filter

# ── Parse "il y a X heures/minutes/jours" → datetime ──────────────────────
_RELATIVE_RE = re.compile(
    r"il y a\s+(\d+)\s+(minute|heure|jour|semaine|mois)s?",
    re.IGNORECASE
)

def parse_relative_date(text):
    """Convert Avito relative timestamps to an aware datetime."""
    now = datetime.now(timezone.utc)
    if not text:
        return now
    m = _RELATIVE_RE.search(text)
    if not m:
        return now
    n, unit = int(m.group(1)), m.group(2).lower()
    if unit == "minute":
        return now - timedelta(minutes=n)
    if unit == "heure":
        return now - timedelta(hours=n)
    if unit == "jour":
        return now - timedelta(days=n)
    if unit == "semaine":
        return now - timedelta(weeks=n)
    if unit == "mois":
        return now - timedelta(days=n * 30)
    return now

# ── City / quartier mapping ─────────────────────────────────────────────────
CITY_TEXT_MAP = {
    "casablanca": "Casablanca",
    "agadir":     "Agadir",
    "marrakech":  "Marrakech",
    "tanger":     "Tanger",
    "rabat":      "Rabat",
    "mohammedia": "Mohammedia",
    "salé":       "Rabat",
    "sale":       "Rabat",
}

# URL path keyword → city
CITY_KEYWORDS = {
    "anfa":              "Casablanca", "maarif":          "Casablanca",
    "gauthier":          "Casablanca", "racine":          "Casablanca",
    "ain_diab":          "Casablanca", "ain_chock":       "Casablanca",
    "hay_mohammadi":     "Casablanca", "val_fleuri":      "Casablanca",
    "californie":        "Casablanca", "bourgogne":       "Casablanca",
    "hay_hassani":       "Casablanca", "ain_sebaa":       "Casablanca",
    "sidi_bernoussi":    "Casablanca", "roches_noires":   "Casablanca",
    "palmier":           "Casablanca", "oasis":           "Casablanca",
    "oulfa":             "Casablanca", "sidi_maarouf":    "Casablanca",
    "moulay_rachid":     "Casablanca", "derb_sultan":     "Casablanca",
    "sbata":             "Casablanca", "c.i.l":           "Casablanca",
    "2_mars":            "Casablanca", "nassim":          "Casablanca",
    "hay_chrifa":        "Casablanca", "ferme_bretone":   "Casablanca",
    "casablanca_finance_city": "Casablanca",
    "guéliz":            "Marrakech",  "gueliz":          "Marrakech",
    "hivernage":         "Marrakech",  "es_saada":        "Marrakech",
    "hay_izdihar":       "Marrakech",  "mabrouka":        "Marrakech",
    "rouidat":           "Marrakech",  "allal_el_fassi":  "Marrakech",
    "targa":             "Marrakech",  "azli":            "Marrakech",
    "route_de_casablanca": "Marrakech","route_de_tahanaoute": "Marrakech",
    "route_d_amezmiz":   "Marrakech",
    "mesnana":           "Tanger",     "manar":           "Tanger",
    "malabata":          "Tanger",
    "souissi":           "Rabat",      "agdal":           "Rabat",
    "la_siesta":         "Mohammedia",
}

# ── Bounding boxes [lat_min, lat_max, lng_min, lng_max] ──────────────────────
QUARTIER_BOUNDS = {
    # Casablanca
    "Ain Diab":       (33.582, 33.596, -7.705, -7.665),
    "Anfa":           (33.583, 33.598, -7.668, -7.640),
    "Casa Anfa":      (33.568, 33.580, -7.665, -7.643),
    "Racine":         (33.583, 33.597, -7.651, -7.628),
    "Gauthier":       (33.582, 33.595, -7.636, -7.611),
    "Maarif":         (33.572, 33.593, -7.648, -7.622),
    "Californie":     (33.562, 33.581, -7.650, -7.622),
    "Triangle D'Or":  (33.585, 33.598, -7.638, -7.618),
    "Centre Ville":   (33.585, 33.603, -7.626, -7.598),
    "Bourgogne":      (33.573, 33.591, -7.626, -7.600),
    "Val Fleuri":     (33.570, 33.589, -7.640, -7.612),
    "Palmier":        (33.564, 33.582, -7.618, -7.590),
    "Belvedere":      (33.578, 33.596, -7.617, -7.589),
    "Derb Sultan":    (33.574, 33.593, -7.612, -7.583),
    "Cil":            (33.557, 33.575, -7.618, -7.589),
    "Sidi Belyout":   (33.591, 33.606, -7.628, -7.603),
    "Hay Mohammadi":  (33.585, 33.603, -7.602, -7.572),
    "Roches Noires":  (33.587, 33.607, -7.587, -7.556),
    "Ain Sebaa":      (33.603, 33.625, -7.590, -7.548),
    "Sidi Bernoussi": (33.594, 33.612, -7.577, -7.545),
    "Oasis":          (33.548, 33.572, -7.650, -7.618),
    "Hay Hassani":    (33.536, 33.563, -7.682, -7.644),
    "Ain Chock":      (33.556, 33.573, -7.636, -7.606),
    "Oulfa":          (33.527, 33.553, -7.670, -7.633),
    "Sidi Maarouf":   (33.521, 33.551, -7.648, -7.613),
    "Hay Riad":       (33.548, 33.565, -7.642, -7.614),
    "Moulay Rachid":  (33.548, 33.566, -7.612, -7.582),
    "Ben M'Sick":     (33.561, 33.577, -7.616, -7.590),
    "Sbata":          (33.570, 33.585, -7.607, -7.582),
    # Agadir
    "Founty":         (30.388, 30.406, -9.625, -9.595),
    "Talborjt":       (30.414, 30.430, -9.600, -9.575),
    "Hay Almassira":  (30.395, 30.415, -9.575, -9.545),
    "Centre Agadir":  (30.418, 30.432, -9.592, -9.568),
    "Dakhla":         (30.402, 30.418, -9.605, -9.580),
    "Anza":           (30.440, 30.465, -9.610, -9.580),
    # Marrakech
    "Gueliz":         (31.630, 31.648, -8.022, -7.992),
    "Hivernage":      (31.614, 31.632, -8.010, -7.985),
    "Medina":         (31.618, 31.636, -7.998, -7.975),
    "Palmeraie":      (31.638, 31.665, -7.960, -7.925),
    "Majorelle":      (31.636, 31.650, -8.002, -7.978),
    "Targa":          (31.596, 31.616, -8.020, -7.995),
    "Massira":        (31.598, 31.618, -7.998, -7.970),
    # Tanger
    "Malabata":       (35.778, 35.796, -5.778, -5.745),
    "Centre Tanger":  (35.765, 35.782, -5.820, -5.790),
    "Marshan":        (35.778, 35.795, -5.825, -5.798),
    "Iberia":         (35.756, 35.775, -5.812, -5.785),
    # Rabat
    "Agdal":          (33.990, 34.010, -6.860, -6.830),
    "Hassan":         (34.010, 34.030, -6.850, -6.820),
    "Souissi":        (33.990, 34.015, -6.825, -6.795),
    "Les Orangers":   (34.005, 34.025, -6.870, -6.840),
    "Yacoub El Mansour": (33.975, 33.998, -6.875, -6.845),
    # Mohammedia
    "Centre Mohammedia": (33.688, 33.706, -7.402, -7.372),
    "Ain Harrouda":   (33.660, 33.682, -7.428, -7.398),
}

CITY_BOUNDS = {
    "Casablanca":  (33.520, 33.630, -7.710, -7.540),
    "Agadir":      (30.380, 30.470, -9.640, -9.540),
    "Marrakech":   (31.580, 31.680, -8.060, -7.920),
    "Tanger":      (35.720, 35.810, -5.870, -5.740),
    "Rabat":       (33.930, 34.060, -6.900, -6.790),
    "Mohammedia":  (33.660, 33.720, -7.430, -7.360),
}

def coords_for(quartier, city):
    box = QUARTIER_BOUNDS.get(quartier) or CITY_BOUNDS.get(city)
    if not box:
        return None, None
    a, b, c, d = box
    return round(random.uniform(a, b), 6), round(random.uniform(c, d), 6)

# ── Parse helpers ──────────────────────────────────────────────────────────
def clean_price(text):
    if not text:
        return None
    m = re.search(r'([\d][\d\s]*)\s*DH', text)
    if not m:
        return None
    try:
        return int(re.sub(r'\s', '', m.group(1)))
    except ValueError:
        return None

def clean_surface(text):
    m = re.search(r'(\d+)\s*m[²2]', text, re.IGNORECASE)
    return int(m.group(1)) if m else None

def clean_rooms(text):
    m = re.search(r'(\d+)\s*pi[eè]ces?', text, re.IGNORECASE)
    if m:
        return int(m.group(1))
    m = re.search(r'(\d+)\s*ch(?:ambres?)?', text, re.IGNORECASE)
    return int(m.group(1)) if m else None

def city_from_location_text(loc):
    m = re.match(r"Appartements dans ([^,]+)", loc or "")
    if not m:
        return None
    raw = m.group(1).strip().lower()
    for key, val in CITY_TEXT_MAP.items():
        if key in raw:
            return val
    return m.group(1).strip().title()

def quartier_from_location_text(loc):
    m = re.match(r"Appartements dans [^,]+,\s*(.+)", loc or "")
    return m.group(1).strip().title() if m else None

def city_from_url(href):
    path = href.lower()
    for kw, city in CITY_KEYWORDS.items():
        if f"/{kw}/" in path:
            return city
    return None

# ── Scrape one page ────────────────────────────────────────────────────────
LISTING_HREF_RE = re.compile(
    r'^https://www\.avito\.ma/fr/[^/]+/appartements/[^/]+\.htm$'
)

def scrape_page(page_num):
    url = f"{BASE_URL}?o={page_num}&{PARAMS}"
    print(f"  Page {page_num:2d}/30  {url}")

    for attempt in range(4):
        try:
            resp = SESSION.get(url, timeout=30)
            resp.raise_for_status()
            break
        except Exception as e:
            wait = (attempt + 1) * 4
            print(f"    Attempt {attempt+1} failed ({e}). Retry in {wait}s...")
            time.sleep(wait)
    else:
        print(f"    !! Page {page_num} skipped after 4 failed attempts.")
        return []

    soup = BeautifulSoup(resp.text, "html.parser")

    anchors = soup.find_all("a", href=LISTING_HREF_RE)
    seen = set()
    listings = []

    for a in anchors:
        href = a.get("href", "")
        if "immoneuf" in href or href in seen:
            continue
        seen.add(href)

        # ── All text lines ──────────────────────────────────────────────
        lines = [l.strip() for l in a.get_text("\n", strip=True).split("\n") if l.strip()]

        # ── Publication timestamp ───────────────────────────────────────
        # "il y a X heures/minutes/jours" appears in the first few lines
        time_raw = next(
            (l for l in lines if re.search(r'il y a\s+\d+\s+\w+', l, re.IGNORECASE)),
            ""
        )
        pub_dt = parse_relative_date(time_raw)
        pub_iso = pub_dt.isoformat(timespec="seconds")

        # ── Location: "Appartements dans <City>, <Quartier>" ───────────
        loc_line = next(
            (l for l in lines if l.startswith("Appartements dans")), None
        )
        city     = city_from_location_text(loc_line) or city_from_url(href)
        quartier = quartier_from_location_text(loc_line)

        # ── Title: line right after the location line ──────────────────
        title = ""
        if loc_line and loc_line in lines:
            idx = lines.index(loc_line)
            if idx + 1 < len(lines):
                title = lines[idx + 1]

        # ── Price ──────────────────────────────────────────────────────
        price_line = next(
            (l for l in lines if "DH" in l or "Demander le prix" in l), ""
        )
        price = clean_price(price_line) if "DH" in price_line else None

        # ── Surface & rooms from full text ─────────────────────────────
        full_text = " ".join(lines)
        surface  = clean_surface(full_text)
        rooms    = clean_rooms(full_text)
        price_m2 = round(price / surface) if price and surface and surface > 0 else None

        # ── Thumbnail image ────────────────────────────────────────────
        img = a.find("img", src=re.compile(r'content\.avito\.ma/classifieds'))
        if not img:
            img = a.find("img", attrs={"data-src": re.compile(r'content\.avito\.ma')})
        img_src = ""
        if img:
            img_src = img.get("src") or img.get("data-src") or ""

        lat, lng = coords_for(quartier, city)

        listings.append({
            "title":       title,
            "city":        city,
            "quartier":    quartier,
            "lat":         lat,
            "lng":         lng,
            "price":       price,
            "surface":     surface,
            "rooms":       rooms,
            "price_m2":    price_m2,
            "location":    loc_line or "",
            "link":        href,
            "images":      [img_src] if img_src else [],
            "published_at": pub_iso,
            "time_text":   time_raw,
            "scraped_at":  datetime.now(timezone.utc).isoformat(timespec="seconds"),
        })

    print(f"    → {len(listings)} listings")
    return listings


# ── Main ───────────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(
        description="Avito.ma scraper — requests + BeautifulSoup"
    )
    parser.add_argument(
        "--pages", type=int, default=None,
        help="Max pages to scrape (default: all 30)"
    )
    parser.add_argument(
        "--date",
        choices=["today", "week", "month", "all"],
        default="all",
        help=(
            "Date filter applied after scraping:\n"
            "  today  = published today\n"
            "  week   = published this calendar week\n"
            "  month  = published this calendar month\n"
            "  all    = no filter (default)"
        )
    )
    args = parser.parse_args()

    pages = range(1, (args.pages or 30) + 1)

    print("=" * 60)
    print("  Avito.ma Scraper  |  requests + BeautifulSoup  v3")
    print(f"  URL   : {BASE_URL}?o=N&{PARAMS}")
    print(f"  Pages : o={pages.start} → o={pages.stop - 1}  ({len(pages)} pages)")
    print(f"  Filter: date={args.date}")
    print("=" * 60)

    all_listings = []

    for page_num in pages:
        batch = scrape_page(page_num)
        all_listings.extend(batch)
        print(f"  Running total: {len(all_listings)}")
        if page_num < pages.stop - 1:
            delay = random.uniform(1.5, 3.0)
            time.sleep(delay)

    # ── Date filtering ─────────────────────────────────────────────────
    cutoff = date_boundary(args.date)
    if cutoff:
        before = len(all_listings)
        all_listings = [
            l for l in all_listings
            if datetime.fromisoformat(l["published_at"]) >= cutoff
        ]
        print(f"\n  Date filter '{args.date}': {before} → {len(all_listings)} listings kept")

    # ── Assign IDs ─────────────────────────────────────────────────────
    for i, l in enumerate(all_listings, 1):
        l["id"] = i

    # ── Summary ────────────────────────────────────────────────────────
    city_counts = Counter(l["city"] for l in all_listings)
    print("\n  Listings by city:")
    for city, count in sorted(city_counts.items(), key=lambda x: -x[1]):
        print(f"    {(city or 'Unknown'):16s}: {count}")

    output = {
        "meta": {
            "source":      "avito.ma",
            "url":         f"{BASE_URL}?{PARAMS}",
            "cities":      ["Casablanca", "Agadir", "Marrakech", "Tanger", "Rabat", "Mohammedia"],
            "date_filter": args.date,
            "total":       len(all_listings),
            "scraped_at":  datetime.now(timezone.utc).isoformat(timespec="seconds"),
        },
        "listings": all_listings,
    }

    with open(OUTPUT, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)

    print(f"\n{'=' * 60}")
    print(f"  DONE — {len(all_listings)} listings saved → {OUTPUT}")
    print(f"  Copy data.json next to index.html and refresh the map.")
    print(f"{'=' * 60}")


if __name__ == "__main__":
    main()
