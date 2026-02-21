#!/usr/bin/env python3
"""
Avito.ma Multi-City Apartment Scraper  v4
==========================================
URL : https://www.avito.ma/fr/maroc/appartements-à_vendre
      ?o=N&cities=13,5,8,90,12,15&price=100000-&has_price=true
Pages: o=1 → o=30

Install:
    pip install requests beautifulsoup4

Run:
    python scraper_avito.py                  # all 30 pages
    python scraper_avito.py --pages 3        # quick test, 3 pages
    python scraper_avito.py --date today     # today's listings only
    python scraper_avito.py --date week      # this calendar week
    python scraper_avito.py --date month     # this calendar month
    python scraper_avito.py --date all       # no date filter (default)

Stats line format on Avito:
    "3 2 187 m²4"  →  rooms=3, baths=2, surface=187, photos=4
    "2 1 63 m²0"   →  rooms=2, baths=1, surface=63
    "3 4 6"        →  rooms=3, baths=4, no surface
"""

import json, re, time, random, argparse
from datetime import datetime, timedelta, timezone
from collections import Counter
import requests
from bs4 import BeautifulSoup

# ── Config ─────────────────────────────────────────────────────────────
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

# ── Date helpers ────────────────────────────────────────────────────────
def date_boundary(mode):
    now = datetime.now(timezone.utc)
    if mode == "today":
        return now.replace(hour=0, minute=0, second=0, microsecond=0)
    if mode == "week":
        d = now - timedelta(days=now.weekday())
        return d.replace(hour=0, minute=0, second=0, microsecond=0)
    if mode == "month":
        return now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    return None

_REL_RE = re.compile(r"il y a\s+(\d+)\s+(minute|heure|jour|semaine|mois)s?", re.I)

def parse_relative_date(text):
    now = datetime.now(timezone.utc)
    m = _REL_RE.search(text or "")
    if not m:
        return now
    n, unit = int(m.group(1)), m.group(2).lower()
    deltas = {"minute": timedelta(minutes=n), "heure": timedelta(hours=n),
              "jour": timedelta(days=n), "semaine": timedelta(weeks=n),
              "mois": timedelta(days=n * 30)}
    return now - deltas.get(unit, timedelta(0))

# ── City detection ──────────────────────────────────────────────────────
CITY_TEXT_MAP = {
    "casablanca": "Casablanca", "agadir": "Agadir",
    "marrakech":  "Marrakech",  "tanger": "Tanger",
    "rabat":      "Rabat",      "mohammedia": "Mohammedia",
    "salé": "Rabat", "sale": "Rabat",
}

def city_from_location(loc):
    """'Appartements dans Casablanca, Gauthier' → 'Casablanca'"""
    m = re.match(r"Appartements dans ([^,]+)", loc or "")
    if not m:
        return None
    raw = m.group(1).strip().lower()
    for key, val in CITY_TEXT_MAP.items():
        if key in raw:
            return val
    return m.group(1).strip().title()

def quartier_from_location(loc):
    """'Appartements dans Casablanca, Gauthier' → 'Gauthier'"""
    m = re.match(r"Appartements dans [^,]+,\s*(.+)", loc or "")
    return m.group(1).strip().title() if m else None

# ── Core stats-line parser ──────────────────────────────────────────────
# Avito renders property details as a single line:
#   "3 2 187 m²4"  →  [rooms] [baths] [surface]m²[photo_count]
#   "2 1 63 m²0"   →  [rooms] [baths] [surface]m²[photo_count]
#   "3 4 6"        →  [rooms] [baths] [photo_count]  (no surface)
#   "1 1 2"        →  [rooms] [baths] [photo_count]  (no surface)

STATS_WITH_SURF = re.compile(r'^(\d+)\s+(\d+)\s+(\d+)\s*m[²2]\d*\s*$', re.I)
STATS_NO_SURF   = re.compile(r'^(\d+)\s+(\d+)\s+\d+\s*$')

def parse_stats_line(line):
    """Return (rooms, surface) from the Avito stats line."""
    if not line:
        return None, None
    line = line.strip()
    m = STATS_WITH_SURF.match(line)
    if m:
        return int(m.group(1)), int(m.group(3))
    m = STATS_NO_SURF.match(line)
    if m:
        return int(m.group(1)), None
    return None, None

# ── Price parser ────────────────────────────────────────────────────────
# Price line: "4 000 000 DH22 233 DH / mois"  or  "700 000 DH3 890 DH / mois"
# We want only the FIRST number before "DH" (the sale price, not the monthly payment)

def parse_price(line):
    """Extract sale price (first DH value) from a price line."""
    if not line:
        return None
    m = re.match(r'\s*([\d][\d\s]*?)\s*DH', line)
    if not m:
        return None
    try:
        return int(re.sub(r'\s', '', m.group(1)))
    except ValueError:
        return None

# ── Bounding boxes ──────────────────────────────────────────────────────
QUARTIER_BOUNDS = {
    # Casablanca
    "Ain Diab":        (33.582, 33.596, -7.705, -7.665),
    "Anfa":            (33.583, 33.598, -7.668, -7.640),
    "Casa Anfa":       (33.568, 33.580, -7.665, -7.643),
    "Racine":          (33.583, 33.597, -7.651, -7.628),
    "Gauthier":        (33.582, 33.595, -7.636, -7.611),
    "Maarif":          (33.572, 33.593, -7.648, -7.622),
    "Californie":      (33.562, 33.581, -7.650, -7.622),
    "Triangle D'Or":   (33.585, 33.598, -7.638, -7.618),
    "Centre Ville":    (33.585, 33.603, -7.626, -7.598),
    "Bourgogne":       (33.573, 33.591, -7.626, -7.600),
    "Val Fleuri":      (33.570, 33.589, -7.640, -7.612),
    "Palmier":         (33.564, 33.582, -7.618, -7.590),
    "Belvedere":       (33.578, 33.596, -7.617, -7.589),
    "Belvédère":       (33.578, 33.596, -7.617, -7.589),
    "Derb Sultan":     (33.574, 33.593, -7.612, -7.583),
    "Cil":             (33.557, 33.575, -7.618, -7.589),
    "Sidi Belyout":    (33.591, 33.606, -7.628, -7.603),
    "Hay Mohammadi":   (33.585, 33.603, -7.602, -7.572),
    "Roches Noires":   (33.587, 33.607, -7.587, -7.556),
    "Ain Sebaa":       (33.603, 33.625, -7.590, -7.548),
    "Sidi Bernoussi":  (33.594, 33.612, -7.577, -7.545),
    "Oasis":           (33.548, 33.572, -7.650, -7.618),
    "Hay Hassani":     (33.536, 33.563, -7.682, -7.644),
    "Ain Chock":       (33.556, 33.573, -7.636, -7.606),
    "Oulfa":           (33.527, 33.553, -7.670, -7.633),
    "Sidi Maarouf":    (33.521, 33.551, -7.648, -7.613),
    "Hay Riad":        (33.548, 33.565, -7.642, -7.614),
    "Moulay Rachid":   (33.548, 33.566, -7.612, -7.582),
    "Ben M'Sick":      (33.561, 33.577, -7.616, -7.590),
    "Sbata":           (33.570, 33.585, -7.607, -7.582),
    "Derb Ghallef":    (33.573, 33.590, -7.619, -7.598),
    "Mers Sultan":     (33.581, 33.598, -7.619, -7.601),
    "Hay Chrifa":      (33.540, 33.558, -7.647, -7.623),
    "Ferme Bretone":   (33.562, 33.578, -7.634, -7.610),
    "Franceville":     (33.575, 33.592, -7.641, -7.618),
    "Les Princesses":  (33.581, 33.596, -7.635, -7.614),
    "Nassim":          (33.530, 33.548, -7.643, -7.618),
    "Maârif Extension":(33.564, 33.582, -7.655, -7.628),
    "Casablanca Finance City": (33.535, 33.552, -7.655, -7.630),
    "Quartier Des Hôpitaux":   (33.581, 33.597, -7.623, -7.600),
    # Agadir
    "Founty":          (30.388, 30.406, -9.625, -9.595),
    "Talborjt":        (30.414, 30.430, -9.600, -9.575),
    "Hay Almassira":   (30.395, 30.415, -9.575, -9.545),
    "Centre Agadir":   (30.418, 30.432, -9.592, -9.568),
    "Dakhla":          (30.402, 30.418, -9.605, -9.580),
    "Anza":            (30.440, 30.465, -9.610, -9.580),
    # Marrakech
    "Gueliz":          (31.630, 31.648, -8.022, -7.992),
    "Guéliz":          (31.630, 31.648, -8.022, -7.992),
    "Hivernage":       (31.614, 31.632, -8.010, -7.985),
    "Medina":          (31.618, 31.636, -7.998, -7.975),
    "Palmeraie":       (31.638, 31.665, -7.960, -7.925),
    "Majorelle":       (31.636, 31.650, -8.002, -7.978),
    "Targa":           (31.596, 31.616, -8.020, -7.995),
    "Massira":         (31.598, 31.618, -7.998, -7.970),
    "Azli":            (31.610, 31.628, -8.005, -7.978),
    "Victor Hugo":     (31.628, 31.645, -8.018, -7.993),
    "Es Saada":        (31.595, 31.614, -8.002, -7.975),
    # Tanger
    "Malabata":        (35.778, 35.796, -5.778, -5.745),
    "Centre Tanger":   (35.765, 35.782, -5.820, -5.790),
    "Marshan":         (35.778, 35.795, -5.825, -5.798),
    "Iberia":          (35.756, 35.775, -5.812, -5.785),
    "Iberie":          (35.756, 35.775, -5.812, -5.785),
    "Zemmouri":        (35.750, 35.768, -5.840, -5.810),
    # Rabat
    "Agdal":           (33.990, 34.010, -6.860, -6.830),
    "Hassan":          (34.010, 34.030, -6.850, -6.820),
    "Souissi":         (33.990, 34.015, -6.825, -6.795),
    "Les Orangers":    (34.005, 34.025, -6.870, -6.840),
    "Yacoub El Mansour":(33.975, 33.998, -6.875, -6.845),
    # Mohammedia
    "Centre Mohammedia":(33.688, 33.706, -7.402, -7.372),
    "Ain Harrouda":    (33.660, 33.682, -7.428, -7.398),
    "La Siesta":       (33.690, 33.708, -7.410, -7.380),
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

# ── Scrape one page ─────────────────────────────────────────────────────
LISTING_HREF_RE = re.compile(
    r'^https://www\.avito\.ma/fr/[^/]+/appartements/[^/]+\.htm$'
)

def scrape_page(page_num):
    url = f"{BASE_URL}?o={page_num}&{PARAMS}"
    print(f"  Page {page_num:2d}/30  →  {url}")

    for attempt in range(4):
        try:
            resp = SESSION.get(url, timeout=30)
            resp.raise_for_status()
            break
        except Exception as e:
            wait = (attempt + 1) * 4
            print(f"    Attempt {attempt+1} failed: {e}. Retry in {wait}s...")
            time.sleep(wait)
    else:
        print(f"    !! Page {page_num} skipped.")
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

        # ── Extract all text lines ───────────────────────────────────────
        lines = [l.strip() for l in a.get_text("\n", strip=True).split("\n") if l.strip()]

        # ── Timestamp ───────────────────────────────────────────────────
        time_raw = next(
            (l for l in lines if re.search(r'il y a\s+\d+', l, re.I)), ""
        )
        pub_dt  = parse_relative_date(time_raw)
        pub_iso = pub_dt.isoformat(timespec="seconds")

        # ── Location line → city + quartier ─────────────────────────────
        loc_line = next(
            (l for l in lines if l.startswith("Appartements dans")), None
        )
        city     = city_from_location(loc_line)
        quartier = quartier_from_location(loc_line)

        # ── Locate the stats line (always 2 lines after loc_line) ────────
        #    loc_line  → "Appartements dans Casablanca, Gauthier"
        #    +1        → "SUPERBE APPARTEMENT TRÈS HAUT STANDING GAUTHIER"  (title)
        #    +2        → "3 3 187 m²2"  (stats)
        #    +3        → "4 000 000 DH22 233 DH / mois"  (price)
        title      = ""
        stats_line = ""
        price_line = ""

        if loc_line and loc_line in lines:
            idx = lines.index(loc_line)
            if idx + 1 < len(lines):
                title = lines[idx + 1]
            if idx + 2 < len(lines):
                stats_line = lines[idx + 2]
            if idx + 3 < len(lines):
                price_line = lines[idx + 3]

        # Fallback: grab price from any line containing "DH"
        if not price_line or "DH" not in price_line:
            price_line = next((l for l in lines if "DH" in l), "")

        # ── Parse stats line → rooms + surface ──────────────────────────
        rooms, surface = parse_stats_line(stats_line)

        # Fallback: surface sometimes appears in the title ("63 m²")
        if surface is None:
            m_surf = re.search(r'(\d{2,3})\s*m[²2]', title, re.I)
            if m_surf:
                surface = int(m_surf.group(1))

        # ── Parse price ─────────────────────────────────────────────────
        price    = parse_price(price_line)
        price_m2 = round(price / surface) if price and surface and surface > 0 else None

        # ── Thumbnail image ──────────────────────────────────────────────
        img = a.find("img", src=re.compile(r'content\.avito\.ma/classifieds'))
        if not img:
            img = a.find("img", attrs={"data-src": re.compile(r'content\.avito\.ma')})
        img_src = (img.get("src") or img.get("data-src") or "") if img else ""

        lat, lng = coords_for(quartier, city)

        listings.append({
            "title":        title,
            "city":         city,
            "quartier":     quartier,
            "lat":          lat,
            "lng":          lng,
            "price":        price,
            "surface":      surface,
            "rooms":        rooms,
            "price_m2":     price_m2,
            "location":     loc_line or "",
            "link":         href,
            "images":       [img_src] if img_src else [],
            "published_at": pub_iso,
            "time_text":    time_raw,
            "scraped_at":   datetime.now(timezone.utc).isoformat(timespec="seconds"),
        })

    print(f"    → {len(listings)} listings extracted")
    return listings


# ── Main ────────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(description="Avito.ma scraper v4")
    parser.add_argument("--pages", type=int, default=None,
                        help="Number of pages (default: 30)")
    parser.add_argument("--date", choices=["today","week","month","all"],
                        default="all",
                        help="Date filter (default: all)")
    args = parser.parse_args()

    pages = range(1, (args.pages or 30) + 1)

    print("=" * 62)
    print("  Avito.ma Scraper v4  |  requests + BeautifulSoup")
    print(f"  URL    : {BASE_URL}?o=N&{PARAMS}")
    print(f"  Pages  : o={pages.start} → o={pages.stop-1}  ({len(pages)} pages)")
    print(f"  Filter : date={args.date}")
    print("=" * 62)

    all_listings = []

    for page_num in pages:
        batch = scrape_page(page_num)
        all_listings.extend(batch)
        print(f"  Running total: {len(all_listings)}")
        if page_num < pages.stop - 1:
            time.sleep(random.uniform(1.5, 3.0))

    # ── Date filter ─────────────────────────────────────────────────────
    cutoff = date_boundary(args.date)
    if cutoff:
        before = len(all_listings)
        all_listings = [
            l for l in all_listings
            if datetime.fromisoformat(l["published_at"]) >= cutoff
        ]
        print(f"\n  Date filter '{args.date}': {before} → {len(all_listings)} kept")

    for i, l in enumerate(all_listings, 1):
        l["id"] = i

    # ── Summary ─────────────────────────────────────────────────────────
    city_counts  = Counter(l["city"]    for l in all_listings)
    null_price   = sum(1 for l in all_listings if l["price"]   is None)
    null_surface = sum(1 for l in all_listings if l["surface"] is None)
    null_rooms   = sum(1 for l in all_listings if l["rooms"]   is None)

    print("\n  Listings by city:")
    for city, count in sorted(city_counts.items(), key=lambda x: -x[1]):
        print(f"    {(city or 'Unknown'):20s}: {count}")
    print(f"\n  Null values  →  price: {null_price} | surface: {null_surface} | rooms: {null_rooms}")

    output = {
        "meta": {
            "source":      "avito.ma",
            "url":         f"{BASE_URL}?{PARAMS}",
            "cities":      ["Casablanca","Agadir","Marrakech","Tanger","Rabat","Mohammedia"],
            "date_filter": args.date,
            "total":       len(all_listings),
            "scraped_at":  datetime.now(timezone.utc).isoformat(timespec="seconds"),
        },
        "listings": all_listings,
    }

    with open(OUTPUT, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)

    print(f"\n{'='*62}")
    print(f"  DONE — {len(all_listings)} listings → {OUTPUT}")
    print(f"  Copy data.json next to index.html and refresh the map.")
    print(f"{'='*62}")


if __name__ == "__main__":
    main()
