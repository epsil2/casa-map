#!/usr/bin/env python3
"""
Avito.ma Multi-City Apartment Scraper
Uses requests + BeautifulSoup — no browser needed, much faster.

URL: https://www.avito.ma/fr/maroc/appartements-à_vendre?o=2&cities=15,8,12,5,90,13
Pages: o=2 through o=10

Install:
    pip install requests beautifulsoup4

Run:
    python scraper_avito.py              # all pages
    python scraper_avito.py --pages 3    # first 3 pages only (for testing)
"""

import json, re, time, random, argparse
from datetime import datetime
import requests
from bs4 import BeautifulSoup

# ── Config ─────────────────────────────────────────────────────────────
OUTPUT    = "data.json"
BASE_URL  = "https://www.avito.ma/fr/maroc/appartements-%C3%A0_vendre"
CITY_PARAM = "cities=15,8,12,5,90,13"
# Pages start at o=2 on Avito (o=1 = first page without param)
PAGE_RANGE = range(2, 11)   # o=2 … o=10

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/121.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "fr-FR,fr;q=0.9,ar;q=0.8",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
    "Referer": "https://www.avito.ma/",
    "DNT": "1",
}

SESSION = requests.Session()
SESSION.headers.update(HEADERS)

# ── City detection from URL path / text ────────────────────────────────
CITY_KEYWORDS = {
    "casablanca": "Casablanca",
    "anfa":       "Casablanca",
    "maarif":     "Casablanca",
    "ain_diab":   "Casablanca",
    "ain_chock":  "Casablanca",
    "hay_mohammadi": "Casablanca",
    "val_fleuri": "Casablanca",
    "gauthier":   "Casablanca",
    "racine":     "Casablanca",
    "californie": "Casablanca",
    "bourgogne":  "Casablanca",
    "hay_hassani":"Casablanca",
    "ain_sebaa":  "Casablanca",
    "sidi_bernoussi": "Casablanca",
    "roches_noires":  "Casablanca",
    "palmier":    "Casablanca",
    "oasis":      "Casablanca",
    "oulfa":      "Casablanca",
    "sidi_maarouf": "Casablanca",
    "moulay_rachid": "Casablanca",
    "derb_sultan": "Casablanca",
    "sbata":      "Casablanca",
    "c.i.l":      "Casablanca",
    "2_mars":     "Casablanca",
    "nassim":     "Casablanca",
    "les_princesses": "Casablanca",
    "maârif":     "Casablanca",
    "agadir":     "Agadir",
    "marrakech":  "Marrakech",
    "guéliz":     "Marrakech",
    "gueliz":     "Marrakech",
    "hivernage":  "Marrakech",
    "es_saada":   "Marrakech",
    "hay_izdihar":"Marrakech",
    "mabrouka":   "Marrakech",
    "rouidat":    "Marrakech",
    "allal_el_fassi": "Marrakech",
    "route_de_casablanca": "Marrakech",
    "route_de_tahanaoute": "Marrakech",
    "route_d_amezmiz": "Marrakech",
    "targa":      "Marrakech",
    "tanger":     "Tanger",
    "mesnana":    "Tanger",
    "manar":      "Tanger",
    "malabata":   "Tanger",
    "rabat":      "Rabat",
    "souissi":    "Rabat",
    "agdal":      "Rabat",
    "hay_riad":   "Rabat",
    "mohammedia": "Mohammedia",
    "la_siesta":  "Mohammedia",
}

# Display text in "Appartements dans <City>, <Quartier>"
CITY_TEXT_MAP = {
    "Casablanca":  "Casablanca",
    "Agadir":      "Agadir",
    "Marrakech":   "Marrakech",
    "Tanger":      "Tanger",
    "Rabat":       "Rabat",
    "Mohammedia":  "Mohammedia",
}

# ── Bounding boxes: [lat_min, lat_max, lng_min, lng_max] ───────────────
QUARTIER_BOUNDS = {
    # Casablanca
    "Ain Diab":       (33.582, 33.596, -7.705, -7.665),
    "Anfa":           (33.583, 33.598, -7.668, -7.640),
    "Casa Anfa":      (33.568, 33.580, -7.665, -7.643),
    "Racine":         (33.583, 33.597, -7.651, -7.628),
    "Gauthier":       (33.582, 33.595, -7.636, -7.611),
    "Maarif":         (33.572, 33.593, -7.648, -7.622),
    "Californie":     (33.562, 33.581, -7.650, -7.622),
    "Triangle d'Or":  (33.585, 33.598, -7.638, -7.618),
    "Centre Ville":   (33.585, 33.603, -7.626, -7.598),
    "Bourgogne":      (33.573, 33.591, -7.626, -7.600),
    "Val Fleuri":     (33.570, 33.589, -7.640, -7.612),
    "Palmier":        (33.564, 33.582, -7.618, -7.590),
    "Belvedere":      (33.578, 33.596, -7.617, -7.589),
    "Derb Sultan":    (33.574, 33.593, -7.612, -7.583),
    "CIL":            (33.557, 33.575, -7.618, -7.589),
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


# ── Parse helpers ──────────────────────────────────────────────────────
def clean_price(text):
    """Extract first numeric price from text like '5 500 000 DH' or '2 100 000 DH11 672 DH / mois'"""
    if not text:
        return None
    # Match the first big number before DH
    m = re.search(r'([\d][\d\s]*)\s*DH', text)
    if not m:
        return None
    nums = re.sub(r'\s', '', m.group(1))
    try:
        return int(nums)
    except ValueError:
        return None

def clean_surface(text):
    m = re.search(r'(\d+)\s*m[²2]', text, re.IGNORECASE)
    return int(m.group(1)) if m else None

def clean_rooms(text):
    # Avito shows: "3 4 260 m²" — first number = bedrooms, but also "3 pièces"
    m = re.search(r'(\d+)\s*pi[eè]ces?', text, re.IGNORECASE)
    if m:
        return int(m.group(1))
    # Try "X ch" pattern
    m = re.search(r'(\d+)\s*ch(?:ambres?)?', text, re.IGNORECASE)
    if m:
        return int(m.group(1))
    return None

def city_from_url(href):
    """Derive city from the avito URL path segment."""
    path = href.lower()
    for keyword, city in CITY_KEYWORDS.items():
        if f"/{keyword}/" in path:
            return city
    return None

def city_from_location_text(location_str):
    """Parse 'Appartements dans Casablanca, Anfa' or 'Appartements dans Marrakech, Guéliz'"""
    m = re.match(r"Appartements dans ([^,]+)", location_str or "")
    if not m:
        return None
    raw = m.group(1).strip()
    for key, val in CITY_TEXT_MAP.items():
        if key.lower() in raw.lower():
            return val
    return raw.title()

def quartier_from_location_text(location_str):
    """Parse 'Appartements dans Casablanca, Anfa' -> 'Anfa'"""
    m = re.match(r"Appartements dans [^,]+,\s*(.+)", location_str or "")
    return m.group(1).strip().title() if m else None

def quartier_from_url(href):
    """Extract quartier from /fr/<quartier>/appartements/..."""
    m = re.match(r'https://www\.avito\.ma/fr/([^/]+)/appartements/', href)
    if not m:
        return None
    raw = m.group(1).replace("_", " ").replace("-", " ")
    # Clean accented chars for matching
    return raw.title()


# ── Scrape one page ────────────────────────────────────────────────────
def scrape_page(page_num):
    url = f"{BASE_URL}?o={page_num}&{CITY_PARAM}"
    print(f"  Fetching page {page_num}: {url}")

    for attempt in range(4):
        try:
            resp = SESSION.get(url, timeout=30)
            resp.raise_for_status()
            break
        except Exception as e:
            wait = (attempt + 1) * 3
            print(f"    Attempt {attempt+1} failed ({e}). Retrying in {wait}s...")
            time.sleep(wait)
    else:
        print(f"    !! Page {page_num} failed after 4 attempts, skipping.")
        return []

    soup = BeautifulSoup(resp.text, "html.parser")

    # Every real listing is an <a> pointing to /fr/<quartier>/appartements/<title>_<id>.htm
    listing_anchors = soup.find_all(
        "a",
        href=re.compile(r'^https://www\.avito\.ma/fr/[^/]+/appartements/[^/]+\.htm$')
    )

    # Deduplicate by href (some pages have duplicate anchors for same card)
    seen_hrefs = set()
    listings = []

    for a in listing_anchors:
        href = a.get("href", "")
        # Skip immoneuf.avito.ma links
        if "immoneuf" in href:
            continue
        if href in seen_hrefs:
            continue
        seen_hrefs.add(href)

        text = a.get_text("\n", strip=True)
        lines = [l.strip() for l in text.split("\n") if l.strip()]

        # Extract image URL (content.avito.ma/classifieds)
        img_tag = a.find("img", src=re.compile(r'content\.avito\.ma/classifieds'))
        img_src = img_tag.get("src", "") if img_tag else ""
        # Also check data-src
        if not img_src:
            img_tag = a.find("img", attrs={"data-src": re.compile(r'content\.avito\.ma')})
            img_src = img_tag.get("data-src", "") if img_tag else ""

        # Find "Appartements dans <City>, <Quartier>" line
        location_line = next(
            (l for l in lines if l.startswith("Appartements dans")),
            None
        )
        city    = city_from_location_text(location_line) or city_from_url(href)
        quartier = quartier_from_location_text(location_line) or quartier_from_url(href)

        # Title: the line that is clearly the listing title
        # It's usually after the location line and before the stats
        title = ""
        if location_line:
            idx = lines.index(location_line)
            if idx + 1 < len(lines):
                title = lines[idx + 1]

        # Price: find "X DH" or "Demander le prix"
        price_raw = next(
            (l for l in lines if "DH" in l or "Demander le prix" in l),
            ""
        )
        price = clean_price(price_raw) if "DH" in price_raw else None

        # Surface and rooms from full text
        full_text = " ".join(lines)
        surface = clean_surface(full_text)
        rooms   = clean_rooms(full_text)
        price_m2 = round(price / surface) if price and surface and surface > 0 else None

        lat, lng = coords_for(quartier, city)

        listings.append({
            "title":      title,
            "city":       city,
            "quartier":   quartier,
            "lat":        lat,
            "lng":        lng,
            "price":      price,
            "surface":    surface,
            "rooms":      rooms,
            "price_m2":   price_m2,
            "location":   location_line or "",
            "link":       href,
            "images":     [img_src] if img_src else [],
            "page":       page_num,
            "scraped_at": datetime.now().isoformat(timespec="seconds"),
        })

    print(f"    -> {len(listings)} listings extracted")
    return listings


# ── Main ───────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(description="Avito.ma scraper — no browser needed")
    parser.add_argument("--pages", type=int, default=None,
                        help="Number of pages to scrape (default: all, o=2 to o=10)")
    args = parser.parse_args()

    if args.pages:
        pages = range(2, 2 + args.pages)
    else:
        pages = PAGE_RANGE

    print("=" * 55)
    print("  Avito.ma Scraper  |  requests + BeautifulSoup")
    print(f"  URL: {BASE_URL}?o=N&{CITY_PARAM}")
    print(f"  Pages: o={pages.start} → o={pages.stop - 1}")
    print("=" * 55)

    all_listings = []

    for page_num in pages:
        batch = scrape_page(page_num)
        all_listings.extend(batch)
        print(f"  Total so far: {len(all_listings)}")
        # Polite delay between pages
        if page_num < pages.stop - 1:
            delay = random.uniform(1.5, 3.5)
            print(f"  Waiting {delay:.1f}s...")
            time.sleep(delay)

    # Assign IDs
    for i, l in enumerate(all_listings, 1):
        l["id"] = i

    # Stats by city
    from collections import Counter
    city_counts = Counter(l["city"] for l in all_listings)
    print("\n  Listings by city:")
    for city, count in sorted(city_counts.items(), key=lambda x: -x[1]):
        print(f"    {city or 'Unknown':15s}: {count}")

    output = {
        "meta": {
            "source":     "avito.ma",
            "url":        f"{BASE_URL}?{CITY_PARAM}",
            "cities":     ["Casablanca", "Agadir", "Marrakech", "Tanger", "Rabat", "Mohammedia"],
            "total":      len(all_listings),
            "scraped_at": datetime.now().isoformat(timespec="seconds"),
        },
        "listings": all_listings,
    }

    with open(OUTPUT, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)

    print(f"\n{'='*55}")
    print(f"  DONE — {len(all_listings)} listings saved to {OUTPUT}")
    print(f"  Place data.json next to index.html and refresh the map.")
    print(f"{'='*55}")


if __name__ == "__main__":
    main()
