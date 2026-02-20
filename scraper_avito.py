#!/usr/bin/env python3
"""
╔══════════════════════════════════════════════════════════════════╗
║  Avito.ma Casablanca Apartment Scraper                           ║
║  Output: data.json  (place next to casablanca_map.html)          ║
╚══════════════════════════════════════════════════════════════════╝

Install once:
    pip install playwright
    playwright install chromium

Run:
    python scraper_avito.py
"""

import json, re, time, random
from datetime import datetime
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout

BASE_URL = "https://www.avito.ma/fr/casablanca/appartements-%C3%A0_vendre"
PAGES    = range(1, 11)   # pages 1-10, ~300 listings
OUTPUT   = "data.json"    # must match what casablanca_map.html fetches
HEADLESS = True           # set False to watch the browser

# ── Precise bounding boxes: [lat_min, lat_max, lng_min, lng_max] ──────────────
# All boxes verified on-land within each quartier boundary.
# Dots are placed randomly inside these boxes — never in the ocean.
QUARTIER_BOUNDS = {
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
}

# Avito uses various spellings — map all to canonical names above
QUARTIER_ALIASES = {
    "belvedere":      "Belvedere",
    "belvédère":      "Belvedere",
    "triangle d'or":  "Triangle d'Or",
    "triangle dor":   "Triangle d'Or",
    "sidi bernoussi": "Sidi Bernoussi",
    "bernoussi":      "Sidi Bernoussi",
    "ain diab":       "Ain Diab",
    "ain sebaa":      "Ain Sebaa",
    "aïn sebaâ":      "Ain Sebaa",
    "hay hassani":    "Hay Hassani",
    "hay mohammadi":  "Hay Mohammadi",
    "hay riad":       "Hay Riad",
    "sidi maarouf":   "Sidi Maarouf",
    "moulay rachid":  "Moulay Rachid",
    "sidi belyout":   "Sidi Belyout",
    "roches noires":  "Roches Noires",
    "val fleuri":     "Val Fleuri",
    "centre ville":   "Centre Ville",
    "casa anfa":      "Casa Anfa",
    "ain chock":      "Ain Chock",
    "ben m'sick":     "Ben M'Sick",
    "ben msick":      "Ben M'Sick",
    "californie":     "Californie",
    "bourgogne":      "Bourgogne",
    "palmier":        "Palmier",
    "gauthier":       "Gauthier",
    "maarif":         "Maarif",
    "racine":         "Racine",
    "anfa":           "Anfa",
    "oasis":          "Oasis",
    "oulfa":          "Oulfa",
    "sbata":          "Sbata",
    "cil":            "CIL",
}

def coords_for(quartier_name):
    """Return a random (lat, lng) strictly inside the quartier bounding box."""
    b = QUARTIER_BOUNDS.get(quartier_name)
    if not b:
        return None, None
    lat_min, lat_max, lng_min, lng_max = b
    return (
        round(random.uniform(lat_min, lat_max), 6),
        round(random.uniform(lng_min, lng_max), 6),
    )

def guess_quartier(text):
    """Match quartier from listing text; longest alias first to avoid false matches."""
    lower = text.lower()
    for alias in sorted(QUARTIER_ALIASES, key=len, reverse=True):
        if alias in lower:
            return QUARTIER_ALIASES[alias]
    return None

def clean_price(text):
    if not text: return None
    nums = re.sub(r"[^\d]", "", text)
    return int(nums) if nums else None

def clean_surface(text):
    if not text: return None
    m = re.search(r"(\d+)\s*m", text, re.IGNORECASE)
    return int(m.group(1)) if m else None

def clean_rooms(text):
    if not text: return None
    m = re.search(r"(\d+)\s*(pi[eè]ces?|ch(?:ambres?)?\.?)", text, re.IGNORECASE)
    return int(m.group(1)) if m else None


def scrape_page(page, page_num):
    url = f"{BASE_URL}?o={page_num}"
    print(f"  GET {url}")

    for attempt in range(3):
        try:
            page.goto(url, wait_until="networkidle", timeout=35_000)
            break
        except PWTimeout:
            if attempt == 0:
                print("    networkidle timeout, retrying with domcontentloaded...")
                try:
                    page.goto(url, wait_until="domcontentloaded", timeout=20_000)
                    page.wait_for_timeout(4000)
                    break
                except PWTimeout:
                    pass
            elif attempt == 2:
                print("    X Page failed, skipping.")
                return []
            time.sleep(3)

    CARD_SELECTORS = [
        "[data-listing-id]",
        "article[data-listing-id]",
        "li[data-listing-id]",
        "[class*='ListingCell']",
        "[class*='listing-cell']",
        "article",
    ]

    cards = []
    for sel in CARD_SELECTORS:
        try:
            page.wait_for_selector(sel, timeout=6000)
            found = page.query_selector_all(sel)
            found = [c for c in found if c.get_attribute("data-listing-id") or sel != "article"]
            if found:
                print(f"    OK '{sel}' -> {len(found)} cards")
                cards = found
                break
        except PWTimeout:
            continue

    if not cards:
        print(f"    X No cards found on page {page_num}.")
        return []

    listings = []
    for card in cards:
        try:
            all_text = card.inner_text()

            title = ""
            for sel in ["h2", "h3", "[class*='title' i]", "[class*='Title']"]:
                el = card.query_selector(sel)
                if el:
                    title = el.inner_text().strip()
                    break
            if not title:
                title = (card.get_attribute("title") or "").strip()

            price_text = ""
            for sel in ["[class*='price' i]", "[class*='Price']", "p[class*='sc-']", "span[class*='sc-']"]:
                el = card.query_selector(sel)
                if el:
                    t = el.inner_text().strip()
                    if "DH" in t or re.search(r"\d{3}", t):
                        price_text = t
                        break
            if not price_text:
                m = re.search(r"[\d\s]{4,}\s*DH", all_text)
                price_text = m.group(0) if m else ""

            price = clean_price(price_text)

            link_el = card.query_selector("a[href]")
            href = link_el.get_attribute("href") if link_el else ""
            link = ("https://www.avito.ma" + href) if href and href.startswith("/") else href

            location_text = ""
            for sel in ["[class*='location' i]", "[class*='Location']", "[class*='address' i]"]:
                el = card.query_selector(sel)
                if el:
                    location_text = el.inner_text().strip()
                    break

            surface  = clean_surface(all_text)
            rooms    = clean_rooms(all_text)
            quartier = guess_quartier(title + " " + location_text + " " + all_text)
            lat, lng = coords_for(quartier)
            price_m2 = round(price / surface) if price and surface and surface > 0 else None

            if not title and not price:
                continue

            listings.append({
                "title":      title,
                "quartier":   quartier,
                "lat":        lat,
                "lng":        lng,
                "price":      price,
                "surface":    surface,
                "rooms":      rooms,
                "price_m2":   price_m2,
                "location":   location_text,
                "link":       link,
                "page":       page_num,
                "scraped_at": datetime.now().isoformat(timespec="seconds"),
            })
        except Exception as exc:
            print(f"    ! Card error: {exc}")
            continue

    return listings


def main():
    print("========================================")
    print("  Avito.ma Casablanca Scraper")
    print("========================================\n")

    all_listings = []

    with sync_playwright() as pw:
        browser = pw.chromium.launch(
            headless=HEADLESS,
            args=["--no-sandbox", "--disable-blink-features=AutomationControlled"],
        )
        context = browser.new_context(
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/121.0.0.0 Safari/537.36"
            ),
            viewport={"width": 1280, "height": 900},
            locale="fr-MA",
            extra_http_headers={"Accept-Language": "fr-FR,fr;q=0.9"},
        )

        # Block images/fonts/media for speed
        context.route("**/*", lambda route: (
            route.abort() if route.request.resource_type in ("image", "font", "media", "stylesheet")
            else route.continue_()
        ))

        page = context.new_page()

        # Accept cookie banner on homepage
        try:
            page.goto("https://www.avito.ma", wait_until="domcontentloaded", timeout=20_000)
            page.wait_for_timeout(1500)
            for label in ["Accepter", "Accept", "J'accepte", "OK", "Fermer"]:
                btn = page.query_selector(f"button:has-text('{label}')")
                if btn:
                    btn.click()
                    print(f"  Cookie banner dismissed ({label})")
                    break
        except Exception:
            pass

        for page_num in PAGES:
            print(f"\n-- Page {page_num}/{max(PAGES)} --")
            page_listings = scrape_page(page, page_num)
            all_listings.extend(page_listings)
            print(f"  Extracted: {len(page_listings)}  |  Total: {len(all_listings)}")
            time.sleep(random.uniform(2.0, 4.0))

        browser.close()

    for i, l in enumerate(all_listings, 1):
        l["id"] = i

    output = {
        "meta": {
            "source":     "avito.ma",
            "city":       "Casablanca",
            "total":      len(all_listings),
            "scraped_at": datetime.now().isoformat(timespec="seconds"),
        },
        "listings": all_listings,
    }

    with open(OUTPUT, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)

    print(f"\n  {len(all_listings)} listings saved -> {OUTPUT}")
    print("  Place data.json next to casablanca_map.html and open the map.")

if __name__ == "__main__":
    main()
