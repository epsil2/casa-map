#!/usr/bin/env python3
"""
Avito.ma Multi-City Apartment Scraper
Cities: Casablanca, Agadir, Marrakech, Tanger, Rabat, Mohammedia
Output: data.json  (place next to index.html)

Install:  pip install playwright && playwright install chromium
Run all:  python scraper_avito.py
One city: python scraper_avito.py --city Casablanca
Images:   python scraper_avito.py --images   (slower, fetches galleries)
Multi-URL:python scraper_avito.py --multi    (single combined URL)
"""

import json, re, time, random, argparse
from datetime import datetime
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout

OUTPUT   = "data.json"
PAGES    = range(1, 11)
HEADLESS = True

MULTI_CITY_URL = (
    "https://www.avito.ma/fr/maroc/appartements-%C3%A0_vendre"
    "?cities=15,8,12,5,90,13"
)

CITY_URLS = {
    "Casablanca": "https://www.avito.ma/fr/casablanca/appartements-%C3%A0_vendre",
    "Agadir":     "https://www.avito.ma/fr/agadir/appartements-%C3%A0_vendre",
    "Marrakech":  "https://www.avito.ma/fr/marrakech/appartements-%C3%A0_vendre",
    "Tanger":     "https://www.avito.ma/fr/tanger/appartements-%C3%A0_vendre",
    "Rabat":      "https://www.avito.ma/fr/rabat/appartements-%C3%A0_vendre",
    "Mohammedia": "https://www.avito.ma/fr/mohammedia/appartements-%C3%A0_vendre",
}

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
    "Founty":              (30.388, 30.406, -9.625, -9.595),
    "Talborjt":            (30.414, 30.430, -9.600, -9.575),
    "Hay Almassira":       (30.395, 30.415, -9.575, -9.545),
    "Centre Agadir":       (30.418, 30.432, -9.592, -9.568),
    "Dakhla":              (30.402, 30.418, -9.605, -9.580),
    "Anza":                (30.440, 30.465, -9.610, -9.580),
    # Marrakech
    "Gueliz":              (31.630, 31.648, -8.022, -7.992),
    "Hivernage":           (31.614, 31.632, -8.010, -7.985),
    "Medina":              (31.618, 31.636, -7.998, -7.975),
    "Palmeraie":           (31.638, 31.665, -7.960, -7.925),
    "Majorelle":           (31.636, 31.650, -8.002, -7.978),
    "Targa":               (31.596, 31.616, -8.020, -7.995),
    "Massira":             (31.598, 31.618, -7.998, -7.970),
    # Tanger
    "Malabata":            (35.778, 35.796, -5.778, -5.745),
    "Centre Tanger":       (35.765, 35.782, -5.820, -5.790),
    "Marshan":             (35.778, 35.795, -5.825, -5.798),
    "Iberia":              (35.756, 35.775, -5.812, -5.785),
    "Achakar":             (35.740, 35.760, -5.870, -5.840),
    # Rabat
    "Agdal":               (33.990, 34.010, -6.860, -6.830),
    "Hassan":              (34.010, 34.030, -6.850, -6.820),
    "Souissi":             (33.990, 34.015, -6.825, -6.795),
    "Les Orangers":        (34.005, 34.025, -6.870, -6.840),
    "Yacoub El Mansour":   (33.975, 33.998, -6.875, -6.845),
    # Mohammedia
    "Centre Mohammedia":   (33.688, 33.706, -7.402, -7.372),
    "Ain Harrouda":        (33.660, 33.682, -7.428, -7.398),
}

CITY_BOUNDS = {
    "Casablanca":  (33.520, 33.630, -7.710, -7.540),
    "Agadir":      (30.380, 30.470, -9.640, -9.540),
    "Marrakech":   (31.580, 31.680, -8.060, -7.920),
    "Tanger":      (35.720, 35.810, -5.870, -5.740),
    "Rabat":       (33.930, 34.060, -6.900, -6.790),
    "Mohammedia":  (33.660, 33.720, -7.430, -7.360),
}

QUARTIER_ALIASES = {
    "belvedere":"Belvedere","belvédère":"Belvedere",
    "triangle d'or":"Triangle d'Or","triangle dor":"Triangle d'Or",
    "sidi bernoussi":"Sidi Bernoussi","bernoussi":"Sidi Bernoussi",
    "ain diab":"Ain Diab","ain sebaa":"Ain Sebaa","aïn sebaâ":"Ain Sebaa",
    "hay hassani":"Hay Hassani","hay mohammadi":"Hay Mohammadi",
    "hay riad":"Hay Riad","sidi maarouf":"Sidi Maarouf",
    "moulay rachid":"Moulay Rachid","sidi belyout":"Sidi Belyout",
    "roches noires":"Roches Noires","val fleuri":"Val Fleuri",
    "centre ville":"Centre Ville","casa anfa":"Casa Anfa",
    "ain chock":"Ain Chock","ben m'sick":"Ben M'Sick","ben msick":"Ben M'Sick",
    "californie":"Californie","bourgogne":"Bourgogne","palmier":"Palmier",
    "gauthier":"Gauthier","maarif":"Maarif","racine":"Racine",
    "anfa":"Anfa","oasis":"Oasis","oulfa":"Oulfa","sbata":"Sbata","cil":"CIL",
    "talborjt":"Talborjt","founty":"Founty","hay almassira":"Hay Almassira",
    "anza":"Anza","dakhla":"Dakhla",
    "gueliz":"Gueliz","guéliz":"Gueliz","hivernage":"Hivernage",
    "médina":"Medina","medina":"Medina","palmeraie":"Palmeraie",
    "majorelle":"Majorelle","targa":"Targa","massira":"Massira",
    "malabata":"Malabata","marshan":"Marshan","iberia":"Iberia","achakar":"Achakar",
    "centre tanger":"Centre Tanger",
    "agdal":"Agdal","hassan":"Hassan","souissi":"Souissi",
    "les orangers":"Les Orangers","yacoub el mansour":"Yacoub El Mansour",
    "centre mohammedia":"Centre Mohammedia","ain harrouda":"Ain Harrouda",
}


def coords_for(quartier, city):
    box = QUARTIER_BOUNDS.get(quartier) or CITY_BOUNDS.get(city)
    if not box:
        return None, None
    lat_min, lat_max, lng_min, lng_max = box
    return round(random.uniform(lat_min, lat_max), 6), round(random.uniform(lng_min, lng_max), 6)

def guess_quartier(text):
    lower = text.lower()
    for alias in sorted(QUARTIER_ALIASES, key=len, reverse=True):
        if alias in lower:
            return QUARTIER_ALIASES[alias]
    return None

def guess_city_from_text(text):
    lower = text.lower()
    for city in ["casablanca","agadir","marrakech","tanger","rabat","mohammedia"]:
        if city in lower:
            return city.capitalize()
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

def scrape_listing_images(img_page, url):
    images = []
    if not url: return images
    try:
        img_page.goto(url, wait_until="domcontentloaded", timeout=20_000)
        img_page.wait_for_timeout(1500)
        for sel in [
            "[class*='slick-slide'] img","[class*='gallery'] img",
            "[class*='Gallery'] img","[class*='photo'] img","img[class*='sc-']",
        ]:
            imgs = img_page.query_selector_all(sel)
            srcs = []
            for img in imgs:
                src = img.get_attribute("src") or img.get_attribute("data-src") or ""
                if src and "avito" in src and src not in srcs and "placeholder" not in src:
                    srcs.append(src)
            if srcs:
                return srcs[:8]
    except Exception as e:
        print(f"      ! Image error: {e}")
    return images

def scrape_page(page, base_url, city, page_num, fetch_images=False, img_page=None):
    url = f"{base_url}?o={page_num}"
    print(f"  [{city or 'ALL'}] page {page_num} -> {url[:60]}...")
    for attempt in range(3):
        try:
            page.goto(url, wait_until="networkidle", timeout=35_000)
            break
        except PWTimeout:
            if attempt == 0:
                try:
                    page.goto(url, wait_until="domcontentloaded", timeout=20_000)
                    page.wait_for_timeout(4000); break
                except PWTimeout: pass
            elif attempt == 2:
                print(f"    ! Page {page_num} failed.")
                return []
            time.sleep(3)

    cards = []
    for sel in ["[data-listing-id]","article[data-listing-id]","li[data-listing-id]",
                "[class*='ListingCell']","[class*='listing-cell']","article"]:
        try:
            page.wait_for_selector(sel, timeout=6000)
            found = page.query_selector_all(sel)
            found = [c for c in found if c.get_attribute("data-listing-id") or sel != "article"]
            if found:
                print(f"    -> {len(found)} cards via '{sel}'")
                cards = found; break
        except PWTimeout:
            continue
    if not cards:
        print(f"    ! No cards found.")
        return []

    listings = []
    for card in cards:
        try:
            all_text = card.inner_text()
            title = ""
            for sel in ["h2","h3","[class*='title' i]","[class*='Title']"]:
                el = card.query_selector(sel)
                if el: title = el.inner_text().strip(); break
            if not title: title = (card.get_attribute("title") or "").strip()

            price_text = ""
            for sel in ["[class*='price' i]","[class*='Price']","p[class*='sc-']","span[class*='sc-']"]:
                el = card.query_selector(sel)
                if el:
                    t = el.inner_text().strip()
                    if "DH" in t or re.search(r"\d{3}", t):
                        price_text = t; break
            if not price_text:
                m = re.search(r"[\d\s]{4,}\s*DH", all_text)
                price_text = m.group(0) if m else ""

            price = clean_price(price_text)
            link_el = card.query_selector("a[href]")
            href = link_el.get_attribute("href") if link_el else ""
            link = ("https://www.avito.ma" + href) if href and href.startswith("/") else href

            location_text = ""
            for sel in ["[class*='location' i]","[class*='Location']","[class*='address' i]"]:
                el = card.query_selector(sel)
                if el: location_text = el.inner_text().strip(); break

            # Thumbnail from card
            thumb = ""
            for sel in ["img[src*='avito']","img[data-src*='avito']","img"]:
                img_el = card.query_selector(sel)
                if img_el:
                    thumb = img_el.get_attribute("src") or img_el.get_attribute("data-src") or ""
                    if thumb and "placeholder" not in thumb: break
                    thumb = ""

            surface  = clean_surface(all_text)
            rooms    = clean_rooms(all_text)
            quartier = guess_quartier(title + " " + location_text + " " + all_text)
            detected_city = city or guess_city_from_text(title + " " + location_text + " " + all_text)
            lat, lng = coords_for(quartier, detected_city)
            price_m2 = round(price / surface) if price and surface and surface > 0 else None

            images = []
            if fetch_images and img_page and link:
                images = scrape_listing_images(img_page, link)
                time.sleep(random.uniform(0.5, 1.2))
            elif thumb:
                images = [thumb]

            if not title and not price: continue
            listings.append({
                "title": title, "city": detected_city, "quartier": quartier,
                "lat": lat, "lng": lng, "price": price, "surface": surface,
                "rooms": rooms, "price_m2": price_m2, "location": location_text,
                "link": link, "images": images, "page": page_num,
                "scraped_at": datetime.now().isoformat(timespec="seconds"),
            })
        except Exception as exc:
            print(f"    ! Card error: {exc}")
    return listings


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--city", help="Single city to scrape", default=None)
    parser.add_argument("--images", action="store_true", help="Fetch full image galleries (slower)")
    parser.add_argument("--multi", action="store_true", help="Use combined multi-city URL")
    args = parser.parse_args()

    print("=" * 50)
    print("  Avito.ma Multi-City Scraper")
    print("=" * 50)

    if args.city:
        if args.city not in CITY_URLS:
            print(f"Unknown city. Choose from: {', '.join(CITY_URLS)}")
            return
        cities_to_scrape = {args.city: CITY_URLS[args.city]}
    elif args.multi:
        cities_to_scrape = {"ALL": MULTI_CITY_URL}
    else:
        cities_to_scrape = CITY_URLS

    all_listings = []

    with sync_playwright() as pw:
        browser = pw.chromium.launch(
            headless=HEADLESS,
            args=["--no-sandbox","--disable-blink-features=AutomationControlled"],
        )
        context = browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
            viewport={"width":1280,"height":900},
            locale="fr-MA",
            extra_http_headers={"Accept-Language":"fr-FR,fr;q=0.9"},
        )
        if not args.images:
            context.route("**/*", lambda route: (
                route.abort() if route.request.resource_type in ("image","font","media","stylesheet")
                else route.continue_()
            ))

        page = context.new_page()
        img_page = context.new_page() if args.images else None

        try:
            page.goto("https://www.avito.ma", wait_until="domcontentloaded", timeout=20_000)
            page.wait_for_timeout(1500)
            for label in ["Accepter","Accept","J'accepte","OK","Fermer"]:
                btn = page.query_selector(f"button:has-text('{label}')")
                if btn: btn.click(); print(f"  Cookie dismissed ({label})"); break
        except Exception:
            pass

        for city, base_url in cities_to_scrape.items():
            print(f"\n{'─'*40}  {city}  {'─'*40}")
            city_total = []
            for page_num in PAGES:
                batch = scrape_page(page, base_url, None if city=="ALL" else city,
                                    page_num, args.images, img_page)
                city_total.extend(batch)
                all_listings.extend(batch)
                print(f"  Page {page_num}: +{len(batch)} | City: {len(city_total)} | Total: {len(all_listings)}")
                time.sleep(random.uniform(2.0, 4.0))
            print(f"  {city}: {len(city_total)} listings collected")

        browser.close()

    for i, l in enumerate(all_listings, 1):
        l["id"] = i

    output = {
        "meta": {
            "source": "avito.ma",
            "cities": list(cities_to_scrape.keys()),
            "total": len(all_listings),
            "scraped_at": datetime.now().isoformat(timespec="seconds"),
        },
        "listings": all_listings,
    }
    with open(OUTPUT, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)

    print(f"\n{'='*50}")
    print(f"  DONE — {len(all_listings)} listings -> {OUTPUT}")
    print(f"  Copy data.json next to index.html and refresh the map.")
    print(f"{'='*50}")

if __name__ == "__main__":
    main()
