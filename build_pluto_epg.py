import os
import re
import json
import time
import requests
import xml.etree.ElementTree as ET
from difflib import SequenceMatcher
from urllib.parse import urlparse

# =========================
# CONFIG (repo-relative)
# =========================
REPO_DIR = os.path.dirname(os.path.abspath(__file__))

ALLOWED_JSON = os.path.join(REPO_DIR, "allowed_channels.json")

OUTPUT_DIR = os.path.join(REPO_DIR, "output")
OUTPUT_M3U = os.path.join(OUTPUT_DIR, "my_playlist.m3u")
OUTPUT_EPG = os.path.join(OUTPUT_DIR, "my_epg.xml")
PROGRAMME_BACKDROPS_JSON = os.path.join(OUTPUT_DIR, "programme_backdrops.json")

POSTERS_DIR = os.path.join(REPO_DIR, "posters")
POSTERS_HTML = os.path.join(POSTERS_DIR, "index.html")
POSTERS_MANIFEST = os.path.join(POSTERS_DIR, "manifest.json")

TMDB_CACHE = os.path.join(REPO_DIR, "tmdb_cache.json")  # kept in repo to reduce API calls

M3U_URL = "https://pluto.freechannels.me/playlist.m3u"
EPG_URL = "https://pluto.freechannels.me/epg.xml"

FUZZY_THRESHOLD = 0.80
TMDB_MIN_MATCH_SCORE = 0.82
TMDB_SLEEP_SECONDS = 0.2

TMDB_API_KEY = os.getenv("TMDB_API_KEY", "").strip()
TMDB_BASE = "https://api.themoviedb.org/3"


# =========================
# Helpers
# =========================
def ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)

def write_text(path: str, text: str) -> None:
    with open(path, "w", encoding="utf-8") as f:
        f.write(text)

def normalize(name: str) -> str:
    if not name:
        return ""
    return (
        name.lower()
        .replace("&amp;", "&")
        .replace("’", "'")
        .replace("&", "and")
        .strip()
    )

def safe_filename(name: str) -> str:
    name = name.strip()
    name = re.sub(r"[^\w\s\-.()]+", "_", name, flags=re.UNICODE)
    name = re.sub(r"\s+", " ", name).strip()
    return name[:120] if len(name) > 120 else name

def ext_from_url(url: str) -> str:
    try:
        path = urlparse(url).path
        _, ext = os.path.splitext(path)
        ext = ext.lower()
        if ext in [".png", ".jpg", ".jpeg", ".webp", ".gif", ".svg"]:
            return ext
    except Exception:
        pass
    return ".jpg"

def download_file(url: str, out_path: str, timeout: int = 30) -> bool:
    try:
        r = requests.get(url, timeout=timeout)
        r.raise_for_status()
        with open(out_path, "wb") as f:
            f.write(r.content)
        return True
    except Exception:
        return False

def indent(elem: ET.Element, level: int = 0) -> None:
    pad = "\n" + level * "  "
    if list(elem):
        if not elem.text or not elem.text.strip():
            elem.text = pad + "  "
        for child in elem:
            indent(child, level + 1)
        if not elem.tail or not elem.tail.strip():
            elem.tail = pad
    else:
        if level and (not elem.tail or not elem.tail.strip()):
            elem.tail = pad

def is_match(name: str, normalized_allowed: list[str], threshold: float = FUZZY_THRESHOLD) -> bool:
    n = normalize(name)

    for allowed in normalized_allowed:

        # partial match first (strong)
        if allowed in n or n in allowed:
            return True

        # fallback fuzzy match
        if SequenceMatcher(None, n, allowed).ratio() >= 0.65:
            return True

    return False

def best_fuzzy_match(name: str, candidates_norm_to_value: dict[str, tuple[str, str]], threshold: float):
    name_n = normalize(name)
    best_id, best_icon, best_score = "", "", 0.0
    for dnorm, (cid, icon) in candidates_norm_to_value.items():
        score = SequenceMatcher(None, name_n, dnorm).ratio()
        if score > best_score:
            best_score = score
            best_id = cid
            best_icon = icon
    if best_score >= threshold:
        return best_id, best_icon, best_score
    return "", "", best_score

def parse_attr(extinf: str, attr: str) -> str:
    key = f'{attr}="'
    i = extinf.find(key)
    if i < 0:
        return ""
    j = extinf.find('"', i + len(key))
    if j < 0:
        return ""
    return extinf[i + len(key): j].strip()

def set_attr(extinf: str, attr: str, value: str) -> str:
    if not value:
        return extinf
    pattern = rf'{re.escape(attr)}="[^"]*"'
    if re.search(pattern, extinf):
        return re.sub(pattern, f'{attr}="{value}"', extinf)
    if "," in extinf:
        left, right = extinf.split(",", 1)
        return f'{left} {attr}="{value}",{right}'
    return f'{extinf} {attr}="{value}"'

def load_allowed_channels(path: str) -> list[str]:
    if not os.path.exists(path):
        return []
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    return [c.strip() for c in data.get("allowed_channels", []) if isinstance(c, str) and c.strip()]


# =========================
# TMDB enrichment
# =========================
def tmdb_get_json(url, params, timeout=20):
    r = requests.get(url, params=params, timeout=timeout)
    r.raise_for_status()
    return r.json()

def tmdb_image_base():
    cfg = tmdb_get_json(f"{TMDB_BASE}/configuration", {"api_key": TMDB_API_KEY})
    images = cfg.get("images", {})
    secure = images.get("secure_base_url", "https://image.tmdb.org/t/p/")
    poster_sizes = images.get("poster_sizes", ["w342", "w500"])
    backdrop_sizes = images.get("backdrop_sizes", ["w780", "w1280"])
    return secure, poster_sizes, backdrop_sizes

def tmdb_multi_search(query):
    params = {"api_key": TMDB_API_KEY, "query": query, "include_adult": "false"}
    return tmdb_get_json(f"{TMDB_BASE}/search/multi", params).get("results", [])

def best_tmdb_match(title: str):
    results = tmdb_multi_search(title)
    tnorm = normalize(title)
    best = None
    best_score = 0.0
    for r in results[:12]:
        name = (r.get("title") or r.get("name") or "").strip()
        if not name:
            continue
        score = SequenceMatcher(None, normalize(name), tnorm).ratio()
        if score > best_score:
            best_score = score
            best = r
    if best and best_score >= TMDB_MIN_MATCH_SCORE:
        return best
    return None

def enrich_programmes_with_tmdb(tv_out: ET.Element):
    if not TMDB_API_KEY:
        print("⚠️ TMDB_API_KEY not set; skipping programme artwork enrichment.")
        return

    secure_base, poster_sizes, backdrop_sizes = tmdb_image_base()
    poster_size = "w342" if "w342" in poster_sizes else poster_sizes[-1]
    backdrop_size = "w780" if "w780" in backdrop_sizes else backdrop_sizes[-1]

    if os.path.exists(TMDB_CACHE):
        with open(TMDB_CACHE, "r", encoding="utf-8") as f:
            cache = json.load(f)
    else:
        cache = {}

    backdrops_sidecar = {}
    programmes = tv_out.findall(".//programme")
    print(f"🎨 Enriching {len(programmes)} programmes with TMDB posters/backdrops...")

    new_lookups = 0

    for i, prog in enumerate(programmes, 1):
        title_el = prog.find("title")
        title = (title_el.text or "").strip() if title_el is not None else ""
        if not title:
            continue

        key = normalize(title)

        if key in cache:
            poster_url = cache[key].get("poster_url", "")
            backdrop_url = cache[key].get("backdrop_url", "")
        else:
            match = best_tmdb_match(title)
            poster_url, backdrop_url = "", ""
            if match:
                poster_path = match.get("poster_path") or ""
                backdrop_path = match.get("backdrop_path") or ""
                if poster_path:
                    poster_url = f"{secure_base}{poster_size}{poster_path}"
                if backdrop_path:
                    backdrop_url = f"{secure_base}{backdrop_size}{backdrop_path}"
            cache[key] = {"poster_url": poster_url, "backdrop_url": backdrop_url}
            new_lookups += 1
            time.sleep(TMDB_SLEEP_SECONDS)

        # add poster to XMLTV programme
        if poster_url:
            icon = prog.find("icon")
            if icon is None:
                icon = ET.SubElement(prog, "icon")
            icon.set("src", poster_url)

        # store backdrop sidecar for hero UI
        if backdrop_url:
            ch = (prog.attrib.get("channel") or "").strip()
            start = (prog.attrib.get("start") or "").strip()
            backdrops_sidecar[f"{ch}|{start}|{title}"] = backdrop_url

        if i % 250 == 0:
            print(f"  ... {i}/{len(programmes)}")

    with open(TMDB_CACHE, "w", encoding="utf-8") as f:
        json.dump(cache, f, indent=2, ensure_ascii=False)

    with open(PROGRAMME_BACKDROPS_JSON, "w", encoding="utf-8") as f:
        json.dump(backdrops_sidecar, f, indent=2, ensure_ascii=False)

    print(f"✅ TMDB enrichment done. New lookups this run: {new_lookups}")


# =========================
# Main
# =========================
def main():
    ensure_dir(OUTPUT_DIR)
    ensure_dir(POSTERS_DIR)

    allowed = load_allowed_channels(ALLOWED_JSON)
    normalized_allowed = [normalize(x) for x in allowed]
    print(f"✅ Loaded {len(allowed)} allowed channels")

    # 0) Download EPG (source of strict channel ids + channel icons)
    print(f"📥 Downloading XMLTV: {EPG_URL}")
    epg_resp = requests.get(EPG_URL, timeout=60)
    epg_resp.raise_for_status()
    epg_root = ET.fromstring(epg_resp.text)

    epg_display_map = {}
    for ch in epg_root.findall(".//channel"):
        cid = (ch.attrib.get("id") or "").strip()
        dname = (ch.findtext("display-name", "") or "").strip()
        icon_elem = ch.find("icon")
        icon_url = (icon_elem.attrib.get("src", "") if icon_elem is not None else "").strip()
        if dname:
            epg_display_map[normalize(dname)] = (cid, icon_url)

    # 1) Download M3U, filter allowed, inject tvg-id + tvg-logo, and cache channel logos
    print(f"📥 Downloading M3U: {M3U_URL}")
    m3u_resp = requests.get(M3U_URL, timeout=30)
    m3u_resp.raise_for_status()

    playlist_entries = ["#EXTM3U"]
    included_channel_ids = set()
    posters_manifest = []

    meta = None
    for line in m3u_resp.text.splitlines():
        line = line.strip()
        if not line:
            continue
        if line.startswith("#EXTINF"):
            meta = line
            continue
        if meta and (line.startswith("http://") or line.startswith("https://")):
            name = parse_attr(meta, "tvg-name")
            if not name and "," in meta:
                name = meta.split(",")[-1].strip()

            if name and is_match(name, normalized_allowed):
                best_cid, best_icon, best_score = best_fuzzy_match(name, epg_display_map, FUZZY_THRESHOLD)

                out_meta = meta

                logo_url = parse_attr(out_meta, "tvg-logo")
                if not logo_url and best_icon:
                    logo_url = best_icon
                    out_meta = set_attr(out_meta, "tvg-logo", logo_url)

                if best_cid:
                    included_channel_ids.add(best_cid)
                    out_meta = set_attr(out_meta, "tvg-id", best_cid)

                playlist_entries.extend([out_meta, line])

                local_file = ""
                if logo_url:
                    fname = safe_filename(name) + ext_from_url(logo_url)
                    out_path = os.path.join(POSTERS_DIR, fname)
                    if not os.path.exists(out_path):
                        if download_file(logo_url, out_path):
                            local_file = os.path.basename(out_path)
                    else:
                        local_file = os.path.basename(out_path)

                posters_manifest.append({
                    "name": name,
                    "channel_id": best_cid or "",
                    "logo_url": logo_url or "",
                    "local_file": local_file,
                    "epg_match_score": f"{best_score:.3f}"
                })

            meta = None

    write_text(OUTPUT_M3U, "\n".join(playlist_entries))
    print(f"✅ Wrote playlist: {OUTPUT_M3U}")
    print(f"   Matched channels: {(len(playlist_entries) - 1) // 2}")
    print(f"   Included XMLTV channel IDs: {len(included_channel_ids)}")

    # 2) Strict XMLTV output: channels + programmes only for included channel IDs
    tv_out = ET.Element("tv")

    for ch in epg_root.findall(".//channel"):
        cid = (ch.attrib.get("id") or "").strip()
        if cid and cid in included_channel_ids:
            tv_out.append(ch)

    for prog in epg_root.findall(".//programme"):
        cid = (prog.attrib.get("channel") or "").strip()
        if cid and cid in included_channel_ids:
            tv_out.append(prog)

    # 3) Enrich strict programmes with TMDB posters/backdrops
    enrich_programmes_with_tmdb(tv_out)

    # Write final EPG
    indent(tv_out)
    ET.ElementTree(tv_out).write(OUTPUT_EPG, encoding="utf-8", xml_declaration=True)
    print(f"✅ Wrote EPG: {OUTPUT_EPG}")

    # 4) posters manifest + HTML gallery
    with open(POSTERS_MANIFEST, "w", encoding="utf-8") as f:
        json.dump(posters_manifest, f, indent=2, ensure_ascii=False)

    cards = []
    for it in posters_manifest:
        img = it.get("local_file") or ""
        img_tag = f'<img src="{img}" alt="{it["name"]}"/>' if img else "<div class='noimg'>No image</div>"
        cards.append(
            f"""
            <div class="card">
              {img_tag}
              <div class="name">{it["name"]}</div>
              <div class="meta">tvg-id: {it.get("channel_id","")}</div>
              <div class="meta">match: {it.get("epg_match_score","")}</div>
            </div>
            """.strip()
        )

    html = f"""
    <!doctype html>
    <html>
    <head>
      <meta charset="utf-8"/>
      <title>Channel Logos Preview</title>
      <style>
        body{{font-family:Arial, sans-serif; margin:20px;}}
        .grid{{display:grid; grid-template-columns:repeat(auto-fill, minmax(180px, 1fr)); gap:14px;}}
        .card{{border:1px solid #ddd; border-radius:12px; padding:10px;}}
        img{{width:100%; height:220px; object-fit:contain; border-radius:10px; background:#f3f3f3;}}
        .noimg{{width:100%; height:220px; display:flex; align-items:center; justify-content:center; background:#f3f3f3; border-radius:10px; color:#666;}}
        .name{{margin-top:8px; font-weight:700; font-size:14px;}}
        .meta{{margin-top:4px; font-size:12px; color:#555;}}
      </style>
    </head>
    <body>
      <h2>Channel Logos Preview</h2>
      <div class="grid">
        {"".join(cards)}
      </div>
    </body>
    </html>
    """.strip()

    write_text(POSTERS_HTML, html)
    print(f"✅ Wrote posters gallery: {POSTERS_HTML}")

    if not TMDB_API_KEY:
        print("⚠️ TMDB enrichment NOT enabled (TMDB_API_KEY missing).")
        print("   Add GitHub secret TMDB_API_KEY to enable enrichment.")
    else:
        print("🎬 TMDB enrichment enabled.")


if __name__ == "__main__":
    main()
