import asyncio
import json
import os
import time
from typing import Dict, List, Optional
from bs4 import BeautifulSoup
from urllib.parse import urljoin
from ddgs import DDGS
from openai import OpenAI
from thefuzz import fuzz

# --- LIBRARIES ---
from crawl4ai import AsyncWebCrawler, BrowserConfig, CrawlerRunConfig, CacheMode
from scraper_helper import StealthBrowser

# --- CONFIGURATION ---
INPUT_FILE = "doctor_names.txt"
OUTPUT_FILE = "doctors_database_v2.json"
BATCH_SIZE = 5
MATCH_THRESHOLD = 70
LLM_API_URL = "http://localhost:8080/v1"
GLOBAL_START_TIME = 0

# --- VISUALS ---
class StepTimer:
    def __init__(self, step_name):
        self.step_name = step_name
        self.start_time = 0
    def __enter__(self):
        self.start_time = time.time()
        elapsed = time.time() - GLOBAL_START_TIME
        m, s = divmod(int(elapsed), 60)
        print(f"\n┌── ⏱️  {m}m {s}s | {self.step_name} " + "─"*30)
        return self
    def __exit__(self, exc_type, exc_val, exc_tb):
        duration = time.time() - self.start_time
        if exc_type:
            print(f"│ ❌ ERROR: {exc_val}")
            print(f"└────────────────────────────────────────── [FAILED]")
        else:
            print(f"└────────────────────────────────────────── [COMPLETED in {duration:.2f}s]")
        return False

# --- ASSET EXTRACTOR ---
def extract_important_assets(html_content: str, base_url: str) -> Dict[str, List[str]]:
    if not html_content or not isinstance(html_content, str): return {"documents": [], "images": []}
    soup = BeautifulSoup(html_content, 'html.parser')
    assets = {"documents": [], "images": []}
    
    for link in soup.find_all('a', href=True):
        href = link['href'].lower()
        if href.endswith(('.pdf', '.doc', '.docx')):
            assets["documents"].append(urljoin(base_url, link['href']))
            
    for img in soup.find_all('img', src=True):
        alt = img.get('alt', '').lower()
        if any(k in alt for k in ['cert', 'award', 'license', 'board']):
             assets["images"].append(urljoin(base_url, img['src']))
        
    return {k: list(set(v)) for k, v in assets.items()}

# --- LLM PARSER ---
def parse_with_local_llm(html: str, query_name: str) -> dict:
    if not html or not isinstance(html, str): return {}
    
    soup = BeautifulSoup(html, 'html.parser')
    for x in soup(["script", "style", "nav", "footer", "svg"]): x.decompose()
    text = soup.get_text(separator=' ', strip=True)[:6500] 

    client = OpenAI(base_url=LLM_API_URL, api_key="sk-none")
    
    prompt = (
        f"Extract doctor profile for: '{query_name}'.\n"
        f"Return STRICT JSON with these fields:\n"
        f"- name (string, full name)\n"
        f"- npi_id (string, 10-digit US ID, else 'N/A')\n"
        f"- license_id (string)\n"
        f"- speciality (string)\n"
        f"- email (string, or 'N/A')\n"
        f"- phone_no (string, clinic phone or 'N/A')\n"
        f"- address (string, full clinic address)\n"
        f"- age (string, estimate if mentioned, else 'N/A')\n"
        f"- hospital_affiliation (string, main hospital)\n"
        f"- education (string, degree/university)\n"
        f"- years_experience (string)\n"
        f"- languages (list of strings)\n"
        f"- summary (string, max 50 words)\n\n"
        f"Source Text:\n{text}"
    )

    try:
        response = client.chat.completions.create(
            model="gpt-3.5-turbo",
            messages=[
                {"role": "system", "content": "You are a JSON extractor. Output ONLY JSON."},
                {"role": "user", "content": prompt}
            ],
            temperature=0.1
        )
        content = response.choices[0].message.content
        if "```json" in content: content = content.split("```json")[1].split("```")[0]
        elif "```" in content: content = content.split("```")[1].split("```")[0]
        return json.loads(content)
    except Exception as e:
        print(f"│ ⚠️  LLM Parse Error: {e}")
        return {}

# --- SMART FETCHER ---
async def smart_fetch(url: str, standard_crawler: AsyncWebCrawler, stealth_browser: StealthBrowser) -> Optional[str]:
    # TIER 1: Standard
    try:
        run_conf = CrawlerRunConfig(cache_mode=CacheMode.BYPASS, page_timeout=15000)
        result = await standard_crawler.arun(url=url, config=run_conf)
        
        html = result.html or ""
        # Basic junk check
        if result.success and len(html) > 500:
            print("│ ⚡ Method: Standard Crawler (Fast)")
            return html
    except Exception:
        pass

    # TIER 2: Nodriver (Stealth)
    print("│ 🛡️  Method: Nodriver (Stealth)")
    html = await stealth_browser.get_html(url)
    
    # !!! ESSENTIAL FIX: Return RAW HTML string, NOT a dictionary !!!
    if html:
        return html  
    
    return None

# --- PROCESSOR ---
async def process_doctor(line_str, index, total, standard_crawler, stealth_browser):
    print(f"\n{'='*70}")
    print(f"😷 PROCESSING [ {index}/{total} ] : {line_str}")
    print(f"{'='*70}")
    
    parts = [p.strip() for p in line_str.split(",")]
    name_query = parts[0]
    entries = []

    # 1. Search
    urls = []
    with StepTimer(f"Searching Web"):
        ddgs = DDGS()
        try:
            results = list(ddgs.text(f"{line_str} profile", max_results=3))
            urls = [r['href'] for r in results if "instagram" not in r['href']]
            print(f"│ 🔍 Found {len(urls)} links")
        except: print("│ ⚠️ Search failed.")

    # 2. Hybrid Scrape
    for i, url in enumerate(urls):
        with StepTimer(f"Scraping Link {i+1}"):
            print(f"│ 🔗 URL: {url}")
            html = await smart_fetch(url, standard_crawler, stealth_browser)
            
            if html:
                # A. Extract Assets
                assets = extract_important_assets(html, url)
                if assets['documents']: print(f"│ 📂 Found {len(assets['documents'])} Docs")
                
                # B. Extract Profile
                print("│ 🧠 Extracting with Local AI...")
                profile = parse_with_local_llm(html, name_query)
                
                if profile and profile.get('name'):
                    score = fuzz.token_set_ratio(name_query, profile['name'])
                    if score >= MATCH_THRESHOLD:
                        profile['source_url'] = url
                        profile['assets'] = assets
                        entries.append(profile)
                        print(f"│ ✅ MATCH ({score}%): {profile['name']}")
                    else:
                        print(f"│ ⚠️  Mismatch ({score}%): Got '{profile['name']}'")
            else:
                print("│ ❌ Failed to fetch content.")

    return entries
