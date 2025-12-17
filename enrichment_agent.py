import asyncio
from ddgs import DDGS
from openai import OpenAI
from bs4 import BeautifulSoup
from crawl4ai import CrawlerRunConfig, CacheMode

class EnrichmentAgent:
    def __init__(self):
        self.ddgs = DDGS()
        self.llm_url = "http://localhost:8080/v1"

    # --- HELPER: LLM EXTRACTION FOR MISSING FIELDS ---
    async def extract_missing(self, html: str, name: str, missing: list) -> dict:
        if not html: return {}
        
        soup = BeautifulSoup(html, 'html.parser')
        for x in soup(["script", "style"]): x.decompose()
        text = soup.get_text(separator=' ', strip=True)[:6000]

        client = OpenAI(base_url=self.llm_url, api_key="sk-none")
        prompt = (
            f"Context: Doctor '{name}' is missing these fields: {missing}.\n"
            f"Analyze the text below. Extract ONLY the missing fields.\n"
            f"Return JSON with keys: {', '.join(missing)}.\n"
            f"If not found, use 'N/A'.\n\nText:\n{text}"
        )
        
        try:
            response = client.chat.completions.create(
                model="gpt-3.5-turbo",
                messages=[{"role": "user", "content": prompt}],
                temperature=0.1
            )
            content = response.choices[0].message.content
            # Clean JSON
            if "```json" in content: content = content.split("```json")[1].split("```")[0]
            elif "```" in content: content = content.split("```")[1].split("```")[0]
            
            data =  json.loads(content)
            # Filter out N/A
            return {k: v for k, v in data.items() if v and v != "N/A"}
        except:
            return {}

    # --- MAIN HUNT FUNCTION ---
    async def hunt_text(self, name, missing_fields, crawler, stealth_browser):
        print(f"   🕵️ Deep Hunting for {name} (Missing: {missing_fields})")
        found = {}
        
        # 1. Aggressive Search (6 results)
        query = f"{name} {' '.join(missing_fields)} profile"
        urls = []
        try:
            results = list(self.ddgs.text(query, max_results=6))
            urls = [r['href'] for r in results]
        except: pass

        # 2. Scrape Each
        for url in urls:
            if len(found) == len(missing_fields): break # Stop if full
            
            print(f"      🕷️ Checking: {url}")
            html = ""
            
            # Try Standard
            try:
                conf = CrawlerRunConfig(cache_mode=CacheMode.BYPASS, page_timeout=15000)
                res = await crawler.arun(url=url, config=conf)
                if res.success: html = res.html
            except: pass
            
            # Try Stealth (Nodriver) if Standard failed
            if not html:
                print("      🛡️ Switching to Stealth...")
                html = await stealth_browser.get_html(url)

            # 3. LLM Extraction
            if html:
                new_data = await self.extract_missing(html, name, missing_fields)
                if new_data:
                    print(f"         ✅ Found: {new_data}")
                    found.update(new_data)
                    # Remove found fields from missing list so we don't look for them again
                    missing_fields = [m for m in missing_fields if m not in found]

        return found
