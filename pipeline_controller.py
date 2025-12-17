import asyncio
from workflow_db import WorkflowDB
from confidence_scorer import ConfidenceScorer
from enrichment_agent import EnrichmentAgent
from search_scraper import process_doctor 
from scraper_helper import StealthBrowser
from crawl4ai import AsyncWebCrawler, BrowserConfig

class RefineryPipeline:
    def __init__(self):
        self.db = WorkflowDB()
        self.scorer = ConfidenceScorer()
        self.enricher = EnrichmentAgent()
        self.stop_signal = False

    def stop(self):
        self.stop_signal = True

    async def run(self, doctor_list):
        self.stop_signal = False
        total = len(doctor_list)
        scraped_batch = {}
        pending_batch = []

        # --- PHASE 1 ---
        yield "PHASE:1"
        yield f"🚀 --- PHASE 1: DISCOVERY ({total} profiles) ---"
        
        browser_conf = BrowserConfig(headless=True)
        stealth = StealthBrowser()
        await stealth.start()
        
        async with AsyncWebCrawler(config=browser_conf) as crawler:
            for i, name in enumerate(doctor_list):
                if self.stop_signal: break
                yield f"🔎 [{i+1}/{total}] Scraping: {name}"
                try:
                    profiles = await process_doctor(name, i+1, total, crawler, stealth)
                    if profiles:
                        best = profiles[0]
                        scraped_batch[name] = best
                        self.db.upsert_doctor(name, "Pending", 0, 0, best, {})
                    else:
                        yield f"⚠️ No data found for {name}"
                        self.db.upsert_doctor(name, "Failed", 0, 0, {}, {})
                except Exception as e:
                    yield f"❌ Error: {e}"

        # --- PHASE 2 ---
        yield "PHASE:2"
        yield f"⚖️ --- PHASE 2: SCORING ---"
        
        for name, profile in scraped_batch.items():
            if self.stop_signal: break
            score, details = self.scorer.evaluate(profile)
            
            if score >= 0.8:
                yield f"✅ Verified: {name} ({int(score*100)}%)"
                self.db.upsert_doctor(name, "Verified", score, score, profile, {})
            else:
                yield f"⚠️ Low Score: {name} ({int(score*100)}%) -> Queued"
                missing = []
                if not details.get('npi'): missing.append('npi_id')
                if not details.get('license'): missing.append('license_id')
                pending_batch.append((name, profile, missing))

        # --- PHASE 3 ---
        if pending_batch:
            yield "PHASE:3"
            yield f"🧬 --- PHASE 3: ENRICHMENT ({len(pending_batch)} records) ---"
            
            async with AsyncWebCrawler(config=browser_conf) as crawler:
                for name, profile, missing in pending_batch:
                    if self.stop_signal: break
                    yield f"📖 Deep Searching for {name}..."
                    
                    # UPDATED CALL: Passing 'stealth' browser here
                    new_data = await self.enricher.hunt_text(name, missing, crawler, stealth)
                    
                    if new_data:
                        profile.update(new_data)
                        yield f"   + Found: {list(new_data.keys())}"
                    
                    f_score, _ = self.scorer.evaluate(profile)
                    status = "Enriched" if f_score >= 0.8 else "Manual_Review"
                    yield f"🏁 Final: {name} -> {int(f_score*100)}%"
                    self.db.upsert_doctor(name, status, 0, f_score, profile, {})

        # Cleanup
        await stealth.close()
        yield "PHASE:4"
        yield "✅ Pipeline Complete."
