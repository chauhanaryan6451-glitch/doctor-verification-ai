import nodriver as uc
import asyncio
from typing import Optional

class StealthBrowser:
    """
    Helper to fetch raw HTML using Nodriver.
    """
    def __init__(self):
        self.browser = None

    async def start(self):
        if not self.browser:
            # We use headless=True. If debugging, set to False.
            self.browser = await uc.start(
                headless=True,
                browser_args=["--no-sandbox", "--disable-setuid-sandbox", "--window-size=1920,1080"]
            )
            print("   🛡️  [Helper] Stealth Browser Started")

    async def get_html(self, url: str, wait_time: int = 4) -> Optional[str]:
        if not self.browser: await self.start()
        try:
            page = await self.browser.get(url)
            await asyncio.sleep(wait_time) 
            await page.scroll_down(200) # Trigger lazy loading
            await asyncio.sleep(1)
            content = await page.get_content()
            return content
        except Exception as e:
            print(f"   ⚠️  [Helper] Fetch Error: {e}")
            return None

    async def close(self):
        if self.browser:
            try:
                # !!! FIX: stop() is synchronous in some versions, do not await it
                self.browser.stop()
            except Exception:
                pass
            self.browser = None
            print("   🛡️  [Helper] Browser Closed")
