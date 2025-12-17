# doctor-verification-ai
"AI-powered pipeline to scrape, validate, and enrich doctor profiles using Multi-Agent workflow."


📊 Workflow
Upload doctor_names.txt.

Run Scraping to build the base database.

Run Confidence Score to filter high-quality data.

Run Enrichment on low-score records.

Export the final verified JSON.


⚠️ Notes
This project uses an SQLite database (workflow.db) which is auto-generated.

Ensure you have the necessary API keys for the LLM strategy used in scraping.
