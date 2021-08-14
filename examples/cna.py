# flake8: noqa: E265
#%%
from job_scraper.indeed import IndeedScraper

#%%
scraper = IndeedScraper(
    job_titles=["cna"],
    location="San Jose, CA",
    sort_by="date",
    exp_lvl=None,
    radius_mi=10,
)

#%%
scraper.scrape(pages=100)

# %%
scraper.save()
