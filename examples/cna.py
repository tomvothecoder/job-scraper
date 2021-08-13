# flake8: noqa: E265
#%%

from job_scraper.indeed import IndeedScraper

#%%
scraper = IndeedScraper(
    job_titles=["cna"],
    location="San Jose, CA",
    sort_by="date",
    exp_lvl=None,
    radius_mi=25,
)

#%%
scraper.scrape(pages=1000)


# %%
scraper.save()s

# %%
