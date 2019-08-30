# %%
import datetime
from typing import List

import pandas as pd
from bs4 import BeautifulSoup as bs
import requests
from tqdm import tqdm


def parse_container(containers: List[str], parsed_df: pd.DataFrame) -> pd.DataFrame:
    """ Parses the containers for the specified fields """
    for container in containers:
        columns = {k: find_strip(container, "span", v) for k, v in span_cols.items()}
        columns["job_title"] = container.a.text
        columns["date_scrapped"] = today
        columns["summary"] = find_strip(container, "div", class_="summary")
        columns['sponsored'] = True if columns["date_posted"] else False
        parsed_df = parsed_df.append(columns, ignore_index=True)
    parsed_df = parsed_df.drop_duplicates()

    return parsed_df


def find_strip(container: str, tag: str, class_: str) -> str:
    """
    Receives the container (the job posting), and searches
    the tag for the specified tag. If found, return the text.strip(),
    else return None.
    """
    found = container.find(tag, class_=class_)
    if found is None:
        return None
    return found.text.strip()


def parse_postings(df: pd.DataFrame) -> pd.DataFrame:
    """ Remove spam companies from the dataframe """
    spam_companies = ['Indeed Prime']
    return df[~df['company'].isin(spam_companies)]


if __name__ == "__main__":
    today = datetime.date.today()
    jobs_df = pd.DataFrame()

    # Query searches and num of pages to search (intervals of 10)
    searches = {"Software+Engineer", "Software+Developer"}
    pages = range(0, 101, 10)

    for search in searches:
        for page in tqdm(pages):
            page_html = requests.get(
                f"https://www.indeed.com/jobs?q={search}&sort=date&l=California&explvl=entry_level&radius=50"
            )
            # Grab containers and parse
            soup = bs(page_html.content, "html.parser")
            job_containers = soup.findAll("div", {"class": "row"})

            span_cols = {
                "company": "company",
                "location": "location",
                "date_posted": "date",
            }


            jobs_df = parse_container(job_containers, jobs_df)

    jobs_df = parse_postings(jobs_df)
# %%
