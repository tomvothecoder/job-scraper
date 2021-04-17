import logging
from datetime import date
from typing import Any, Dict, List, Optional

import pandas as pd
import requests
from bs4 import BeautifulSoup as bs

# Get an instance of a logger
logger = logging.getLogger(__name__)


def parse_date(days_ago: str) -> Optional[str]:
    """Parses the date field based to return the correct value.
    If the value is 'Just Posted' or 'Today', set day = 0.
    """
    if not days_ago:
        return None

    if days_ago in {"Just posted", "Active today", "Today"}:
        days_ago = "0"
    else:
        days_ago = days_ago.replace("Active", "")

    return days_ago


class IndeedScraper:
    """A class representing an Indeed scraper."""

    def __init__(self, pages=None):
        self.job_titles: List[str] = ["CNA", "Certified Nursing Assistant"]
        self.location = "California"
        self.exp_lvl = None

        # Every page contains 10 posts, so iterate in counts of 10
        # self.pages = pages if pages else range(0, 1001, 10)
        self.pages = [0, 10]

        self.fields: List[str] = [
            "title",
            "company",
            "location",
            "description",
            "days_ago",
            "salary",
            "link",
        ]

        self.df: pd.DataFrame = pd.DataFrame(columns=self.fields)

    def generate_url(self, job_title: str, page: int) -> str:
        """Generates the Indeed query URL

        :param job_title: Title of the job
        :type job_title: str
        :return: Indeed query URL
        :rtype: str
        """
        url = f"https://www.indeed.com/jobs?q={job_title}&l={self.location}&sort=date&start={page}"
        return url

    def scrape(self):
        """Performs HTTP GET request for generated Indeed URL

        :raises TypeError: [description]
        """
        logger.info("Executing scraper")

        for job_title in self.job_titles:
            for page in self.pages:
                url = self.generate_url(job_title, page)

                # TODO: Add an interval sleep to avoid HTTP requests blockage
                page_html = requests.get(url)

                if page_html:
                    soup = bs(page_html.text, "html.parser")
                    containers = soup.findAll("div", "jobsearch-SerpJobCard")
                    self.parse_containers(containers)
                else:
                    raise TypeError(page_html)

        self.post_processing()
        self.save_posts()

    def parse_containers(self, containers: List[bs]):
        """Parses the container with the job information

        :param containers: Job containers
        :type containers: List[bs]
        """

        for container in containers:
            fields: Dict[str, Any] = {field: None for field in self.fields}

            a_tag = container.h2.a
            post_url = a_tag.get("href")
            fields["link"] = f"https://indeed.com/{post_url}"

            try:
                fields["title"] = a_tag.get("title")
            except AttributeError:
                fields["title"] = None
            try:
                fields["company"] = container.find("span", "company").text.strip()
            except AttributeError:
                fields["company"] = None
            try:
                fields["location"] = container.find("div", "recJobLoc").get(
                    "data-rc-loc"
                )
            except AttributeError:
                fields["location"] = None
            try:
                fields["description"] = container.find("div", "summary").text.strip()
            except AttributeError:
                fields["description"] = None
            try:
                fields["days_ago"] = container.find("span", "date").text.strip()
            except AttributeError:
                fields["days_ago"] = None
            try:
                fields["salary"] = container.find("span", "salaryText").text.strip()
            except AttributeError:
                fields["salary"] = None

            self.df = self.df.append(fields, ignore_index=True)

    def post_processing(self):
        """Parses the dataframe containing all of the job posts.

        Duplicate entries are dropped based on the 'company', 'days_ago', and
        'title' fields. Often times there are multiple of the same job posting
        listed on different days.
        """
        logger.info("Parsing posts")

        self.df = self.df.dropna(subset=["company"])
        self.df = self.df.drop_duplicates(subset=["company", "days_ago", "title"])

    def save_posts(self):
        """Saves each dataframe row as a record using get_or_create()."""
        today = date.today().strftime("%m_%d_%y")
        self.df.to_csv(f"jobs_{today}.csv")
        self.df.to_excel(f"jobs_{today}.xlsx")


if __name__ == "__main__":
    scraper = IndeedScraper()
