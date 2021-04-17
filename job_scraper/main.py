import logging
from datetime import date
from typing import Any, Dict, List, Optional, Union

import pandas as pd
import requests
from bs4 import BeautifulSoup as bs

# Get an instance of a logger
logger = logging.getLogger(__name__)


def scrape_indeed(pages: List[int] = None):
    """
    Performs the web scrapping process using BeautifulSoup4 and pandas
    for data processing. The web page is fetched using the request
    library. Each job posting on the page are stored as
    'containers' with a class of 'row'. After fetching each container,
    perform parsing the fields of a post.
    """
    scraper = IndeedScraper(pages=pages)
    scraper.scrape()
    # scraper.save_posts()


def parse_date(date_posted: str) -> Optional[str]:
    """Parses the date field based to return the correct value.
    If the value is 'Just Posted' or 'Today', set day = 0.
    """
    if not date_posted:
        return None

    if date_posted in {"Just posted", "Active today", "Today"}:
        days_ago = "0"
    else:
        days_ago = date_posted.replace("Active", "")

    return days_ago


def find_span(container: bs, tag: str, class_: str) -> Union[str, None]:
    """Searches a specified tag in a job post container.
    If the tag is found, return the stripped version of the text.
    Otherwise return None.
    """

    found = container.find(tag, class_=class_)
    if found is None:
        return None
    return found.text.strip()


class IndeedScraper:
    """A class representing an Indeed scraper."""

    def __init__(self, pages=None):
        # The search queries to be used in requests
        self.job_titles: List[str] = ["CNA", "Certified Nursing Assistant"]
        self.location = "California"
        self.exp_lvl = None

        # Every page contains 10 posts, so iterate in counts of 10
        # self.pages = pages if pages else range(0, 1001, 10)
        self.pages = [0, 10]

        # Stores the posts
        self.fields: List[str] = [
            "title",
            "company",
            "description",
            "salary",
            "location",
            "date_posted",
            "date_added_db",
            "link",
            "position",
        ]
        self.df: pd.DataFrame = pd.DataFrame(columns=self.fields)

    def generate_url(self, job_title: str) -> str:
        """Generates the Indeed query URL

        :param job_title: Title of the job
        :type job_title: str
        :return: Indeed query URL
        :rtype: str
        """

        url = f"https://www.indeed.com/jobs?q={job_title}&l={self.location}"
        return url

    def scrape(self):
        logger.info("Executing scraper")

        for job_title in self.job_titles:
            for page in self.pages:
                url = self.generate_url(job_title)
                page_html = requests.get(url)
                soup = bs(page_html.content, "html.parser")
                post_container = soup.findAll("div", {"class": "row"})
                self.parse_container(post_container)

        self.parse_posts()

    def parse_container(self, containers: List[bs]):
        """Parses the container with the job information

        :param containers: Job containers
        :type containers: List[bs]
        """

        today = date.today()
        span_fields = {"company", "location", "date"}

        for container in containers:
            post_href = container.find("a", {"class": "jobtitle"})["href"]

            fields: Dict[str, Any] = {
                f: find_span(container, "span", f) for f in span_fields
            }

            fields.update(
                {
                    "date_posted": parse_date(fields["date"]),
                    "title": container.a.text,
                    "date_added_db": today,
                    "description": find_span(container, "div", class_="summary"),
                    "link": f"https://indeed.com/{post_href}",
                }
            )

            self.df = self.df.append(fields, ignore_index=True)

    def parse_posts(self):
        """Parses the dataframe containing all of the job posts.
        The first step is to remove companies that are spam, which is
        mainly 'Indeed Prime'. Afterwards, duplicate entries are dropped
        based on the 'company', 'date_posted', and 'title' fields. Often
        times there are multiple of the same job posting listed on
        different days which is not useful to search through for the end-user.
        """
        logger.info("Parsing posts")

        self.df.title = self.df.title.str.strip()

        spam_companies = ["Indeed Prime"]
        self.df = self.df[~self.df["company"].isin(spam_companies)]
        self.df = self.df.dropna(subset=["company"])
        self.df = self.df.drop_duplicates(subset=["company", "date_posted", "title"])

    # def save_posts(self):
    #     """Saves each dataframe row as a record using get_or_create()."""
    #     logger.info("Savings posts to database")
    #     records = self.df.to_dict("records")

    #     for record in records:
    #         Company.objects.get_or_create(name=record["company"])

    #         Post.objects.get_or_create(
    #             title=record["title"],
    #             company_id=record["company"],
    #             defaults={
    #                 "date_posted": record["date_posted"],
    #                 "description": record["description"],
    #                 "location": record["location"],
    #                 "is_sponsored": False,
    #                 "date_added_db": record["date_added_db"],
    #                 "source_id": record["source"],
    #                 "link": record["link"],
    #             },
    #         )


if __name__ == "__main__":
    scraper = scrape_indeed()
