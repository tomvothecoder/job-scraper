import logging
from datetime import datetime
from random import random
from time import sleep
from typing import TYPE_CHECKING, Dict, List, Optional, Tuple, TypedDict, Union

import pandas as pd
import requests
from bs4 import BeautifulSoup as bs
from tqdm import tqdm
from typing_extensions import Literal

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    from bs4.element import PageElement, ResultSet


class IndeedScraper:
    """A class representing an Indeed scraper."""

    base_url = "https://indeed.com"

    def __init__(
        self,
        job_titles: Union[str, List, str],
        location: str,
        sort_by: Optional[Literal["relevance", "date"]] = "relevance",
        exp_lvl: Optional[Literal["entry_level", "mid_level", "senior_level"]] = None,
        radius: Optional[int] = None,
    ):
        self.job_titles = job_titles
        self.location = location
        self.sort_by = sort_by
        self.exp_lvl = exp_lvl
        self.radius = radius

        self.df: pd.DataFrame = pd.DataFrame()

    def scrape(self, pages: int = 5):
        """Performs HTTP GET request for generated Indeed URL

        :raises TypeError: [description]
        """
        logger.info("Executing scraper")

        for job_title in self.job_titles:
            url = self.generate_url(job_title)

            for i in tqdm(range(pages)):
                logger.info(f"Scrapping URL: {url}")
                page_html = requests.get(url)

                if not page_html:
                    break

                soup = bs(page_html.text, "html.parser")
                cards = soup.find_all("div", "jobsearch-SerpJobCard")
                self.parse_cards(cards)

                try:
                    pagination = soup.find("a", {"aria-label": "Next"}).get("href")
                    url = IndeedScraper.base_url + pagination

                    # Wait a random number of seconds between scrapping
                    wait_time = random() * 10
                    print(f"Waiting {wait_time} before scrapping again...")
                    sleep(wait_time)
                except AttributeError:
                    break

        self.post_processing()
        self.save_posts()

    def generate_url(self, job_title: str) -> str:
        """Generates the Indeed query URL

        :param job_title: Title of the job
        :type job_title: str
        :return: Indeed query URL
        :rtype: str
        """
        url = f"{IndeedScraper.base_url}/jobs?q={job_title}&l={self.location}&sort={self.sort_by}"

        if self.exp_lvl:
            url += f"&explvl={self.exp_lvl}"

        if self.radius:
            url += f"&radius={self.radius}"

        return url

    def parse_cards(self, cards: "ResultSet"):
        """Parses cards containing the job post information.

        :param cards: A ResultSet of PageElements.
        :type cards: ResultSet
        """

        job_posts: List[Dict[str, Optional[str]]] = []

        for card in cards:
            job_post = self.parse_html(card)
            job_posts.append(job_post)

        self.df = self.df.append(job_posts, ignore_index=True)

    def parse_html(self, card: "PageElement") -> Dict[str, Optional[str]]:
        """Parses the raw HTML job card for the target text inside a matched HTML tag and class.

        :param card: Contains the HTML text for the job card
        :type card: PageElement
        :return: [description]
        :rtype: Dict[str, Optional[str]]
        """

        job_post: Dict[str, Optional[str]] = {}

        # Parse anchor tag fields first
        anchor_tag = card.h2.a
        job_post["url"] = f"{IndeedScraper.base_url}{anchor_tag.get('href')}"
        try:
            job_post["title"] = anchor_tag.get("title")
        except AttributeError:
            job_post["title"] = None

        HTML_ELEMENT = TypedDict(
            "HTML_ELEMENT",
            {
                "html_tag": str,
                "html_class": Optional[Union[str, Tuple[str, str]]],
            },
        )

        # Parse additional fields
        fields: Dict[str, HTML_ELEMENT] = {
            "company": {"html_tag": "span", "html_class": "company"},
            "location": {"html_tag": "div", "html_class": ("recJobLoc", "data-rc-loc")},
            "description": {"html_tag": "div", "html_class": "summary"},
            "days_ago": {"html_tag": "span", "html_class": "date"},
            "salary": {"html_tag": "span", "html_class": "salaryText"},
        }

        for key, value in fields.items():
            html_class = value["html_class"]
            html_tag = value["html_tag"]

            try:
                if isinstance(html_class, tuple):
                    matching_text = card.find(html_tag, html_class[0]).get(
                        html_class[1]
                    )
                else:
                    matching_text = card.find(html_tag, html_class).text.strip()
            except AttributeError:
                logger.warn(
                    f"No HTML element match found for tag={html_tag} and class={html_class}"
                )
                matching_text = None

            job_post[key] = matching_text

        return job_post

    def post_processing(self):
        """Parses the dataframe containing all of the job posts.

        Duplicate entries are dropped based on the URL.
        Often times there are multiple of the same job posting listed on
        different days.
        """
        logger.info("Parsing posts")
        self.df = self.df.drop_duplicates("url")

    def save_posts(self):
        """Saves the job posts to CSV and Excel files"""
        today = datetime.now().strftime("%m_%d_%y %H_%M_%S")
        self.df.to_csv(f"../output/jobs_{today}.csv")
        self.df.to_excel(f"../output/jobs_{today}.xlsx")


if __name__ == "__main__":
    scraper = IndeedScraper(
        job_titles=["CNA"],
        location="San Jose California",
        radius=30,
    )
