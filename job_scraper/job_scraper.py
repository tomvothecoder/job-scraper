import logging
from random import random
from time import sleep
from typing import TYPE_CHECKING, Dict, List, Optional, Tuple, TypedDict, Union

import pandas as pd
import requests
from bs4 import BeautifulSoup as bs
from dotenv import dotenv_values
from tqdm import tqdm
from typing_extensions import Literal

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    from bs4.element import PageElement, ResultSet

config = dotenv_values(".env")


class IndeedScraper:
    """A class representing an Indeed scraper."""

    BASE_URL = "https://indeed.com"
    OUTPUT_FILE = config["OUTPUT_FILE"]

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
                cards = soup.find_all("a", "result")
                self.parse_cards_to_df(cards)

                try:
                    pagination = soup.find("a", {"aria-label": "Next"}).get("href")
                    url = IndeedScraper.BASE_URL + pagination

                    # Wait a random number of seconds between scrapping
                    wait_time = random() * 10
                    print(f"Waiting {wait_time} before scrapping again...")
                    sleep(wait_time)
                except AttributeError:
                    # Reached the end
                    break

        self.post_process_df()
        self.save()

    def generate_url(self, job_title: str) -> str:
        """Generates the Indeed query URL.

        :param job_title: Title of the job
        :type job_title: str
        :return: Indeed query URL
        :rtype: str
        """
        url = (
            f"{IndeedScraper.BASE_URL}/jobs?q={job_title}"
            f"&l={self.location}"
            f"&sort={self.sort_by}"
        )
        if self.exp_lvl:
            url += f"&explvl={self.exp_lvl}"
        if self.radius:
            url += f"&radius={self.radius}"

        return url

    def parse_cards_to_df(self, cards: "ResultSet"):
        """Parses cards containing the job post information.

        :param cards: A ResultSet of PageElements.
        :type cards: ResultSet
        """

        job_posts: List[Dict[str, Optional[str]]] = []

        for card in cards:
            job_post = self.parse_card_html(card)
            job_posts.append(job_post)

        self.df = self.df.append(job_posts, ignore_index=True)

    def parse_card_html(self, card: "PageElement") -> Dict[str, Optional[str]]:
        """Parses job card HTML for text matching HTML tag and class.

        :param card: Contains the HTML text for the job card
        :type card: PageElement
        :return: [description]
        :rtype: Dict[str, Optional[str]]
        """

        job_post: Dict[str, Optional[str]] = {}
        job_post["url"] = f"{IndeedScraper.BASE_URL}{card.get('href')}"

        HTML_ELEMENT = TypedDict(
            "HTML_ELEMENT",
            {
                "html_tag": str,
                "html_class": Optional[Union[str, Tuple[str, str]]],
            },
        )

        # Parse additional fields
        fields: Dict[str, HTML_ELEMENT] = {
            "title": {"html_tag": "h2", "html_class": "jobTitle"},
            "company": {"html_tag": "span", "html_class": "companyName"},
            "location": {"html_tag": "div", "html_class": "companyLocation"},
            "description": {"html_tag": "div", "html_class": "job-snippet"},
            "days_ago": {"html_tag": "span", "html_class": "date"},
            "salary": {"html_tag": "span", "html_class": "salary-snippet"},
            "employer_rating": {"html_tag": "span", "html_class": "ratingNumber"},
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
                logger.debug(
                    f"No HTML element match found for tag={html_tag} and class={html_class}"
                )
                matching_text = None

            job_post[key] = matching_text

        return job_post

    def post_process_df(self):
        """Parses the dataframe containing all of the job posts.

        Duplicate entries are dropped based on the URL.
        Often times there are multiple of the same job posting listed on
        different days.
        """
        logger.info("Parsing posts")
        self.df = self.df.drop_duplicates("url")
        # self.df["url"] = self.df["url"].apply(lambda x: f"=HYPERLINK('{x}'', 'URL')")

    def save(self):
        """Saves the job posts to CSV and Excel files"""
        # today = datetime.now().strftime("%m_%d_%y %H_%M_%S")
        self.df.to_excel(IndeedScraper.OUTPUT_FILE)


if __name__ == "__main__":
    scraper = IndeedScraper(
        job_titles=["CNA"],
        location="San Jose California",
        radius=30,
    )
