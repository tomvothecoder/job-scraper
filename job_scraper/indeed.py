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
BASE_URL = "https://indeed.com"
OUTPUT_FILE = config["OUTPUT_FILE"]


class IndeedScraper:
    """A class representing an Indeed scraper.

    Examples
    --------
    Scraping entry-level software engineering jobs within 10 miles of San Jose, CA:

    >>> jobs = IndeedScraper(job_titles=[software engineer, software developer])
    >>> scraper = IndeedScraper(
    >>>     job_titles=["software engineer", "software developer"],
    >>>     location="San Jose, CA",
    >>>     sort_by="relevance",
    >>>     exp_lvl=None,
    >>>     radius_mi=10,
    >>> )
    >>> scraper.scrape(pages=5)  # Leave pages blank to scrape all available pages
    >>> print(scraper.df.head())
                                                        url  ... employer_rating
        0  https://indeed.com/pagead/clk?mo=r&ad=-6NYlbfk...  ...            None
        1  https://indeed.com/pagead/clk?mo=r&ad=-6NYlbfk...  ...             3.4
        2  https://indeed.com/pagead/clk?mo=r&ad=-6NYlbfk...  ...             3.0
        3  https://indeed.com/pagead/clk?mo=r&ad=-6NYlbfk...  ...            None
        4  https://indeed.com/pagead/clk?mo=r&ad=-6NYlbfk...  ...            None

        [5 rows x 8 columns]
    >>> scraper.save(output="excel")
    """

    def __init__(
        self,
        job_titles: Union[str, List[str]],
        location: str,
        sort_by: Optional[Literal["relevance", "date"]] = "relevance",
        exp_lvl: Optional[Literal["entry_level", "mid_level", "senior_level"]] = None,
        radius_mi: Optional[int] = None,
    ):
        self.job_titles = job_titles
        self.location = location
        self.sort_by = sort_by
        self.exp_lvl = exp_lvl
        self.radius_mi = radius_mi

        # DataFrame containing the job posts with parsed fields.
        self.df: pd.DataFrame = pd.DataFrame()

    def scrape(self, pages: int = 5, output="excel"):
        """Scrapes an Indeed URL for job posts.

        This function performs an HTTP GET request for the initial page
        using a URL generated with query parameters. It parses the HTML content
        for all of the existing job containers and performs HTML element matching
        for the job fields (e.g., title, salary, company).

        The next page (pagination) is retrieved in the HTML, which is then used
        to perform the next query. There is a random wait time of 0-10 seconds
        to avoid being rate limited by the Indeed server.

        Args:
            pages (int, optional): Number of pages to scrape, starting from the
            most recent. Defaults to 5.
        """

        logger.info("Executing scraper")

        for job_title in self.job_titles:
            url = self.generate_url_query(job_title)

            for i in tqdm(range(pages)):
                logger.info(f"Scrapping URL: {url}")
                page_html = requests.get(url)

                if not page_html:
                    break

                soup = bs(page_html.text, "html.parser")
                job_containers = soup.find_all("a", "result")
                self.parse_containers(job_containers)

                # Generate the next page URL
                try:
                    pagination = soup.find("a", {"aria-label": "Next"}).get("href")
                    url = BASE_URL + pagination

                    # Wait a random number of seconds between scrapping to avoid
                    # HTTP rate limiting.
                    wait_time = random() * 10
                    print(f"Waiting {wait_time} before scrapping again...")
                    sleep(wait_time)
                except AttributeError:
                    # Reached the end
                    break

        self.post_process_df()

    def generate_url_query(self, job_title: str) -> str:
        """Generates a URL for job posts using query parameters.

        Args:
            job_title (str): Title of the job.

        Returns:
            str: URL query.
        """
        url = (
            f"{BASE_URL}/jobs?q={job_title}"
            f"&l={self.location}"
            f"&sort={self.sort_by}"
        )
        if self.exp_lvl:
            url += f"&explvl={self.exp_lvl}"
        if self.radius_mi:
            url += f"&radius={self.radius_mi}"

        return url

    def parse_containers(self, containers: "ResultSet"):
        job_posts: List[Dict[str, Optional[str]]] = []

        for card in containers:
            job_post = self.parse_container(card)
            job_posts.append(job_post)

        self.df = self.df.append(job_posts, ignore_index=True)

    def parse_container(self, container: "PageElement") -> Dict[str, Optional[str]]:
        """Parses job card HTML for text matching HTML tag and class.

        Args:
            container (PageElement): Contains the HTML text for the job post.

        Returns:
            Dict[str, Optional[str]]: Parsed job post fields.
        """
        HTML_ELEMENT = TypedDict(
            "HTML_ELEMENT",
            {
                "html_tag": str,
                "html_class": Optional[Union[str, Tuple[str, str]]],
            },
        )

        job_post: Dict[str, Optional[str]] = {}
        job_post["url"] = f"{BASE_URL}{container.get('href')}"
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
                    matching_text = container.find(html_tag, html_class[0]).get(
                        html_class[1]
                    )
                else:
                    matching_text = container.find(html_tag, html_class).text.strip()
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

    def save(self, output):
        """Saves the job posts to an existing output file."""
        if output == "excel":
            self.df.to_excel(OUTPUT_FILE)
            print(f"Updated file {OUTPUT_FILE}")
