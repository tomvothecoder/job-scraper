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
    >>> print(scraper.df_processed.head())
                                                        url  ... company_rating
        0  https://indeed.com/pagead/clk?mo=r&ad=-6NYlbfk...  ...            None
        1  https://indeed.com/pagead/clk?mo=r&ad=-6NYlbfk...  ...             3.4
        2  https://indeed.com/pagead/clk?mo=r&ad=-6NYlbfk...  ...             3.0
        3  https://indeed.com/pagead/clk?mo=r&ad=-6NYlbfk...  ...            None
        4  https://indeed.com/pagead/clk?mo=r&ad=-6NYlbfk...  ...            None

        [5 rows x 8 columns]
    >>> scraper.save()
    """

    def __init__(
        self,
        job_titles: Union[str, List[str]],
        location: str,
        sort_by: Optional[Literal["relevance", "date"]] = "date",
        exp_lvl: Optional[Literal["entry_level", "mid_level", "senior_level"]] = None,
        radius_mi: Optional[Literal[5, 10, 15, 25, 50, 100]] = 25,
    ):
        """IndeedScraper initialization.

        Parameters
        ----------
        job_titles : Union[str, List[str]]
            The job titles.
        location : str
            The location. Should be <CITY>,<STATE> (e.g., San Jose, CA).
        sort_by : Optional[Literal[, optional
            Sort by "relevance" or "date", by default "date"
        exp_lvl : Optional[Literal["entry_level", "mid_level", "senior_level"]]
            Experience level, by default None (all levels).
        radius_mi : Optional[Literal[5, 10, 15, 25, 50, 100]], optional
            Radius in miles around the location, by default 25.
        """
        self.job_titles = job_titles
        self.location = location
        self.sort_by = sort_by
        self.exp_lvl = exp_lvl
        self.radius_mi = radius_mi

        # DataFrame containing the job posts with raw fields.
        self.df: pd.DataFrame = None
        # DataFrame containing the job posts with processed fields.
        self.df_processed: pd.DataFrame = None

    def scrape(self, pages: int = 5):
        """Scrapes an Indeed URL for job posts.

        This function performs an HTTP GET request for the initial page
        using a URL generated with query parameters. It parses the HTML content
        for all of the existing job containers and performs HTML element matching
        for the job fields (e.g., title, pay, company).

        The next page (pagination) is retrieved in the HTML, which is then used
        to perform the next query. There is a random wait time of 0-10 seconds
        to avoid being rate limited by the Indeed server.

        Set this to a high value such as 1000 to scrape all available pages.

        Parameters
        ----------
        pages : int, optional
            Number of pages to scrape, starting from the most recent, by default 5
        """
        self.df = pd.DataFrame()

        for job_title in self.job_titles:
            url = self.generate_url_query(job_title)

            for i in tqdm(range(pages)):
                print(f"\nScrapping page #{i+1}")

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
                    print(f"Done, waiting {round(wait_time,2)} secs...")
                    sleep(wait_time)
                except AttributeError:
                    # Reached the end
                    print(f"\nReached last page (#{i+1}), scrapper complete.")
                    break

        self.postprocess()

    def generate_url_query(self, job_title: str) -> str:
        """Generates a URL for job posts using query parameters.

        Parameters
        ----------
        job_title : str
            Title of the job.

        Returns
        -------
        str
            URL query.
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
        """Parses each HTML container containing job post information.

        Parameters
        ----------
        containers : ResultSet
            A set of HTML job post containers.
        """
        job_posts: List[Dict[str, Optional[str]]] = []

        for card in containers:
            job_post = self.parse_container(card)
            job_posts.append(job_post)

        self.df = self.df.append(job_posts, ignore_index=True)

    def parse_container(self, container: "PageElement") -> Dict[str, Optional[str]]:
        """Parses job card HTML for text matching HTML tag and class.

        Parameters
        ----------
        container : PageElement
            Contains the HTML text for the job post.

        Returns
        -------
        Dict[str, Optional[str]]
            Parsed job post fields.
        """

        HTML_ELEMENT = TypedDict(
            "HTML_ELEMENT",
            {
                "html_tag": str,
                "html_class": Optional[Union[str, Tuple[str, str]]],
            },
        )
        FIELDS: Dict[str, HTML_ELEMENT] = {
            "title": {"html_tag": "h2", "html_class": "jobTitle"},
            "description": {"html_tag": "div", "html_class": "job-snippet"},
            "company": {"html_tag": "span", "html_class": "companyName"},
            "company_rating": {"html_tag": "span", "html_class": "ratingNumber"},
            "location": {"html_tag": "div", "html_class": "companyLocation"},
            "days_ago": {"html_tag": "span", "html_class": "date"},
            "pay": {"html_tag": "span", "html_class": "salary-snippet"},
        }

        job_post: Dict[str, Optional[str]] = {}

        for key, value in FIELDS.items():
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

        job_post["url"] = f"{BASE_URL}{container.get('href')}"
        return job_post

    def postprocess(self):
        """Parses the dataframe columns for additional metadata or clean up."""
        df = self.df.copy()

        # Add user input columns
        df = df.assign(date_applied=None, notes=None)

        # Update string columns
        df["title"] = df["title"].str.replace(r"^new", "", regex=True)

        # Parse location string into separate fields.
        df["city"] = df.location.str.extract(r"(?P<city>.*[A-Z][A-Z])")
        # NOTE: These aren't exported, but can be added back in if desired.
        df["zip"] = df.location.str.extract(r"(?P<zip>\d\d\d\d\d)")
        df["area"] = df.location.str.extract(r"(?P<area>(?<=\().+?(?=\)))")

        # Parse days_ago into an int and calculate when the job was posted since
        # Indeed doesn't list the dates for when jobs are posted.
        # NOTE: 30+ days will usually be indicated by exactly 30 day difference
        # between date_scraped and date_posted
        df["days_ago"] = (
            df["days_ago"]
            .str.replace("Active ", "")
            .str.replace("Just posted", "0")
            .str.replace("Today", "0")
            .str.replace("+", "", regex=False)
            .str.replace("day ago", "")
            .str.replace("days ago", "")
            .str.strip()
        ).astype(int)
        df["date_scraped"] = pd.to_datetime("now")
        df["date_posted"] = df.date_scraped - pd.to_timedelta(df.days_ago, unit="d")
        df["date_scraped"] = df.date_scraped.dt.date
        df["date_posted"] = df.date_posted.dt.date

        # Sort order of columns
        df = df[
            [
                "city",
                "company",
                "company_rating",
                "title",
                "description",
                "pay",
                "date_posted",
                "date_scraped",
                "date_applied",
                "notes",
                "url",
            ]
        ]
        self.df_processed = df

    def save(self):
        """Saves the job posts to an Excel output file.

        This function concatenates to an existing file and drops duplicates
        based on the title and url rows. An output file path must be specified
        in the .env file.
        """
        df_current: pd.DataFrame = pd.read_excel(OUTPUT_FILE, index_col=0)
        df_final = (
            pd.concat([df_current, self.df_processed], ignore_index=False, sort=False)
            .drop_duplicates(["title", "company"], keep="first")
            .reset_index(drop=True)
        )

        df_final.to_excel(OUTPUT_FILE)
        print(f"Updated file {OUTPUT_FILE} with latest jobs.")
