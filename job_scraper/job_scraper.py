import logging
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple, TypedDict, Union

import pandas as pd
import requests
from bs4 import BeautifulSoup as bs

logger = logging.getLogger(__name__)


class IndeedScraper:
    """A class representing an Indeed scraper."""

    def __init__(self, pages=None):
        self.job_titles: List[str] = ["CNA", "Certified Nursing Assistant"]
        self.location = "California"
        self.exp_lvl = None

        # Every page contains 10 posts, so iterate in counts of 10
        # self.pages = pages if pages else range(0, 1001, 10)
        self.pages = [0, 10]
        self.df: pd.DataFrame = pd.DataFrame()

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

    def parse_fields_from_html(
        self, container: bs, html_tag: str, html_class: Union[str, Tuple[str, str]]
    ) -> Optional[str]:
        """Parses the raw HTML job container for the target text inside a matched HTML tag and class.

        Args:
            container (bs): Raw HTML job container
            html_tag (str): HTML tag that contains target text
            html_class (Union[str, Tuple[str, str]]): HTML class that contains the target text

        Returns:
            Optional[str]: [description]
        """
        try:
            if isinstance(html_class, tuple):
                return container.find(html_tag, html_class[0]).get(html_class[1])
            return container.find(html_tag, html_class).text.strip()
        except AttributeError:
            return None

    def parse_containers(self, containers: List[bs]):
        """Parses the container with the job information

        :param containers: Job containers
        :type containers: List[bs]
        """

        fields: Dict[str, Dict[str, Union[str, Tuple[str, str]]]] = {
            "company": {"html_tag": "span", "html_class": "company"},
            "location": {"html_tag": "div", "html_class": ("recJobLoc", "data-rc-loc")},
            "description": {"html_tag": "div", "html_class": "summary"},
            "days_ago": {"html_tag": "span", "html_class": "date"},
            "salary": {"html_tag": "span", "html_class": "salaryText"},
        }
        job_posts: List[Dict[str, Any]] = []

        for container in containers:
            job_post: Dict[str, Optional[str]] = {}
            a_tag = container.h2.a

            try:
                job_post["title"] = a_tag.get("title")
            except AttributeError:
                job_post["title"] = None

            for key, value in fields.items():
                html_tag = value["html_tag"]
                html_class: Union[str, Tuple[str, str]] = value["html_class"]

                if (
                    html_tag is None
                    or not isinstance(html_tag, str)
                    or html_class is None
                ):
                    raise TypeError

                job_post[key] = self.parse_fields_from_html(
                    container, html_tag, html_class
                )

            job_post["url"] = f"https://indeed.com/{a_tag.get('href')}"
            job_posts.append(job_post)

        self.df = pd.DataFrame(job_posts)

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
        self.df.to_csv(f"../outputs/jobs_{today}.csv")
        self.df.to_excel(f"../outputs/jobs_{today}.xlsx")


if __name__ == "__main__":
    scraper = IndeedScraper()
