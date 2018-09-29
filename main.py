import datetime

import pandas as pd
from bs4 import BeautifulSoup as bs
from sqlalchemy import create_engine, types
import requests
from tqdm import tqdm

engine = create_engine('postgresql://postgres:elm0izc00l@localhost:5432/jobs_db')


def main():
    pages = [10, 20, 30, 40, 50, 60, 70, 80, 90, 100]

    for page in tqdm(pages):
        page_html = requests.get(f'https://www.indeed.com/jobs?q=Software+Developer&l=California&jt=fulltime&explvl=entry_level&sort=date&start={page}')

        content = page_html.content
        # HTML Parsing
        soup = bs(content, "html.parser")
        # print(soup.prettify())

        # Grabs each container
        job_containers = soup.findAll("div", {"class": "row"})

        # Lists of data scraped from job posting container
        job_titles = []
        companies = []
        locations = []
        summaries = []
        job_skills = []
        dates = []
        listing_type = []
        date_scraped = []

        today = datetime.date.today()

        for container in job_containers:

            job_title = container.a.text
            company = container.find('span', class_='company')
            location = container.find('span', class_='location')
            summary = container.find('span', class_='summary')
            skills = container.find('div', class_='v2Experience')
            listing_date = container.find('span', class_='date')
            sponsored = container.find('span', class_=' sponsoredGray ')

            job_titles.append(job_title)
            companies.append(company.text.strip() if company else "N/A")
            locations.append(location.text.strip() if location else "N/A")
            summaries.append(summary.text.strip() if summary else "N/A")
            job_skills.append(skills.text.strip() if skills else "N/A")
            listing_type.append(sponsored.text.strip() if sponsored else "Regular listing")
            dates.append(listing_date.text.strip() if listing_date else "N/A")
            date_scraped.append(today)

        jobs_df = pd.DataFrame({'job_title': job_titles,
                                'company': companies,
                                'location': locations,
                                'summary': summaries,
                                'skills': job_skills,
                                'listing_type': listing_type,
                                'date': dates,
                                'date_scraped': date_scraped})

        print("Adding to SQL ...")
        jobs_df.to_sql("job_listings", engine, if_exists='append', index=False,
                       dtype={'job_title': types.VARCHAR(255),
                              'company': types.VARCHAR(255),
                              'location': types.VARCHAR(255),
                              'skills': types.VARCHAR(255),
                              'listing_type': types.VARCHAR(255),
                              'date': types.VARCHAR(255),
                              'date_scraped': types.DATE})


if __name__ == '__main__':
    main()
