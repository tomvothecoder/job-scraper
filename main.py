import pandas as pd
from bs4 import BeautifulSoup as bs
from sqlalchemy import create_engine, types
import requests
import re

engine = create_engine('postgresql://root@localhost:5432/jobs_db')


def main():
    page_html = requests.get('https://www.indeed.com/jobs?q=Software+Developer&l=California')
    content = page_html.content
    # HTML Parsing
    soup = bs(content, "html.parser")
    
    # text_file = open("Output.txt", "w")
    # text_file.write(page_soup.prettify())
    # text_file.close()

    # Grabs each container
    job_containers = soup.findAll("div", {"class": "row"})
    print(len(job_containers))
    
    job_titles = []
    companies = []
    locations = []
    summaries = []
    job_skills = []
    for container in job_containers:
        try:
            job_title = container.a.text
            company = container.find('span', class_='company').text.strip()
            location = container.find('div', class_='location').text.strip()
            summary = container.find('div', class_='paddedSummaryExperience').text.strip()
            skills = container.find('div', class_='v2Experience').span.text.strip()
            
        except Exception as e:
            location = "N/A"
            summary = "N/A"
            skills = "N/A"

        job_titles.append(job_title)
        companies.append(company)
        locations.append(location)
        summaries.append(summary)
        job_skills.append(skills)

        print("Job title: " + job_title)
        print("Company: " + company)
        print("Location: " + location)
        print("Summary: " + summary)
        print("Skills: " + skills)
        print("\n")

    jobs_df = pd.DataFrame({'job_title': job_titles,
                            'company': companies,
                            'location': locations, 
                            'summary': summaries,
                            'skills': job_skills})

    jobs_df.to_sql("job_listings", engine, if_exists='append', index=False, 
                   dtype=types.VARCHAR(255))


if __name__ == '__main__':
    main()
