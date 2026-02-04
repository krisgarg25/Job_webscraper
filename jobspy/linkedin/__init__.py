"""
Simplified LinkedIn Scraper - Only collects fields needed for Job schema
"""
from __future__ import annotations

import math
import random
import time
from datetime import datetime
from typing import Optional

from bs4 import BeautifulSoup
from bs4.element import Tag

from jobspy.linkedin.constant import headers
from jobspy.linkedin.util import job_type_code
from jobspy.model import (
    JobPost,
    Location,
    JobResponse,
    Country,
    Compensation,
    DescriptionFormat,
    Scraper,
    ScraperInput,
    Site,
)
from jobspy.util import (
    currency_parser,
    markdown_converter,
    create_session,
    remove_attributes,
    create_logger,
)

log = create_logger("LinkedIn")


class LinkedIn(Scraper):
    base_url = "https://www.linkedin.com"
    delay = 3
    band_delay = 4
    jobs_per_page = 25

    def __init__(
        self, proxies: list[str] | str | None = None, ca_cert: str | None = None, user_agent: str | None = None
    ):
        """
        Initializes LinkedInScraper
        """
        super().__init__(Site.LINKEDIN, proxies=proxies, ca_cert=ca_cert)
        self.session = create_session(
            proxies=self.proxies,
            ca_cert=ca_cert,
            is_tls=False,
            has_retry=True,
            delay=5,
            clear_cookies=True,
        )
        self.session.headers.update(headers)
        self.scraper_input = None
        self.country = "worldwide"

    def scrape(self, scraper_input: ScraperInput) -> JobResponse:
        """
        Scrapes LinkedIn for jobs
        """
        self.scraper_input = scraper_input
        job_list: list[JobPost] = []
        seen_ids = set()
        start = scraper_input.offset // 10 * 10 if scraper_input.offset else 0
        request_count = 0
        seconds_old = scraper_input.hours_old * 3600 if scraper_input.hours_old else None
        
        continue_search = lambda: len(job_list) < scraper_input.results_wanted and start < 1000
        
        while continue_search():
            request_count += 1
            log.info(f"search page: {request_count}")
            
            params = {
                "keywords": scraper_input.search_term,
                "location": scraper_input.location,
                "distance": scraper_input.distance,
                "f_WT": 2 if scraper_input.is_remote else None,
                "f_JT": job_type_code(scraper_input.job_type) if scraper_input.job_type else None,
                "pageNum": 0,
                "start": start,
                "f_AL": "true" if scraper_input.easy_apply else None,
                "f_C": ",".join(map(str, scraper_input.linkedin_company_ids)) if scraper_input.linkedin_company_ids else None,
            }
            if seconds_old is not None:
                params["f_TPR"] = f"r{seconds_old}"

            params = {k: v for k, v in params.items() if v is not None}
            
            try:
                response = self.session.get(
                    f"{self.base_url}/jobs-guest/jobs/api/seeMoreJobPostings/search?",
                    params=params,
                    timeout=10,
                )
                if response.status_code not in range(200, 400):
                    log.error(f"LinkedIn response status code {response.status_code}")
                    return JobResponse(jobs=job_list)
            except Exception as e:
                log.error(f"LinkedIn: {str(e)}")
                return JobResponse(jobs=job_list)

            soup = BeautifulSoup(response.text, "html.parser")
            job_cards = soup.find_all("div", class_="base-search-card")
            
            if len(job_cards) == 0:
                return JobResponse(jobs=job_list)

            for job_card in job_cards:
                href_tag = job_card.find("a", class_="base-card__full-link")
                if href_tag and "href" in href_tag.attrs:
                    href = href_tag.attrs["href"].split("?")[0]
                    job_id = href.split("-")[-1]

                    if job_id in seen_ids:
                        continue
                    seen_ids.add(job_id)

                    job_post = self._process_job(job_card, job_id)
                    if job_post:
                        job_list.append(job_post)
                    if not continue_search():
                        break

            if continue_search():
                time.sleep(random.uniform(self.delay, self.delay + self.band_delay))
                start += len(job_cards)

        return JobResponse(jobs=job_list[: scraper_input.results_wanted])

    def _process_job(self, job_card: Tag, job_id: str) -> Optional[JobPost]:
        """
        Parses job card into simplified JobPost (only needed fields)
        """
        # Get salary if available
        compensation = None
        salary_tag = job_card.find("span", class_="job-search-card__salary-info")
        if salary_tag:
            salary_text = salary_tag.get_text(separator=" ").strip()
            try:
                salary_values = [currency_parser(value) for value in salary_text.split("-")]
                if len(salary_values) >= 2:
                    compensation = Compensation(
                        min_amount=int(salary_values[0]),
                        max_amount=int(salary_values[1]),
                        currency=salary_text[0] if salary_text[0] != "$" else "USD",
                    )
            except:
                pass

        # Get title
        title_tag = job_card.find("span", class_="sr-only")
        title = title_tag.get_text(strip=True) if title_tag else "N/A"

        # Get company name
        company_tag = job_card.find("h4", class_="base-search-card__subtitle")
        company_a_tag = company_tag.find("a") if company_tag else None
        company = company_a_tag.get_text(strip=True) if company_a_tag else "N/A"

        # Get location
        location = self._get_location(job_card.find("div", class_="base-search-card__metadata"))

        # Get date posted
        metadata_card = job_card.find("div", class_="base-search-card__metadata")
        datetime_tag = metadata_card.find("time", class_="job-search-card__listdate") if metadata_card else None
        date_posted = None
        if datetime_tag and "datetime" in datetime_tag.attrs:
            try:
                date_posted = datetime.strptime(datetime_tag["datetime"], "%Y-%m-%d")
            except:
                pass

        # Get description from job page (only if needed)
        description = None
        if self.scraper_input.linkedin_fetch_description:
            description = self._get_description(job_id)

        return JobPost(
            id=f"li-{job_id}",
            title=title,
            company_name=company,
            location=location,
            date_posted=date_posted,
            job_url=f"{self.base_url}/jobs/view/{job_id}",
            compensation=compensation,
            description=description,
        )

    def _get_description(self, job_id: str) -> str | None:
        """
        Fetches job description from job page
        """
        try:
            response = self.session.get(f"{self.base_url}/jobs/view/{job_id}", timeout=5)
            response.raise_for_status()
        except:
            return None
            
        if "linkedin.com/signup" in response.url:
            return None

        soup = BeautifulSoup(response.text, "html.parser")
        div_content = soup.find("div", class_=lambda x: x and "show-more-less-html__markup" in x)
        
        if div_content:
            div_content = remove_attributes(div_content)
            description = div_content.prettify(formatter="html")
            if self.scraper_input.description_format == DescriptionFormat.MARKDOWN:
                description = markdown_converter(description)
            return description
        
        return None

    def _get_location(self, metadata_card: Optional[Tag]) -> Location:
        """
        Extracts location from job metadata card
        """
        location = Location(country=Country.from_string(self.country))
        
        if metadata_card:
            location_tag = metadata_card.find("span", class_="job-search-card__location")
            if location_tag:
                location_string = location_tag.text.strip()
                parts = location_string.split(", ")
                if len(parts) == 2:
                    location = Location(city=parts[0], state=parts[1], country=Country.from_string(self.country))
                elif len(parts) == 3:
                    location = Location(city=parts[0], state=parts[1], country=Country.from_string(parts[2]))
        
        return location
