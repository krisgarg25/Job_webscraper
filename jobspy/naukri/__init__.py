"""
Simplified Naukri Scraper - Only collects fields needed for Job schema
"""
from __future__ import annotations

import math
import random
import time
from datetime import datetime, date, timedelta
from typing import Optional

import regex as re

from jobspy.naukri.constant import headers as naukri_headers
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
    markdown_converter,
    create_session,
    create_logger,
)

log = create_logger("Naukri")


class Naukri(Scraper):
    base_url = "https://www.naukri.com/jobapi/v3/search"
    delay = 3
    band_delay = 4
    jobs_per_page = 20

    def __init__(
        self, proxies: list[str] | str | None = None, ca_cert: str | None = None, user_agent: str | None = None
    ):
        """
        Initializes NaukriScraper
        """
        super().__init__(Site.NAUKRI, proxies=proxies, ca_cert=ca_cert)
        self.session = create_session(
            proxies=self.proxies,
            ca_cert=ca_cert,
            is_tls=False,
            has_retry=True,
            delay=5,
            clear_cookies=True,
        )
        self.session.headers.update(naukri_headers)
        self.scraper_input = None
        log.info("Naukri scraper initialized")

    def scrape(self, scraper_input: ScraperInput) -> JobResponse:
        """
        Scrapes Naukri API for jobs
        """
        self.scraper_input = scraper_input
        job_list: list[JobPost] = []
        seen_ids = set()
        page = 1
        seconds_old = scraper_input.hours_old * 3600 if scraper_input.hours_old else None
        
        continue_search = lambda: len(job_list) < scraper_input.results_wanted and page <= 50

        while continue_search():
            log.info(f"Scraping page {page} for: {scraper_input.search_term}")
            
            params = {
                "noOfResults": self.jobs_per_page,
                "urlType": "search_by_keyword",
                "searchType": "adv",
                "keyword": scraper_input.search_term,
                "pageNo": page,
                "k": scraper_input.search_term,
                "seoKey": f"{scraper_input.search_term.lower().replace(' ', '-')}-jobs",
                "src": "jobsearchDesk",
                "location": scraper_input.location,
                "remote": "true" if scraper_input.is_remote else None,
            }
            if seconds_old:
                params["days"] = seconds_old // 86400
            params = {k: v for k, v in params.items() if v is not None}
            
            try:
                response = self.session.get(self.base_url, params=params, timeout=10)
                if response.status_code not in range(200, 400):
                    log.error(f"Naukri API error: {response.status_code}")
                    return JobResponse(jobs=job_list)
                    
                data = response.json()
                job_details = data.get("jobDetails", [])
                
                if not job_details:
                    break
            except Exception as e:
                log.error(f"Naukri API request failed: {str(e)}")
                return JobResponse(jobs=job_list)

            for job in job_details:
                job_id = job.get("jobId")
                if not job_id or job_id in seen_ids:
                    continue
                seen_ids.add(job_id)

                job_post = self._process_job(job, job_id)
                if job_post:
                    job_list.append(job_post)
                if not continue_search():
                    break

            if continue_search():
                time.sleep(random.uniform(self.delay, self.delay + self.band_delay))
                page += 1

        return JobResponse(jobs=job_list[:scraper_input.results_wanted])

    def _process_job(self, job: dict, job_id: str) -> Optional[JobPost]:
        """
        Parses job into simplified JobPost (only needed fields)
        """
        title = job.get("title", "N/A")
        company = job.get("companyName", "N/A")
        
        # Get location
        location = self._get_location(job.get("placeholders", []))
        
        # Get compensation
        compensation = self._get_compensation(job.get("placeholders", []))
        
        # Get date
        date_posted = self._parse_date(job.get("footerPlaceholderLabel"), job.get("createdDate"))
        
        # Get URL
        job_url = f"https://www.naukri.com{job.get('jdURL', f'/job/{job_id}')}"
        
        # Get description
        description = job.get("jobDescription")
        if description and self.scraper_input.description_format == DescriptionFormat.MARKDOWN:
            description = markdown_converter(description)

        return JobPost(
            id=f"nk-{job_id}",
            title=title,
            company_name=company,
            location=location,
            date_posted=date_posted,
            job_url=job_url,
            compensation=compensation,
            description=description,
        )

    def _get_location(self, placeholders: list[dict]) -> Location:
        """
        Extracts location from placeholders
        """
        for placeholder in placeholders:
            if placeholder.get("type") == "location":
                location_str = placeholder.get("label", "")
                parts = location_str.split(", ")
                return Location(
                    city=parts[0] if parts else None,
                    state=parts[1] if len(parts) > 1 else None,
                    country=Country.INDIA
                )
        return Location(country=Country.INDIA)

    def _get_compensation(self, placeholders: list[dict]) -> Optional[Compensation]:
        """
        Extracts compensation from placeholders (Indian Lakhs/Crores format)
        """
        for placeholder in placeholders:
            if placeholder.get("type") == "salary":
                salary_text = placeholder.get("label", "").strip()
                if salary_text == "Not disclosed":
                    return None

                # Parse Indian salary format: "12-16 Lacs P.A."
                salary_match = re.match(
                    r"(\d+(?:\.\d+)?)\s*-\s*(\d+(?:\.\d+)?)\s*(Lacs|Lakh|Cr)",
                    salary_text, re.IGNORECASE
                )
                if salary_match:
                    min_sal, max_sal, unit = salary_match.groups()[:3]
                    min_sal, max_sal = float(min_sal), float(max_sal)
                    
                    # Convert to INR
                    if unit.lower() in ("lacs", "lakh"):
                        min_sal *= 100000
                        max_sal *= 100000
                    elif unit.lower() == "cr":
                        min_sal *= 10000000
                        max_sal *= 10000000

                    return Compensation(
                        min_amount=int(min_sal),
                        max_amount=int(max_sal),
                        currency="INR",
                    )
        return None

    def _parse_date(self, label: str, created_date: int) -> Optional[date]:
        """
        Parses date from label or timestamp
        """
        today = datetime.now()
        
        if not label:
            if created_date:
                return datetime.fromtimestamp(created_date / 1000).date()
            return None
            
        label = label.lower()
        if "today" in label or "just now" in label or "few hours" in label:
            return today.date()
        elif "ago" in label:
            match = re.search(r"(\d+)\s*day", label)
            if match:
                days = int(match.group(1))
                return (today - timedelta(days=days)).date()
        elif created_date:
            return datetime.fromtimestamp(created_date / 1000).date()
            
        return None