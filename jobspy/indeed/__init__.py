"""
Simplified Indeed Scraper - Only collects fields needed for Job schema
"""
from __future__ import annotations

import math
from datetime import datetime
from typing import Tuple

from jobspy.indeed.constant import job_search_query, api_headers
from jobspy.indeed.util import get_compensation, get_job_type
from jobspy.model import (
    Scraper,
    ScraperInput,
    Site,
    JobPost,
    Location,
    JobResponse,
    JobType,
    DescriptionFormat,
)
from jobspy.util import (
    markdown_converter,
    create_session,
    create_logger,
)

log = create_logger("Indeed")


class Indeed(Scraper):
    def __init__(
        self, proxies: list[str] | str | None = None, ca_cert: str | None = None, user_agent: str | None = None
    ):
        """
        Initializes IndeedScraper with the Indeed API url
        """
        super().__init__(Site.INDEED, proxies=proxies)

        self.session = create_session(
            proxies=self.proxies, ca_cert=ca_cert, is_tls=False
        )
        self.scraper_input = None
        self.jobs_per_page = 100
        self.seen_urls = set()
        self.api_country_code = None
        self.base_url = None
        self.api_url = "https://apis.indeed.com/graphql"

    def scrape(self, scraper_input: ScraperInput) -> JobResponse:
        """
        Scrapes Indeed for jobs with scraper_input criteria
        """
        self.scraper_input = scraper_input
        domain, self.api_country_code = self.scraper_input.country.indeed_domain_value
        self.base_url = f"https://{domain}.indeed.com"
        job_list = []
        page = 1
        cursor = None

        while len(self.seen_urls) < scraper_input.results_wanted + scraper_input.offset:
            log.info(f"search page: {page}")
            jobs, cursor = self._scrape_page(cursor)
            if not jobs:
                log.info(f"found no jobs on page: {page}")
                break
            job_list += jobs
            page += 1
            
        return JobResponse(
            jobs=job_list[scraper_input.offset : scraper_input.offset + scraper_input.results_wanted]
        )

    def _scrape_page(self, cursor: str | None) -> Tuple[list[JobPost], str | None]:
        """
        Scrapes a page of Indeed for jobs
        """
        search_term = (
            self.scraper_input.search_term.replace('"', '\\"')
            if self.scraper_input.search_term
            else ""
        )
        query = job_search_query.format(
            what=(f'what: "{search_term}"' if search_term else ""),
            location=(
                f'location: {{where: "{self.scraper_input.location}", radius: {self.scraper_input.distance}, radiusUnit: MILES}}'
                if self.scraper_input.location
                else ""
            ),
            cursor=f'cursor: "{cursor}"' if cursor else "",
            filters=self._build_filters(),
        )
        
        api_headers_temp = api_headers.copy()
        api_headers_temp["indeed-co"] = self.api_country_code
        
        response = self.session.post(
            self.api_url,
            headers=api_headers_temp,
            json={"query": query},
            timeout=10,
            verify=False,
        )
        
        if not response.ok:
            log.info(f"responded with status code: {response.status_code}")
            return [], None
            
        data = response.json()
        jobs = data["data"]["jobSearch"]["results"]
        new_cursor = data["data"]["jobSearch"]["pageInfo"]["nextCursor"]

        job_list = []
        for job in jobs:
            processed_job = self._process_job(job["job"])
            if processed_job:
                job_list.append(processed_job)

        return job_list, new_cursor

    def _build_filters(self):
        """
        Builds the filters for job type/date filtering
        """
        if self.scraper_input.hours_old:
            return f"""
            filters: {{
                date: {{
                  field: "dateOnIndeed",
                  start: "{self.scraper_input.hours_old}h"
                }}
            }}
            """
        elif self.scraper_input.job_type or self.scraper_input.is_remote:
            job_type_key_mapping = {
                JobType.FULL_TIME: "CF3CP",
                JobType.PART_TIME: "75GKK",
                JobType.CONTRACT: "NJXCK",
                JobType.INTERNSHIP: "VDTG7",
            }
            keys = []
            if self.scraper_input.job_type:
                keys.append(job_type_key_mapping[self.scraper_input.job_type])
            if self.scraper_input.is_remote:
                keys.append("DSQF7")
            if keys:
                keys_str = '", "'.join(keys)
                return f"""
                filters: {{
                  composite: {{
                    filters: [{{
                      keyword: {{
                        field: "attributes",
                        keys: ["{keys_str}"]
                      }}
                    }}]
                  }}
                }}
                """
        return ""

    def _process_job(self, job: dict) -> JobPost | None:
        """
        Parses job dict into simplified JobPost (only needed fields)
        """
        job_url = f'{self.base_url}/viewjob?jk={job["key"]}'
        if job_url in self.seen_urls:
            return None
        self.seen_urls.add(job_url)
        
        # Get description
        description = job["description"]["html"]
        if self.scraper_input.description_format == DescriptionFormat.MARKDOWN:
            description = markdown_converter(description)

        # Get job type from attributes
        job_type = get_job_type(job.get("attributes", []))
        
        # Get date posted
        timestamp_seconds = job["datePublished"] / 1000
        date_posted = datetime.fromtimestamp(timestamp_seconds).strftime("%Y-%m-%d")
        
        # Get location
        loc = job.get("location", {})
        location = Location(
            city=loc.get("city"),
            state=loc.get("admin1Code"),
            country=loc.get("countryCode"),
        )
        
        # Get company name
        company_name = job["employer"].get("name") if job.get("employer") else None
        
        return JobPost(
            id=f'in-{job["key"]}',
            title=job["title"],
            description=description,
            company_name=company_name,
            location=location,
            job_type=job_type,
            compensation=get_compensation(job.get("compensation", {})),
            date_posted=date_posted,
            job_url=job_url,
        )
