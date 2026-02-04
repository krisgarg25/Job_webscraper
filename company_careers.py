"""
Company Careers Scraper
Scrapes jobs directly from tech companies with OPEN APIs.
Companies: Amazon, GitLab, Pinterest, PhonePe, Razorpay, Flipkart
"""
import requests
from datetime import datetime
from typing import List, Dict
import re
import json

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "application/json",
}

def clean_html(raw_html):
    """Clean HTML tags"""
    if not raw_html: return ""
    cleanr = re.compile('<.*?>')
    return re.sub(cleanr, '', raw_html)

def scrape_amazon_jobs(search_term: str = "software engineer", location: str = "India", limit: int = 20) -> List[Dict]:
    """Amazon Jobs API"""
    jobs = []
    try:
        url = "https://www.amazon.jobs/en/search.json"
        params = {
            "base_query": search_term,
            "loc_query": location,
            "result_limit": limit,
            "sort": "relevant"
        }
        response = requests.get(url, params=params, headers=HEADERS, timeout=15)
        if response.ok:
            data = response.json()
            for job in data.get("jobs", []):
                desc = job.get("description_short", "") or job.get("basic_qualifications", "")
                jobs.append({
                    "title": job.get("title"),
                    "company_name": "Amazon",
                    "location": job.get("normalized_location", job.get("city", "")),
                    "description": desc[:500] if desc else "",
                    "short_description": desc[:200] if desc else "",
                    "apply_url": f"https://www.amazon.jobs{job.get('job_path', '')}",
                    "posted_at": job.get("posted_date"),
                    "source": "Amazon Careers",
                    "employment_type": "Full-time",
                    "salary_range": None
                })
    except Exception as e:
        print(f"  Amazon: Error - {e}")
    return jobs

def scrape_gitlab_jobs(search_term: str = "software", location: str = "India", limit: int = 20) -> List[Dict]:
    """GitLab Jobs"""
    jobs = []
    try:
        url = "https://boards-api.greenhouse.io/v1/boards/gitlab/jobs?content=true"
        response = requests.get(url, headers=HEADERS, timeout=15)
        if response.ok:
            data = response.json()
            count = 0
            for job in data.get("jobs", []):
                if count >= limit: break
                title = job.get("title", "")
                loc = job.get("location", {}).get("name", "")
                if search_term.lower() in title.lower():
                    if location.lower() in loc.lower() or "remote" in loc.lower() or "everywhere" in loc.lower():
                        content = clean_html(job.get("content", ""))
                        jobs.append({
                            "title": title,
                            "company_name": "GitLab",
                            "location": loc,
                            "description": content[:500],
                            "short_description": content[:200],
                            "apply_url": job.get("absolute_url"),
                            "posted_at": job.get("updated_at", "")[:10],
                            "source": "GitLab Careers",
                            "employment_type": "Full-time",
                            "salary_range": None
                        })
                        count += 1
    except Exception as e:
        print(f"  GitLab: Error - {e}")
    return jobs

def scrape_pinterest_jobs(search_term: str = "software", location: str = "India", limit: int = 20) -> List[Dict]:
    """Pinterest Jobs"""
    jobs = []
    try:
        url = "https://boards-api.greenhouse.io/v1/boards/pinterest/jobs?content=true"
        response = requests.get(url, headers=HEADERS, timeout=15)
        if response.ok:
            data = response.json()
            count = 0
            for job in data.get("jobs", []):
                if count >= limit: break
                title = job.get("title", "")
                loc = job.get("location", {}).get("name", "")
                if search_term.lower() in title.lower() and (location.lower() in loc.lower() or "remote" in loc.lower()):
                    content = clean_html(job.get("content", ""))
                    jobs.append({
                        "title": title,
                        "company_name": "Pinterest",
                        "location": loc,
                        "description": content[:500],
                        "short_description": content[:200],
                        "apply_url": job.get("absolute_url"),
                        "posted_at": job.get("updated_at", "")[:10],
                        "source": "Pinterest Careers",
                        "employment_type": "Full-time",
                        "salary_range": None
                    })
                    count += 1
    except Exception:
        pass
    return jobs

def scrape_phonepe_jobs(search_term: str = "engineer", location: str = "India", limit: int = 20) -> List[Dict]:
    """PhonePe Jobs"""
    jobs = []
    try:
        url = "https://boards-api.greenhouse.io/v1/boards/phonepe/jobs?content=true"
        response = requests.get(url, headers=HEADERS, timeout=15)
        if response.ok:
            data = response.json()
            count = 0
            for job in data.get("jobs", []):
                if count >= limit: break
                if search_term.lower() in job.get("title", "").lower():
                    content = clean_html(job.get("content", ""))
                    jobs.append({
                        "title": job.get("title"),
                        "company_name": "PhonePe",
                        "location": job.get("location", {}).get("name", "Bangalore"),
                        "description": content[:500],
                        "short_description": content[:200],
                        "apply_url": job.get("absolute_url"),
                        "posted_at": job.get("updated_at", "")[:10],
                        "source": "PhonePe Careers",
                        "employment_type": "Full-time",
                        "salary_range": None
                    })
                    count += 1
    except Exception as e:
        print(f"  PhonePe: Error - {e}")
    return jobs

def scrape_razorpay_jobs(search_term: str = "engineer", location: str = "India", limit: int = 20) -> List[Dict]:
    """Razorpay Jobs"""
    jobs = []
    try:
        url = "https://boards-api.greenhouse.io/v1/boards/razorpaysoftwareprivatelimited/jobs?content=true"
        response = requests.get(url, headers=HEADERS, timeout=15)
        if response.ok:
            data = response.json()
            count = 0
            for job in data.get("jobs", []):
                if count >= limit: break
                if search_term.lower() in job.get("title", "").lower():
                    content = clean_html(job.get("content", ""))
                    jobs.append({
                        "title": job.get("title"),
                        "company_name": "Razorpay",
                        "location": job.get("location", {}).get("name", "Bangalore"),
                        "description": content[:500],
                        "short_description": content[:200],
                        "apply_url": job.get("absolute_url"),
                        "posted_at": job.get("updated_at", "")[:10],
                        "source": "Razorpay Careers",
                        "employment_type": "Full-time",
                        "salary_range": None
                    })
                    count += 1
    except Exception as e:
        print(f"  Razorpay: Error - {e}")
    return jobs

def scrape_flipkart_jobs(search_term: str = "engineer", location: str = "India", limit: int = 20) -> List[Dict]:
    """Flipkart Jobs"""
    jobs = []
    try:
        url = "https://api.lever.co/v0/postings/flipkart?mode=json"
        response = requests.get(url, headers=HEADERS, timeout=15)
        if response.ok:
            data = response.json()
            count = 0
            for job in data:
                if count >= limit: break
                if search_term.lower() in job.get("text", "").lower():
                    jobs.append({
                        "title": job.get("text"),
                        "company_name": "Flipkart",
                        "location": job.get("categories", {}).get("location", "Bangalore"),
                        "description": job.get("descriptionPlain", "")[:500],
                        "short_description": job.get("descriptionPlain", "")[:200],
                        "apply_url": job.get("hostedUrl"),
                        "posted_at": datetime.now().strftime("%Y-%m-%d"),
                        "source": "Flipkart Careers",
                        "employment_type": job.get("categories", {}).get("commitment", "Full-time"),
                        "salary_range": None
                    })
                    count += 1
    except Exception as e:
        print(f"  Flipkart: Error - {e}")
    return jobs

def scrape_all_company_careers(search_term: str = "software engineer", location: str = "India", limit: int = 15) -> List[Dict]:
    """Scrape all company career pages concurrently"""
    from concurrent.futures import ThreadPoolExecutor, as_completed
    
    simple_term = "engineer"
    
        # Removed BrowserStack
    scrapers = [
        ("Amazon", lambda: scrape_amazon_jobs(search_term, location, limit)),
        ("GitLab", lambda: scrape_gitlab_jobs("software", location, limit)),
        ("Pinterest", lambda: scrape_pinterest_jobs("software", location, limit)),
        ("PhonePe", lambda: scrape_phonepe_jobs(simple_term, location, limit)),
        ("Razorpay", lambda: scrape_razorpay_jobs(simple_term, location, limit)),
        ("Flipkart", lambda: scrape_flipkart_jobs(simple_term, location, limit)),
    ]
    
    all_jobs = []
    print("Scraping company career pages...")
    
    with ThreadPoolExecutor(max_workers=6) as executor:
        future_to_company = {executor.submit(scraper): name for name, scraper in scrapers}
        
        for future in as_completed(future_to_company):
            company = future_to_company[future]
            try:
                jobs = future.result()
                print(f"  {company}: {len(jobs)} jobs")
                all_jobs.extend(jobs)
            except Exception as e:
                print(f"  {company}: Error - {e}")
    
    return all_jobs

if __name__ == "__main__":
    print("test run")
    jobs = scrape_all_company_careers(search_term="software engineer", location="India", limit=15)
    print(f"Total: {len(jobs)}")
    for j in jobs:
        print(f"{j['company_name']} - {j['title']}")
