"""
FastAPI Server for Job Scraper API
Deploy on Render and trigger via POST /scrape
Includes: Auto-scraping every 12 hours + Keep-alive ping every 12 minutes
"""
from fastapi import FastAPI, BackgroundTasks, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import Optional
from contextlib import asynccontextmanager
import os
import httpx

# Import scraping modules
from jobspy import scrape_jobs
from company_careers import scrape_all_company_careers
from dotenv import load_dotenv
from pymongo import MongoClient
import certifi
from datetime import datetime, date
import math
import re

# APScheduler for cron jobs
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.interval import IntervalTrigger

# Load environment variables
load_dotenv()

# Get the app URL from environment (set this in Render)
APP_URL = os.getenv("APP_URL", "http://localhost:8000")

# Initialize scheduler
scheduler = BackgroundScheduler()

# Track scraping status
scraping_status = {"is_running": False, "last_run": None, "jobs_found": 0}


# =========================
# SCHEDULED JOBS
# =========================
def keep_alive():
    """Ping the server every 12 minutes to prevent Render spin-down"""
    try:
        with httpx.Client() as client:
            response = client.get(f"{APP_URL}/health", timeout=10)
            print(f"🏓 Keep-alive ping: {response.status_code}")
    except Exception as e:
        print(f"❌ Keep-alive ping failed: {e}")


def scheduled_scrape():
    """Auto-scrape every 12 hours with default parameters"""
    global scraping_status
    if scraping_status["is_running"]:
        print("⏭️ Scheduled scrape skipped - already running")
        return
    
    print("🕐 Starting scheduled scrape (every 12 hours)...")
    try:
        # Import here to use the function defined below
        run_scraper(
            search_term="software engineer",
            location="Gurugram, Haryana",
            results_wanted=30,
            hours_old=96
        )
        print("✅ Scheduled scrape completed")
    except Exception as e:
        print(f"❌ Scheduled scrape failed: {e}")


# =========================
# APP LIFESPAN (startup/shutdown)
# =========================
@asynccontextmanager
async def lifespan(app: FastAPI):
    """Start scheduler on startup, stop on shutdown"""
    # Start scheduler
    scheduler.add_job(
        keep_alive,
        IntervalTrigger(minutes=12),
        id="keep_alive",
        name="Keep server alive",
        replace_existing=True
    )
    scheduler.add_job(
        scheduled_scrape,
        IntervalTrigger(hours=12),
        id="scheduled_scrape",
        name="Auto-scrape jobs",
        replace_existing=True
    )
    scheduler.start()
    print("🚀 Scheduler started - Keep-alive: 12min, Auto-scrape: 12h")
    
    yield  # App runs here
    
    # Shutdown
    scheduler.shutdown()
    print("🛑 Scheduler stopped")


app = FastAPI(
    title="Job Scraper API",
    description="API to trigger job scraping from LinkedIn, Indeed, Naukri & company careers",
    version="1.0.0",
    lifespan=lifespan
)

# CORS - Allow your frontend to call this API
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Update with your frontend URL in production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# =========================
# REQUEST/RESPONSE MODELS
# =========================
class ScrapeRequest(BaseModel):
    search_term: str = "software engineer"
    location: str = "Gurugram, Haryana"
    results_wanted: int = 30
    hours_old: int = 12


class ScrapeResponse(BaseModel):
    status: str
    message: str


# =========================
# HELPER FUNCTIONS
# =========================
def clean_nan(value):
    """Convert NaN to None"""
    if isinstance(value, float) and math.isnan(value):
        return None
    return value


def extract_indian_salary(description):
    """Extract salary from Indian job descriptions"""
    if not description:
        return None
    
    text = str(description)
    
    # Indeed format: "Pay: ₹200,000.00 - ₹300,000.00 per year"
    pay_pattern = r'Pay:\s*(?:From\s*)?(?:Up\s*to\s*)?₹?\s*([\d,]+(?:\.\d+)?)\s*(?:-|to|–)?\s*₹?\s*([\d,]+(?:\.\d+)?)?\s*(?:per\s*)?(year|month|annum)'
    match = re.search(pay_pattern, text, re.IGNORECASE)
    if match:
        min_sal, max_sal, period = match.group(1), match.group(2), match.group(3).lower()
        period_str = "per year" if period in ["year", "annum"] else "per month"
        return f"₹{min_sal} - ₹{max_sal} {period_str}" if max_sal else f"₹{min_sal} {period_str}"
    
    # LPA format: "10-15 LPA"
    lpa_pattern = r'(\d+(?:\.\d+)?)\s*(?:-|to|–)\s*(\d+(?:\.\d+)?)\s*(?:lpa|lakhs?\s*(?:per\s*)?(?:annum|p\.a\.?))'
    match = re.search(lpa_pattern, text, re.IGNORECASE)
    if match:
        return f"₹{match.group(1)} - {match.group(2)} LPA"
    
    # Single LPA: "15 LPA"
    single_lpa = r'(?:upto\s*|up\s*to\s*)?(\d+(?:\.\d+)?)\s*(?:lpa|lakhs?\s*(?:per\s*)?annum)'
    match = re.search(single_lpa, text, re.IGNORECASE)
    if match:
        return f"₹{match.group(1)} LPA"
    
    # CTC format: "CTC: 10-15 lakhs"
    ctc_pattern = r'ctc[:\s]+(\d+(?:\.\d+)?)\s*(?:-|to|–)\s*(\d+(?:\.\d+)?)\s*(?:lakhs?|lpa)?'
    match = re.search(ctc_pattern, text, re.IGNORECASE)
    if match:
        return f"₹{match.group(1)} - {match.group(2)} LPA"
    
    return None


def map_job_type(job_type_str):
    """Map job_type to employment_type enum"""
    if not job_type_str:
        return "Full-time"
    job_type_lower = str(job_type_str).lower()
    if "intern" in job_type_lower:
        return "Internship"
    elif "contract" in job_type_lower or "temporary" in job_type_lower:
        return "Contract"
    return "Full-time"


def transform_jobspy_to_schema(doc):
    """Transform jobspy scraped data to Job schema"""
    for k, v in doc.items():
        doc[k] = clean_nan(v)
    
    # Parse date
    posted_date = doc.get("date_posted")
    if isinstance(posted_date, date):
        posted_date = datetime.combine(posted_date, datetime.min.time())
    elif isinstance(posted_date, (int, float)) and posted_date:
        posted_date = datetime.fromtimestamp(posted_date / 1000)
    
    # Build salary
    salary_range = None
    min_amt, max_amt = doc.get("min_amount"), doc.get("max_amount")
    if min_amt and max_amt:
        currency = doc.get("currency", "INR")
        interval = doc.get("interval", "yearly")
        salary_range = f"{currency} {min_amt} - {max_amt} ({interval})"
    elif min_amt:
        salary_range = f"{doc.get('currency', 'INR')} {min_amt}"
    else:
        salary_range = extract_indian_salary(doc.get("description"))
    
    # Build description
    description = str(doc.get("description") or "")
    short_description = description[:200] + "..." if len(description) > 200 else description
    
    return {
        "title": doc.get("title", "Unknown Title"),
        "company_name": doc.get("company_name") or doc.get("company", "Unknown Company"),
        "location": str(doc.get("location", "Unknown Location")),
        "description": description,
        "short_description": short_description,
        "employment_type": map_job_type(doc.get("job_type")),
        "salary_range": salary_range,
        "apply_url": doc.get("job_url"),
        "eligibility_criteria": {
            "min_cgpa": 0,
            "allowed_branches": [],
            "batch_years": []
        },
        "source": str(doc.get("site", "Unknown")).capitalize(),
        "is_verified": False,
        "is_active": True,
        "posted_at": posted_date or datetime.now(),
        "expires_at": None
    }


def transform_company_to_schema(job):
    """Transform company careers job to Job schema"""
    posted_at = job.get("posted_at")
    if isinstance(posted_at, str):
        try:
            posted_at = datetime.strptime(posted_at, "%Y-%m-%d")
        except:
            posted_at = datetime.now()
    
    description = job.get("description", "")
    return {
        "title": job.get("title", "Unknown Title"),
        "company_name": job.get("company_name", "Unknown Company"),
        "location": job.get("location", "Unknown Location"),
        "description": description,
        "short_description": job.get("short_description", description[:200]),
        "employment_type": job.get("employment_type", "Full-time"),
        "salary_range": job.get("salary_range"),
        "apply_url": job.get("apply_url"),
        "eligibility_criteria": {
            "min_cgpa": 0,
            "allowed_branches": [],
            "batch_years": []
        },
        "source": job.get("source", "Company Careers"),
        "is_verified": False,
        "is_active": True,
        "posted_at": posted_at or datetime.now(),
        "expires_at": None
    }


def run_scraper(search_term: str, location: str, results_wanted: int, hours_old: int):
    """Main scraping function"""
    global scraping_status
    scraping_status["is_running"] = True
    
    try:
        # Scrape from job boards
        print(f"📌 Scraping job boards for '{search_term}' in '{location}'...")
        jobs = scrape_jobs(
            site_name=["indeed", "linkedin", "naukri"],
            search_term=search_term,
            location=location,
            results_wanted=results_wanted,
            hours_old=hours_old,
            country_indeed="India",
        )
        print(f"   Found {len(jobs)} jobs from job boards")
        
        # Filter to required columns
        REQUIRED_FIELDS = [
            'title', 'company', 'company_name', 'location', 'description', 
            'job_type', 'job_url', 'site', 'date_posted',
            'min_amount', 'max_amount', 'currency', 'interval'
        ]
        available_cols = [col for col in REQUIRED_FIELDS if col in jobs.columns]
        jobs_filtered = jobs[available_cols]
        jobs_data = jobs_filtered.to_dict(orient="records")
        
        # Scrape from company careers
        print("📌 Scraping company career pages...")
        company_jobs = scrape_all_company_careers(
            search_term=search_term,
            location="India",
            limit=10
        )
        print(f"   Found {len(company_jobs)} jobs from company careers")
        
        # Transform all jobs
        transformed_jobs = []
        for doc in jobs_data:
            transformed_jobs.append(transform_jobspy_to_schema(doc))
        for job in company_jobs:
            transformed_jobs.append(transform_company_to_schema(job))
        
        print(f"📌 Total jobs to insert: {len(transformed_jobs)}")
        
        # Insert to MongoDB
        mongo_url = os.getenv("MongoDB_URL")
        if mongo_url and transformed_jobs:
            client = MongoClient(
                mongo_url,
                tls=True,
                tlsAllowInvalidCertificates=False,
                tlsCAFile=certifi.where()
            )
            db = client.get_database("ideathon")
            collection = db.get_collection("jobs")
            result = collection.insert_many(transformed_jobs)
            print(f"✅ Inserted {len(result.inserted_ids)} jobs into MongoDB")
            
            scraping_status["jobs_found"] = len(result.inserted_ids)
        else:
            scraping_status["jobs_found"] = len(transformed_jobs)
        
        scraping_status["last_run"] = datetime.now().isoformat()
        
    except Exception as e:
        print(f"❌ Scraping Error: {e}")
        raise e
    finally:
        scraping_status["is_running"] = False


# =========================
# API ENDPOINTS
# =========================
@app.get("/")
async def root():
    """Health check endpoint"""
    return {"status": "ok", "message": "Job Scraper API is running"}


@app.get("/health")
async def health():
    """Health check for Render"""
    return {"status": "healthy"}


@app.post("/scrape", response_model=ScrapeResponse)
async def trigger_scrape(request: ScrapeRequest, background_tasks: BackgroundTasks):
    """
    Trigger job scraping process.
    This runs in the background so the API returns immediately.
    """
    if scraping_status["is_running"]:
        raise HTTPException(status_code=409, detail="Scraping is already in progress")
    
    background_tasks.add_task(
        run_scraper,
        request.search_term,
        request.location,
        request.results_wanted,
        request.hours_old
    )
    
    return ScrapeResponse(
        status="started",
        message=f"Scraping started for '{request.search_term}' in '{request.location}'"
    )


@app.get("/status")
async def get_status():
    """Get current scraping status"""
    return scraping_status


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
