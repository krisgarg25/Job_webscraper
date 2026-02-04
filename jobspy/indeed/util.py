"""
Simplified Indeed utilities - only functions needed for Job schema
"""
from jobspy.model import CompensationInterval, JobType, Compensation
from jobspy.util import get_enum_from_job_type


def get_job_type(attributes: list) -> list[JobType]:
    """
    Parses the attributes to get list of job types
    """
    job_types: list[JobType] = []
    for attribute in attributes:
        job_type_str = attribute["label"].replace("-", "").replace(" ", "").lower()
        job_type = get_enum_from_job_type(job_type_str)
        if job_type:
            job_types.append(job_type)
    return job_types


def get_compensation(compensation: dict) -> Compensation | None:
    """
    Parses the job compensation data
    """
    if not compensation:
        return None
        
    if not compensation.get("baseSalary") and not compensation.get("estimated"):
        return None
        
    comp = (
        compensation["baseSalary"]
        if compensation.get("baseSalary")
        else compensation.get("estimated", {}).get("baseSalary")
    )
    
    if not comp:
        return None
        
    interval = _get_compensation_interval(comp.get("unitOfWork", ""))
    if not interval:
        return None
        
    range_data = comp.get("range", {})
    min_range = range_data.get("min")
    max_range = range_data.get("max")
    
    return Compensation(
        interval=interval,
        min_amount=int(min_range) if min_range is not None else None,
        max_amount=int(max_range) if max_range is not None else None,
        currency=(
            compensation.get("estimated", {}).get("currencyCode")
            or compensation.get("currencyCode")
            or "USD"
        ),
    )


def _get_compensation_interval(interval: str) -> CompensationInterval | None:
    """
    Maps interval string to CompensationInterval enum
    """
    if not interval:
        return None
        
    interval_mapping = {
        "DAY": "DAILY",
        "YEAR": "YEARLY",
        "HOUR": "HOURLY",
        "WEEK": "WEEKLY",
        "MONTH": "MONTHLY",
    }
    
    mapped_interval = interval_mapping.get(interval.upper())
    if mapped_interval and mapped_interval in CompensationInterval.__members__:
        return CompensationInterval[mapped_interval]
    
    return None
