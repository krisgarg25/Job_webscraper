"""
Simplified LinkedIn utilities - only functions needed for Job schema
"""
from jobspy.model import JobType


def job_type_code(job_type_enum: JobType) -> str:
    """
    Maps JobType enum to LinkedIn filter code
    """
    return {
        JobType.FULL_TIME: "F",
        JobType.PART_TIME: "P",
        JobType.INTERNSHIP: "I",
        JobType.CONTRACT: "C",
        JobType.TEMPORARY: "T",
    }.get(job_type_enum, "")
