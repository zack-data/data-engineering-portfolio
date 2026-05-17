#!/usr/bin/env python3
"""Updates the Fyxer duration in CV.md based on months elapsed since Aug 2025."""

import re
from datetime import date

START_YEAR = 2025
START_MONTH = 8  # August

CV_PATH = "CV.md"


def format_duration(total_months: int) -> str:
    years, months = divmod(total_months, 12)
    if years == 0:
        return f"{months} month{'s' if months != 1 else ''}"
    if months == 0:
        return f"{years} year{'s' if years != 1 else ''}"
    return f"{years} year{'s' if years != 1 else ''} {months} month{'s' if months != 1 else ''}"


def months_elapsed(start_year: int, start_month: int) -> int:
    today = date.today()
    return (today.year - start_year) * 12 + (today.month - start_month)


def update_cv(path: str) -> None:
    with open(path, "r") as f:
        content = f.read()

    total_months = months_elapsed(START_YEAR, START_MONTH)
    duration = format_duration(total_months)

    # Matches the duration on the Fyxer role line, e.g. _9 months_ or _1 year 2 months_
    pattern = r"(`Aug 2025 – Present` &nbsp;·&nbsp; _)[^_]+(_)"
    replacement = rf"\g<1>{duration}\g<2>"

    updated, count = re.subn(pattern, replacement, content)

    if count == 0:
        raise ValueError("Duration pattern not found in CV.md — check the regex.")

    with open(path, "w") as f:
        f.write(updated)

    print(f"Updated Fyxer duration to: {duration}")


if __name__ == "__main__":
    update_cv(CV_PATH)
