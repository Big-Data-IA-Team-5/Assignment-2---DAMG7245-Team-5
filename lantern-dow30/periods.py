#!/usr/bin/env python3
"""
Period inference utilities for earnings reports.
Extracts YYYY-Q# format from titles and publication dates.
"""

import re
import logging
from datetime import datetime
from typing import Optional, Tuple

logger = logging.getLogger(__name__)

# Regex patterns for period extraction
QUARTER_PATTERNS = [
    # Q1 2024, Q2 2024, etc.
    r'(?i)q([1-4])\s+(\d{4})',
    # First Quarter 2024, Second Quarter 2024, etc.
    r'(?i)(first|second|third|fourth)\s+quarter\s+(\d{4})',
    # 2024 Q1, 2024 Q2, etc.
    r'(\d{4})\s+q([1-4])',
    # Three months ended March 31, 2024 (Q1)
    r'(?i)three\s+months?\s+ended\s+(?:march|june|september|december)\s+\d{1,2},?\s+(\d{4})',
    # Quarter ended March 31, 2024 (Q1)
    r'(?i)quarter\s+ended\s+(?:march|june|september|december)\s+\d{1,2},?\s+(\d{4})',
    # Fiscal patterns: FY24 Q1, etc.
    r'(?i)fy(\d{2,4})\s+q([1-4])',
    # 1Q24, 2Q24, etc.
    r'([1-4])q(\d{2,4})',
]

# Month to quarter mapping
MONTH_TO_QUARTER = {
    1: 1, 2: 1, 3: 1,     # Q1: Jan-Mar
    4: 2, 5: 2, 6: 2,     # Q2: Apr-Jun
    7: 3, 8: 3, 9: 3,     # Q3: Jul-Sep
    10: 4, 11: 4, 12: 4   # Q4: Oct-Dec
}

# Quarter end months (common patterns)
QUARTER_END_MONTHS = {
    'march': 1, 'mar': 1,
    'june': 2, 'jun': 2,
    'september': 3, 'sep': 3, 'sept': 3,
    'december': 4, 'dec': 4
}

# Word to number mapping for quarters
QUARTER_WORDS = {
    'first': 1, '1st': 1,
    'second': 2, '2nd': 2,
    'third': 3, '3rd': 3,
    'fourth': 4, '4th': 4
}


def extract_period_from_title(title: str) -> Optional[str]:
    """Extract period (YYYY-Q#) from report title."""
    if not title:
        return None
    
    title_clean = title.strip()
    logger.debug(f"Extracting period from title: {title_clean}")
    
    # Try each pattern
    for pattern in QUARTER_PATTERNS:
        match = re.search(pattern, title_clean)
        if match:
            groups = match.groups()
            
            # Handle different pattern formats
            if len(groups) == 2:
                if groups[0].isdigit() and groups[1].isdigit():
                    # Could be (quarter, year) or (year, quarter)
                    first, second = groups
                    if len(first) == 4:  # year first
                        year, quarter = first, second
                    else:  # quarter first
                        quarter, year = first, second
                elif groups[0] in QUARTER_WORDS:
                    # Word form like "first quarter 2024"
                    quarter = str(QUARTER_WORDS[groups[0].lower()])
                    year = groups[1]
                else:
                    continue
                    
                # Normalize year format
                if len(year) == 2:
                    year = f"20{year}" if int(year) < 50 else f"19{year}"
                    
                # Validate quarter and year
                if 1 <= int(quarter) <= 4 and 2000 <= int(year) <= 2030:
                    period = f"{year}-Q{quarter}"
                    logger.info(f"Extracted period from title: {period}")
                    return period
    
    logger.debug("No period pattern found in title")
    return None


def extract_period_from_date(published_iso: str) -> Optional[str]:
    """Extract period (YYYY-Q#) from publication date."""
    if not published_iso:
        return None
        
    try:
        # Parse ISO date
        if 'T' in published_iso:
            date_part = published_iso.split('T')[0]
        else:
            date_part = published_iso
            
        date_obj = datetime.fromisoformat(date_part)
        year = date_obj.year
        month = date_obj.month
        
        # Map month to quarter
        quarter = MONTH_TO_QUARTER.get(month)
        if quarter:
            period = f"{year}-Q{quarter}"
            logger.debug(f"Extracted period from date {published_iso}: {period}")
            return period
            
    except (ValueError, AttributeError) as e:
        logger.warning(f"Failed to parse date {published_iso}: {e}")
    
    return None


def infer_period(title: str, published_iso: str, report_type: str = "press") -> str:
    """Infer period from title and/or publication date."""
    logger.info(f"Inferring period for report: {title[:100]}...")
    
    # Try title first (more accurate for quarterly reports)
    period = extract_period_from_title(title)
    if period:
        return period
    
    # Fall back to publication date
    period = extract_period_from_date(published_iso)
    if period:
        logger.info(f"Using period from publication date: {period}")
        return period
    
    # If all else fails, raise error
    raise ValueError(f"Cannot determine period from title '{title}' or date '{published_iso}'")


def normalize_period(period_str: str) -> str:
    """Normalize period string to YYYY-Q# format."""
    if not period_str:
        raise ValueError("Period string cannot be empty")
    
    period_clean = period_str.strip().upper()
    
    # Already in correct format
    if re.match(r'^\d{4}-Q[1-4]$', period_clean):
        return period_clean
    
    # Try to extract and normalize
    match = re.search(r'(\d{4}).*?([1-4])', period_clean)
    if match:
        year, quarter = match.groups()
        return f"{year}-Q{quarter}"
    
    raise ValueError(f"Invalid period format: {period_str}")
