"""
Company-specific navigation patterns for finding investor relations pages.
This module contains structured navigation rules for each Dow 30 company.
"""

COMPANY_NAVIGATION_PATTERNS = {
    "DIS": {  # Walt Disney
        "navigation_hints": {
            "primary_location": "navbar",
            "search_terms": ["investor relations", "investors"],
            "element_types": ["nav", ".navigation", ".menu"],
            "fallback_location": "footer"
        }
    },
    "DOW": {  # Dow Inc.
        "navigation_hints": {
            "primary_location": "footer",
            "search_terms": ["investors", "investor relations"],
            "element_types": ["footer", ".footer", ".site-footer"],
            "scroll_required": True
        }
    },
    "GS": {  # Goldman Sachs
        "navigation_hints": {
            "primary_location": "navbar",
            "search_terms": ["investor relations", "investors"],
            "element_types": ["nav", ".navigation", ".header-nav"],
            "fallback_location": "main",
            "main_domain_paths": ["/investor-relations/", "/investor-relations/default.aspx"]
        }
    },
    "HD": {  # Home Depot
        "navigation_hints": {
            "primary_location": "navbar",
            "search_terms": ["investor relations", "investors"],
            "element_types": ["nav", ".navigation", ".main-nav"],
            "multi_step": True,
            "fallback_location": "footer"
        }
    },
    "HON": {  # Honeywell
        "navigation_hints": {
            "primary_location": "footer",
            "search_terms": ["investors", "investor relations"],
            "element_types": ["footer", ".footer", ".site-footer"],
            "scroll_required": True
        }
    },
    "IBM": {  # IBM
        "navigation_hints": {
            "primary_location": "footer",
            "search_terms": ["investor relations", "investors", "investor"],
            "element_types": ["footer", ".footer", ".site-footer", ".footer-links"],
            "scroll_required": True
        }
    },
    "JNJ": {  # Johnson & Johnson
        "navigation_hints": {
            "primary_location": "navbar",
            "search_terms": ["investors", "investor relations"],
            "element_types": ["nav", ".navigation", ".main-nav"],
            "multi_step": True,
            "fallback_location": "footer"
        }
    },
    "JPM": {  # JPMorgan Chase
        "navigation_hints": {
            "primary_location": "navbar",
            "search_terms": ["investor relations", "investors", "ir"],
            "element_types": ["nav", ".navigation", ".mobile-nav", ".hamburger-menu"],
            "mobile_menu": True,
            "multi_step": True
        }
    },
    "MCD": {  # McDonald's
        "navigation_hints": {
            "primary_location": "navbar",
            "search_terms": ["investors", "investor relations"],
            "element_types": ["nav", ".navigation", ".main-nav"],
            "multi_step": True,
            "fallback_location": "footer"
        }
    },
    "MRK": {  # Merck
        "navigation_hints": {
            "primary_location": "navbar",
            "search_terms": ["investors", "investor relations"],
            "element_types": ["nav", ".navigation", ".main-menu", ".menu"],
            "menu_required": True,
            "fallback_location": "footer"
        }
    },
    "MSFT": {  # Microsoft
        "navigation_hints": {
            "primary_location": "footer",
            "search_terms": ["investors", "investor relations"],
            "element_types": ["footer", ".footer", ".site-footer"],
            "section_keywords": ["company"],
            "scroll_required": True
        }
    },
    "NKE": {  # Nike
        "navigation_hints": {
            "primary_location": "footer",
            "search_terms": ["investors", "investor relations"],
            "element_types": ["footer", ".footer", ".site-footer"],
            "section_keywords": ["company"],
            "scroll_required": True
        }
    },
    "NVDA": {  # NVIDIA
        "navigation_hints": {
            "primary_location": "footer",
            "search_terms": ["investors", "investor relations"],
            "element_types": ["footer", ".footer", ".site-footer"],
            "section_keywords": ["company"],
            "scroll_required": True
        }
    },
    "PG": {  # Procter & Gamble
        "navigation_hints": {
            "primary_location": "footer",
            "search_terms": ["investors", "investor relations"],
            "element_types": ["footer", ".footer", ".site-footer"],
            "section_keywords": ["company"],
            "scroll_required": True
        }
    },
    "CRM": {  # Salesforce
        "navigation_hints": {
            "primary_location": "footer",
            "search_terms": ["investors", "investor relations"],
            "element_types": ["footer", ".footer", ".site-footer"],
            "section_keywords": ["company"],
            "scroll_required": True
        }
    },
    "SHW": {  # Sherwin-Williams
        "navigation_hints": {
            "primary_location": "footer",
            "search_terms": ["investors", "investor relations"],
            "element_types": ["footer", ".footer", ".site-footer"],
            "section_keywords": ["company"],
            "scroll_required": True
        }
    },
    "TRV": {  # Travelers
        "navigation_hints": {
            "primary_location": "footer",
            "search_terms": ["investors", "investor relations"],
            "element_types": ["footer", ".footer", ".site-footer"],
            "section_keywords": ["company"],
            "scroll_required": True
        }
    },
    "UNH": {  # UnitedHealth Group
        "navigation_hints": {
            "primary_location": "footer",
            "search_terms": ["investors", "investor relations"],
            "element_types": ["footer", ".footer", ".site-footer"],
            "section_keywords": ["company"],
            "scroll_required": True
        }
    },
    "VZ": {  # Verizon
        "navigation_hints": {
            "primary_location": "footer",
            "search_terms": ["investors", "investor relations"],
            "element_types": ["footer", ".footer", ".site-footer"],
            "section_keywords": ["about"],
            "scroll_required": True
        }
    },
    "WMT": {  # Walmart
        "navigation_hints": {
            "primary_location": "footer",
            "search_terms": ["investors", "investor relations"],
            "element_types": ["footer", ".footer", ".site-footer"],
            "section_keywords": ["company"],
            "scroll_required": True
        }
    },
    "MMM": {  # 3M
        "navigation_hints": {
            "primary_location": "footer",
            "search_terms": ["investors", "investor relations", "reports", "earnings"],
            "element_types": ["footer", ".footer", ".site-footer"],
            "scroll_required": True,
            "subdomain_patterns": ["investors."],
            "path_patterns": ["/investor-relations/", "/investor-relations/default.aspx"]
        }
    },
    "AMZN": {  # Amazon
        "navigation_hints": {
            "primary_location": "main",
            "search_terms": ["investor resources", "investors", "investor relations"],
            "element_types": ["main", ".content", ".press-content"],
            "scroll_required": True
        }
    },
    "AXP": {  # American Express
        "navigation_hints": {
            "primary_location": "footer",
            "search_terms": ["investor relations", "investors"],
            "element_types": ["footer", ".footer", ".site-footer"],
            "multi_step": True,
            "scroll_required": True
        }
    },
    "AMGN": {  # Amgen
        "navigation_hints": {
            "primary_location": "footer",
            "search_terms": ["investors", "investor relations"],
            "element_types": ["footer", ".footer", ".site-footer"],
            "scroll_required": True
        }
    },
    "AAPL": {  # Apple
        "navigation_hints": {
            "primary_location": "footer",
            "search_terms": ["investors", "investor relations"],
            "element_types": ["footer", ".footer", ".site-footer"],
            "scroll_required": True
        }
    },
    "BA": {  # Boeing
        "navigation_hints": {
            "primary_location": "footer",
            "search_terms": ["investors", "investor relations"],
            "element_types": ["footer", ".footer", ".site-footer"],
            "scroll_required": True
        }
    },
    "CAT": {  # Caterpillar
        "navigation_hints": {
            "primary_location": "footer",
            "search_terms": ["investors", "investor relations", "financial information"],
            "element_types": ["footer", ".footer", ".site-footer"],
            "scroll_required": True,
            "subdomain_patterns": ["investors."],
            "path_patterns": ["/investors/", "/investors/default.aspx"]
        }
    },
    "CVX": {  # Chevron
        "navigation_hints": {
            "primary_location": "navbar",
            "search_terms": ["investors", "investor relations"],
            "element_types": ["nav", ".navigation", ".main-nav"],
            "fallback_location": "footer",
            "subdomain_patterns": ["investor."],
            "path_patterns": ["/", "/home/"]
        }
    },
    "CSCO": {  # Cisco
        "navigation_hints": {
            "primary_location": "search",
            "search_terms": ["investors", "investor relations"],
            "element_types": ["#search", ".search", "[type='search']"],
            "search_required": True
        }
    },
    "KO": {  # Coca-Cola
        "navigation_hints": {
            "primary_location": "navbar",
            "search_terms": ["investors", "investor relations", "financial information"],
            "element_types": ["nav", ".navigation", ".main-nav"],
            "fallback_location": "footer"
        }
    },
    "V": {  # Visa
        "navigation_hints": {
            "primary_location": "footer",
            "search_terms": ["investors", "investor relations"],
            "element_types": ["footer", ".footer", ".site-footer"],
            "scroll_required": True,
            "subdomain_patterns": ["investor."],
            "path_patterns": ["/home/", "/home/default.aspx", "/"]
        }
    }
}

# Additional patterns for companies with missing IR pages
MISSING_IR_COMPANIES = ["V", "AMGN", "WMT", "CSCO", "NKE", "DOW"]

def get_company_pattern(ticker: str) -> dict:
    """Get navigation pattern for a specific company ticker."""
    return COMPANY_NAVIGATION_PATTERNS.get(ticker, {})

def has_custom_pattern(ticker: str) -> bool:
    """Check if a company has a custom navigation pattern."""
    return ticker in COMPANY_NAVIGATION_PATTERNS

def get_all_patterns() -> dict:
    """Get all company navigation patterns."""
    return COMPANY_NAVIGATION_PATTERNS.copy()