#!/usr/bin/env python3
"""
Guidance + Google AI structured extraction for earnings reports.
Uses Guidance with Gemini backend to extract structured JSON from Docling text.
"""

import json
import logging
import re
from typing import Dict, Any, Optional, List
from pathlib import Path

import guidance
import google.generativeai as genai
from google.ai.generativelanguage_v1beta.types import content

logger = logging.getLogger(__name__)

# Schema for structured earnings data
EARNINGS_SCHEMA = {
    "type": "object",
    "properties": {
        "ticker": {"type": "string", "pattern": "^[A-Z]{1,5}$"},
        "period": {"type": "string", "pattern": "^\\d{4}-Q[1-4]$"},
        "published": {"type": "string", "format": "date"},
        "report_type": {"type": "string", "enum": ["press", "presentation", "supplemental"]},
        "revenue": {
            "type": "object",
            "properties": {
                "value": {"type": "number"},
                "unit": {"type": "string", "enum": ["USD"]},
                "scale": {"type": "string", "enum": ["B", "M", "K", "null"]}
            },
            "required": ["value", "unit", "scale"]
        },
        "eps_diluted": {"type": "number"},
        "guidance_summary": {"type": "string"},
        "key_highlights": {
            "type": "array",
            "items": {"type": "string"},
            "maxItems": 5
        },
        "fields_missing": {
            "type": "array", 
            "items": {"type": "string"}
        },
        "confidence": {"type": "number", "minimum": 0, "maximum": 1}
    },
    "required": ["ticker", "period", "published", "report_type", "confidence"]
}

class GuidanceExtractor:
    """Handles structured extraction using Guidance + Google AI."""
    
    def __init__(self, 
                 api_key: Optional[str] = None,
                 model_name: str = "gemini-1.5-pro",
                 temperature: float = 0.1):
        """
        Initialize the extractor.
        
        Args:
            api_key: Google AI API key (if None, uses env var)
            model_name: Gemini model to use
            temperature: Generation temperature
        """
        self.model_name = model_name
        self.temperature = temperature
        
        # Configure Google AI
        if api_key:
            genai.configure(api_key=api_key)
        
        # Set up Guidance with Google backend
        try:
            guidance.llm = guidance.models.GoogleAI(model_name, api_key=api_key)
            logger.info(f"Initialized Guidance with Google AI model: {model_name}")
        except Exception as e:
            logger.error(f"Failed to initialize Guidance with Google AI: {e}")
            raise
        
        # Create the guidance template
        self._create_template()
    
    def _create_template(self):
        """Create the Guidance template for structured extraction."""
        self.template = guidance('''
{{#system~}}
You are an expert financial data extraction system. Extract structured information from earnings reports into valid JSON.

CRITICAL REQUIREMENTS:
1. Output ONLY valid JSON matching the exact schema provided
2. No explanatory text, comments, or markdown formatting
3. All fields must match the specified data types and constraints
4. Use "null" (string) for missing numerical values in scale field
5. Be conservative with confidence scores - only use >0.8 for very clear data

REVENUE EXTRACTION RULES:
- Look for: "Revenue", "Net sales", "Total revenue", "Net revenues"  
- Convert to millions: Billions → multiply by 1000, Thousands → divide by 1000
- Scale: "B" for billions, "M" for millions, "K" for thousands, "null" if unclear
- If multiple revenue figures, use the primary/total revenue

EPS EXTRACTION RULES:
- Look for: "Diluted EPS", "Earnings per share - diluted", "Diluted earnings per share"
- Use actual reported number, not adjusted/non-GAAP unless only option
- Negative values are valid for losses

PERIOD EXTRACTION:
- Format as YYYY-Q# (e.g., "2024-Q1", "2024-Q2")
- Q1: Jan-Mar, Q2: Apr-Jun, Q3: Jul-Sep, Q4: Oct-Dec
{{~/system}}

{{#user~}}
Extract structured data from this earnings report text into JSON matching this exact schema:

SCHEMA:
```json
{
  "ticker": "string (1-5 uppercase letters)",
  "period": "string (YYYY-Q# format)", 
  "published": "string (YYYY-MM-DD format)",
  "report_type": "string (press|presentation|supplemental)",
  "revenue": {
    "value": "number (in millions)",
    "unit": "USD", 
    "scale": "string (B|M|K|null)"
  },
  "eps_diluted": "number (can be negative)",
  "guidance_summary": "string (brief summary of forward guidance)",
  "key_highlights": ["array of up to 5 key points"],
  "fields_missing": ["array of field names that couldn't be extracted"],
  "confidence": "number (0-1, overall confidence in extraction)"
}
```

EARNINGS REPORT TEXT:
{{text}}

TABLE DATA (if available):
{{table_text}}

METADATA:
- Ticker: {{ticker}}
- Period: {{period}} 
- Published: {{published}}
- Report Type: {{report_type}}

OUTPUT (valid JSON only):
{{~/user}}

{{#assistant~}}
{{gen 'structured_json' pattern='\\{[^}]*\\}' max_tokens=2000}}
{{~/assistant}}
''')
    
    def extract_from_text(self,
                         text: str,
                         ticker: str,
                         period: str,
                         published: str,
                         report_type: str,
                         table_text: str = "") -> Dict[str, Any]:
        """
        Extract structured data from earnings report text.
        
        Args:
            text: Main text content from Docling
            ticker: Stock ticker symbol
            period: Period in YYYY-Q# format
            published: Publication date (YYYY-MM-DD)
            report_type: Type of report
            table_text: Optional table content
            
        Returns:
            Structured data dictionary
        """
        logger.info(f"Extracting structured data for {ticker} {period}")
        
        try:
            # Truncate text if too long (Gemini context limits)
            max_chars = 100000
            if len(text) > max_chars:
                text = text[:max_chars] + "\n\n[TEXT TRUNCATED]"
                logger.warning(f"Truncated text to {max_chars} characters")
            
            if len(table_text) > 10000:
                table_text = table_text[:10000] + "\n\n[TABLES TRUNCATED]"
            
            # Execute the guidance template
            result = self.template(
                text=text,
                table_text=table_text,
                ticker=ticker,
                period=period,
                published=published,
                report_type=report_type
            )
            
            # Extract and parse the JSON
            json_str = result['structured_json'].strip()
            
            # Clean up any potential formatting issues
            json_str = self._clean_json_output(json_str)
            
            # Parse JSON
            structured_data = json.loads(json_str)
            
            # Validate against schema
            self._validate_output(structured_data)
            
            # Ensure required fields from metadata
            structured_data.update({
                'ticker': ticker,
                'period': period,
                'published': published,
                'report_type': report_type
            })
            
            logger.info(f"Successfully extracted structured data with confidence {structured_data.get('confidence', 0)}")
            return structured_data
            
        except Exception as e:
            logger.error(f"Failed to extract structured data: {e}")
            
            # Return fallback structure
            return self._create_fallback_structure(ticker, period, published, report_type, str(e))
    
    def _clean_json_output(self, json_str: str) -> str:
        """Clean and normalize JSON output from the model."""
        # Remove any markdown formatting
        json_str = re.sub(r'```json\s*', '', json_str)
        json_str = re.sub(r'```\s*$', '', json_str)
        
        # Remove any leading/trailing whitespace
        json_str = json_str.strip()
        
        # Ensure it starts with { and ends with }
        if not json_str.startswith('{'):
            # Try to find the first {
            start_idx = json_str.find('{')
            if start_idx >= 0:
                json_str = json_str[start_idx:]
        
        if not json_str.endswith('}'):
            # Try to find the last }
            end_idx = json_str.rfind('}')
            if end_idx >= 0:
                json_str = json_str[:end_idx + 1]
        
        return json_str
    
    def _validate_output(self, data: Dict[str, Any]):
        """Basic validation of extracted data."""
        required_fields = ['ticker', 'period', 'published', 'report_type', 'confidence']
        
        for field in required_fields:
            if field not in data:
                raise ValueError(f"Missing required field: {field}")
        
        # Validate confidence range
        confidence = data.get('confidence')
        if not isinstance(confidence, (int, float)) or not 0 <= confidence <= 1:
            raise ValueError(f"Invalid confidence value: {confidence}")
        
        # Validate period format
        period = data.get('period')
        if not re.match(r'^\d{4}-Q[1-4]$', period):
            raise ValueError(f"Invalid period format: {period}")
        
        # Validate ticker format  
        ticker = data.get('ticker')
        if not re.match(r'^[A-Z]{1,5}$', ticker):
            raise ValueError(f"Invalid ticker format: {ticker}")
    
    def _create_fallback_structure(self,
                                 ticker: str,
                                 period: str, 
                                 published: str,
                                 report_type: str,
                                 error_msg: str) -> Dict[str, Any]:
        """Create fallback structure when extraction fails."""
        return {
            'ticker': ticker,
            'period': period,
            'published': published,
            'report_type': report_type,
            'revenue': {
                'value': None,
                'unit': 'USD',
                'scale': 'null'
            },
            'eps_diluted': None,
            'guidance_summary': 'Extraction failed - see error log',
            'key_highlights': [],
            'fields_missing': ['revenue', 'eps_diluted', 'guidance_summary', 'key_highlights'],
            'confidence': 0.0,
            'extraction_error': error_msg
        }
    
    def extract_from_files(self,
                          text_file: Path,
                          ticker: str,
                          period: str,
                          published: str,
                          report_type: str,
                          table_files: Optional[List[Path]] = None) -> Dict[str, Any]:
        """
        Extract structured data from Docling output files.
        
        Args:
            text_file: Path to main text file
            ticker: Stock ticker
            period: Period in YYYY-Q# format  
            published: Publication date
            report_type: Report type
            table_files: Optional list of table CSV files
            
        Returns:
            Structured data dictionary
        """
        # Read main text
        try:
            with open(text_file, 'r', encoding='utf-8') as f:
                text = f.read()
        except Exception as e:
            logger.error(f"Failed to read text file {text_file}: {e}")
            text = ""
        
        # Read table data if available
        table_text = ""
        if table_files:
            table_contents = []
            for table_file in table_files[:5]:  # Limit to first 5 tables
                try:
                    with open(table_file, 'r', encoding='utf-8') as f:
                        table_content = f.read()
                        table_contents.append(f"TABLE: {table_file.name}\n{table_content}")
                except Exception as e:
                    logger.warning(f"Failed to read table file {table_file}: {e}")
            
            table_text = "\n\n".join(table_contents)
        
        return self.extract_from_text(
            text=text,
            ticker=ticker,
            period=period,
            published=published,
            report_type=report_type,
            table_text=table_text
        )


def create_extractor(api_key: Optional[str] = None, 
                    model_name: str = "gemini-1.5-pro") -> GuidanceExtractor:
    """
    Factory function to create a GuidanceExtractor instance.
    
    Args:
        api_key: Google AI API key
        model_name: Gemini model name
        
    Returns:
        Configured GuidanceExtractor instance
    """
    return GuidanceExtractor(api_key=api_key, model_name=model_name)
