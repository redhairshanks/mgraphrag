"""
Data Validation Utilities for PKG 2.0 GraphRAG Implementation

Provides data cleaning and validation functions to ensure data quality
and prevent database indexing issues with large text values.
"""

import logging
from typing import Dict, Any, Optional
import re

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class DataValidator:
    """
    Data validation and cleaning utilities for knowledge graph ingestion.
    Handles text size limits, data type validation, and content cleaning.
    """
    
    # Neo4j RANGE index limits (approximate)
    MAX_INDEXED_TEXT_LENGTH = 32000  # ~32KB limit for RANGE indexes
    MAX_TITLE_LENGTH = 8000          # Reasonable limit for titles
    MAX_ABSTRACT_LENGTH = 50000      # Reasonable limit for abstracts
    
    def __init__(self):
        """Initialize data validator"""
        self.validation_stats = {
            "truncated_titles": 0,
            "truncated_abstracts": 0,
            "cleaned_text_fields": 0,
            "validation_errors": 0
        }
    
    def clean_text(self, text: Optional[str]) -> Optional[str]:
        """
        Clean text content by removing problematic characters and normalizing whitespace
        
        Args:
            text: Input text to clean
            
        Returns:
            Cleaned text or None if input was None/empty
        """
        if not text or not isinstance(text, str):
            return None
        
        try:
            # Remove control characters except newline and tab
            text = re.sub(r'[\x00-\x08\x0B\x0C\x0E-\x1F\x7F]', '', text)
            
            # Normalize whitespace
            text = re.sub(r'\s+', ' ', text)
            
            # Strip leading/trailing whitespace
            text = text.strip()
            
            # Return None if empty after cleaning
            if not text:
                return None
            
            self.validation_stats["cleaned_text_fields"] += 1
            return text
            
        except Exception as e:
            logger.warning(f"Error cleaning text: {str(e)}")
            self.validation_stats["validation_errors"] += 1
            return text  # Return original if cleaning fails
    
    def validate_title(self, title: Optional[str], entity_type: str = "Paper") -> Optional[str]:
        """
        Validate and truncate title if necessary
        
        Args:
            title: Input title
            entity_type: Type of entity (for logging)
            
        Returns:
            Validated title within size limits
        """
        if not title:
            return None
        
        # Clean the text first
        title = self.clean_text(title)
        if not title:
            return None
        
        # Check length and truncate if necessary
        if len(title) > self.MAX_TITLE_LENGTH:
            original_length = len(title)
            title = title[:self.MAX_TITLE_LENGTH].rstrip()
            
            # Try to break at a word boundary
            last_space = title.rfind(' ')
            if last_space > self.MAX_TITLE_LENGTH * 0.8:  # If we can break at 80% of max length
                title = title[:last_space]
            
            title += "..."  # Indicate truncation
            
            logger.warning(f"{entity_type} title truncated from {original_length} to {len(title)} characters")
            self.validation_stats["truncated_titles"] += 1
        
        return title
    
    def validate_abstract(self, abstract: Optional[str], entity_type: str = "Paper") -> Optional[str]:
        """
        Validate and truncate abstract if necessary
        
        Args:
            abstract: Input abstract
            entity_type: Type of entity (for logging)
            
        Returns:
            Validated abstract within size limits
        """
        if not abstract:
            return None
        
        # Clean the text first
        abstract = self.clean_text(abstract)
        if not abstract:
            return None
        
        # Check length and truncate if necessary
        if len(abstract) > self.MAX_ABSTRACT_LENGTH:
            original_length = len(abstract)
            abstract = abstract[:self.MAX_ABSTRACT_LENGTH].rstrip()
            
            # Try to break at a sentence boundary
            last_period = abstract.rfind('. ')
            if last_period > self.MAX_ABSTRACT_LENGTH * 0.9:  # If we can break at 90% of max length
                abstract = abstract[:last_period + 1]
            
            abstract += "... [TRUNCATED]"
            
            logger.warning(f"{entity_type} abstract truncated from {original_length} to {len(abstract)} characters")
            self.validation_stats["truncated_abstracts"] += 1
        
        return abstract
    
    def validate_paper_data(self, paper_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Validate and clean paper data
        
        Args:
            paper_data: Dictionary containing paper data
            
        Returns:
            Cleaned paper data
        """
        try:
            # Validate title
            if 'title' in paper_data:
                paper_data['title'] = self.validate_title(paper_data['title'], "Paper")
            
            # Validate abstract
            if 'abstract' in paper_data:
                paper_data['abstract'] = self.validate_abstract(paper_data['abstract'], "Paper")
            
            # Clean other text fields
            text_fields = ['journal_title', 'authors_string', 'keywords']
            for field in text_fields:
                if field in paper_data:
                    paper_data[field] = self.clean_text(paper_data[field])
            
            return paper_data
            
        except Exception as e:
            logger.error(f"Error validating paper data: {str(e)}")
            self.validation_stats["validation_errors"] += 1
            return paper_data
    
    def validate_patent_data(self, patent_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Validate and clean patent data
        
        Args:
            patent_data: Dictionary containing patent data
            
        Returns:
            Cleaned patent data
        """
        try:
            # Validate title
            if 'title' in patent_data:
                patent_data['title'] = self.validate_title(patent_data['title'], "Patent")
            
            # Validate abstract
            if 'abstract' in patent_data:
                patent_data['abstract'] = self.validate_abstract(patent_data['abstract'], "Patent")
            
            # Clean other text fields
            text_fields = ['assignee_name', 'inventor_names']
            for field in text_fields:
                if field in patent_data:
                    patent_data[field] = self.clean_text(patent_data[field])
            
            return patent_data
            
        except Exception as e:
            logger.error(f"Error validating patent data: {str(e)}")
            self.validation_stats["validation_errors"] += 1
            return patent_data
    
    def validate_clinical_trial_data(self, trial_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Validate and clean clinical trial data
        
        Args:
            trial_data: Dictionary containing clinical trial data
            
        Returns:
            Cleaned trial data
        """
        try:
            # Validate titles
            if 'brief_title' in trial_data:
                trial_data['brief_title'] = self.validate_title(trial_data['brief_title'], "ClinicalTrial")
            
            if 'official_title' in trial_data:
                trial_data['official_title'] = self.validate_title(trial_data['official_title'], "ClinicalTrial")
            
            # Validate summary (treat as abstract)
            if 'brief_summaries' in trial_data:
                trial_data['brief_summaries'] = self.validate_abstract(trial_data['brief_summaries'], "ClinicalTrial")
            
            # Clean other text fields
            text_fields = ['lead_sponsor', 'collaborators', 'conditions']
            for field in text_fields:
                if field in trial_data:
                    trial_data[field] = self.clean_text(trial_data[field])
            
            return trial_data
            
        except Exception as e:
            logger.error(f"Error validating clinical trial data: {str(e)}")
            self.validation_stats["validation_errors"] += 1
            return trial_data
    
    def validate_batch_data(self, batch_data: list, entity_type: str) -> list:
        """
        Validate a batch of entity data
        
        Args:
            batch_data: List of entity dictionaries
            entity_type: Type of entities being validated
            
        Returns:
            List of validated entity dictionaries
        """
        if not batch_data:
            return batch_data
        
        validation_functions = {
            'Paper': self.validate_paper_data,
            'Patent': self.validate_patent_data,
            'ClinicalTrial': self.validate_clinical_trial_data
        }
        
        validation_func = validation_functions.get(entity_type)
        if not validation_func:
            # For entities without specific validation, just clean text fields
            return [self._clean_generic_entity(entity) for entity in batch_data]
        
        return [validation_func(entity) for entity in batch_data]
    
    def _clean_generic_entity(self, entity_data: Dict[str, Any]) -> Dict[str, Any]:
        """Clean generic entity data by cleaning all text fields"""
        try:
            for key, value in entity_data.items():
                if isinstance(value, str):
                    entity_data[key] = self.clean_text(value)
            return entity_data
        except Exception as e:
            logger.warning(f"Error cleaning generic entity: {str(e)}")
            return entity_data
    
    def get_validation_stats(self) -> Dict[str, Any]:
        """Get validation statistics"""
        return {
            **self.validation_stats,
            "total_validations": sum(self.validation_stats.values())
        }
    
    def reset_stats(self):
        """Reset validation statistics"""
        self.validation_stats = {
            "truncated_titles": 0,
            "truncated_abstracts": 0,
            "cleaned_text_fields": 0,
            "validation_errors": 0
        }


# Global validator instance
validator = DataValidator()