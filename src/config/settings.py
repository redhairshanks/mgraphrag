"""
Configuration settings for PKG 2.0 GraphRAG Neo4j Implementation

This module provides comprehensive configuration management for database connections,
file paths, batch sizes, and performance settings.
"""

import os
from pathlib import Path
from typing import Dict, Any
from dataclasses import dataclass
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

@dataclass
class Neo4jConfig:
    """Neo4j database configuration"""
    uri: str = os.getenv("NEO4J_URI", "bolt://localhost:7687")
    user: str = os.getenv("NEO4J_USER", "neo4j")
    password: str = os.getenv("NEO4J_PASSWORD", "alexander_123")
    database: str = os.getenv("NEO4J_DATABASE", "neo4j")
    
    # Connection pool settings
    max_connection_lifetime: int = 3600  # 1 hour
    max_connection_pool_size: int = 50
    connection_acquisition_timeout: int = 30


@dataclass
class DataPaths:
    """File paths for TSV data files"""
    base_dir: Path = Path(".")
    
    # Entity files
    papers: str = "C01_Papers.tsv"
    authors: str = "C07_Authors.tsv"
    patents: str = "C15_Patents.tsv"
    clinical_trials: str = "C11_ClinicalTrials.tsv"
    
    # Relationship files
    papers_authors: str = "C02_Link_Papers_Authors.tsv"
    papers_references: str = "C04_ReferenceList_Papers.tsv"
    papers_bioentities: str = "C06_Link_Papers_BioEntities.tsv"
    papers_journals: str = "C10_Link_Papers_Journals.tsv"
    papers_clinical_trials: str = "C12_Link_Papers_Clinicaltrials.tsv"
    clinical_trials_bioentities: str = "C13_Link_ClinicalTrials_BioEntities.tsv"
    investigators: str = "C14_Investigators.tsv"
    patents_papers: str = "C16_Link_Patents_Papers.tsv"
    patents_bioentities: str = "C18_Link_Patents_BioEntities.tsv"
    affiliations: str = "C03_Affiliations.tsv"
    bioentities: str = "C23_BioEntities.tsv"
    
    # Additional files
    pis: str = "C05_PIs.tsv"  # Project/funding data
    assignees: str = "C17_Assignees.tsv"
    inventors: str = "C19_Inventors.tsv"
    
    def get_full_path(self, filename: str) -> Path:
        """Get full path for a TSV file"""
        return self.base_dir / filename
    
    def check_file_exists(self, filename: str) -> bool:
        """Check if a TSV file exists"""
        return self.get_full_path(filename).exists()


@dataclass
class BatchSizes:
    """Batch sizes for different entity types based on memory requirements"""
    
    # Entity loading batch sizes
    authors: int = 5000        # Small records
    papers: int = 2000         # Medium records
    patents: int = 1000        # Large records (with abstracts)
    clinical_trials: int = 500  # Very large records (with descriptions)
    bioentities: int = 10000   # Small records
    journals: int = 5000       # Small records
    institutions: int = 5000   # Small records
    projects: int = 5000       # Small records
    
    # Relationship loading batch sizes
    authored_by: int = 10000      # Simple relationships
    cites: int = 10000           # Simple relationships
    mentions: int = 5000         # Medium complexity (position data)
    published_in: int = 10000    # Simple relationships
    funded_by: int = 10000       # Simple relationships
    affiliated_with: int = 5000   # Medium complexity
    investigates: int = 10000    # Simple relationships


@dataclass
class PerformanceSettings:
    """Performance optimization settings"""
    
    # Memory management
    max_memory_usage_gb: float = 8.0
    gc_frequency: int = 1000  # Run garbage collection every N batches
    
    # Progress tracking
    progress_report_frequency: int = 100  # Report progress every N batches
    checkpoint_frequency: int = 1000      # Create checkpoint every N batches
    
    # Parallel processing
    max_workers: int = 4
    enable_parallel_loading: bool = True
    
    # Transaction settings
    transaction_timeout: int = 300  # 5 minutes
    
    # Indexing
    create_indexes_before_loading: bool = False  # Create after for better performance
    rebuild_indexes_after_loading: bool = True


@dataclass
class ValidationSettings:
    """Data validation and quality settings"""
    
    # Error handling
    max_errors_per_batch: int = 100
    continue_on_error: bool = True
    log_failed_records: bool = True
    
    # Data quality checks
    check_foreign_keys: bool = True
    check_data_types: bool = True
    check_required_fields: bool = True
    
    # Duplicate handling
    skip_duplicates: bool = True
    log_duplicates: bool = True


class Config:
    """Main configuration class combining all settings"""
    
    def __init__(self):
        self.neo4j = Neo4jConfig()
        self.paths = DataPaths()
        self.batch_sizes = BatchSizes()
        self.performance = PerformanceSettings()
        self.validation = ValidationSettings()
    
    def validate_configuration(self) -> Dict[str, Any]:
        """Validate configuration and return status report"""
        status = {
            "neo4j_connection": False,
            "required_files": {},
            "memory_settings": True,
            "batch_sizes": True
        }
        
        # Check required files
        required_files = [
            self.paths.papers,
            self.paths.authors,
            self.paths.patents,
            self.paths.clinical_trials,
            self.paths.papers_authors,
            self.paths.papers_references,
            self.paths.papers_bioentities
        ]
        
        for file in required_files:
            exists = self.paths.check_file_exists(file)
            status["required_files"][file] = exists
            if not exists:
                print(f"WARNING: Required file {file} not found")
        
        return status
    
    def get_entity_loading_order(self) -> list:
        """Get the correct order for loading entities"""
        return [
            ("authors", self.paths.authors),
            ("papers", self.paths.papers),
            ("patents", self.paths.patents),
            ("clinical_trials", self.paths.clinical_trials),
            ("journals", self.paths.papers_journals),  # Extract unique
            ("institutions", self.paths.affiliations),  # Extract unique
            ("projects", self.paths.pis),  # Extract unique
            ("bioentities", None)  # Extract from relationship files
        ]
    
    def get_relationship_loading_order(self) -> list:
        """Get the correct order for loading relationships"""
        return [
            ("AUTHORED_BY", self.paths.papers_authors),
            ("PUBLISHED_IN", self.paths.papers_journals),
            ("AFFILIATED_WITH", self.paths.affiliations),
            ("CITES_PAPER", self.paths.papers_references),
            ("CITES_TRIAL", self.paths.papers_clinical_trials),
            ("CITES_PATENT", self.paths.patents_papers),
            ("MENTIONS_IN_PAPER", self.paths.papers_bioentities),
            ("MENTIONS_IN_TRIAL", self.paths.clinical_trials_bioentities),
            ("MENTIONS_IN_PATENT", self.paths.patents_bioentities),
            ("FUNDED_BY", self.paths.pis),
            ("INVESTIGATES", self.paths.investigators)
        ]


# Global configuration instance
config = Config()

# Export commonly used settings
NEO4J_CONFIG = config.neo4j
DATA_PATHS = config.paths
BATCH_SIZES = config.batch_sizes
PERFORMANCE = config.performance
VALIDATION = config.validation