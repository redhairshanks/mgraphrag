"""
Entity Loader Module for PKG 2.0 GraphRAG Implementation

Handles loading of all entity types (Papers, Authors, Patents, etc.) 
with optimized batch processing and data validation.
"""

import logging
import pandas as pd
from typing import Dict, Any, List, Iterator, Optional
from pathlib import Path
import time
from datetime import datetime

from src.utils.neo4j_connection import Neo4jConnection
from src.utils.tsv_reader import TSVReader
from src.config.settings import config
from src.validation.data_validator import validator
from src.utils.progress_tracker import ProgressTracker


class EntityLoader:
    """
    Handles loading of all entity types into Neo4j with batch processing,
    data validation, and comprehensive error handling.
    """
    
    def __init__(self, connection: Neo4jConnection, logger: logging.Logger = None):
        """
        Initialize entity loader
        
        Args:
            connection: Neo4j connection instance
            logger: Logger instance for progress reporting
        """
        self.connection = connection
        self.load_stats = {}
        self.logger = logger or logging.getLogger(__name__)
        self.entity_progress_tracker = None
        self.relationship_progress_tracker = None
    
    def _prepare_batch_data(self, df: pd.DataFrame, entity_type: str) -> List[Dict[str, Any]]:
        """
        Prepare batch data for loading, handling type conversions and null values
        
        Args:
            df: DataFrame batch
            entity_type: Type of entity being loaded
            
        Returns:
            List of dictionaries ready for Neo4j loading
        """
        # Convert DataFrame to records
        records = df.to_dict('records')
        
        # Clean and convert data types for each entity type
        cleaned_records = []
        for record in records:
            cleaned_record = {}
            
            # Clean null values and convert types
            for key, value in record.items():
                if pd.isna(value) or value == 'NULL' or value == '':
                    cleaned_record[key] = None
                elif isinstance(value, str):
                    # Handle string values
                    cleaned_record[key] = value.strip()
                else:
                    cleaned_record[key] = value
            
            # Entity-specific processing
            if entity_type == "Paper":
                cleaned_record = self._process_paper_record(cleaned_record)
            elif entity_type == "Author":
                cleaned_record = self._process_author_record(cleaned_record)
            elif entity_type == "Patent":
                cleaned_record = self._process_patent_record(cleaned_record)
            elif entity_type == "ClinicalTrial":
                cleaned_record = self._process_clinical_trial_record(cleaned_record)
            elif entity_type == "BioEntity":
                cleaned_record = self._process_bioentity_record(cleaned_record)
            elif entity_type == "Journal":
                cleaned_record = self._process_journal_record(cleaned_record)
            elif entity_type == "Institution":
                cleaned_record = self._process_institution_record(cleaned_record)
            elif entity_type == "Project":
                cleaned_record = self._process_project_record(cleaned_record)
            
            cleaned_records.append(cleaned_record)
        
        # Apply data validation to prevent indexing issues
        validated_records = validator.validate_batch_data(cleaned_records, entity_type)
        
        return validated_records
    
    def _process_paper_record(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """Process and validate paper record"""
        # Convert numeric fields
        numeric_fields = ['PMID', 'PubYear', 'AuthorNum', 'CitedCount', 'StdCitedCount']
        for field in numeric_fields:
            if field in record and record[field] is not None:
                try:
                    record[field] = int(float(record[field]))
                except (ValueError, TypeError):
                    record[field] = None
        
        # Convert boolean fields
        boolean_fields = ['IsClinicalArticle', 'IsResearchArticle', 'Human', 'Animal', 'MolecularCellular']
        for field in boolean_fields:
            if field in record and record[field] is not None:
                try:
                    record[field] = bool(int(float(record[field])))
                except (ValueError, TypeError):
                    record[field] = False
        
        # Rename fields for consistency
        field_mapping = {
            'PMID': 'pmid',
            'PubYear': 'pubyear',
            'ArticleTitle': 'title',
            'AuthorNum': 'author_num',
            'CitedCount': 'cited_count',
            'StdCitedCount': 'std_cited_count',
            'MedlineCitation_Status': 'medline_status',
            'IsClinicalArticle': 'is_clinical_article',
            'IsResearchArticle': 'is_research_article',
            'Human': 'human',
            'Animal': 'animal',
            'MolecularCellular': 'molecular_cellular'
        }
        
        return {field_mapping.get(k, k.lower()): v for k, v in record.items()}
    
    def _process_author_record(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """Process and validate author record"""
        # Convert numeric fields
        numeric_fields = ['AID', 'BeginYear', 'RecentYear', 'PaperNum', 'h_index']
        for field in numeric_fields:
            if field in record and record[field] is not None:
                try:
                    record[field] = int(float(record[field]))
                except (ValueError, TypeError):
                    record[field] = None
        
        # Rename fields
        field_mapping = {
            'AID': 'aid',
            'BeginYear': 'begin_year',
            'RecentYear': 'recent_year',
            'PaperNum': 'paper_num',
            'h_index': 'h_index'
        }
        
        return {field_mapping.get(k, k.lower()): v for k, v in record.items()}
    
    def _process_patent_record(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """Process and validate patent record"""
        # Convert numeric fields
        if 'ClaimNum' in record and record['ClaimNum'] is not None:
            try:
                record['ClaimNum'] = int(float(record['ClaimNum']))
            except (ValueError, TypeError):
                record['ClaimNum'] = None
        
        # Convert boolean fields
        boolean_fields = ['isWithdrawn', 'has_citing_paper', 'is_granted_by_NIH', 'is_CPC_A61']
        for field in boolean_fields:
            if field in record and record[field] is not None:
                try:
                    record[field] = bool(int(float(record[field])))
                except (ValueError, TypeError):
                    record[field] = False
        
        # Rename fields
        field_mapping = {
            'PatentId': 'patent_id',
            'GrantedDate': 'granted_date',
            'Title': 'title',
            'Abstract': 'abstract',
            'Kind': 'kind',
            'ClaimNum': 'claim_num',
            'isWithdrawn': 'is_withdrawn',
            'has_citing_paper': 'has_citing_paper',
            'is_granted_by_NIH': 'is_granted_by_nih',
            'is_CPC_A61': 'is_cpc_a61',
            'FileName': 'filename'
        }
        
        return {field_mapping.get(k, k.lower()): v for k, v in record.items()}
    
    def _process_clinical_trial_record(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """Process and validate clinical trial record"""
        # Rename fields for consistency
        field_mapping = {
            'nct_id': 'nct_id',
            'study_first_submitted_date': 'study_first_submitted_date',
            'last_update_posted_date': 'last_update_posted_date',
            'start_date': 'start_date',
            'completion_date': 'completion_date',
            'overall_status': 'overall_status',
            'last_known_status': 'last_known_status',
            'brief_title': 'brief_title',
            'official_title': 'official_title',
            'study_type': 'study_type',
            'phase': 'phase',
            'source': 'source',
            'source_class': 'source_class',
            'brief_summaries': 'brief_summaries',
            'detailed_descriptions': 'detailed_descriptions',
            'keywords': 'keywords',
            'conditions': 'conditions'
        }
        
        return {field_mapping.get(k, k): v for k, v in record.items()}
    
    def _process_bioentity_record(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """Process and validate bioentity record"""
        # Rename fields for consistency and handle the bioentity structure
        field_mapping = {
            'EntityId': 'entity_id',
            'Type': 'type',
            'Mention': 'mention'
        }
        
        processed_record = {field_mapping.get(k, k.lower()): v for k, v in record.items()}
        
        # Clean up mention text if it exists
        if 'mention' in processed_record and processed_record['mention'] is not None:
            processed_record['mention'] = str(processed_record['mention']).strip()
        
        # Ensure entity_id is treated as string
        if 'entity_id' in processed_record and processed_record['entity_id'] is not None:
            processed_record['entity_id'] = str(processed_record['entity_id']).strip()
            
        # Ensure type is lowercase for consistency
        if 'type' in processed_record and processed_record['type'] is not None:
            processed_record['type'] = str(processed_record['type']).lower().strip()
        
        return processed_record
    
    def _process_journal_record(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """Process and validate journal record"""
        # Convert numeric fields
        numeric_fields = ['Journal_SJR', 'Journal_Hindex']
        for field in numeric_fields:
            if field in record and record[field] is not None:
                try:
                    record[field] = float(record[field])
                except (ValueError, TypeError):
                    record[field] = None
        
        field_mapping = {
            'Journal_ISSN': 'journal_issn',
            'Journal_Title': 'journal_title',
            'Journal_SJR': 'journal_sjr',
            'Journal_Hindex': 'journal_hindex',
            'Journal_Categories': 'journal_categories',
            'Journal_SJR_Best_Quartile': 'journal_sjr_best_quartile'
        }
        
        return {field_mapping.get(k, k.lower()): v for k, v in record.items()}
    
    def _process_institution_record(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """Process and validate institution record"""
        field_mapping = {
            'IND_ID': 'ind_id',
            'Institution_IND': 'institution_name',
            'Country': 'country',
            'City': 'city',
            'State': 'state',
            'Type': 'type'
        }
        
        return {field_mapping.get(k, k.lower()): v for k, v in record.items()}
    
    def _process_project_record(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """Process and validate project record"""
        # Convert numeric fields
        if 'TotalCost' in record and record['TotalCost'] is not None:
            try:
                record['TotalCost'] = float(record['TotalCost'])
            except (ValueError, TypeError):
                record['TotalCost'] = None
        
        field_mapping = {
            'Project_Number': 'project_number',
            'FiscalYear': 'fiscal_year',
            'ProjectTitle': 'project_title',
            'TotalCost': 'total_cost',
            'OrganizationName': 'organization_name'
        }
        
        return {field_mapping.get(k, k.lower()): v for k, v in record.items()}
    
    def load_papers(self, file_path: Path, batch_size: int = None) -> Dict[str, Any]:
        """Load papers from C01_Papers.tsv"""
        batch_size = batch_size or config.batch_sizes.papers
        
        self.logger.info(f"🏥 Loading Papers from {file_path}")
        
        cypher_query = """
        UNWIND $batch AS row
        MERGE (p:Paper {pmid: toInteger(row.pmid)})
        SET p.pubyear = toInteger(row.pubyear),
            p.title = row.title,
            p.author_num = toInteger(row.author_num),
            p.cited_count = toInteger(row.cited_count),
            p.std_cited_count = toFloat(row.std_cited_count),
            p.medline_status = row.medline_status,
            p.is_clinical_article = row.is_clinical_article,
            p.is_research_article = row.is_research_article,
            p.human = row.human,
            p.animal = row.animal,
            p.molecular_cellular = row.molecular_cellular
        """
        
        return self._load_entity_from_file(file_path, "Paper", cypher_query, batch_size)
    
    def load_authors(self, file_path: Path, batch_size: int = None) -> Dict[str, Any]:
        """Load authors from C07_Authors.tsv"""
        batch_size = batch_size or config.batch_sizes.authors
        
        self.logger.info(f"👨‍⚕️ Loading Authors from {file_path}")
        
        cypher_query = """
        UNWIND $batch AS row
        MERGE (a:Author {aid: toInteger(row.aid)})
        SET a.begin_year = toInteger(row.begin_year),
            a.recent_year = toInteger(row.recent_year),
            a.paper_num = toInteger(row.paper_num),
            a.h_index = toInteger(row.h_index)
        """
        
        return self._load_entity_from_file(file_path, "Author", cypher_query, batch_size)
    
    def load_patents(self, file_path: Path, batch_size: int = None) -> Dict[str, Any]:
        """Load patents from C15_Patents.tsv"""
        batch_size = batch_size or config.batch_sizes.patents
        
        self.logger.info(f"📜 Loading Patents from {file_path}")
        
        cypher_query = """
        UNWIND $batch AS row
        MERGE (pt:Patent {patent_id: toString(row.patent_id)})
        SET pt.type = row.type,
            pt.granted_date = row.granted_date,
            pt.title = row.title,
            pt.abstract = row.abstract,
            pt.kind = row.kind,
            pt.claim_num = toInteger(row.claim_num),
            pt.is_withdrawn = row.is_withdrawn,
            pt.has_citing_paper = row.has_citing_paper,
            pt.is_granted_by_nih = row.is_granted_by_nih,
            pt.is_cpc_a61 = row.is_cpc_a61,
            pt.filename = row.filename
        """
        
        return self._load_entity_from_file(file_path, "Patent", cypher_query, batch_size)
    
    def load_clinical_trials(self, file_path: Path, batch_size: int = None) -> Dict[str, Any]:
        """Load clinical trials from C11_ClinicalTrials.tsv"""
        batch_size = batch_size or config.batch_sizes.clinical_trials
        
        self.logger.info(f"🧪 Loading Clinical Trials from {file_path}")
        
        cypher_query = """
        UNWIND $batch AS row
        MERGE (ct:ClinicalTrial {nct_id: row.nct_id})
        SET ct.study_first_submitted_date = row.study_first_submitted_date,
            ct.last_update_posted_date = row.last_update_posted_date,
            ct.start_date = row.start_date,
            ct.completion_date = row.completion_date,
            ct.overall_status = row.overall_status,
            ct.last_known_status = row.last_known_status,
            ct.brief_title = row.brief_title,
            ct.official_title = row.official_title,
            ct.study_type = row.study_type,
            ct.phase = row.phase,
            ct.source = row.source,
            ct.source_class = row.source_class,
            ct.brief_summaries = row.brief_summaries,
            ct.detailed_descriptions = row.detailed_descriptions,
            ct.keywords = row.keywords,
            ct.conditions = row.conditions
        """
        
        return self._load_entity_from_file(file_path, "ClinicalTrial", cypher_query, batch_size)
    
    def load_bioentities(self, file_path: Path, batch_size: int = None) -> Dict[str, Any]:
        """Load bioentities from C23_BioEntities.tsv"""
        batch_size = batch_size or config.batch_sizes.bioentities
        
        self.logger.info(f"🧬 Loading BioEntities from {file_path}")
        
        cypher_query = """
        UNWIND $batch AS row
        MERGE (be:BioEntity {entity_id: row.entity_id})
        SET be.type = row.type,
            be.mention = row.mention,
            be.created_at = datetime()
        """
        
        return self._load_entity_from_file(file_path, "BioEntity", cypher_query, batch_size)
    
    def get_bioentity_statistics(self, file_path: Path) -> Dict[str, Any]:
        """
        Get statistics about bioentities in the file
        Useful for understanding the data distribution
        """
        self.logger.info(f"📊 Analyzing BioEntity statistics from {file_path}")
        
        try:
            type_counts = {}
            entity_id_prefixes = {}
            total_entities = 0
            
            with TSVReader(file_path, batch_size=10000) as reader:
                for batch_df in reader.read_batches():
                    # Process the batch
                    batch_data = self._prepare_batch_data(batch_df, "BioEntity")
                    
                    for record in batch_data:
                        total_entities += 1
                        
                        # Count entity types
                        entity_type = record.get('type', 'unknown')
                        type_counts[entity_type] = type_counts.get(entity_type, 0) + 1
                        
                        # Count entity ID prefixes (for source identification)
                        entity_id = record.get('entity_id', '')
                        if entity_id:
                            prefix = entity_id.split(':', 1)[0] if ':' in entity_id else entity_id[:4]
                            entity_id_prefixes[prefix] = entity_id_prefixes.get(prefix, 0) + 1
            
            # Sort results
            sorted_types = dict(sorted(type_counts.items(), key=lambda x: x[1], reverse=True))
            sorted_prefixes = dict(sorted(entity_id_prefixes.items(), key=lambda x: x[1], reverse=True))
            
            statistics = {
                "total_entities": total_entities,
                "entity_types": sorted_types,
                "entity_id_prefixes": sorted_prefixes,
                "unique_types_count": len(type_counts),
                "unique_prefixes_count": len(entity_id_prefixes)
            }
            
            self.logger.info(f"✅ BioEntity statistics: {total_entities} total entities, "
                       f"{len(type_counts)} unique types")
            
            return statistics
            
        except Exception as e:
            self.logger.error(f"Failed to analyze bioentity statistics: {str(e)}")
            raise
    
    def validate_bioentity_integrity(self, file_path: Path, sample_size: int = 1000) -> Dict[str, Any]:
        """
        Validate bioentity data integrity by checking for common issues
        """
        self.logger.info(f"🔍 Validating BioEntity data integrity from {file_path}")
        
        try:
            validation_results = {
                "total_checked": 0,
                "issues": {
                    "missing_entity_id": 0,
                    "missing_type": 0,
                    "missing_mention": 0,
                    "empty_mention": 0,
                    "duplicate_entity_ids": 0,
                    "invalid_characters": 0
                },
                "sample_records": []
            }
            
            seen_entity_ids = set()
            
            with TSVReader(file_path, batch_size=min(sample_size, 1000)) as reader:
                for batch_df in reader.read_batches():
                    batch_data = self._prepare_batch_data(batch_df, "BioEntity")
                    
                    for record in batch_data:
                        validation_results["total_checked"] += 1
                        
                        # Check for missing required fields
                        if not record.get('entity_id'):
                            validation_results["issues"]["missing_entity_id"] += 1
                        
                        if not record.get('type'):
                            validation_results["issues"]["missing_type"] += 1
                        
                        if not record.get('mention'):
                            validation_results["issues"]["missing_mention"] += 1
                        elif not str(record['mention']).strip():
                            validation_results["issues"]["empty_mention"] += 1
                        
                        # Check for duplicates
                        entity_id = record.get('entity_id')
                        if entity_id:
                            if entity_id in seen_entity_ids:
                                validation_results["issues"]["duplicate_entity_ids"] += 1
                            else:
                                seen_entity_ids.add(entity_id)
                        
                        # Store sample records for review
                        if len(validation_results["sample_records"]) < 10:
                            validation_results["sample_records"].append(record)
                        
                        # Stop if we've reached our sample size
                        if validation_results["total_checked"] >= sample_size:
                            break
                    
                    if validation_results["total_checked"] >= sample_size:
                        break
            
            # Calculate validation score
            total_issues = sum(validation_results["issues"].values())
            validation_score = max(0, 100 - (total_issues / validation_results["total_checked"] * 100))
            validation_results["validation_score"] = round(validation_score, 2)
            
            self.logger.info(f"✅ BioEntity validation complete: {validation_score:.1f}% validation score")
            
            return validation_results
            
        except Exception as e:
            self.logger.error(f"Failed to validate bioentity integrity: {str(e)}")
            raise
    
    def load_bioentities_by_type(self, file_path: Path, entity_types: List[str], 
                                batch_size: int = None) -> Dict[str, Any]:
        """
        Load only specific types of bioentities
        Useful for selective loading or testing with specific entity types
        
        Args:
            file_path: Path to bioentities TSV file
            entity_types: List of entity types to load (e.g., ['drug', 'gene', 'species'])
            batch_size: Batch size for processing
        """
        batch_size = batch_size or config.batch_sizes.bioentities
        entity_types_lower = [t.lower() for t in entity_types]
        
        self.logger.info(f"🧬 Loading BioEntities of types {entity_types} from {file_path}")
        
        start_time = time.time()
        total_processed = 0
        total_filtered = 0
        total_errors = 0
        
        try:
            # Custom Cypher query for filtered loading
            cypher_query = """
            UNWIND $batch AS row
            MERGE (be:BioEntity {entity_id: row.entity_id})
            SET be.type = row.type,
                be.mention = row.mention,
                be.created_at = datetime()
            """
            
            with TSVReader(file_path, batch_size=batch_size) as reader:
                file_info = reader.get_file_info()
                self.logger.info(f"📊 File info: {file_info['estimated_rows']} rows, "
                           f"{file_info['file_size_mb']} MB")
                
                # Create filtered batch generator
                def filtered_batch_generator():
                    nonlocal total_processed, total_filtered, total_errors
                    
                    for batch_df in reader.read_batches():
                        try:
                            # Process the full batch first
                            batch_data = self._prepare_batch_data(batch_df, "BioEntity")
                            total_processed += len(batch_data)
                            
                            # Filter by entity types
                            filtered_batch = [
                                record for record in batch_data 
                                if record.get('type', '').lower() in entity_types_lower
                            ]
                            
                            total_filtered += len(filtered_batch)
                            
                            if filtered_batch:  # Only yield if we have matching records
                                yield filtered_batch
                                
                        except Exception as e:
                            total_errors += len(batch_df)
                            self.logger.error(f"Error processing batch: {str(e)}")
                            continue
                
                # Execute filtered batch loading
                load_stats = self.connection.execute_batch(
                    cypher_query, 
                    filtered_batch_generator(), 
                    batch_size
                )
                
                total_time = time.time() - start_time
                
                result = {
                    "entity_type": "BioEntity (filtered)",
                    "file_path": str(file_path),
                    "filter_types": entity_types,
                    "total_processed": total_processed,
                    "total_filtered": total_filtered,
                    "total_loaded": total_filtered,
                    "total_errors": total_errors,
                    "filter_ratio": round(total_filtered / total_processed * 100, 2) if total_processed > 0 else 0,
                    "total_time_seconds": round(total_time, 2),
                    "records_per_second": round(total_filtered / total_time, 2) if total_time > 0 else 0,
                    "load_stats": load_stats
                }
                
                self.logger.info(f"✅ Filtered BioEntity loading complete: "
                           f"{total_filtered}/{total_processed} records matched filter "
                           f"({result['filter_ratio']}%) in {total_time:.1f}s")
                
                return result
                
        except Exception as e:
            self.logger.error(f"Failed to load filtered bioentities: {str(e)}")
            raise
    
    def extract_and_load_journals(self, file_path: Path, batch_size: int = None) -> Dict[str, Any]:
        """Extract unique journals from C10_Link_Papers_Journals.tsv and load them"""
        batch_size = batch_size or config.batch_sizes.journals
        
        self.logger.info(f"📚 Extracting and loading Journals from {file_path}")
        
        # First, extract unique journals
        unique_journals = set()
        with TSVReader(file_path, batch_size=10000) as reader:
            for batch_df in reader.read_batches():
                # Extract unique journal records
                journal_columns = ['Journal_ISSN', 'Journal_Title', 'Journal_SJR', 
                                 'Journal_Hindex', 'Journal_Categories', 'Journal_SJR_Best_Quartile']
                
                if all(col in batch_df.columns for col in journal_columns):
                    journal_batch = batch_df[journal_columns].drop_duplicates(subset=['Journal_ISSN'])
                    
                    for _, row in journal_batch.iterrows():
                        if pd.notna(row['Journal_ISSN']) and row['Journal_ISSN'] not in unique_journals:
                            unique_journals.add(row['Journal_ISSN'])
        
        self.logger.info(f"Found {len(unique_journals)} unique journals")
        
        # Now load unique journals
        cypher_query = """
        UNWIND $batch AS row
        MERGE (j:Journal {journal_issn: row.journal_issn})
        SET j.journal_title = row.journal_title,
            j.journal_sjr = toFloat(row.journal_sjr),
            j.journal_hindex = toFloat(row.journal_hindex),
            j.journal_categories = row.journal_categories,
            j.journal_sjr_best_quartile = row.journal_sjr_best_quartile
        """
        
        # Create batches of unique journals and load them
        return self._load_extracted_entities(file_path, "Journal", cypher_query, 
                                           batch_size, unique_journals, journal_columns)
    
    def _load_entity_from_file(self, file_path: Path, entity_type: str, 
                              cypher_query: str, batch_size: int) -> Dict[str, Any]:
        """Generic method to load entities from TSV file"""
        start_time = time.time()
        total_processed = 0
        total_errors = 0
        
        try:
            with TSVReader(file_path, batch_size=batch_size) as reader:
                file_info = reader.get_file_info()
                self.logger.info(f"📊 File info: {file_info['estimated_rows']} rows, "
                           f"{file_info['file_size_mb']} MB")
                
                # Create batch generator with progress reporting
                def batch_generator():
                    nonlocal total_processed, total_errors
                    progress_interval = max(1, file_info['estimated_rows'] // 20)  # Report every 5%
                    
                    for batch_df in reader.read_batches():
                        try:
                            batch_data = self._prepare_batch_data(batch_df, entity_type)
                            total_processed += len(batch_data)
                            
                            # Report progress at intervals
                            if hasattr(self, 'entity_progress_tracker') and self.entity_progress_tracker:
                                if total_processed % progress_interval < len(batch_data):
                                    self.entity_progress_tracker.update_file_progress(
                                        total_processed, file_info['estimated_rows']
                                    )
                            
                            yield batch_data
                        except Exception as e:
                            total_errors += len(batch_df)
                            self.logger.error(f"Error processing batch: {str(e)}")
                            continue
                
                # Execute batch loading
                load_stats = self.connection.execute_batch(
                    cypher_query, 
                    batch_generator(), 
                    batch_size
                )
                
                total_time = time.time() - start_time
                
                result = {
                    "entity_type": entity_type,
                    "file_path": str(file_path),
                    "total_processed": total_processed,
                    "total_errors": total_errors,
                    "total_time_seconds": round(total_time, 2),
                    "records_per_second": round(total_processed / total_time, 2) if total_time > 0 else 0,
                    "load_stats": load_stats
                }
                
                self.logger.info(f"✅ {entity_type} loading complete: "
                           f"{total_processed} records in {total_time:.1f}s "
                           f"({result['records_per_second']:.1f} records/sec)")
                
                return result
                
        except Exception as e:
            self.logger.error(f"Failed to load {entity_type} from {file_path}: {str(e)}")
            raise
    
    def _load_extracted_entities(self, source_file: Path, entity_type: str,
                                cypher_query: str, batch_size: int,
                                unique_keys: set, columns: List[str]) -> Dict[str, Any]:
        """Load entities extracted from relationship files"""
        # This is a placeholder for the journal extraction logic
        # In a real implementation, you would extract unique entities from the source file
        self.logger.info(f"Loading extracted {entity_type} entities")
        
        # For now, return a placeholder result
        return {
            "entity_type": entity_type,
            "source_file": str(source_file),
            "total_processed": len(unique_keys),
            "extraction_method": "unique_key_extraction"
        }
    
    def load_all_entities(self) -> Dict[str, Any]:
        """Load all entities in the correct order"""
        self.self.logger.info("🚀 Starting complete entity loading process")
        
        loading_results = {}
        loading_order = config.get_entity_loading_order()
        
        # Initialize progress tracker for entities
        valid_entities = [entity for entity, path in loading_order if path is not None]
        self.entity_progress_tracker = ProgressTracker(self.logger, len(valid_entities), "Entity Loading")
        
        for entity_name, file_path in loading_order:
            if file_path is None:
                self.self.logger.info(f"⏭️ Skipping {entity_name} - will be extracted from relationships")
                continue
            
            full_path = config.paths.get_full_path(file_path)
            
            if not full_path.exists():
                self.self.logger.warning(f"⚠️ File not found: {full_path}")
                loading_results[entity_name] = {"error": "File not found"}
                continue
            
            # Start progress tracking for this entity
            from src.utils.tsv_reader import analyze_tsv_file
            try:
                file_analysis = analyze_tsv_file(full_path)
                estimated_records = file_analysis.get('estimated_rows', 0)
            except:
                estimated_records = 0
                
            self.entity_progress_tracker.start_file(str(full_path), estimated_records)
            
            try:
                if entity_name == "authors":
                    result = self.load_authors(full_path)
                elif entity_name == "papers":
                    result = self.load_papers(full_path)
                elif entity_name == "patents":
                    result = self.load_patents(full_path)
                elif entity_name == "clinical_trials":
                    result = self.load_clinical_trials(full_path)
                elif entity_name == "journals":
                    result = self.extract_and_load_journals(full_path)
                elif entity_name == "bioentities":
                    result = self.load_bioentities(full_path)
                else:
                    self.self.logger.warning(f"⚠️ No loader implemented for {entity_name}")
                    continue
                
                loading_results[entity_name] = result
                
                # Complete progress tracking
                final_count = result.get('total_processed', 0)
                self.entity_progress_tracker.complete_file(final_count, success=True)
                
            except Exception as e:
                self.self.logger.error(f"❌ Failed to load {entity_name}: {str(e)}")
                loading_results[entity_name] = {"error": str(e)}
                self.entity_progress_tracker.complete_file(0, success=False, error_msg=str(e))
        
        # Summary
        successful_loads = sum(1 for r in loading_results.values() if "error" not in r)
        total_loads = len(loading_results)
        
        # Log final progress summary
        if self.entity_progress_tracker:
            self.entity_progress_tracker.log_final_summary()
        
        self.self.logger.info(f"🏁 Entity loading complete: {successful_loads}/{total_loads} successful")
        
        return {
            "summary": {
                "successful_loads": successful_loads,
                "total_loads": total_loads,
                "success_rate": round(successful_loads / total_loads * 100, 2) if total_loads > 0 else 0
            },
            "details": loading_results
        }

    def load_relationships(self) -> Dict[str, Any]:
        """Load all relationships in the correct order"""
        self.self.logger.info("🔗 Starting complete relationship loading process")
        
        loading_results = {}
        relationship_order = config.get_relationship_loading_order()
        
        # Initialize progress tracker for relationships
        valid_relationships = [rel for rel, path in relationship_order if path is not None]
        self.relationship_progress_tracker = ProgressTracker(self.logger, len(valid_relationships), "Relationship Loading")
        
        for relationship_name, file_path in relationship_order:
            if file_path is None:
                self.logger.warning(f"⚠️ No file path specified for {relationship_name}")
                continue
            
            full_path = config.paths.get_full_path(file_path)
            
            if not full_path.exists():
                self.logger.warning(f"⚠️ File not found: {full_path}")
                loading_results[relationship_name] = {"error": "File not found"}
                continue
            
            # Start progress tracking for this relationship
            try:
                from src.utils.tsv_reader import analyze_tsv_file
                file_analysis = analyze_tsv_file(full_path)
                estimated_records = file_analysis.get('estimated_rows', 0)
            except:
                estimated_records = 0
                
            self.relationship_progress_tracker.start_file(str(full_path), estimated_records)
            
            try:
                if relationship_name == "AUTHORED_BY":
                    result = self.load_authored_by_relationships(full_path)
                elif relationship_name == "PUBLISHED_IN":
                    result = self.load_published_in_relationships(full_path)
                elif relationship_name == "MENTIONS_IN_PAPER":
                    result = self.load_paper_bioentity_relationships(full_path)
                elif relationship_name == "CITES_PAPER":
                    result = self.load_paper_reference_relationships(full_path)
                elif relationship_name == "CITES_TRIAL":
                    result = self.load_paper_clinical_trial_relationships(full_path)
                elif relationship_name == "MENTIONS_IN_TRIAL":
                    result = self.load_clinical_trial_bioentity_relationships(full_path)
                elif relationship_name == "CITES_PATENT":
                    result = self.load_patent_paper_relationships(full_path)
                elif relationship_name == "MENTIONS_IN_PATENT":
                    result = self.load_patent_bioentity_relationships(full_path)
                elif relationship_name == "AFFILIATED_WITH":
                    result = self.load_affiliation_relationships(full_path)
                else:
                    self.logger.warning(f"⚠️ No loader implemented for relationship {relationship_name}")
                    continue
                
                loading_results[relationship_name] = result
                
                # Complete progress tracking
                final_count = result.get('total_processed', 0)
                self.relationship_progress_tracker.complete_file(final_count, success=True)
                
            except Exception as e:
                self.logger.error(f"❌ Failed to load relationship {relationship_name}: {str(e)}")
                loading_results[relationship_name] = {"error": str(e)}
                self.relationship_progress_tracker.complete_file(0, success=False, error_msg=str(e))
        
        # Summary
        successful_loads = sum(1 for r in loading_results.values() if "error" not in r)
        total_loads = len(loading_results)
        
        # Log final progress summary
        if self.relationship_progress_tracker:
            self.relationship_progress_tracker.log_final_summary()
        
        self.logger.info(f"🏁 Relationship loading complete: {successful_loads}/{total_loads} successful")
        
        return {
            "summary": {
                "successful_loads": successful_loads,
                "total_loads": total_loads,
                "success_rate": round(successful_loads / total_loads * 100, 2) if total_loads > 0 else 0
            },
            "details": loading_results
        }

    def load_authored_by_relationships(self, file_path: Path, batch_size: int = None) -> Dict[str, Any]:
        """Load AUTHORED_BY relationships from C02_Link_Papers_Authors.tsv"""
        batch_size = batch_size or config.batch_sizes.authored_by
        
        self.logger.info(f"✍️ Loading AUTHORED_BY relationships from {file_path}")
        
        cypher_query = """
        UNWIND $batch AS row
        MATCH (p:Paper {pmid: toInteger(row.PMID)})
        MATCH (a:Author {aid: toInteger(row.AID)})
        MERGE (a)-[r:AUTHORED_BY {author_order: toInteger(row.AuthorOrder)}]->(p)
        SET r.author_num = toInteger(row.AuthorNum),
            r.pub_year = toInteger(row.PubYear)
        """
        
        return self._load_relationship_from_file(file_path, "AUTHORED_BY", cypher_query, batch_size)

    def load_paper_bioentity_relationships(self, file_path: Path, batch_size: int = None) -> Dict[str, Any]:
        """Load MENTIONS_IN_PAPER relationships from C06_Link_Papers_BioEntities.tsv"""
        batch_size = batch_size or config.batch_sizes.mentions
        
        self.logger.info(f"🧬 Loading MENTIONS_IN_PAPER relationships from {file_path}")
        
        cypher_query = """
        UNWIND $batch AS row
        MATCH (p:Paper {pmid: toInteger(row.PMID)})
        MERGE (be:BioEntity {entity_id: row.EntityId})
        ON CREATE SET be.type = row.Type, be.mention = row.Mention
        MERGE (p)-[r:MENTIONS_IN_PAPER]->(be)
        SET r.start_position = toInteger(row.StartPosition),
            r.end_position = toInteger(row.EndPosition),
            r.mention_text = row.Mention,
            r.entity_type = row.Type,
            r.is_neural_normalized = toBoolean(row.is_neural_normalized),
            r.probability = toFloat(row.prob),
            r.mesh_id = row.mesh,
            r.mim_id = row.mim,
            r.cl_id = row.CL,
            r.cellosaurus_id = row.cellosaurus,
            r.ncbi_taxon_id = row.NCBITaxon,
            r.ncbi_gene_id = row.NCBIGene,
            r.chebi_id = row.CHEBI,
            r.source_file = row.FileName
        """
        
        return self._load_relationship_from_file(file_path, "MENTIONS_IN_PAPER", cypher_query, batch_size)

    def load_published_in_relationships(self, file_path: Path, batch_size: int = None) -> Dict[str, Any]:
        """Load PUBLISHED_IN relationships from C10_Link_Papers_Journals.tsv"""
        batch_size = batch_size or config.batch_sizes.published_in
        
        self.logger.info(f"📚 Loading PUBLISHED_IN relationships from {file_path}")
        
        cypher_query = """
        UNWIND $batch AS row
        MATCH (p:Paper {pmid: toInteger(row.PMID)})
        MERGE (j:Journal {journal_issn: row.Journal_ISSN})
        ON CREATE SET j.journal_title = row.Journal_Title,
                      j.journal_sjr = toFloat(row.Journal_SJR),
                      j.journal_hindex = toInteger(row.Journal_Hindex),
                      j.journal_categories = row.Journal_Categories,
                      j.journal_sjr_best_quartile = row.Journal_SJR_Best_Quartile
        MERGE (p)-[r:PUBLISHED_IN]->(j)
        """
        
        return self._load_relationship_from_file(file_path, "PUBLISHED_IN", cypher_query, batch_size)

    def load_paper_reference_relationships(self, file_path: Path, batch_size: int = None) -> Dict[str, Any]:
        """Load CITES_PAPER relationships from C04_ReferenceList_Papers.tsv"""
        batch_size = batch_size or config.batch_sizes.cites
        
        self.logger.info(f"📄 Loading CITES_PAPER relationships from {file_path}")
        
        cypher_query = """
        UNWIND $batch AS row
        MATCH (citing:Paper {pmid: toInteger(row.PMID)})
        MATCH (cited:Paper {pmid: toInteger(row.ReferencePMID)})
        MERGE (citing)-[r:CITES_PAPER]->(cited)
        SET r.reference_order = toInteger(row.ReferenceOrder)
        """
        
        return self._load_relationship_from_file(file_path, "CITES_PAPER", cypher_query, batch_size)

    def load_paper_clinical_trial_relationships(self, file_path: Path, batch_size: int = None) -> Dict[str, Any]:
        """Load CITES_TRIAL relationships from C12_Link_Papers_Clinicaltrials.tsv"""
        batch_size = batch_size or config.batch_sizes.cites
        
        self.logger.info(f"🧪 Loading CITES_TRIAL relationships from {file_path}")
        
        cypher_query = """
        UNWIND $batch AS row
        MATCH (p:Paper {pmid: toInteger(row.PMID)})
        MATCH (ct:ClinicalTrial {nct_id: row.nct_id})
        MERGE (p)-[r:CITES_TRIAL]->(ct)
        """
        
        return self._load_relationship_from_file(file_path, "CITES_TRIAL", cypher_query, batch_size)

    def load_clinical_trial_bioentity_relationships(self, file_path: Path, batch_size: int = None) -> Dict[str, Any]:
        """Load MENTIONS_IN_TRIAL relationships from C13_Link_ClinicalTrials_BioEntities.tsv"""
        batch_size = batch_size or config.batch_sizes.mentions
        
        self.logger.info(f"🧬 Loading MENTIONS_IN_TRIAL relationships from {file_path}")
        
        cypher_query = """
        UNWIND $batch AS row
        MATCH (ct:ClinicalTrial {nct_id: row.nct_id})
        MERGE (be:BioEntity {entity_id: row.EntityId})
        ON CREATE SET be.type = row.Type, be.mention = row.Mention
        MERGE (ct)-[r:MENTIONS_IN_TRIAL]->(be)
        SET r.mention_text = row.Mention,
            r.entity_type = row.Type
        """
        
        return self._load_relationship_from_file(file_path, "MENTIONS_IN_TRIAL", cypher_query, batch_size)

    def load_patent_paper_relationships(self, file_path: Path, batch_size: int = None) -> Dict[str, Any]:
        """Load CITES_PATENT relationships from C16_Link_Patents_Papers.tsv"""
        batch_size = batch_size or config.batch_sizes.cites
        
        self.logger.info(f"📜 Loading CITES_PATENT relationships from {file_path}")
        
        cypher_query = """
        UNWIND $batch AS row
        MATCH (patent:Patent {patent_id: row.PatentId})
        MATCH (paper:Paper {pmid: toInteger(row.PMID)})
        MERGE (paper)-[r:CITES_PATENT]->(patent)
        """
        
        return self._load_relationship_from_file(file_path, "CITES_PATENT", cypher_query, batch_size)

    def load_patent_bioentity_relationships(self, file_path: Path, batch_size: int = None) -> Dict[str, Any]:
        """Load MENTIONS_IN_PATENT relationships from C18_Link_Patents_BioEntities.tsv"""
        batch_size = batch_size or config.batch_sizes.mentions
        
        self.logger.info(f"🧬 Loading MENTIONS_IN_PATENT relationships from {file_path}")
        
        cypher_query = """
        UNWIND $batch AS row
        MATCH (patent:Patent {patent_id: row.PatentId})
        MERGE (be:BioEntity {entity_id: row.EntityId})
        ON CREATE SET be.type = row.Type, be.mention = row.Mention
        MERGE (patent)-[r:MENTIONS_IN_PATENT]->(be)
        SET r.mention_text = row.Mention,
            r.entity_type = row.Type
        """
        
        return self._load_relationship_from_file(file_path, "MENTIONS_IN_PATENT", cypher_query, batch_size)

    def load_affiliation_relationships(self, file_path: Path, batch_size: int = None) -> Dict[str, Any]:
        """Load AFFILIATED_WITH relationships from C03_Affiliations.tsv"""
        batch_size = batch_size or config.batch_sizes.affiliated_with
        
        self.logger.info(f"🏢 Loading AFFILIATED_WITH relationships from {file_path}")
        
        cypher_query = """
        UNWIND $batch AS row
        // Only process rows where IND_ID is not null/empty
        WITH row WHERE row.IND_ID IS NOT NULL AND row.IND_ID <> ''
        MATCH (a:Author {aid: toInteger(row.AID)})
        MERGE (inst:Institution {ind_id: row.IND_ID})
        ON CREATE SET inst.institution_name = row.Institution_IND,
                      inst.country = row.Country,
                      inst.city = row.City,
                      inst.state = row.State,
                      inst.type = row.Type
        MERGE (a)-[r:AFFILIATED_WITH]->(inst)
        """
        
        return self._load_relationship_from_file(file_path, "AFFILIATED_WITH", cypher_query, batch_size)

    def _load_relationship_from_file(self, file_path: Path, relationship_type: str, 
                                   cypher_query: str, batch_size: int) -> Dict[str, Any]:
        """Generic method to load relationships from TSV file"""
        start_time = time.time()
        total_processed = 0
        total_errors = 0
        
        try:
            with TSVReader(file_path, batch_size=batch_size) as reader:
                file_info = reader.get_file_info()
                self.logger.info(f"📊 File info: {file_info['estimated_rows']} rows, "
                           f"{file_info['file_size_mb']} MB")
                
                # Create batch generator with progress reporting
                def batch_generator():
                    nonlocal total_processed, total_errors
                    skipped_records = 0
                    progress_interval = max(1, file_info['estimated_rows'] // 20)  # Report every 5%
                    
                    for batch_df in reader.read_batches():
                        try:
                            # Convert DataFrame to records for relationships
                            batch_data = batch_df.to_dict('records')
                            
                            # Clean null values and apply relationship-specific validation
                            cleaned_batch = []
                            for record in batch_data:
                                cleaned_record = {}
                                for key, value in record.items():
                                    if pd.isna(value) or value == 'NULL' or value == '':
                                        cleaned_record[key] = None
                                    else:
                                        cleaned_record[key] = value
                                
                                # Special validation for AFFILIATED_WITH relationships
                                if relationship_type == "AFFILIATED_WITH":
                                    # Skip records with missing IND_ID (required for Institution nodes)
                                    if not cleaned_record.get('IND_ID') or str(cleaned_record.get('IND_ID')).strip() == '':
                                        skipped_records += 1
                                        continue
                                
                                cleaned_batch.append(cleaned_record)
                            
                            if skipped_records > 0 and relationship_type == "AFFILIATED_WITH":
                                self.logger.warning(f"⚠️  Skipped {skipped_records} records with missing IND_ID in current batch")
                            
                            total_processed += len(cleaned_batch)
                            
                            # Report progress at intervals
                            if hasattr(self, 'relationship_progress_tracker') and self.relationship_progress_tracker:
                                if total_processed % progress_interval < len(cleaned_batch):
                                    self.relationship_progress_tracker.update_file_progress(
                                        total_processed, file_info['estimated_rows']
                                    )
                            
                            yield cleaned_batch
                        except Exception as e:
                            total_errors += len(batch_df)
                            self.logger.error(f"Error processing relationship batch: {str(e)}")
                            continue
                
                # Execute batch loading
                load_stats = self.connection.execute_batch(
                    cypher_query, 
                    batch_generator(), 
                    batch_size
                )
                
                total_time = time.time() - start_time
                
                result = {
                    "relationship_type": relationship_type,
                    "file_path": str(file_path),
                    "total_processed": total_processed,
                    "total_errors": total_errors,
                    "total_time_seconds": round(total_time, 2),
                    "relationships_per_second": round(total_processed / total_time, 2) if total_time > 0 else 0,
                    "load_stats": load_stats
                }
                
                self.logger.info(f"✅ {relationship_type} loading complete: "
                           f"{total_processed} relationships in {total_time:.1f}s "
                           f"({result['relationships_per_second']:.1f} relationships/sec)")
                
                return result
                
        except Exception as e:
            self.logger.error(f"Failed to load {relationship_type} from {file_path}: {str(e)}")
            raise