"""
Duplicate Detection Utilities for PKG 2.0 GraphRAG Implementation

Provides tools to detect and analyze duplicate entries in TSV files
before ingestion to prevent constraint violation errors.
"""

import logging
import pandas as pd
from typing import Dict, Any, List, Tuple, Optional
from pathlib import Path
from collections import Counter
import time

from src.utils.tsv_reader import TSVReader

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class DuplicateDetector:
    """
    Detects and analyzes duplicate entries in TSV data files
    to prevent constraint violations during Neo4j ingestion.
    """
    
    def __init__(self):
        """Initialize duplicate detector"""
        self.duplicate_stats = {
            "files_analyzed": 0,
            "total_records_checked": 0,
            "duplicate_records_found": 0,
            "files_with_duplicates": 0
        }
    
    def detect_paper_duplicates(self, file_path: Path) -> Dict[str, Any]:
        """
        Detect duplicate papers based on PMID
        
        Args:
            file_path: Path to C01_Papers.tsv file
            
        Returns:
            Dictionary with duplicate analysis results
        """
        logger.info(f"🔍 Analyzing papers for duplicates: {file_path}")
        
        pmid_counts = Counter()
        total_records = 0
        duplicate_pmids = []
        
        try:
            with TSVReader(file_path, batch_size=10000) as reader:
                for batch_df in reader.read_batches():
                    # Count PMIDs in this batch
                    if 'PMID' in batch_df.columns:
                        batch_pmids = batch_df['PMID'].dropna().astype(str)
                        pmid_counts.update(batch_pmids)
                        total_records += len(batch_df)
                    else:
                        logger.warning("PMID column not found in papers file")
                        return {"error": "PMID column not found"}
            
            # Find duplicates
            duplicate_pmids = [pmid for pmid, count in pmid_counts.items() if count > 1]
            
            # Get detailed duplicate info
            duplicate_details = []
            if duplicate_pmids:
                # Re-read file to get full details of duplicates
                with TSVReader(file_path, batch_size=10000) as reader:
                    for batch_df in reader.read_batches():
                        if 'PMID' in batch_df.columns:
                            duplicates_in_batch = batch_df[
                                batch_df['PMID'].astype(str).isin(duplicate_pmids)
                            ]
                            
                            for _, row in duplicates_in_batch.iterrows():
                                duplicate_details.append({
                                    'pmid': str(row['PMID']),
                                    'title': str(row.get('ArticleTitle', 'N/A'))[:100] + '...' if len(str(row.get('ArticleTitle', ''))) > 100 else str(row.get('ArticleTitle', 'N/A')),
                                    'pubyear': row.get('PubYear', 'N/A'),
                                    'author_num': row.get('AuthorNum', 'N/A')
                                })
            
            result = {
                "file_type": "Papers",
                "total_records": total_records,
                "unique_pmids": len(pmid_counts),
                "duplicate_pmids": len(duplicate_pmids),
                "duplicate_rate": round(len(duplicate_pmids) / len(pmid_counts) * 100, 2) if pmid_counts else 0,
                "most_common_duplicates": pmid_counts.most_common(10),
                "duplicate_details": duplicate_details[:50]  # Limit to first 50 for readability
            }
            
            # Update stats
            self.duplicate_stats["files_analyzed"] += 1
            self.duplicate_stats["total_records_checked"] += total_records
            self.duplicate_stats["duplicate_records_found"] += len(duplicate_pmids)
            if duplicate_pmids:
                self.duplicate_stats["files_with_duplicates"] += 1
            
            return result
            
        except Exception as e:
            logger.error(f"Error analyzing paper duplicates: {str(e)}")
            return {"error": str(e)}
    
    def detect_author_duplicates(self, file_path: Path) -> Dict[str, Any]:
        """
        Detect duplicate authors based on AID
        
        Args:
            file_path: Path to C07_Authors.tsv file
            
        Returns:
            Dictionary with duplicate analysis results
        """
        logger.info(f"🔍 Analyzing authors for duplicates: {file_path}")
        
        aid_counts = Counter()
        total_records = 0
        
        try:
            with TSVReader(file_path, batch_size=10000) as reader:
                for batch_df in reader.read_batches():
                    if 'AID' in batch_df.columns:
                        batch_aids = batch_df['AID'].dropna().astype(str)
                        aid_counts.update(batch_aids)
                        total_records += len(batch_df)
                    else:
                        logger.warning("AID column not found in authors file")
                        return {"error": "AID column not found"}
            
            duplicate_aids = [aid for aid, count in aid_counts.items() if count > 1]
            
            result = {
                "file_type": "Authors",
                "total_records": total_records,
                "unique_aids": len(aid_counts),
                "duplicate_aids": len(duplicate_aids),
                "duplicate_rate": round(len(duplicate_aids) / len(aid_counts) * 100, 2) if aid_counts else 0,
                "most_common_duplicates": aid_counts.most_common(10)
            }
            
            # Update stats
            self.duplicate_stats["files_analyzed"] += 1
            self.duplicate_stats["total_records_checked"] += total_records
            self.duplicate_stats["duplicate_records_found"] += len(duplicate_aids)
            if duplicate_aids:
                self.duplicate_stats["files_with_duplicates"] += 1
            
            return result
            
        except Exception as e:
            logger.error(f"Error analyzing author duplicates: {str(e)}")
            return {"error": str(e)}
    
    def detect_patent_duplicates(self, file_path: Path) -> Dict[str, Any]:
        """
        Detect duplicate patents based on Patent_ID
        
        Args:
            file_path: Path to C15_Patents.tsv file
            
        Returns:
            Dictionary with duplicate analysis results
        """
        logger.info(f"🔍 Analyzing patents for duplicates: {file_path}")
        
        patent_id_counts = Counter()
        total_records = 0
        
        try:
            with TSVReader(file_path, batch_size=10000) as reader:
                for batch_df in reader.read_batches():
                    if 'Patent_ID' in batch_df.columns:
                        batch_patent_ids = batch_df['Patent_ID'].dropna().astype(str)
                        patent_id_counts.update(batch_patent_ids)
                        total_records += len(batch_df)
                    else:
                        logger.warning("Patent_ID column not found in patents file")
                        return {"error": "Patent_ID column not found"}
            
            duplicate_patent_ids = [pid for pid, count in patent_id_counts.items() if count > 1]
            
            result = {
                "file_type": "Patents",
                "total_records": total_records,
                "unique_patent_ids": len(patent_id_counts),
                "duplicate_patent_ids": len(duplicate_patent_ids),
                "duplicate_rate": round(len(duplicate_patent_ids) / len(patent_id_counts) * 100, 2) if patent_id_counts else 0,
                "most_common_duplicates": patent_id_counts.most_common(10)
            }
            
            # Update stats
            self.duplicate_stats["files_analyzed"] += 1
            self.duplicate_stats["total_records_checked"] += total_records
            self.duplicate_stats["duplicate_records_found"] += len(duplicate_patent_ids)
            if duplicate_patent_ids:
                self.duplicate_stats["files_with_duplicates"] += 1
            
            return result
            
        except Exception as e:
            logger.error(f"Error analyzing patent duplicates: {str(e)}")
            return {"error": str(e)}
    
    def detect_clinical_trial_duplicates(self, file_path: Path) -> Dict[str, Any]:
        """
        Detect duplicate clinical trials based on NCT_ID
        
        Args:
            file_path: Path to C11_ClinicalTrials.tsv file
            
        Returns:
            Dictionary with duplicate analysis results
        """
        logger.info(f"🔍 Analyzing clinical trials for duplicates: {file_path}")
        
        nct_id_counts = Counter()
        total_records = 0
        
        try:
            with TSVReader(file_path, batch_size=10000) as reader:
                for batch_df in reader.read_batches():
                    if 'NCT_ID' in batch_df.columns:
                        batch_nct_ids = batch_df['NCT_ID'].dropna().astype(str)
                        nct_id_counts.update(batch_nct_ids)
                        total_records += len(batch_df)
                    else:
                        logger.warning("NCT_ID column not found in clinical trials file")
                        return {"error": "NCT_ID column not found"}
            
            duplicate_nct_ids = [nct for nct, count in nct_id_counts.items() if count > 1]
            
            result = {
                "file_type": "ClinicalTrials",
                "total_records": total_records,
                "unique_nct_ids": len(nct_id_counts),
                "duplicate_nct_ids": len(duplicate_nct_ids),
                "duplicate_rate": round(len(duplicate_nct_ids) / len(nct_id_counts) * 100, 2) if nct_id_counts else 0,
                "most_common_duplicates": nct_id_counts.most_common(10)
            }
            
            # Update stats
            self.duplicate_stats["files_analyzed"] += 1
            self.duplicate_stats["total_records_checked"] += total_records
            self.duplicate_stats["duplicate_records_found"] += len(duplicate_nct_ids)
            if duplicate_nct_ids:
                self.duplicate_stats["files_with_duplicates"] += 1
            
            return result
            
        except Exception as e:
            logger.error(f"Error analyzing clinical trial duplicates: {str(e)}")
            return {"error": str(e)}
    
    def analyze_all_files(self, base_dir: Path) -> Dict[str, Any]:
        """
        Analyze all entity files for duplicates
        
        Args:
            base_dir: Base directory containing TSV files
            
        Returns:
            Comprehensive duplicate analysis report
        """
        logger.info("📊 Starting comprehensive duplicate analysis...")
        start_time = time.time()
        
        files_to_analyze = [
            ("papers", "C01_Papers.tsv", self.detect_paper_duplicates),
            ("authors", "C07_Authors.tsv", self.detect_author_duplicates),
            ("patents", "C15_Patents.tsv", self.detect_patent_duplicates),
            ("clinical_trials", "C11_ClinicalTrials.tsv", self.detect_clinical_trial_duplicates)
        ]
        
        results = {}
        
        for entity_name, filename, analyze_func in files_to_analyze:
            file_path = base_dir / filename
            
            if file_path.exists():
                logger.info(f"📋 Analyzing {entity_name}...")
                results[entity_name] = analyze_func(file_path)
            else:
                logger.warning(f"⚠️ File not found: {file_path}")
                results[entity_name] = {"error": "File not found"}
        
        # Generate summary
        total_time = time.time() - start_time
        
        summary = {
            "analysis_summary": {
                "files_analyzed": self.duplicate_stats["files_analyzed"],
                "total_records_checked": self.duplicate_stats["total_records_checked"],
                "duplicate_records_found": self.duplicate_stats["duplicate_records_found"],
                "files_with_duplicates": self.duplicate_stats["files_with_duplicates"],
                "analysis_time_seconds": round(total_time, 2)
            },
            "detailed_results": results,
            "recommendations": self._generate_recommendations(results)
        }
        
        return summary
    
    def _generate_recommendations(self, results: Dict[str, Any]) -> List[str]:
        """Generate recommendations based on duplicate analysis"""
        recommendations = []
        
        for entity_type, result in results.items():
            if "error" in result:
                continue
            
            duplicate_rate = result.get("duplicate_rate", 0)
            
            if duplicate_rate > 0:
                recommendations.append(
                    f"⚠️ {entity_type.title()}: {duplicate_rate}% duplicate rate detected. "
                    f"MERGE queries will handle these gracefully."
                )
            else:
                recommendations.append(f"✅ {entity_type.title()}: No duplicates detected.")
        
        if any("⚠️" in rec for rec in recommendations):
            recommendations.append(
                "💡 Tip: Use MERGE instead of CREATE in Cypher queries to handle duplicates automatically."
            )
            recommendations.append(
                "🔧 Your entity loaders have been updated to use MERGE for idempotent operations."
            )
        
        return recommendations
    
    def get_duplicate_stats(self) -> Dict[str, Any]:
        """Get current duplicate detection statistics"""
        return self.duplicate_stats.copy()
    
    def reset_stats(self):
        """Reset duplicate detection statistics"""
        self.duplicate_stats = {
            "files_analyzed": 0,
            "total_records_checked": 0,
            "duplicate_records_found": 0,
            "files_with_duplicates": 0
        }


# Global detector instance
detector = DuplicateDetector()