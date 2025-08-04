#!/usr/bin/env python3
"""
Duplicate Analysis Tool for PKG 2.0 GraphRAG Implementation

Analyzes TSV files for duplicate primary keys that could cause
constraint violations during Neo4j ingestion.
"""

import sys
import logging
from pathlib import Path
from src.validation.duplicate_detector import detector

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def print_duplicate_summary(results: dict):
    """Print a formatted summary of duplicate analysis"""
    
    print("\n" + "="*80)
    print("📊 DUPLICATE ANALYSIS SUMMARY")
    print("="*80)
    
    summary = results.get("analysis_summary", {})
    
    print(f"📁 Files Analyzed: {summary.get('files_analyzed', 0)}")
    print(f"📋 Total Records: {summary.get('total_records_checked', 0):,}")
    print(f"🔄 Duplicate Records: {summary.get('duplicate_records_found', 0):,}")
    print(f"⚠️ Files with Duplicates: {summary.get('files_with_duplicates', 0)}")
    print(f"⏱️ Analysis Time: {summary.get('analysis_time_seconds', 0)} seconds")
    
    print("\n" + "-"*80)
    print("📋 DETAILED RESULTS BY FILE TYPE")
    print("-"*80)
    
    detailed = results.get("detailed_results", {})
    
    for entity_type, result in detailed.items():
        if "error" in result:
            print(f"\n❌ {entity_type.upper()}: {result['error']}")
            continue
        
        print(f"\n📄 {entity_type.upper()}:")
        print(f"   Total Records: {result.get('total_records', 0):,}")
        print(f"   Unique IDs: {result.get('unique_pmids', result.get('unique_aids', result.get('unique_patent_ids', result.get('unique_nct_ids', 0)))):,}")
        print(f"   Duplicates: {result.get('duplicate_pmids', result.get('duplicate_aids', result.get('duplicate_patent_ids', result.get('duplicate_nct_ids', 0)))):,}")
        print(f"   Duplicate Rate: {result.get('duplicate_rate', 0)}%")
        
        # Show most common duplicates
        most_common = result.get('most_common_duplicates', [])
        if most_common and any(count > 1 for _, count in most_common):
            print(f"   Most Common Duplicates:")
            for item_id, count in most_common:
                if count > 1:
                    print(f"     - ID {item_id}: {count} occurrences")
        
        # Show paper duplicate details if available
        if entity_type == "papers" and result.get('duplicate_details'):
            print(f"   Example Duplicate Papers:")
            seen_pmids = set()
            for detail in result['duplicate_details'][:5]:  # Show first 5
                pmid = detail['pmid']
                if pmid not in seen_pmids:
                    seen_pmids.add(pmid)
                    print(f"     - PMID {pmid}: {detail['title']}")
    
    print("\n" + "-"*80)
    print("💡 RECOMMENDATIONS")
    print("-"*80)
    
    recommendations = results.get("recommendations", [])
    for rec in recommendations:
        print(f"   {rec}")

def main():
    """Main function to analyze duplicates in TSV files"""
    
    print("🔍 Duplicate Detection Tool for PKG 2.0 GraphRAG")
    print("=" * 60)
    
    try:
        # Set base directory (current working directory)
        base_dir = Path(".")
        
        print(f"📂 Analyzing files in: {base_dir.absolute()}")
        
        # Run comprehensive duplicate analysis
        print("\n🔄 Starting duplicate analysis...")
        results = detector.analyze_all_files(base_dir)
        
        # Print formatted results
        print_duplicate_summary(results)
        
        # Check if duplicates were found
        summary = results.get("analysis_summary", {})
        duplicate_count = summary.get("duplicate_records_found", 0)
        
        if duplicate_count > 0:
            print(f"\n⚠️  Found {duplicate_count} duplicate records across files.")
            print("✅ Good news: Your entity loaders now use MERGE queries to handle duplicates safely!")
            print("\n💡 You can now run your ingestion without constraint violation errors.")
            return 0
        else:
            print("\n🎉 No duplicates found! Your data is clean.")
            return 0
            
    except Exception as e:
        logger.error(f"Error during duplicate analysis: {str(e)}")
        print(f"\n❌ Error: {str(e)}")
        print("\nTroubleshooting:")
        print("1. Make sure you're running this from the project root directory")
        print("2. Check that your TSV files exist")
        print("3. Verify file permissions")
        return 1

if __name__ == "__main__":
    sys.exit(main())