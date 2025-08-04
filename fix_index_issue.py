#!/usr/bin/env python3
"""
Quick fix script for Neo4j index size limit issue

This script drops the problematic paper_title_text_idx RANGE index
and ensures full-text indexes are in place for search functionality.
"""

import sys
import logging
from src.config.settings import config
from src.utils.neo4j_connection import Neo4jConnection
from src.utils.schema_manager import SchemaManager

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def main():
    """Fix the indexing issue with large paper titles"""
    
    print("🔧 Neo4j Index Fix Tool")
    print("=" * 50)
    
    try:
        # Connect to Neo4j
        print("📡 Connecting to Neo4j...")
        with Neo4jConnection(
            uri=config.neo4j.uri,
            user=config.neo4j.user,
            password=config.neo4j.password,
            database=config.neo4j.database
        ) as conn:
            
            # Create schema manager
            schema_manager = SchemaManager(conn)
            
            print("🔍 Checking current schema...")
            schema_info = schema_manager.get_schema_info()
            print(f"   Current indexes: {schema_info.get('indexes_count', 0)}")
            
            # Fix large text issues
            print("\n🛠️ Fixing large text property indexing issues...")
            fix_results = schema_manager.fix_large_text_issues()
            
            if fix_results["success"]:
                print("✅ Successfully fixed indexing issues!")
                
                # Show what was done
                dropped = fix_results["dropped_indexes"]
                for index_name, success in dropped.items():
                    status = "✓" if success else "✗"
                    print(f"   {status} Dropped index: {index_name}")
                
                fulltext = fix_results["fulltext_indexes"]
                print(f"\n📝 Full-text indexes status:")
                for index_name, success in fulltext.items():
                    status = "✓" if success else "✗"
                    print(f"   {status} {index_name}")
                
                print("\n🎉 Your data ingestion should now work without index size errors!")
                print("\nNote: Paper titles will still be searchable via full-text search:")
                print("   Use: CALL db.index.fulltext.queryNodes('papers_fulltext', 'search terms')")
                
            else:
                print("❌ Some issues occurred during the fix:")
                print(f"   Dropped indexes: {fix_results['dropped_indexes']}")
                print(f"   Full-text indexes: {fix_results['fulltext_indexes']}")
                return 1
    
    except Exception as e:
        logger.error(f"Failed to fix indexing issues: {str(e)}")
        print(f"\n❌ Error: {str(e)}")
        print("\nTroubleshooting:")
        print("1. Check your Neo4j connection settings in config.env")
        print("2. Ensure Neo4j is running and accessible")
        print("3. Verify your credentials are correct")
        return 1
    
    return 0

if __name__ == "__main__":
    sys.exit(main())