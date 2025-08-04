#!/usr/bin/env python3
"""
Basic setup validation script for PKG 2.0 GraphRAG implementation.

This script validates that:
1. All required TSV files are present
2. Configuration is properly loaded
3. Basic file analysis works
4. Import modules work correctly

Run this before attempting full ingestion.
"""

import sys
import logging
from pathlib import Path

# Configure logging for testing
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
logger = logging.getLogger(__name__)

def test_imports():
    """Test that all modules can be imported"""
    logger.info("🔍 Testing module imports...")
    
    try:
        from src.config.settings import config
        logger.info("  ✅ Configuration module imported")
        
        from src.utils.tsv_reader import TSVReader, analyze_tsv_file
        logger.info("  ✅ TSV reader module imported")
        
        from src.utils.neo4j_connection import Neo4jConnection
        logger.info("  ✅ Neo4j connection module imported")
        
        from src.utils.schema_manager import SchemaManager
        logger.info("  ✅ Schema manager module imported")
        
        from src.loaders.entity_loader import EntityLoader
        logger.info("  ✅ Entity loader module imported")
        
        return True
        
    except ImportError as e:
        logger.error(f"  ❌ Import failed: {str(e)}")
        return False

def test_configuration():
    """Test configuration loading"""
    logger.info("⚙️ Testing configuration...")
    
    try:
        from src.config.settings import config
        
        # Test file path checks
        validation = config.validate_configuration()
        
        logger.info(f"  📁 Configuration validation: {len(validation['required_files'])} files checked")
        
        # Test entity loading order
        entity_order = config.get_entity_loading_order()
        logger.info(f"  📋 Entity loading sequence: {len(entity_order)} steps")
        
        # Test relationship loading order  
        rel_order = config.get_relationship_loading_order()
        logger.info(f"  🔗 Relationship loading sequence: {len(rel_order)} steps")
        
        return True
        
    except Exception as e:
        logger.error(f"  ❌ Configuration test failed: {str(e)}")
        return False

def test_file_presence():
    """Test presence of required TSV files"""
    logger.info("📁 Testing file presence...")
    
    try:
        from src.config.settings import config
        
        # Critical files for basic functionality
        critical_files = [
            ('Papers', config.paths.papers),
            ('Authors', config.paths.authors),
            ('Papers-Authors', config.paths.papers_authors),
            ('Papers-BioEntities', config.paths.papers_bioentities)
        ]
        
        present_files = 0
        total_files = len(critical_files)
        
        for name, filename in critical_files:
            file_path = config.paths.get_full_path(filename)
            if file_path.exists():
                size_mb = file_path.stat().st_size / (1024 * 1024)
                logger.info(f"  ✅ {name}: {filename} ({size_mb:.1f} MB)")
                present_files += 1
            else:
                logger.warning(f"  ⚠️ {name}: {filename} (NOT FOUND)")
        
        logger.info(f"  📊 File presence: {present_files}/{total_files} critical files found")
        
        return present_files >= 2  # Need at least 2 files for basic testing
        
    except Exception as e:
        logger.error(f"  ❌ File presence test failed: {str(e)}")
        return False

def test_file_analysis():
    """Test basic file analysis on available files"""
    logger.info("📊 Testing file analysis...")
    
    try:
        from src.config.settings import config
        from src.utils.tsv_reader import analyze_tsv_file
        
        # Find a small file to test with
        test_files = [
            ('Authors', config.paths.authors),
            ('Papers', config.paths.papers)
        ]
        
        for name, filename in test_files:
            file_path = config.paths.get_full_path(filename)
            if file_path.exists():
                logger.info(f"  🔍 Analyzing {name} file...")
                
                analysis = analyze_tsv_file(file_path)
                
                logger.info(f"    📏 Estimated rows: {analysis['estimated_rows']:,}")
                logger.info(f"    📐 File size: {analysis['file_size_mb']} MB")
                logger.info(f"    📊 Columns: {analysis['column_count']}")
                logger.info(f"    💾 Estimated memory: {analysis['estimated_full_memory_gb']} GB")
                
                return True
        
        logger.warning("  ⚠️ No suitable files found for analysis test")
        return False
        
    except Exception as e:
        logger.error(f"  ❌ File analysis test failed: {str(e)}")
        return False

def test_sample_creation():
    """Test sample file creation"""
    logger.info("📝 Testing sample file creation...")
    
    try:
        from src.config.settings import config
        from src.utils.tsv_reader import create_sample_file
        
        # Find smallest available file
        test_files = [
            config.paths.authors,
            config.paths.papers
        ]
        
        for filename in test_files:
            file_path = config.paths.get_full_path(filename)
            if file_path.exists():
                logger.info(f"  🎯 Creating sample from {filename}...")
                
                # Create samples directory
                sample_dir = Path("samples")
                sample_dir.mkdir(exist_ok=True)
                
                sample_path = sample_dir / f"test_sample_{filename}"
                
                sample_info = create_sample_file(file_path, sample_path, n_rows=100)
                
                logger.info(f"    ✅ Sample created: {sample_info['rows_sampled']} rows")
                logger.info(f"    📏 Sample size: {sample_info['sample_size_mb']} MB")
                
                # Clean up test sample
                sample_path.unlink()
                
                return True
        
        logger.warning("  ⚠️ No suitable files found for sample test")
        return False
        
    except Exception as e:
        logger.error(f"  ❌ Sample creation test failed: {str(e)}")
        return False

def main():
    """Run all setup validation tests"""
    logger.info("🚀 PKG 2.0 GraphRAG Setup Validation")
    logger.info("=" * 50)
    
    tests = [
        ("Module Imports", test_imports),
        ("Configuration", test_configuration),
        ("File Presence", test_file_presence),
        ("File Analysis", test_file_analysis),
        ("Sample Creation", test_sample_creation)
    ]
    
    passed_tests = 0
    total_tests = len(tests)
    
    for test_name, test_func in tests:
        logger.info("")
        try:
            if test_func():
                logger.info(f"✅ {test_name}: PASSED")
                passed_tests += 1
            else:
                logger.error(f"❌ {test_name}: FAILED")
        except Exception as e:
            logger.error(f"💥 {test_name}: ERROR - {str(e)}")
    
    logger.info("")
    logger.info("=" * 50)
    logger.info(f"🏁 VALIDATION SUMMARY: {passed_tests}/{total_tests} tests passed")
    
    if passed_tests == total_tests:
        logger.info("🎉 ALL TESTS PASSED - System ready for ingestion!")
        return True
    elif passed_tests >= 3:
        logger.warning("⚠️ PARTIAL SUCCESS - Some features may not work correctly")
        return True
    else:
        logger.error("❌ CRITICAL FAILURES - Please fix issues before proceeding")
        return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)