#!/usr/bin/env python3
"""
Test Script for Enhanced Logging Functionality

This script demonstrates how to test the enhanced logging features 
for monitoring long-running ingestion processes.

Usage:
    python test_enhanced_logging.py
"""

import sys
from pathlib import Path
from datetime import datetime

# Add the project root to Python path
sys.path.append(str(Path(__file__).parent))

from src.utils.progress_tracker import LoggingConfig, ProgressTracker


def test_basic_progress_tracker():
    """Test basic progress tracker functionality"""
    print("Testing Basic Progress Tracker...")
    
    # Set up enhanced logging
    logger = LoggingConfig.setup_ingestion_logging(
        process_name="test_progress_tracker"
    )
    
    # Simulate a multi-file process
    test_files = [
        "sample_file_1.tsv",
        "sample_file_2.tsv", 
        "sample_file_3.tsv",
        "sample_file_4.tsv"
    ]
    
    tracker = ProgressTracker(logger, len(test_files), "Test File Processing")
    
    # Simulate processing each file
    import time
    import random
    
    for file_path in test_files:
        # Simulate file analysis
        estimated_records = random.randint(1000, 10000)
        tracker.start_file(file_path, estimated_records)
        
        # Simulate batch processing
        processed = 0
        batch_size = random.randint(100, 500)
        
        while processed < estimated_records:
            time.sleep(0.1)  # Simulate processing time
            processed += min(batch_size, estimated_records - processed)
            
            # Report progress every few batches
            if processed % (batch_size * 3) == 0 or processed >= estimated_records:
                tracker.update_file_progress(processed, estimated_records)
        
        # Complete the file
        success = random.random() > 0.1  # 90% success rate
        if success:
            tracker.complete_file(processed, success=True)
        else:
            tracker.complete_file(0, success=False, error_msg="Simulated error")
    
    # Log final summary
    tracker.log_final_summary()
    
    print("✅ Basic progress tracker test completed!")
    return True


def test_logging_configuration():
    """Test different logging configurations"""
    print("Testing Logging Configuration...")
    
    # Test with different process names
    logger1 = LoggingConfig.setup_ingestion_logging(
        process_name="test_full_ingestion"
    )
    logger1.info("This is a test message from full ingestion logger")
    
    logger2 = LoggingConfig.setup_ingestion_logging(
        process_name="test_entity_loading"
    )
    logger2.info("This is a test message from entity loading logger")
    
    # Test different log levels
    logger1.debug("Debug message (should not appear)")
    logger1.info("Info message")
    logger1.warning("Warning message")
    logger1.error("Error message")
    
    print("✅ Logging configuration test completed!")
    return True


def test_progress_monitoring_simulation():
    """Simulate a realistic ingestion scenario for monitoring"""
    print("Testing Progress Monitoring Simulation...")
    
    logger = LoggingConfig.setup_ingestion_logging(
        process_name="medgraph_full_simulation"
    )
    
    # Simulate realistic file sizes and processing times
    realistic_files = [
        ("C01_Papers.tsv", 45000000),         # 45M records
        ("C07_Authors.tsv", 8500000),         # 8.5M records  
        ("C02_Link_Papers_Authors.tsv", 95000000),  # 95M records
        ("C06_Link_Papers_BioEntities.tsv", 150000000),  # 150M records
        ("C15_Patents.tsv", 7500000),         # 7.5M records
        ("C11_ClinicalTrials.tsv", 450000),   # 450K records
    ]
    
    tracker = ProgressTracker(logger, len(realistic_files), "Realistic Full Ingestion Simulation")
    
    for filename, estimated_records in realistic_files:
        tracker.start_file(filename, estimated_records)
        
        # Simulate realistic processing speeds (records per second)
        if "Papers" in filename:
            records_per_sec = 5000
        elif "Authors" in filename:
            records_per_sec = 8000
        elif "Link" in filename:
            records_per_sec = 12000
        else:
            records_per_sec = 6000
        
        # Simulate processing with realistic progress updates
        processed = 0
        import time
        
        # Process in chunks to demonstrate progress updates
        while processed < min(estimated_records, 50000):  # Limit for demo
            chunk_size = min(records_per_sec // 10, estimated_records - processed)  # Process 1/10 second chunks
            processed += chunk_size
            
            time.sleep(0.1)  # Simulate processing time
            
            # Update progress every 5% or every 10 seconds
            if processed % (estimated_records // 20) < chunk_size or processed >= estimated_records:
                tracker.update_file_progress(processed, min(estimated_records, 50000))
        
        tracker.complete_file(processed, success=True)
    
    tracker.log_final_summary()
    
    print("✅ Progress monitoring simulation completed!")
    return True


def main():
    """Run all tests"""
    print("🧪 Starting Enhanced Logging Tests")
    print("=" * 60)
    
    tests = [
        ("Basic Progress Tracker", test_basic_progress_tracker),
        ("Logging Configuration", test_logging_configuration),
        ("Progress Monitoring Simulation", test_progress_monitoring_simulation)
    ]
    
    results = []
    for test_name, test_func in tests:
        print(f"\n🔄 Running: {test_name}")
        try:
            success = test_func()
            results.append((test_name, success))
            print(f"✅ {test_name}: PASSED")
        except Exception as e:
            results.append((test_name, False))
            print(f"❌ {test_name}: FAILED - {str(e)}")
        print("-" * 40)
    
    # Summary
    print("\n" + "=" * 60)
    print("TEST SUMMARY")
    print("=" * 60)
    
    passed = sum(1 for _, success in results if success)
    total = len(results)
    
    for test_name, success in results:
        status = "✅ PASSED" if success else "❌ FAILED"
        print(f"{test_name}: {status}")
    
    print(f"\nOverall: {passed}/{total} tests passed")
    
    if passed == total:
        print("🎉 All tests passed! Enhanced logging is ready for use.")
        print("\n📝 To use enhanced logging for full ingestion:")
        print("   python main.py --mode full --load-type both")
        print("   Monitor the timestamped log file created in the current directory")
        print("   Use 'tail -f <logfile>' to monitor progress in real-time")
    else:
        print("⚠️ Some tests failed. Please check the implementation.")
    
    return passed == total


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
