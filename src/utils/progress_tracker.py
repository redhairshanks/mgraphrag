"""
Progress Tracking Utility for MedGraph Ingestion

Provides comprehensive progress tracking and logging for long-running ingestion processes
with file-by-file monitoring and estimated completion times.
"""

import time
import logging
from typing import Dict, Any, List, Optional
from datetime import datetime, timedelta
from pathlib import Path


class ProgressTracker:
    """
    Track progress across multiple files and provide detailed logging
    for monitoring long-running ingestion processes.
    """
    
    def __init__(self, logger: logging.Logger, total_files: int, process_name: str = "Ingestion"):
        """
        Initialize progress tracker
        
        Args:
            logger: Logger instance for progress reporting
            total_files: Total number of files to process
            process_name: Name of the process being tracked
        """
        self.logger = logger
        self.total_files = total_files
        self.process_name = process_name
        self.completed_files = 0
        self.current_file = ""
        self.current_file_start_time = None
        self.overall_start_time = time.time()
        self.file_stats = []
        self.current_file_progress = {"processed": 0, "total": 0}
        
        # Log process initiation
        self.logger.info(f"🚀 Starting {self.process_name} - {self.total_files} files to process")
        self.logger.info("=" * 80)
    
    def start_file(self, file_path: str, estimated_records: int = 0) -> None:
        """
        Mark the start of processing a new file
        
        Args:
            file_path: Path to the file being processed
            estimated_records: Estimated number of records in the file
        """
        self.current_file = Path(file_path).name
        self.current_file_start_time = time.time()
        self.current_file_progress = {"processed": 0, "total": estimated_records}
        
        progress_pct = (self.completed_files / self.total_files) * 100
        
        self.logger.info(f"📂 [{self.completed_files + 1}/{self.total_files}] ({progress_pct:.1f}%) Starting: {self.current_file}")
        if estimated_records > 0:
            self.logger.info(f"   📊 Estimated records: {estimated_records:,}")
        
        # Show ETA if we have previous file data
        if self.file_stats:
            avg_time_per_file = sum(stat['duration'] for stat in self.file_stats) / len(self.file_stats)
            remaining_files = self.total_files - self.completed_files
            eta_seconds = avg_time_per_file * remaining_files
            eta_time = datetime.now() + timedelta(seconds=eta_seconds)
            self.logger.info(f"   ⏰ ETA: {eta_time.strftime('%Y-%m-%d %H:%M:%S')} ({self._format_duration(eta_seconds)} remaining)")
    
    def update_file_progress(self, processed_records: int, total_records: int = None) -> None:
        """
        Update progress within the current file
        
        Args:
            processed_records: Number of records processed so far
            total_records: Total records in file (if different from initial estimate)
        """
        self.current_file_progress["processed"] = processed_records
        if total_records is not None:
            self.current_file_progress["total"] = total_records
        
        if self.current_file_progress["total"] > 0:
            file_pct = (processed_records / self.current_file_progress["total"]) * 100
            overall_pct = ((self.completed_files + (processed_records / self.current_file_progress["total"])) / self.total_files) * 100
            
            # Calculate processing speed
            elapsed = time.time() - self.current_file_start_time if self.current_file_start_time else 0
            records_per_sec = processed_records / elapsed if elapsed > 0 else 0
            
            self.logger.info(f"   ⚡ {processed_records:,}/{self.current_file_progress['total']:,} records "
                           f"({file_pct:.1f}%) | Overall: {overall_pct:.1f}% | {records_per_sec:.0f} records/sec")
    
    def complete_file(self, final_record_count: int, success: bool = True, error_msg: str = None) -> None:
        """
        Mark completion of the current file
        
        Args:
            final_record_count: Final number of records processed
            success: Whether the file was processed successfully
            error_msg: Error message if processing failed
        """
        if not self.current_file_start_time:
            return
        
        duration = time.time() - self.current_file_start_time
        records_per_sec = final_record_count / duration if duration > 0 else 0
        
        # Store file statistics
        file_stat = {
            "filename": self.current_file,
            "records": final_record_count,
            "duration": duration,
            "records_per_sec": records_per_sec,
            "success": success,
            "error": error_msg
        }
        self.file_stats.append(file_stat)
        
        if success:
            self.completed_files += 1
            self.logger.info(f"✅ {self.current_file} completed: {final_record_count:,} records in "
                           f"{self._format_duration(duration)} ({records_per_sec:.0f} records/sec)")
        else:
            self.logger.error(f"❌ {self.current_file} failed: {error_msg}")
        
        # Show overall progress
        overall_pct = (self.completed_files / self.total_files) * 100
        elapsed_total = time.time() - self.overall_start_time
        
        self.logger.info(f"📈 Overall Progress: {self.completed_files}/{self.total_files} files "
                        f"({overall_pct:.1f}%) in {self._format_duration(elapsed_total)}")
        
        # Show summary statistics
        if len(self.file_stats) >= 5 and self.completed_files % 5 == 0:  # Every 5 files
            self._log_summary_stats()
        
        self.logger.info("-" * 60)
    
    def get_summary(self) -> Dict[str, Any]:
        """Get comprehensive summary of the ingestion process"""
        total_duration = time.time() - self.overall_start_time
        successful_files = sum(1 for stat in self.file_stats if stat['success'])
        failed_files = len(self.file_stats) - successful_files
        total_records = sum(stat['records'] for stat in self.file_stats)
        avg_records_per_sec = total_records / total_duration if total_duration > 0 else 0
        
        return {
            "process_name": self.process_name,
            "total_files": self.total_files,
            "completed_files": self.completed_files,
            "successful_files": successful_files,
            "failed_files": failed_files,
            "total_records_processed": total_records,
            "total_duration_seconds": round(total_duration, 2),
            "total_duration_formatted": self._format_duration(total_duration),
            "avg_records_per_second": round(avg_records_per_sec, 2),
            "success_rate": round(successful_files / len(self.file_stats) * 100, 2) if self.file_stats else 0,
            "file_details": self.file_stats
        }
    
    def log_final_summary(self) -> None:
        """Log the final summary of the ingestion process"""
        summary = self.get_summary()
        
        self.logger.info("=" * 80)
        self.logger.info(f"🏁 {self.process_name.upper()} COMPLETE")
        self.logger.info("=" * 80)
        self.logger.info(f"📊 Files: {summary['completed_files']}/{summary['total_files']} "
                        f"({summary['success_rate']:.1f}% success rate)")
        self.logger.info(f"📈 Records: {summary['total_records_processed']:,} total")
        self.logger.info(f"⏱️  Duration: {summary['total_duration_formatted']}")
        self.logger.info(f"⚡ Speed: {summary['avg_records_per_second']:.0f} records/second average")
        
        if summary['failed_files'] > 0:
            self.logger.warning(f"⚠️ {summary['failed_files']} files failed:")
            for stat in summary['file_details']:
                if not stat['success']:
                    self.logger.warning(f"   ❌ {stat['filename']}: {stat['error']}")
        
        # Performance breakdown by file type/size
        self._log_performance_breakdown()
        
        self.logger.info("=" * 80)
    
    def _log_summary_stats(self) -> None:
        """Log summary statistics for recent files"""
        if not self.file_stats:
            return
        
        recent_stats = self.file_stats[-5:]  # Last 5 files
        avg_duration = sum(s['duration'] for s in recent_stats) / len(recent_stats)
        avg_records_per_sec = sum(s['records_per_sec'] for s in recent_stats) / len(recent_stats)
        
        self.logger.info(f"📊 Recent 5 files avg: {avg_records_per_sec:.0f} records/sec, "
                        f"{self._format_duration(avg_duration)} per file")
    
    def _log_performance_breakdown(self) -> None:
        """Log performance breakdown by file characteristics"""
        if not self.file_stats:
            return
        
        # Group files by size ranges
        small_files = [s for s in self.file_stats if s['records'] < 100000]
        medium_files = [s for s in self.file_stats if 100000 <= s['records'] < 1000000]
        large_files = [s for s in self.file_stats if s['records'] >= 1000000]
        
        if small_files:
            avg_speed = sum(s['records_per_sec'] for s in small_files) / len(small_files)
            self.logger.info(f"🔸 Small files (<100K records): {len(small_files)} files, "
                           f"avg {avg_speed:.0f} records/sec")
        
        if medium_files:
            avg_speed = sum(s['records_per_sec'] for s in medium_files) / len(medium_files)
            self.logger.info(f"🔸 Medium files (100K-1M records): {len(medium_files)} files, "
                           f"avg {avg_speed:.0f} records/sec")
        
        if large_files:
            avg_speed = sum(s['records_per_sec'] for s in large_files) / len(large_files)
            self.logger.info(f"🔸 Large files (>1M records): {len(large_files)} files, "
                           f"avg {avg_speed:.0f} records/sec")
    
    def _format_duration(self, seconds: float) -> str:
        """Format duration in a human-readable way"""
        if seconds < 60:
            return f"{seconds:.1f}s"
        elif seconds < 3600:
            minutes = int(seconds // 60)
            secs = seconds % 60
            return f"{minutes}m {secs:.0f}s"
        else:
            hours = int(seconds // 3600)
            minutes = int((seconds % 3600) // 60)
            return f"{hours}h {minutes}m"


class LoggingConfig:
    """
    Enhanced logging configuration for long-running ingestion processes
    """
    
    @staticmethod
    def setup_ingestion_logging(log_dir: Path = None, process_name: str = "medgraph") -> logging.Logger:
        """
        Set up comprehensive logging for ingestion processes
        
        Args:
            log_dir: Directory to store log files (default: current directory)
            process_name: Name of the process for log file naming
            
        Returns:
            Configured logger instance
        """
        if log_dir is None:
            log_dir = Path.cwd()
        log_dir.mkdir(exist_ok=True)
        
        # Create timestamped log filename
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        log_filename = log_dir / f"{process_name}_ingestion_{timestamp}.log"
        
        # Configure logging with both file and console output
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s | %(levelname)-8s | %(name)s | %(message)s',
            handlers=[
                logging.FileHandler(log_filename, mode='w', encoding='utf-8'),
                logging.StreamHandler()
            ],
            force=True  # Override any existing configuration
        )
        
        logger = logging.getLogger(process_name)
        
        # Log setup information
        logger.info("🔧 Logging system initialized")
        logger.info(f"📝 Log file: {log_filename}")
        logger.info(f"🕐 Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info("=" * 80)
        
        return logger


def create_progress_tracker(logger: logging.Logger, file_list: List[str], 
                          process_name: str) -> ProgressTracker:
    """
    Convenience function to create a progress tracker
    
    Args:
        logger: Logger instance
        file_list: List of files to be processed
        process_name: Name of the process
        
    Returns:
        Configured ProgressTracker instance
    """
    return ProgressTracker(logger, len(file_list), process_name)
