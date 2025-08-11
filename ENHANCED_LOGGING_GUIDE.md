# Enhanced Logging Guide for MedGraph Ingestion

This guide explains how to use the enhanced logging functionality for monitoring long-running full ingestion processes in tmux or screen sessions.

## Features

### 📝 Timestamped Log Files
- Automatically creates timestamped log files (e.g., `medgraph_full_ingestion_20241220_143052.log`)
- Logs are written to both file and console
- Easy to identify and locate specific ingestion runs

### 📊 File-by-File Progress Tracking
- Shows current file being processed
- Displays progress as `[X/Y files]` with percentage completion
- Estimates time remaining based on previous file processing speeds
- Shows processing speed in records per second

### 🔄 Real-Time Progress Updates
- Progress updates within each file (every 5% completion)
- Batch processing statistics
- Memory-friendly streaming for large files

### 📈 Comprehensive Summaries
- Performance breakdown by file size categories
- Success/failure rates
- Total processing time and speeds
- Detailed error reporting

## Usage

### Running Full Ingestion with Enhanced Logging

```bash
# Full ingestion with both entities and relationships
python main.py --mode full --load-type both

# Entities only
python main.py --mode full --load-type entities

# Relationships only  
python main.py --mode full --load-type relationships
```

### Monitoring in tmux/screen

1. **Start a tmux/screen session:**
```bash
# Using tmux
tmux new-session -s medgraph-ingestion

# Using screen
screen -S medgraph-ingestion
```

2. **Run the ingestion:**
```bash
python main.py --mode full --load-type both
```

3. **Detach and monitor the log file:**
```bash
# Detach from tmux: Ctrl+b, then d
# Detach from screen: Ctrl+a, then d

# Monitor the log file in real-time
tail -f medgraph_full_ingestion_*.log
```

4. **Reattach to check progress:**
```bash
# Reattach to tmux
tmux attach-session -t medgraph-ingestion

# Reattach to screen
screen -r medgraph-ingestion
```

## Log Output Examples

### Phase Progress
```
🚀 Starting Full Ingestion (both) - 5 files to process
================================================================================
📂 [1/5] (0.0%) Starting: Environment Validation
✅ Environment Validation completed: 0 records in 0.5s (0 records/sec)
📈 Overall Progress: 1/5 files (20.0%) in 0.5s
```

### File Processing Progress  
```
📂 [2/5] (20.0%) Starting: /path/to/C01_Papers.tsv
   📊 Estimated records: 45,000,000
   ⏰ ETA: 2024-12-20 16:45:32 (2h 15m remaining)
   ⚡ 2,500,000/45,000,000 records (5.6%) | Overall: 22.8% | 5,234 records/sec
   ⚡ 5,000,000/45,000,000 records (11.1%) | Overall: 25.6% | 5,187 records/sec
```

### Final Summary
```
🏁 FULL INGESTION COMPLETE
================================================================================
📊 Files: 5/5 (100.0% success rate)
📈 Records: 297,450,000 total
⏱️  Duration: 8h 23m
⚡ Speed: 9,842 records/second average
🔸 Small files (<100K records): 1 files, avg 8,543 records/sec
🔸 Medium files (100K-1M records): 1 files, avg 7,234 records/sec  
🔸 Large files (>1M records): 3 files, avg 10,123 records/sec
================================================================================
```

## Log File Locations

- **Current Directory**: Log files are created in the same directory where you run the script
- **Naming Pattern**: `medgraph_{mode}_{timestamp}.log`
  - `medgraph_full_ingestion_20241220_143052.log` (full mode)
  - `medgraph_test_20241220_144523.log` (test mode)

## Monitoring Commands

### Real-time Log Monitoring
```bash
# Follow the latest log file
tail -f medgraph_full_ingestion_*.log | grep -E "(📂|⚡|✅|❌|🏁)"

# Show only progress updates
tail -f medgraph_full_ingestion_*.log | grep "⚡"

# Show only file completions
tail -f medgraph_full_ingestion_*.log | grep "✅"
```

### Log Analysis
```bash
# Count successful file loads
grep "✅.*completed" medgraph_full_ingestion_*.log | wc -l

# Find errors
grep "❌" medgraph_full_ingestion_*.log

# Extract processing speeds
grep "records/sec" medgraph_full_ingestion_*.log
```

## Performance Monitoring

The enhanced logging provides several metrics for performance monitoring:

1. **Records per Second**: Real-time processing speed for each file
2. **ETA Calculations**: Dynamic time estimates based on current performance
3. **Memory Usage**: Batch processing prevents memory overflow
4. **Success Rates**: File-level and overall success tracking
5. **Error Details**: Specific error messages with context

## Troubleshooting

### Common Issues

**Log File Not Created:**
- Check write permissions in the current directory
- Ensure the script starts without initialization errors

**Progress Updates Too Frequent:**
- Progress updates occur every ~5% of file completion
- For very large files, this might still be frequent
- Updates are optimized to balance informativeness with log size

**Missing Progress in Log:**
- Ensure you're running in `full` mode (other modes use simpler logging)
- Check that the EntityLoader is properly initialized with the logger

### Log Levels

- **INFO**: Normal progress and completion messages  
- **WARNING**: Skipped files, configuration issues
- **ERROR**: Failed operations, connection problems

## Testing the Enhanced Logging

Run the test script to verify functionality:

```bash
python test_enhanced_logging.py
```

This script demonstrates:
- Progress tracking with simulated data
- Different logging configurations
- Realistic ingestion scenarios

## Tips for Long-Running Sessions

1. **Use adequate disk space**: Log files can grow large (100MB+ for full ingestion)
2. **Monitor system resources**: Keep an eye on CPU and memory usage
3. **Set up log rotation**: Consider implementing log rotation for very long runs
4. **Regular checkpoints**: The system provides natural checkpoints at file completion
5. **Error recovery**: Failed files are logged with details for manual retry

## Configuration

The logging system uses these default settings:
- **Log Level**: INFO
- **Format**: `timestamp | level | module | message`
- **Progress Interval**: Every 5% of file completion
- **File Encoding**: UTF-8

These can be customized in `src/utils/progress_tracker.py` if needed.
