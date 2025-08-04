"""
TSV Reader Module for PKG 2.0 GraphRAG Implementation

Handles robust TSV file parsing with proper encoding, error handling,
and memory-efficient batch processing for large files.
"""

import pandas as pd
import numpy as np
from pathlib import Path
from typing import Iterator, Dict, Any, Optional, List
import logging
from contextlib import contextmanager
import csv
from io import StringIO

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class TSVReader:
    """
    Memory-efficient TSV reader with robust error handling and data validation.
    Designed to handle very large files (50GB+) through batch processing.
    """
    
    def __init__(self, 
                 file_path: Path,
                 batch_size: int = 1000,
                 encoding: str = 'utf-8',
                 low_memory: bool = True):
        """
        Initialize TSV reader
        
        Args:
            file_path: Path to TSV file
            batch_size: Number of records per batch
            encoding: File encoding (utf-8, latin-1, etc.)
            low_memory: Use low memory mode for large files
        """
        self.file_path = Path(file_path)
        self.batch_size = batch_size
        self.encoding = encoding
        self.low_memory = low_memory
        self.total_rows = None
        self.columns = None
        
        if not self.file_path.exists():
            raise FileNotFoundError(f"TSV file not found: {file_path}")
    
    def __enter__(self):
        """Context manager entry"""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit"""
        # Cleanup if needed
        pass
    
    def get_file_info(self) -> Dict[str, Any]:
        """Get basic file information without loading all data"""
        try:
            # Read first few rows to get column info
            sample_df = pd.read_csv(
                self.file_path, 
                sep='\t', 
                nrows=5,
                encoding=self.encoding,
                low_memory=self.low_memory
            )
            
            # Get file size
            file_size_mb = self.file_path.stat().st_size / (1024 * 1024)
            
            # Estimate total rows (rough estimate)
            with open(self.file_path, 'r', encoding=self.encoding) as f:
                # Count lines in first MB to estimate
                first_mb = f.read(1024 * 1024)
                lines_in_mb = first_mb.count('\n')
                estimated_rows = int((file_size_mb * lines_in_mb) - 1)  # -1 for header
            
            return {
                'file_path': str(self.file_path),
                'file_size_mb': round(file_size_mb, 2),
                'estimated_rows': estimated_rows,
                'columns': list(sample_df.columns),
                'column_count': len(sample_df.columns),
                'sample_data': sample_df.head(3).to_dict('records')
            }
            
        except Exception as e:
            logger.error(f"Error reading file info from {self.file_path}: {str(e)}")
            raise
    
    def read_batches(self, 
                    columns: Optional[List[str]] = None,
                    skip_rows: int = 0,
                    error_bad_lines: bool = False,
                    warn_bad_lines: bool = True) -> Iterator[pd.DataFrame]:
        """
        Read TSV file in batches for memory-efficient processing
        
        Args:
            columns: Specific columns to read (None for all)
            skip_rows: Number of rows to skip from beginning
            error_bad_lines: If True, raise error on malformed lines. If False, skip them.
            warn_bad_lines: If True, log warnings for skipped lines
            
        Yields:
            DataFrame batches
        """
        try:
            # Prepare pandas read_csv parameters
            read_csv_params = {
                'sep': '\t',
                'chunksize': self.batch_size,
                'encoding': self.encoding,
                'low_memory': self.low_memory,
                'usecols': columns,
                'skiprows': range(1, skip_rows + 1) if skip_rows > 0 else None,
                'dtype': str,  # Read everything as string initially
                'na_values': ['NULL', 'null', '', 'NaN', 'nan'],
                'keep_default_na': True,
                'engine': 'python'  # Use python engine for better error handling
            }
            
            # Add on_bad_lines parameter if available (pandas >= 1.3.0)
            try:
                import pandas as pd_version
                if hasattr(pd, '__version__'):
                    version_parts = pd.__version__.split('.')
                    major, minor = int(version_parts[0]), int(version_parts[1])
                    if major > 1 or (major == 1 and minor >= 3):
                        read_csv_params['on_bad_lines'] = 'skip' if not error_bad_lines else 'error'
            except:
                pass  # Ignore version check errors
            
            # Use pandas chunker for efficient batch reading with error handling
            chunk_reader = pd.read_csv(self.file_path, **read_csv_params)
            
            batch_count = 0
            total_rows_processed = 0
            skipped_lines = 0
            
            for batch_df in chunk_reader:
                batch_count += 1
                total_rows_processed += len(batch_df)
                
                # Clean and validate batch
                batch_df = self._clean_batch(batch_df)
                
                if batch_count % 100 == 0:
                    logger.info(f"Processed {batch_count} batches, {total_rows_processed} total rows")
                
                yield batch_df
                
        except Exception as e:
            # Try fallback approach with more robust error handling
            error_msg = str(e)
            if any(pattern in error_msg for pattern in [
                "Expected", "saw", "expected after", "Error tokenizing", 
                "expected after", "CSV", "Tokenizing", "fields"
            ]):
                logger.warning(f"Pandas CSV parser failed: {error_msg}")
                logger.warning("Switching to robust fallback parser...")
                yield from self._read_batches_robust(columns, skip_rows)
            else:
                logger.error(f"Error reading batches from {self.file_path}: {str(e)}")
                raise
    
    def _clean_batch(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean and standardize batch data"""
        # Strip whitespace from string columns
        string_columns = df.select_dtypes(include=['object']).columns
        for col in string_columns:
            df[col] = df[col].astype(str).str.strip()
            # Replace 'nan' strings with actual NaN (suppress warning)
            df[col] = df[col].replace('nan', np.nan).infer_objects(copy=False)
        
        return df
    
    def _read_batches_robust(self, 
                           columns: Optional[List[str]] = None,
                           skip_rows: int = 0) -> Iterator[pd.DataFrame]:
        """
        Robust TSV reader that handles malformed lines by parsing manually
        
        Args:
            columns: Specific columns to read
            skip_rows: Number of rows to skip
            
        Yields:
            DataFrame batches
        """
        logger.info(f"🛠️ Using robust TSV parser for {self.file_path}")
        
        try:
            with open(self.file_path, 'r', encoding=self.encoding) as file:
                # Read header
                header_line = file.readline().strip()
                header_columns = header_line.split('\t')
                expected_field_count = len(header_columns)
                
                logger.info(f"Expected {expected_field_count} fields based on header: {header_columns}")
                
                # Skip specified rows
                for _ in range(skip_rows):
                    file.readline()
                
                batch_data = []
                line_number = skip_rows + 1  # +1 for header
                total_processed = 0
                skipped_lines = 0
                batch_count = 0
                
                for line in file:
                    line_number += 1
                    line = line.strip()
                    
                    if not line:  # Skip empty lines
                        continue
                    
                    # Handle potential quote issues by using a more robust split
                    try:
                        # Try csv.reader for proper quote handling
                        csv_reader = csv.reader(StringIO(line), delimiter='\t', quotechar='"')
                        fields = next(csv_reader)
                    except:
                        # Fallback to simple split if csv.reader fails
                        fields = line.split('\t')
                        # Clean up any problematic quotes
                        fields = [field.replace('""', '"').strip('"') for field in fields]
                    
                    # Handle malformed lines
                    if len(fields) != expected_field_count:
                        skipped_lines += 1
                        
                        # Log first few malformed lines for debugging
                        if skipped_lines <= 5:
                            logger.warning(f"Line {line_number}: Expected {expected_field_count} fields, "
                                         f"got {len(fields)}. Skipping malformed line.")
                            logger.debug(f"Malformed line content: {line[:200]}...")
                        
                        # Attempt to fix common issues
                        if len(fields) > expected_field_count:
                            # Too many fields - try to merge last fields
                            fixed_fields = fields[:expected_field_count-1] + ['\t'.join(fields[expected_field_count-1:])]
                            if len(fixed_fields) == expected_field_count:
                                fields = fixed_fields
                                logger.debug(f"Fixed line {line_number} by merging excess fields")
                            else:
                                continue  # Still malformed, skip
                        else:
                            # Too few fields - pad with empty strings
                            fields.extend([''] * (expected_field_count - len(fields)))
                            logger.debug(f"Fixed line {line_number} by padding with empty fields")
                    
                    # Create record
                    record = dict(zip(header_columns, fields))
                    
                    # Filter columns if specified
                    if columns:
                        record = {k: v for k, v in record.items() if k in columns}
                    
                    batch_data.append(record)
                    total_processed += 1
                    
                    # Yield batch when full
                    if len(batch_data) >= self.batch_size:
                        batch_df = pd.DataFrame(batch_data)
                        batch_df = self._clean_batch(batch_df)
                        
                        batch_count += 1
                        if batch_count % 100 == 0:
                            logger.info(f"Robust parser: Processed {batch_count} batches, "
                                      f"{total_processed} rows, skipped {skipped_lines} malformed lines")
                        
                        yield batch_df
                        batch_data = []
                
                # Yield final batch if not empty
                if batch_data:
                    batch_df = pd.DataFrame(batch_data)
                    batch_df = self._clean_batch(batch_df)
                    yield batch_df
                
                if skipped_lines > 0:
                    logger.warning(f"Robust parser completed: Skipped {skipped_lines} malformed lines out of {total_processed + skipped_lines} total lines")
                else:
                    logger.info(f"Robust parser completed: Successfully processed {total_processed} lines")
                    
        except Exception as e:
            logger.error(f"Robust parser failed: {str(e)}")
            raise
    
    def read_sample(self, n_rows: int = 1000) -> pd.DataFrame:
        """Read a sample of n rows for testing and validation"""
        try:
            sample_df = pd.read_csv(
                self.file_path,
                sep='\t',
                nrows=n_rows,
                encoding=self.encoding,
                low_memory=self.low_memory,
                dtype=str,
                na_values=['NULL', 'null', '', 'NaN', 'nan']
            )
            
            return self._clean_batch(sample_df)
            
        except Exception as e:
            logger.error(f"Error reading sample from {self.file_path}: {str(e)}")
            raise
    
    def validate_columns(self, expected_columns: List[str]) -> Dict[str, Any]:
        """Validate that file has expected columns"""
        try:
            sample_df = pd.read_csv(
                self.file_path,
                sep='\t',
                nrows=1,
                encoding=self.encoding
            )
            
            actual_columns = list(sample_df.columns)
            missing_columns = set(expected_columns) - set(actual_columns)
            extra_columns = set(actual_columns) - set(expected_columns)
            
            return {
                'valid': len(missing_columns) == 0,
                'actual_columns': actual_columns,
                'expected_columns': expected_columns,
                'missing_columns': list(missing_columns),
                'extra_columns': list(extra_columns)
            }
            
        except Exception as e:
            logger.error(f"Error validating columns for {self.file_path}: {str(e)}")
            raise


@contextmanager
def tsv_reader(file_path: Path, **kwargs):
    """Context manager for TSV reader"""
    reader = TSVReader(file_path, **kwargs)
    try:
        yield reader
    finally:
        # Cleanup if needed
        pass


def create_sample_file(source_path: Path, 
                      sample_path: Path, 
                      n_rows: int = 10000,
                      encoding: str = 'utf-8') -> Dict[str, Any]:
    """
    Create a sample TSV file from a large source file for testing
    
    Args:
        source_path: Path to source TSV file
        sample_path: Path for sample output file
        n_rows: Number of rows to sample
        encoding: File encoding
        
    Returns:
        Dictionary with sample file information
    """
    try:
        logger.info(f"Creating sample file from {source_path}")
        
        # Read sample
        with tsv_reader(source_path, encoding=encoding) as reader:
            sample_df = reader.read_sample(n_rows)
        
        # Write sample
        sample_df.to_csv(sample_path, sep='\t', index=False, encoding=encoding)
        
        # Get info about created sample
        sample_size_mb = sample_path.stat().st_size / (1024 * 1024)
        
        return {
            'source_file': str(source_path),
            'sample_file': str(sample_path),
            'rows_sampled': len(sample_df),
            'sample_size_mb': round(sample_size_mb, 2),
            'columns': list(sample_df.columns)
        }
        
    except Exception as e:
        logger.error(f"Error creating sample file: {str(e)}")
        raise


def analyze_tsv_file(file_path: Path) -> Dict[str, Any]:
    """
    Analyze TSV file and return comprehensive information
    
    Args:
        file_path: Path to TSV file
        
    Returns:
        Dictionary with file analysis
    """
    try:
        with tsv_reader(file_path) as reader:
            info = reader.get_file_info()
            
            # Additional analysis
            sample_df = reader.read_sample(1000)
            
            # Analyze data types and null values
            null_counts = sample_df.isnull().sum().to_dict()
            data_types = sample_df.dtypes.astype(str).to_dict()
            
            # Memory usage estimation
            memory_usage_mb = sample_df.memory_usage(deep=True).sum() / (1024 * 1024)
            estimated_full_memory_gb = (memory_usage_mb * info['estimated_rows'] / 1000) / 1024
            
            info.update({
                'null_counts': null_counts,
                'data_types': data_types,
                'sample_memory_mb': round(memory_usage_mb, 2),
                'estimated_full_memory_gb': round(estimated_full_memory_gb, 2)
            })
            
            return info
            
    except Exception as e:
        logger.error(f"Error analyzing TSV file {file_path}: {str(e)}")
        raise