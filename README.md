# PKG 2.0 GraphRAG Neo4j Implementation

A comprehensive system for ingesting PKG 2.0 biomedical knowledge graph data into Neo4j for GraphRAG applications.

## Overview

This implementation handles:
- **36M+ Papers** from PubMed
- **26M+ Authors** with disambiguation
- **1.3M Patents** from USPTO
- **480K Clinical Trials**
- **357K BioEntities** (genes, drugs, diseases, etc.)
- **Complex relationships** between all entities


## Required files in root 
<img width="344" height="421" alt="Screenshot 2025-08-04 at 4 24 55 PM" src="https://github.com/user-attachments/assets/cc942912-14a3-4af5-a7a0-eebe0a44859a" />

### Config.env

```
Neo4j Database Configuration
NEO4J_URI=bolt://localhost:7687
NEO4J_USER=neo4j
NEO4J_PASSWORD=*****
NEO4J_DATABASE=medgraph

# Optional: Memory and Performance Settings
MAX_MEMORY_GB=8
MAX_WORKERS=4
ENABLE_PARALLEL=true

# Optional: Validation Settings
CONTINUE_ON_ERROR=true
MAX_ERRORS_PER_BATCH=100
```

## Quick Start

### 1. Environment Setup

```bash
# Create and activate virtual environment
python3 -m venv medgraph_env
source medgraph_env/bin/activate

# Install dependencies
pip install -r requirements.txt
```

### 2. Neo4j Setup

Install and configure Neo4j:
```bash
# Download Neo4j from https://neo4j.com/download/
Create a new project
neo4j is default database - no need to do anything
```

### 3. Configuration

Copy and configure environment:
```bash
cp config.env.example .env
# Edit .env with your Neo4j credentials
# OR can directly add to settings.py file
```

### 4. Run Test Ingestion

```bash
# Test with sample data (recommended first)
python main.py --mode test --load-type entities|relationships|both

# Analyze data files
python main.py --mode analyze --load-type entities|relationships|both

# Setup schema only
python main.py --mode schema --load-type entities|relationships|both

# Full ingestion (WARNING: Very resource intensive!)
python main.py --mode full --load-type entities|relationships|both
```

## Data Requirements

Ensure these TSV files are present in the project directory:

### Core Entity Files
- `C01_Papers.tsv` (5.4GB) - Scientific papers metadata
- `C07_Authors.tsv` (569MB) - Author information
- `C15_Patents.tsv` (977MB) - Patent documents
- `C11_ClinicalTrials.tsv` (1.1GB) - Clinical trial data
- `C23_Bioentities.tsv` - Bioentities data

### Relationship Files
- `C02_Link_Papers_Authors.tsv` (5.3GB) - Paper-author relationships
- `C04_ReferenceList_Papers.tsv` (13GB) - Citation networks
- `C06_Link_Papers_BioEntities.tsv` (58GB) - Entity mentions in papers
- Additional relationship files (see documentation)

## System Requirements

### Minimum Requirements
- **RAM**: 16GB (32GB+ recommended for full ingestion)
- **Storage**: 200GB+ free space
- **Neo4j**: 4GB+ heap memory
- **Python**: 3.8+

### Performance Recommendations
- **RAM**: 64GB+ for full dataset
- **Storage**: SSD strongly recommended
- **Neo4j**: 8GB+ heap memory
- **CPU**: Multi-core for parallel processing

## Usage Modes

### Test Mode (Recommended First)
```bash
python main.py --mode test --load-type entities|relatinoships|both
Can select one of entities, relationships or both for first testing them individually
```
- Creates 10K record samples from each file
- Tests complete pipeline
- Validates schema and data integrity
- Completes in ~10-30 minutes

### Full Mode (Production)
```bash
python main.py --mode full
```
- Processes complete datasets
- May take 6-24+ hours depending on hardware
- Requires significant memory and storage
- Monitors progress with detailed logging

### Schema Mode
```bash
python main.py --mode schema
```
- Sets up constraints and indexes only
- Prepares database for manual loading
- Useful for custom ingestion workflows

### Analyze Mode
```bash
python main.py --mode analyze
```
- Analyzes TSV files without loading
- Reports file sizes, structures, memory requirements
- Helps plan ingestion strategy

## Architecture

### Entity Types
- **Paper**: Scientific publications with metadata
- **Author**: Disambiguated researchers
- **Patent**: USPTO patent documents
- **ClinicalTrial**: Clinical studies
- **BioEntity**: Biomedical entities (genes, drugs, etc.)
- **Journal**: Publication venues
- **Institution**: Research organizations
- **Project**: NIH funding projects

### Relationship Types
- **AUTHORED_BY**: Paper ↔ Author
- **CITES**: Citations between documents
- **MENTIONS**: Entity mentions in documents
- **PUBLISHED_IN**: Paper ↔ Journal
- **FUNDED_BY**: Document ↔ Project
- **AFFILIATED_WITH**: Author ↔ Institution
- **INVESTIGATES**: Author ↔ Clinical Trial

### GraphRAG Optimization
- Full-text search indexes on all text fields
- Optimized traversal indexes for multi-hop queries
- Composite indexes for complex filtering
- Constraint-based data integrity

## Performance Monitoring

The system provides comprehensive logging and metrics:
- Real-time progress tracking
- Memory usage monitoring
- Query performance metrics
- Error reporting and recovery
- Data integrity validation

## Troubleshooting

### Memory Issues
```bash
# Reduce batch sizes in src/config/settings.py
# Increase Neo4j heap memory
# Close other applications
```

### Connection Issues
```bash
# Verify Neo4j is running: http://localhost:7474
# Check credentials in .env file
# Ensure port 7687 is accessible
```

### Data Issues
```bash
# Run validation: python main.py --mode validate
# Check log files for specific errors
# Verify TSV file integrity
```

## Development

### Project Structure
```
src/
├── config/          # Configuration management
├── loaders/         # Data loading modules
├── utils/           # Database and utility functions
└── validation/      # Data validation modules

tests/               # Test suites
samples/            # Sample data for testing
```

### Adding New Entity Types
1. Extend `EntityLoader` class
2. Add configuration in `settings.py`
3. Update schema in `SchemaManager`
4. Add validation rules

### Performance Tuning
- Adjust batch sizes in configuration
- Optimize Neo4j memory settings
- Enable parallel processing
- Monitor system resources

## License

This implementation is provided for research and educational purposes.

## Support

For issues and questions:
1. Check the logs in `medgraph_ingestion.log`
2. Review the troubleshooting section
3. Validate your data files and Neo4j setup
4. Consider starting with test mode
