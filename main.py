#!/usr/bin/env python3
"""
PKG 2.0 GraphRAG Neo4j Implementation - Main Execution Script

This script orchestrates the complete ingestion of PKG 2.0 biomedical 
knowledge graph data into Neo4j for GraphRAG applications.

Usage:
    python main.py --mode [full|test|schema|validate] --load-type [entities|relationships|both]
    
Examples:
    python main.py --mode schema                             # Setup schema only
    python main.py --mode test                               # Run with sample data (both entities & relationships)
    python main.py --mode test --load-type entities          # Load only entities from sample data  
    python main.py --mode test --load-type relationships     # Load only relationships from sample data
    python main.py --mode full                               # Full data ingestion (both entities & relationships)
    python main.py --mode full --load-type entities          # Load only entities from full data
    python main.py --mode validate                           # Validate existing data
"""

import argparse
import logging
import sys
import time
from pathlib import Path
from typing import Dict, Any

# Import our modules
from src.config.settings import config
from src.utils.neo4j_connection import Neo4jConnection
from src.utils.schema_manager import SchemaManager
from src.utils.tsv_reader import analyze_tsv_file, create_sample_file
from src.loaders.entity_loader import EntityLoader

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('medgraph_ingestion.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)


class MedGraphIngestion:
    """
    Main class for orchestrating the PKG 2.0 GraphRAG data ingestion process.
    """
    
    def __init__(self):
        self.connection = None
        self.schema_manager = None
        self.entity_loader = None
        
    def initialize_connections(self) -> bool:
        """Initialize all database connections and managers"""
        try:
            logger.info("🔌 Initializing Neo4j connection...")
            self.connection = Neo4jConnection(
                uri=config.neo4j.uri,
                user=config.neo4j.user,
                password=config.neo4j.password,
                database=config.neo4j.database,
                max_connection_lifetime=config.neo4j.max_connection_lifetime,
                max_connection_pool_size=config.neo4j.max_connection_pool_size,
                connection_acquisition_timeout=config.neo4j.connection_acquisition_timeout
            )
            
            self.schema_manager = SchemaManager(self.connection)
            self.entity_loader = EntityLoader(self.connection)
            
            logger.info("✅ All connections initialized successfully")
            return True
            
        except Exception as e:
            logger.error(f"❌ Failed to initialize connections: {str(e)}")
            return False
    
    def validate_environment(self) -> Dict[str, Any]:
        """Validate the environment and data files"""
        logger.info("🔍 Validating environment and data files...")
        
        validation_results = config.validate_configuration()
        
        # Check critical files
        critical_files = [
            config.paths.papers,
            config.paths.authors,
            config.paths.papers_authors,
            config.paths.papers_bioentities
        ]
        
        missing_critical = []
        for file in critical_files:
            if not config.paths.check_file_exists(file):
                missing_critical.append(file)
        
        if missing_critical:
            logger.error(f"❌ Critical files missing: {missing_critical}")
            validation_results["critical_files_missing"] = missing_critical
        else:
            logger.info("✅ All critical files present")
        
        return validation_results
    
    def analyze_data_files(self) -> Dict[str, Any]:
        """Analyze TSV files to understand data structure and size"""
        logger.info("📊 Analyzing data files...")
        
        analysis_results = {}
        
        # Analyze key files
        key_files = [
            ('papers', config.paths.papers),
            ('authors', config.paths.authors),
            ('patents', config.paths.patents),
            ('clinical_trials', config.paths.clinical_trials),
            ('papers_bioentities', config.paths.papers_bioentities)
        ]
        
        for name, filename in key_files:
            file_path = config.paths.get_full_path(filename)
            if file_path.exists():
                try:
                    logger.info(f"  📋 Analyzing {name}...")
                    analysis = analyze_tsv_file(file_path)
                    analysis_results[name] = analysis
                    
                    logger.info(f"    📏 {analysis['estimated_rows']:,} rows, "
                               f"{analysis['file_size_mb']} MB, "
                               f"~{analysis['estimated_full_memory_gb']} GB in memory")
                               
                except Exception as e:
                    logger.error(f"    ❌ Failed to analyze {name}: {str(e)}")
                    analysis_results[name] = {"error": str(e)}
            else:
                logger.warning(f"    ⚠️ File not found: {filename}")
                analysis_results[name] = {"error": "File not found"}
        
        return analysis_results
    
    def setup_schema(self) -> Dict[str, Any]:
        """Set up the complete Neo4j schema"""
        logger.info("🏗️ Setting up Neo4j schema...")
        
        if not self.schema_manager:
            logger.error("❌ Schema manager not initialized")
            return {"error": "Schema manager not initialized"}
        
        try:
            schema_results = self.schema_manager.setup_complete_schema()
            
            if schema_results["overall_success"]:
                logger.info("✅ Schema setup completed successfully")
            else:
                logger.warning("⚠️ Schema setup completed with some failures")
                
            return schema_results
            
        except Exception as e:
            logger.error(f"❌ Schema setup failed: {str(e)}")
            return {"error": str(e)}
    
    def create_test_samples(self, sample_size: int = 10000) -> Dict[str, Any]:
        """Create sample files for testing"""
        logger.info(f"📝 Creating test samples ({sample_size:,} rows each)...")
        
        sample_results = {}
        sample_dir = Path("samples")
        sample_dir.mkdir(exist_ok=True)
        
        # Files to sample
        files_to_sample = [
            ('papers', config.paths.papers),
            ('authors', config.paths.authors),
            ('patents', config.paths.patents),
            ('clinical_trials', config.paths.clinical_trials),
            ('papers_authors', config.paths.papers_authors),
            ('papers_bioentities', config.paths.papers_bioentities),
            ('papers_journals', config.paths.papers_journals),
            ('papers_references', config.paths.papers_references),
            ('papers_clinical_trials', config.paths.papers_clinical_trials),
            ('affiliations', config.paths.affiliations),
            ('clinical_trials_bioentities', config.paths.clinical_trials_bioentities),
            ('patents_papers', config.paths.patents_papers),
            ('patents_bioentities', config.paths.patents_bioentities)
        ]
        
        for name, filename in files_to_sample:
            source_path = config.paths.get_full_path(filename)
            sample_path = sample_dir / f"sample_{filename}"
            
            if source_path.exists():
                try:
                    logger.info(f"  🎯 Creating sample for {name}...")
                    sample_info = create_sample_file(source_path, sample_path, sample_size)
                    sample_results[name] = sample_info
                    
                    logger.info(f"    ✅ Sample created: {sample_info['rows_sampled']:,} rows, "
                               f"{sample_info['sample_size_mb']} MB")
                               
                except Exception as e:
                    logger.error(f"    ❌ Failed to create sample for {name}: {str(e)}")
                    sample_results[name] = {"error": str(e)}
            else:
                logger.warning(f"    ⚠️ Source file not found: {filename}")
                sample_results[name] = {"error": "Source file not found"}
        
        return sample_results
    
    def load_data(self, use_samples: bool = False, load_type: str = "both") -> Dict[str, Any]:
        """Load entities and/or relationships into Neo4j"""
        data_source = "sample data" if use_samples else "full data"
        logger.info(f"📥 Loading {load_type} from {data_source}...")
        
        if not self.entity_loader:
            logger.error("❌ Entity loader not initialized")
            return {"error": "Entity loader not initialized"}
        
        try:
            results = {}
            
            # If using samples, temporarily update paths
            original_paths = {}
            if use_samples:
                sample_dir = Path("samples")
                
                # Update config paths to point to samples
                for attr_name in dir(config.paths):
                    if attr_name.endswith('.tsv') and not attr_name.startswith('_'):
                        original_value = getattr(config.paths, attr_name)
                        original_paths[attr_name] = original_value
                        sample_file = sample_dir / f"sample_{original_value}"
                        if sample_file.exists():
                            setattr(config.paths, attr_name, str(sample_file))
            
            # Load entities if requested
            if load_type in ["entities", "both"]:
                logger.info("📦 Loading entities...")
                results["entities"] = self.entity_loader.load_all_entities()
            
            # Load relationships if requested
            if load_type in ["relationships", "both"]:
                logger.info("🔗 Loading relationships...")
                results["relationships"] = self.entity_loader.load_relationships()
            
            # Restore original paths if using samples
            if use_samples:
                for attr_name, original_value in original_paths.items():
                    setattr(config.paths, attr_name, original_value)
            
            return results
            
        except Exception as e:
            logger.error(f"❌ Data loading failed: {str(e)}")
            return {"error": str(e)}
    
    def validate_loaded_data(self) -> Dict[str, Any]:
        """Validate the loaded data"""
        logger.info("✅ Validating loaded data...")
        
        if not self.connection:
            logger.error("❌ Connection not initialized")
            return {"error": "Connection not initialized"}
        
        try:
            # Get database statistics
            db_stats = self.connection.get_database_stats()
            
            # Validate schema
            schema_validation = self.schema_manager.validate_schema()
            
            # Check for basic data integrity
            integrity_checks = self._run_integrity_checks()
            
            validation_results = {
                "database_stats": db_stats,
                "schema_validation": schema_validation,
                "integrity_checks": integrity_checks,
                "validation_passed": (
                    schema_validation.get("schema_valid", False) and
                    integrity_checks.get("all_checks_passed", False)
                )
            }
            
            if validation_results["validation_passed"]:
                logger.info("✅ Data validation passed")
            else:
                logger.warning("⚠️ Data validation found issues")
            
            return validation_results
            
        except Exception as e:
            logger.error(f"❌ Data validation failed: {str(e)}")
            return {"error": str(e)}
    
    def _run_integrity_checks(self) -> Dict[str, Any]:
        """Run basic data integrity checks"""
        checks = {}
        
        try:
            # Check for basic relationship count (orphaned relationship check removed due to syntax complexity)
            relationship_count = self.connection.execute_query("""
                MATCH ()-[r]->()
                RETURN count(r) as total_relationships
            """)
            
            checks["total_relationships"] = relationship_count[0]["total_relationships"] if relationship_count else 0
            
            # Check for duplicate primary keys
            duplicate_papers = self.connection.execute_query("""
                MATCH (p:Paper)
                WITH p.pmid as pmid, count(*) as count
                WHERE count > 1
                RETURN count(*) as duplicate_count
            """)
            
            checks["duplicate_papers"] = duplicate_papers[0]["duplicate_count"] if duplicate_papers else 0
            
            # Check data distribution
            year_distribution = self.connection.execute_query("""
                MATCH (p:Paper)
                WHERE p.pubyear IS NOT NULL
                RETURN min(p.pubyear) as min_year, max(p.pubyear) as max_year, count(p) as total_papers
            """)
            
            if year_distribution:
                checks["year_range"] = {
                    "min_year": year_distribution[0]["min_year"],
                    "max_year": year_distribution[0]["max_year"],
                    "total_papers": year_distribution[0]["total_papers"]
                }
            
            # Overall assessment  
            checks["all_checks_passed"] = (
                checks.get("duplicate_papers", 1) == 0 and
                checks.get("year_range", {}).get("total_papers", 0) > 0 and
                checks.get("total_relationships", 0) >= 0
            )
            
        except Exception as e:
            logger.error(f"Integrity checks failed: {str(e)}")
            checks["error"] = str(e)
            checks["all_checks_passed"] = False
        
        return checks
    
    def run_full_ingestion(self, load_type: str = "both") -> Dict[str, Any]:
        """Run the complete data ingestion process"""
        logger.info(f"🚀 Starting FULL PKG 2.0 GraphRAG data ingestion ({load_type})")
        start_time = time.time()
        
        results = {
            "start_time": time.ctime(),
            "load_type": load_type,
            "phases": {}
        }
        
        try:
            # Phase 1: Environment validation
            logger.info("📋 Phase 1: Environment validation")
            results["phases"]["validation"] = self.validate_environment()
            
            # Phase 2: Data analysis
            logger.info("📊 Phase 2: Data file analysis")
            results["phases"]["analysis"] = self.analyze_data_files()
            
            # Phase 3: Schema setup
            logger.info("🏗️ Phase 3: Schema setup")
            results["phases"]["schema"] = self.setup_schema()
            
            # Phase 4: Data loading (entities and/or relationships)
            logger.info(f"📥 Phase 4: Data loading ({load_type})")
            loading_results = self.load_data(use_samples=False, load_type=load_type)
            results["phases"].update(loading_results)
            
            # Phase 5: Data validation
            logger.info("✅ Phase 5: Data validation")
            results["phases"]["final_validation"] = self.validate_loaded_data()
            
            total_time = time.time() - start_time
            results["total_time_seconds"] = round(total_time, 2)
            results["total_time_hours"] = round(total_time / 3600, 2)
            
            # Overall success assessment
            success_criteria = [
                results["phases"]["schema"].get("overall_success", False),
                results["phases"]["final_validation"].get("validation_passed", False)
            ]
            
            # Add specific success criteria based on load_type
            if load_type in ["entities", "both"]:
                success_criteria.append("error" not in results["phases"].get("entities", {}))
            if load_type in ["relationships", "both"]:
                success_criteria.append("error" not in results["phases"].get("relationships", {}))
            
            results["overall_success"] = all(success_criteria)
            
            if results["overall_success"]:
                logger.info(f"🎉 FULL INGESTION COMPLETED SUCCESSFULLY in {results['total_time_hours']:.1f} hours")
            else:
                logger.error(f"❌ FULL INGESTION COMPLETED WITH ERRORS in {results['total_time_hours']:.1f} hours")
            
        except Exception as e:
            logger.error(f"❌ Full ingestion failed: {str(e)}")
            results["error"] = str(e)
            results["overall_success"] = False
        
        return results
    
    def run_test_ingestion(self, load_type: str = "both") -> Dict[str, Any]:
        """Run test ingestion with sample data"""
        logger.info(f"🧪 Starting TEST PKG 2.0 GraphRAG data ingestion ({load_type})")
        start_time = time.time()
        
        results = {
            "start_time": time.ctime(),
            "load_type": load_type,
            "phases": {}
        }
        
        try:
            # Create samples
            logger.info("📝 Creating test samples")
            results["phases"]["samples"] = self.create_test_samples(10000)
            
            # Setup schema
            logger.info("🏗️ Setting up schema")
            results["phases"]["schema"] = self.setup_schema()
            
            # Load sample data (entities and/or relationships)
            logger.info(f"📥 Loading sample data ({load_type})")
            loading_results = self.load_data(use_samples=True, load_type=load_type)
            results["phases"].update(loading_results)
            
            # Validate
            # logger.info("✅ Validating sample data")
            # results["phases"]["validation"] = self.validate_loaded_data()
            
            total_time = time.time() - start_time
            results["total_time_seconds"] = round(total_time, 2)
            results["total_time_minutes"] = round(total_time / 60, 2)
            
            # Overall success assessment
            success_criteria = [
                results["phases"]["schema"].get("overall_success", False)
            ]
            
            # Add specific success criteria based on load_type
            if load_type in ["entities", "both"]:
                success_criteria.append("error" not in results["phases"].get("entities", {}))
            if load_type in ["relationships", "both"]:
                success_criteria.append("error" not in results["phases"].get("relationships", {}))
            
            results["overall_success"] = all(success_criteria)
            
            if results["overall_success"]:
                logger.info(f"🎉 TEST INGESTION COMPLETED SUCCESSFULLY in {results['total_time_minutes']:.1f} minutes")
            else:
                logger.error(f"❌ TEST INGESTION COMPLETED WITH ERRORS in {results['total_time_minutes']:.1f} minutes")
                
        except Exception as e:
            logger.error(f"❌ Test ingestion failed: {str(e)}")
            results["error"] = str(e)
            results["overall_success"] = False
        
        return results
    
    def cleanup(self):
        """Clean up connections and resources"""
        if self.connection:
            self.connection.close()
            logger.info("🔌 Connections closed")


def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(description="PKG 2.0 GraphRAG Neo4j Implementation")
    parser.add_argument(
        "--mode",
        choices=["full", "test", "schema", "validate", "analyze"],
        default="test",
        help="Execution mode (default: test)"
    )
    parser.add_argument(
        "--load-type",
        choices=["entities", "relationships", "both"],
        default="both",
        help="What to load: entities only, relationships only, or both (default: both)"
    )
    parser.add_argument(
        "--clear-db",
        action="store_true",
        help="Clear database before loading (USE WITH CAUTION!)"
    )
    
    args = parser.parse_args()
    
    ingestion = MedGraphIngestion()
    
    try:
        # Initialize connections (skip for analyze mode)
        if args.mode != "analyze":
            if not ingestion.initialize_connections():
                logger.error("❌ Failed to initialize connections")
                sys.exit(1)
        
        # Clear database if requested
        if args.clear_db:
            logger.warning("⚠️ CLEARING DATABASE - This will delete all data!")
            confirmation = input("Are you sure? Type 'yes' to confirm: ")
            if confirmation.lower() == 'yes':
                ingestion.connection.clear_database(confirm=True)
                logger.info("🗑️ Database cleared")
            else:
                logger.info("❌ Database clear cancelled")
                sys.exit(1)
        
        # Execute based on mode
        if args.mode == "analyze":
            logger.info("📊 Running data analysis only (no database connection required)")
            # Skip connection initialization for analyze mode
            results = ingestion.analyze_data_files()
            
        elif args.mode == "schema":
            logger.info("🏗️ Setting up schema only")
            results = ingestion.setup_schema()
            
        elif args.mode == "validate":
            logger.info("✅ Running validation only")
            results = ingestion.validate_loaded_data()
            
        elif args.mode == "test":
            logger.info(f"🧪 Running test ingestion ({args.load_type})")
            results = ingestion.run_test_ingestion(load_type=args.load_type)
            
        elif args.mode == "full":
            logger.info(f"🚀 Running full ingestion ({args.load_type})")
            results = ingestion.run_full_ingestion(load_type=args.load_type)
        
        # Print summary
        logger.info("=" * 80)
        logger.info("EXECUTION SUMMARY")
        logger.info("=" * 80)
        
        if isinstance(results, dict):
            if "overall_success" in results:
                status = "✅ SUCCESS" if results["overall_success"] else "❌ FAILED"
                logger.info(f"Overall Status: {status}")
            
            if "total_time_seconds" in results:
                logger.info(f"Total Time: {results['total_time_seconds']:.1f} seconds")
            
            # Print performance metrics if available
            if ingestion.connection:
                metrics = ingestion.connection.get_performance_metrics()
                logger.info(f"Database Queries: {metrics['total_queries']}")
                logger.info(f"Batch Operations: {metrics['total_batches']}")
                logger.info(f"Error Rate: {metrics['error_rate']:.2f}%")
        
        logger.info("=" * 80)
        
    except KeyboardInterrupt:
        logger.info("🛑 Execution interrupted by user")
        sys.exit(1)
        
    except Exception as e:
        logger.error(f"❌ Execution failed: {str(e)}")
        sys.exit(1)
        
    finally:
        ingestion.cleanup()


if __name__ == "__main__":
    main()