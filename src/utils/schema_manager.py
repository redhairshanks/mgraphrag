"""
Neo4j Schema Manager for PKG 2.0 GraphRAG Implementation

Creates and manages database constraints, indexes, and schema optimizations
for the biomedical knowledge graph.
"""

import logging
from typing import List, Dict, Any
from src.utils.neo4j_connection import Neo4jConnection

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class SchemaManager:
    """
    Manages Neo4j database schema including constraints, indexes,
    and performance optimizations for the PKG 2.0 knowledge graph.
    """
    
    def __init__(self, connection: Neo4jConnection):
        """
        Initialize schema manager
        
        Args:
            connection: Neo4j connection instance
        """
        self.connection = connection
    
    def create_all_constraints(self) -> Dict[str, bool]:
        """Create all required constraints for the knowledge graph"""
        constraints = {
            # Primary key constraints (unique identifiers)
            "paper_pmid": "CREATE CONSTRAINT paper_pmid IF NOT EXISTS FOR (p:Paper) REQUIRE p.pmid IS UNIQUE",
            "author_aid": "CREATE CONSTRAINT author_aid IF NOT EXISTS FOR (a:Author) REQUIRE a.aid IS UNIQUE",
            "patent_id": "CREATE CONSTRAINT patent_id IF NOT EXISTS FOR (pt:Patent) REQUIRE pt.patent_id IS UNIQUE",
            "clinical_trial_nct": "CREATE CONSTRAINT clinical_trial_nct IF NOT EXISTS FOR (ct:ClinicalTrial) REQUIRE ct.nct_id IS UNIQUE",
            "bioentity_id": "CREATE CONSTRAINT bioentity_id IF NOT EXISTS FOR (be:BioEntity) REQUIRE be.entity_id IS UNIQUE",
            "journal_issn": "CREATE CONSTRAINT journal_issn IF NOT EXISTS FOR (j:Journal) REQUIRE j.journal_issn IS UNIQUE",
            "institution_id": "CREATE CONSTRAINT institution_id IF NOT EXISTS FOR (i:Institution) REQUIRE i.ind_id IS UNIQUE",
            "project_number": "CREATE CONSTRAINT project_number IF NOT EXISTS FOR (pr:Project) REQUIRE pr.project_number IS UNIQUE",
            
            # Composite constraints for relationship uniqueness
            "paper_author_unique": """
                CREATE CONSTRAINT paper_author_unique IF NOT EXISTS 
                FOR ()-[r:AUTHORED_BY]-() REQUIRE (r.pmid, r.aid) IS UNIQUE
            """,
            
            "paper_reference_unique": """
                CREATE CONSTRAINT paper_reference_unique IF NOT EXISTS 
                FOR ()-[r:CITES]-() REQUIRE (r.citing_pmid, r.cited_pmid) IS UNIQUE
            """,
            
            # Required field constraints
            "paper_pubyear_required": "CREATE CONSTRAINT paper_pubyear_required IF NOT EXISTS FOR (p:Paper) REQUIRE p.pubyear IS NOT NULL",
            "author_aid_required": "CREATE CONSTRAINT author_aid_required IF NOT EXISTS FOR (a:Author) REQUIRE a.aid IS NOT NULL"
        }
        
        results = {}
        for constraint_name, constraint_query in constraints.items():
            try:
                results[constraint_name] = self.connection.create_constraint(constraint_query)
                logger.info(f"✓ Constraint created: {constraint_name}")
            except Exception as e:
                logger.error(f"✗ Failed to create constraint {constraint_name}: {str(e)}")
                results[constraint_name] = False
        
        return results
    
    def create_all_indexes(self) -> Dict[str, bool]:
        """Create all performance indexes for the knowledge graph"""
        indexes = {
            # Range indexes for filtering and sorting
            "paper_pubyear_idx": "CREATE INDEX paper_pubyear_idx IF NOT EXISTS FOR (p:Paper) ON (p.pubyear)",
            "paper_cited_count_idx": "CREATE INDEX paper_cited_count_idx IF NOT EXISTS FOR (p:Paper) ON (p.cited_count)",
            "author_h_index_idx": "CREATE INDEX author_h_index_idx IF NOT EXISTS FOR (a:Author) ON (a.h_index)",
            "author_paper_num_idx": "CREATE INDEX author_paper_num_idx IF NOT EXISTS FOR (a:Author) ON (a.paper_num)",
            "author_recent_year_idx": "CREATE INDEX author_recent_year_idx IF NOT EXISTS FOR (a:Author) ON (a.recent_year)",
            "patent_granted_date_idx": "CREATE INDEX patent_granted_date_idx IF NOT EXISTS FOR (pt:Patent) ON (pt.granted_date)",
            "clinical_trial_start_date_idx": "CREATE INDEX clinical_trial_start_date_idx IF NOT EXISTS FOR (ct:ClinicalTrial) ON (ct.start_date)",
            "bioentity_type_idx": "CREATE INDEX bioentity_type_idx IF NOT EXISTS FOR (be:BioEntity) ON (be.type)",
            "journal_sjr_idx": "CREATE INDEX journal_sjr_idx IF NOT EXISTS FOR (j:Journal) ON (j.journal_sjr)",
            
            # Composite indexes for complex queries
            "paper_year_cited_idx": "CREATE INDEX paper_year_cited_idx IF NOT EXISTS FOR (p:Paper) ON (p.pubyear, p.cited_count)",
            "author_recent_h_index_idx": "CREATE INDEX author_recent_h_index_idx IF NOT EXISTS FOR (a:Author) ON (a.recent_year, a.h_index)",
            "bioentity_type_mention_idx": "CREATE INDEX bioentity_type_mention_idx IF NOT EXISTS FOR (be:BioEntity) ON (be.type, be.mention)",
            
            # Text indexes for search functionality (excluding large text fields that have full-text indexes)
            # Note: Paper titles are indexed via full-text index to handle large titles (>32KB limit)
            "patent_title_text_idx": "CREATE INDEX patent_title_text_idx IF NOT EXISTS FOR (pt:Patent) ON (pt.title)",
            "clinical_trial_title_text_idx": "CREATE INDEX clinical_trial_title_text_idx IF NOT EXISTS FOR (ct:ClinicalTrial) ON (ct.brief_title)",
            "bioentity_mention_text_idx": "CREATE INDEX bioentity_mention_text_idx IF NOT EXISTS FOR (be:BioEntity) ON (be.mention)",
            "journal_title_text_idx": "CREATE INDEX journal_title_text_idx IF NOT EXISTS FOR (j:Journal) ON (j.journal_title)",
            "institution_name_text_idx": "CREATE INDEX institution_name_text_idx IF NOT EXISTS FOR (i:Institution) ON (i.institution_name)",
            
            # Relationship property indexes
            "authored_by_order_idx": "CREATE INDEX authored_by_order_idx IF NOT EXISTS FOR ()-[r:AUTHORED_BY]-() ON (r.au_order)",
            "mentions_type_idx": "CREATE INDEX mentions_type_idx IF NOT EXISTS FOR ()-[r:MENTIONS]-() ON (r.type)",
            "mentions_position_idx": "CREATE INDEX mentions_position_idx IF NOT EXISTS FOR ()-[r:MENTIONS]-() ON (r.start_position, r.end_position)"
        }
        
        results = {}
        for index_name, index_query in indexes.items():
            try:
                results[index_name] = self.connection.create_index(index_query)
                logger.info(f"✓ Index created: {index_name}")
            except Exception as e:
                logger.error(f"✗ Failed to create index {index_name}: {str(e)}")
                results[index_name] = False
        
        return results
    
    def create_full_text_indexes(self) -> Dict[str, bool]:
        """Create full-text search indexes"""
        fulltext_indexes = {
            "papers_fulltext": """
                CREATE FULLTEXT INDEX papers_fulltext IF NOT EXISTS
                FOR (p:Paper) ON EACH [p.title, p.abstract]
            """,
            
            "patents_fulltext": """
                CREATE FULLTEXT INDEX patents_fulltext IF NOT EXISTS
                FOR (pt:Patent) ON EACH [pt.title, pt.abstract]
            """,
            
            "clinical_trials_fulltext": """
                CREATE FULLTEXT INDEX clinical_trials_fulltext IF NOT EXISTS
                FOR (ct:ClinicalTrial) ON EACH [ct.brief_title, ct.official_title, ct.brief_summaries]
            """,
            
            "bioentities_fulltext": """
                CREATE FULLTEXT INDEX bioentities_fulltext IF NOT EXISTS
                FOR (be:BioEntity) ON EACH [be.mention, be.synonyms]
            """
        }
        
        results = {}
        for index_name, index_query in fulltext_indexes.items():
            try:
                results[index_name] = self.connection.create_index(index_query)
                logger.info(f"✓ Full-text index created: {index_name}")
            except Exception as e:
                logger.error(f"✗ Failed to create full-text index {index_name}: {str(e)}")
                results[index_name] = False
        
        return results
    
    def setup_complete_schema(self) -> Dict[str, Any]:
        """Set up complete database schema with constraints and indexes"""
        logger.info("🚀 Setting up complete Neo4j schema for PKG 2.0 GraphRAG")
        
        # Create constraints first (they also create indexes)
        logger.info("📋 Creating constraints...")
        constraint_results = self.create_all_constraints()
        
        # Create performance indexes
        logger.info("🔍 Creating performance indexes...")
        index_results = self.create_all_indexes()
        
        # Create full-text indexes
        logger.info("📝 Creating full-text search indexes...")
        fulltext_results = self.create_full_text_indexes()
        
        # Summary
        total_constraints = len(constraint_results)
        successful_constraints = sum(constraint_results.values())
        
        total_indexes = len(index_results)
        successful_indexes = sum(index_results.values())
        
        total_fulltext = len(fulltext_results)
        successful_fulltext = sum(fulltext_results.values())
        
        summary = {
            "constraints": {
                "total": total_constraints,
                "successful": successful_constraints,
                "failed": total_constraints - successful_constraints,
                "details": constraint_results
            },
            "indexes": {
                "total": total_indexes,
                "successful": successful_indexes,
                "failed": total_indexes - successful_indexes,
                "details": index_results
            },
            "fulltext_indexes": {
                "total": total_fulltext,
                "successful": successful_fulltext,
                "failed": total_fulltext - successful_fulltext,
                "details": fulltext_results
            },
            "overall_success": (
                successful_constraints == total_constraints and
                successful_indexes == total_indexes and
                successful_fulltext == total_fulltext
            )
        }
        
        logger.info(f"✅ Schema setup complete: "
                   f"{successful_constraints}/{total_constraints} constraints, "
                   f"{successful_indexes}/{total_indexes} indexes, "
                   f"{successful_fulltext}/{total_fulltext} full-text indexes")
        
        return summary
    
    def get_schema_info(self) -> Dict[str, Any]:
        """Get current database schema information"""
        try:
            # Get constraints
            constraints = self.connection.execute_query("SHOW CONSTRAINTS")
            
            # Get indexes
            indexes = self.connection.execute_query("SHOW INDEXES")
            
            # Get labels
            labels = self.connection.execute_query("CALL db.labels()")
            
            # Get relationship types
            rel_types = self.connection.execute_query("CALL db.relationshipTypes()")
            
            return {
                "constraints_count": len(constraints),
                "indexes_count": len(indexes),
                "node_labels": [label["label"] for label in labels],
                "relationship_types": [rel["relationshipType"] for rel in rel_types],
                "constraints": constraints,
                "indexes": indexes
            }
            
        except Exception as e:
            logger.error(f"Failed to get schema info: {str(e)}")
            return {"error": str(e)}
    
    def drop_problematic_indexes(self) -> Dict[str, bool]:
        """Drop indexes that cause issues with large text values"""
        indexes_to_drop = [
            "paper_title_text_idx",  # Causes issues with large titles
        ]
        
        results = {}
        
        for index_name in indexes_to_drop:
            try:
                self.connection.execute_query(f"DROP INDEX {index_name} IF EXISTS")
                results[index_name] = True
                logger.info(f"✓ Dropped problematic index: {index_name}")
            except Exception as e:
                # Index might not exist, which is fine
                if "not found" in str(e).lower() or "does not exist" in str(e).lower():
                    results[index_name] = True
                    logger.info(f"✓ Index {index_name} already removed or doesn't exist")
                else:
                    results[index_name] = False
                    logger.error(f"✗ Failed to drop index {index_name}: {str(e)}")
        
        return results
    
    def fix_large_text_issues(self) -> Dict[str, Any]:
        """Fix issues with large text properties that exceed index limits"""
        logger.info("🔧 Fixing large text property indexing issues...")
        
        # Drop problematic indexes
        drop_results = self.drop_problematic_indexes()
        
        # Verify full-text indexes exist for search functionality
        fulltext_results = self.create_full_text_indexes()
        
        return {
            "dropped_indexes": drop_results,
            "fulltext_indexes": fulltext_results,
            "success": all(drop_results.values()) and all(fulltext_results.values())
        }
    
    def validate_schema(self) -> Dict[str, Any]:
        """Validate that the schema is properly set up"""
        try:
            schema_info = self.get_schema_info()
            
            # Expected entities
            expected_labels = {
                "Paper", "Author", "Patent", "ClinicalTrial", 
                "BioEntity", "Journal", "Institution", "Project"
            }
            
            # Expected relationships
            expected_relationships = {
                "AUTHORED_BY", "CITES", "MENTIONS", "PUBLISHED_IN",
                "FUNDED_BY", "AFFILIATED_WITH", "INVESTIGATES"
            }
            
            actual_labels = set(schema_info.get("node_labels", []))
            actual_relationships = set(schema_info.get("relationship_types", []))
            
            missing_labels = expected_labels - actual_labels
            missing_relationships = expected_relationships - actual_relationships
            
            validation_result = {
                "schema_valid": len(missing_labels) == 0 and len(missing_relationships) == 0,
                "constraints_count": schema_info.get("constraints_count", 0),
                "indexes_count": schema_info.get("indexes_count", 0),
                "expected_labels": expected_labels,
                "actual_labels": actual_labels,
                "missing_labels": missing_labels,
                "expected_relationships": expected_relationships,
                "actual_relationships": actual_relationships,
                "missing_relationships": missing_relationships
            }
            
            if validation_result["schema_valid"]:
                logger.info("✅ Schema validation passed")
            else:
                logger.warning(f"⚠️ Schema validation issues: "
                             f"Missing labels: {missing_labels}, "
                             f"Missing relationships: {missing_relationships}")
            
            return validation_result
            
        except Exception as e:
            logger.error(f"Schema validation failed: {str(e)}")
            return {"error": str(e), "schema_valid": False}