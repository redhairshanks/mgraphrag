"""
Neo4j Connection and Database Utilities for PKG 2.0 GraphRAG Implementation

Provides robust database connection management, transaction handling,
and batch operations optimized for large-scale data ingestion.
"""

import logging
from contextlib import contextmanager
from typing import Dict, Any, List, Optional, Iterator, Tuple
import time
from neo4j import GraphDatabase, Driver, Session, Transaction
from neo4j.exceptions import Neo4jError, TransientError

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class Neo4jConnection:
    """
    Robust Neo4j connection manager with automatic retry, connection pooling,
    and optimized batch operations for large-scale data ingestion.
    """
    
    def __init__(self, 
                 uri: str,
                 user: str,
                 password: str,
                 database: str = "neo4j",
                 max_connection_lifetime: int = 3600,
                 max_connection_pool_size: int = 50,
                 connection_acquisition_timeout: int = 30):
        """
        Initialize Neo4j connection
        
        Args:
            uri: Neo4j connection URI
            user: Username
            password: Password
            database: Database name
            max_connection_lifetime: Max connection lifetime in seconds
            max_connection_pool_size: Max connections in pool
            connection_acquisition_timeout: Connection timeout in seconds
        """
        self.uri = uri
        self.user = user
        self.password = password
        self.database = database
        self.driver: Optional[Driver] = None
        
        # Connection pool settings
        self.max_connection_lifetime = max_connection_lifetime
        self.max_connection_pool_size = max_connection_pool_size
        self.connection_acquisition_timeout = connection_acquisition_timeout
        
        # Performance tracking
        self.query_count = 0
        self.total_query_time = 0.0
        self.batch_count = 0
        self.error_count = 0
        
        # Initialize connection
        self._connect()
    
    def _connect(self):
        """Establish connection to Neo4j"""
        try:
            self.driver = GraphDatabase.driver(
                self.uri,
                auth=(self.user, self.password),
                max_connection_lifetime=self.max_connection_lifetime,
                max_connection_pool_size=self.max_connection_pool_size,
                connection_acquisition_timeout=self.connection_acquisition_timeout
            )
            
            # Test connection
            with self.driver.session(database=self.database) as session:
                result = session.run("RETURN 1 as test")
                test_value = result.single()["test"]
                if test_value == 1:
                    logger.info(f"Successfully connected to Neo4j at {self.uri}")
                else:
                    raise Exception("Connection test failed")
                    
        except Exception as e:
            logger.error(f"Failed to connect to Neo4j: {str(e)}")
            raise
    
    def close(self):
        """Close database connection"""
        if self.driver:
            self.driver.close()
            logger.info("Neo4j connection closed")
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
    
    @contextmanager
    def session(self):
        """Context manager for Neo4j session"""
        session = None
        try:
            session = self.driver.session(database=self.database)
            yield session
        except Exception as e:
            logger.error(f"Session error: {str(e)}")
            raise
        finally:
            if session:
                session.close()
    
    @contextmanager
    def transaction(self):
        """Context manager for Neo4j transaction"""
        with self.session() as session:
            tx = None
            try:
                tx = session.begin_transaction()
                yield tx
                tx.commit()
            except Exception as e:
                if tx:
                    tx.rollback()
                logger.error(f"Transaction error: {str(e)}")
                raise
            finally:
                if tx:
                    tx.close()
    
    def execute_query(self, 
                     query: str, 
                     parameters: Optional[Dict[str, Any]] = None,
                     max_retries: int = 3) -> List[Dict[str, Any]]:
        """
        Execute a single query with retry logic
        
        Args:
            query: Cypher query
            parameters: Query parameters
            max_retries: Maximum retry attempts
            
        Returns:
            List of result records
        """
        start_time = time.time()
        parameters = parameters or {}
        
        for attempt in range(max_retries + 1):
            try:
                with self.session() as session:
                    result = session.run(query, parameters)
                    records = [record.data() for record in result]
                    
                    # Update metrics
                    self.query_count += 1
                    self.total_query_time += time.time() - start_time
                    
                    return records
                    
            except TransientError as e:
                if attempt < max_retries:
                    wait_time = 2 ** attempt  # Exponential backoff
                    logger.warning(f"Transient error, retrying in {wait_time}s: {str(e)}")
                    time.sleep(wait_time)
                    continue
                else:
                    self.error_count += 1
                    logger.error(f"Max retries exceeded for query: {str(e)}")
                    raise
                    
            except Exception as e:
                self.error_count += 1
                logger.error(f"Query execution error: {str(e)}")
                raise
    
    def execute_batch(self, 
                     query: str, 
                     data_batches: Iterator[List[Dict[str, Any]]],
                     batch_size: int = 1000,
                     max_retries: int = 3) -> Dict[str, Any]:
        """
        Execute batch operations with optimized performance
        
        Args:
            query: Cypher query template
            data_batches: Iterator of batch data
            batch_size: Records per batch
            max_retries: Maximum retry attempts per batch
            
        Returns:
            Batch operation statistics
        """
        total_processed = 0
        total_batches = 0
        total_errors = 0
        start_time = time.time()
        
        try:
            for batch_data in data_batches:
                if not batch_data:
                    continue
                
                # Process batch with retry logic
                for attempt in range(max_retries + 1):
                    try:
                        with self.transaction() as tx:
                            tx.run(query, {"batch": batch_data})
                            
                        total_processed += len(batch_data)
                        total_batches += 1
                        self.batch_count += 1
                        
                        # Progress logging
                        if total_batches % 100 == 0:
                            elapsed = time.time() - start_time
                            rate = total_processed / elapsed if elapsed > 0 else 0
                            logger.info(f"Processed {total_batches} batches, "
                                      f"{total_processed} records, "
                                      f"{rate:.1f} records/sec")
                        
                        break  # Success, break retry loop
                        
                    except TransientError as e:
                        if attempt < max_retries:
                            wait_time = 2 ** attempt
                            logger.warning(f"Batch retry {attempt + 1}, waiting {wait_time}s: {str(e)}")
                            time.sleep(wait_time)
                            continue
                        else:
                            total_errors += 1
                            logger.error(f"Batch failed after {max_retries} retries: {str(e)}")
                            break
                            
                    except Exception as e:
                        total_errors += 1
                        logger.error(f"Batch processing error: {str(e)}")
                        break
        
        except Exception as e:
            logger.error(f"Batch operation failed: {str(e)}")
            raise
        
        total_time = time.time() - start_time
        
        return {
            "total_processed": total_processed,
            "total_batches": total_batches,
            "total_errors": total_errors,
            "total_time_seconds": round(total_time, 2),
            "records_per_second": round(total_processed / total_time, 2) if total_time > 0 else 0,
            "success_rate": round((total_batches - total_errors) / total_batches * 100, 2) if total_batches > 0 else 0
        }
    
    def create_constraint(self, constraint_query: str) -> bool:
        """
        Create a database constraint
        
        Args:
            constraint_query: Cypher constraint creation query
            
        Returns:
            True if successful
        """
        try:
            self.execute_query(constraint_query)
            logger.info(f"Constraint created successfully")
            return True
        except Exception as e:
            # Constraint might already exist
            if "already exists" in str(e).lower() or "equivalent" in str(e).lower():
                logger.info(f"Constraint already exists")
                return True
            else:
                logger.error(f"Failed to create constraint: {str(e)}")
                raise
    
    def create_index(self, index_query: str) -> bool:
        """
        Create a database index
        
        Args:
            index_query: Cypher index creation query
            
        Returns:
            True if successful
        """
        try:
            self.execute_query(index_query)
            logger.info(f"Index created successfully")
            return True
        except Exception as e:
            if "already exists" in str(e).lower() or "equivalent" in str(e).lower():
                logger.info(f"Index already exists")
                return True
            else:
                logger.error(f"Failed to create index: {str(e)}")
                raise
    
    def clear_database(self, confirm: bool = False) -> bool:
        """
        Clear all data from database (USE WITH CAUTION!)
        
        Args:
            confirm: Must be True to actually clear database
            
        Returns:
            True if successful
        """
        if not confirm:
            logger.warning("Database clear not confirmed - no action taken")
            return False
        
        try:
            # Clear in batches to avoid memory issues
            batch_size = 10000
            while True:
                result = self.execute_query(f"""
                    MATCH (n)
                    WITH n LIMIT {batch_size}
                    DETACH DELETE n
                    RETURN count(n) as deleted
                """)
                
                deleted = result[0]['deleted'] if result else 0
                if deleted == 0:
                    break
                    
                logger.info(f"Deleted {deleted} nodes")
            
            logger.info("Database cleared successfully")
            return True
            
        except Exception as e:
            logger.error(f"Failed to clear database: {str(e)}")
            raise
    
    def get_database_stats(self) -> Dict[str, Any]:
        """Get database statistics"""
        try:
            # Try basic stats first (no APOC required)
            basic_stats = self.execute_query("""
                MATCH (n)
                OPTIONAL MATCH ()-[r]->()
                RETURN count(DISTINCT n) as total_nodes, count(r) as total_relationships
            """)
            
            result = {
                "total_nodes": basic_stats[0]["total_nodes"] if basic_stats else 0,
                "total_relationships": basic_stats[0]["total_relationships"] if basic_stats else 0
            }
            
            # Try to get labels without APOC
            try:
                labels = self.execute_query("CALL db.labels()")
                label_counts = {}
                
                for label_record in labels:
                    label = label_record["label"]
                    try:
                        count_result = self.execute_query(f"MATCH (n:{label}) RETURN count(n) as count")
                        label_counts[label] = count_result[0]["count"] if count_result else 0
                    except Exception:
                        label_counts[label] = 0
                
                result["node_counts"] = label_counts
                
            except Exception as e:
                logger.warning(f"Could not get label stats: {str(e)}")
                result["note"] = "Label stats not available"
            
            # Try to get relationship types without APOC
            try:
                rel_types = self.execute_query("CALL db.relationshipTypes()")
                rel_counts = {}
                
                for rel_record in rel_types:
                    rel_type = rel_record["relationshipType"]
                    try:
                        count_result = self.execute_query(f"MATCH ()-[r:{rel_type}]->() RETURN count(r) as count")
                        rel_counts[rel_type] = count_result[0]["count"] if count_result else 0
                    except Exception:
                        rel_counts[rel_type] = 0
                
                result["relationship_counts"] = rel_counts
                
            except Exception as e:
                logger.warning(f"Could not get relationship type stats: {str(e)}")
                result["note"] = result.get("note", "") + " Relationship stats not available"
            
            return result
            
        except Exception as e:
            logger.error(f"Failed to get database stats: {str(e)}")
            return {"error": str(e), "total_nodes": 0, "total_relationships": 0}
    
    def get_performance_metrics(self) -> Dict[str, Any]:
        """Get connection performance metrics"""
        avg_query_time = self.total_query_time / self.query_count if self.query_count > 0 else 0
        
        return {
            "total_queries": self.query_count,
            "total_batches": self.batch_count,
            "total_errors": self.error_count,
            "total_query_time": round(self.total_query_time, 2),
            "average_query_time": round(avg_query_time, 4),
            "error_rate": round(self.error_count / max(self.query_count, 1) * 100, 2)
        }