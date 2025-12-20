"""
Connection Pool Module

Manages connection pooling for database connectors to improve
performance and resource utilization.
"""

from typing import Dict, Any, Optional
from queue import Queue, Empty
from threading import Lock
import logging


class ConnectionPool:
    """
    Connection pool for managing database connections.
    
    Implements connection reuse and lifecycle management to reduce
    overhead of creating new connections.
    """
    
    def __init__(self, 
                 connector_class: type,
                 config: Dict[str, Any],
                 pool_size: int = 5,
                 max_overflow: int = 10):
        """
        Initialize connection pool.
        
        Args:
            connector_class: Class of connector to pool
            config: Configuration for connectors
            pool_size: Core pool size
            max_overflow: Maximum additional connections beyond pool_size
        """
        self.connector_class = connector_class
        self.config = config
        self.pool_size = pool_size
        self.max_overflow = max_overflow
        
        self.pool: Queue = Queue(maxsize=pool_size + max_overflow)
        self.lock = Lock()
        self.current_size = 0
        self.logger = logging.getLogger(__name__)
        
        # Initialize core pool
        self._initialize_pool()
    
    def _initialize_pool(self) -> None:
        """Create initial pool connections."""
        for _ in range(self.pool_size):
            conn = self._create_connection()
            self.pool.put(conn)
            
    def _create_connection(self) -> Any:
        """
        Create a new connection.
        
        Returns:
            New connector instance
        """
        with self.lock:
            if self.current_size < self.pool_size + self.max_overflow:
                conn = self.connector_class(self.config)
                conn.connect()
                self.current_size += 1
                self.logger.info(f"Created new connection, pool size: {self.current_size}")
                return conn
            else:
                raise RuntimeError("Connection pool exhausted")
    
    def get_connection(self, timeout: Optional[float] = None) -> Any:
        """
        Get a connection from the pool.
        
        Args:
            timeout: Maximum time to wait for a connection
            
        Returns:
            Database connector instance
            
        Raises:
            Empty: If no connection available within timeout
        """
        try:
            conn = self.pool.get(timeout=timeout)
            self.logger.debug("Retrieved connection from pool")
            return conn
        except Empty:
            # Try to create new connection if overflow allows
            if self.current_size < self.pool_size + self.max_overflow:
                return self._create_connection()
            else:
                raise RuntimeError("No connections available and pool is at max capacity")
    
    def return_connection(self, conn: Any) -> None:
        """
        Return a connection to the pool.
        
        Args:
            conn: Connection to return
        """
        try:
            self.pool.put_nowait(conn)
            self.logger.debug("Returned connection to pool")
        except Exception as e:
            self.logger.error(f"Error returning connection to pool: {str(e)}")
            # Close the connection if we can't return it
            try:
                conn.disconnect()
            except:
                pass
    
    def close_all(self) -> None:
        """Close all connections in the pool."""
        self.logger.info("Closing all connections in pool")
        while not self.pool.empty():
            try:
                conn = self.pool.get_nowait()
                conn.disconnect()
            except Empty:
                break
        self.current_size = 0
    
    def __enter__(self):
        """Context manager entry."""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close_all()


class PooledConnection:
    """
    Wrapper for pooled connections that automatically returns
    connection to pool when done.
    """
    
    def __init__(self, pool: ConnectionPool, timeout: Optional[float] = None):
        """
        Initialize pooled connection wrapper.
        
        Args:
            pool: ConnectionPool instance
            timeout: Timeout for getting connection
        """
        self.pool = pool
        self.connection = pool.get_connection(timeout)
    
    def __enter__(self):
        """Context manager entry."""
        return self.connection
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit - returns connection to pool."""
        self.pool.return_connection(self.connection)
