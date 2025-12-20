#!/usr/bin/env python3
"""
Script to test database connections
"""

import sys
import yaml
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.connectors import RedshiftConnector, TeradataConnector, BigQueryConnector
from src.utils import get_logger

logger = get_logger('ConnectionTest')

def test_connections(config_path='config/database_connections.yaml'):
    """
    Test all database connections.
    
    Args:
        config_path: Path to database connections config file
    """
    # Load configuration
    with open(config_path, 'r') as f:
        config = yaml.safe_load(f)
    
    connections = config.get('connections', {})
    results = {}
    
    # Test Redshift
    if 'redshift' in connections:
        logger.info("Testing Redshift connection...")
        try:
            conn = RedshiftConnector(connections['redshift'])
            success = conn.test_connection()
            results['redshift'] = 'SUCCESS' if success else 'FAILED'
        except Exception as e:
            logger.error(f"Redshift connection failed: {str(e)}")
            results['redshift'] = 'FAILED'
    
    # Test Teradata
    if 'teradata' in connections:
        logger.info("Testing Teradata connection...")
        try:
            conn = TeradataConnector(connections['teradata'])
            success = conn.test_connection()
            results['teradata'] = 'SUCCESS' if success else 'FAILED'
        except Exception as e:
            logger.error(f"Teradata connection failed: {str(e)}")
            results['teradata'] = 'FAILED'
    
    # Test BigQuery
    if 'bigquery' in connections:
        logger.info("Testing BigQuery connection...")
        try:
            conn = BigQueryConnector(connections['bigquery'])
            success = conn.test_connection()
            results['bigquery'] = 'SUCCESS' if success else 'FAILED'
        except Exception as e:
            logger.error(f"BigQuery connection failed: {str(e)}")
            results['bigquery'] = 'FAILED'
    
    # Print results
    print("\n" + "="*50)
    print("CONNECTION TEST RESULTS")
    print("="*50)
    for source, status in results.items():
        print(f"{source.upper()}: {status}")
    print("="*50)
    
    # Return exit code based on results
    all_success = all(status == 'SUCCESS' for status in results.values())
    return 0 if all_success else 1


if __name__ == '__main__':
    import argparse
    
    parser = argparse.ArgumentParser(description='Test database connections')
    parser.add_argument('--config', default='config/database_connections.yaml',
                       help='Path to database connections config')
    
    args = parser.parse_args()
    
    exit_code = test_connections(args.config)
    sys.exit(exit_code)
