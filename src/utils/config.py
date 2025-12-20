"""
Configuration management utilities
Handles loading and validating job configurations
"""
import json
import yaml
import boto3
from typing import Dict, Any, Optional
from pathlib import Path


class ConfigManager:
    """Manages configuration for Glue jobs"""
    
    def __init__(self, config_path: Optional[str] = None, s3_config_path: Optional[str] = None):
        """
        Initialize config manager
        
        Args:
            config_path: Local path to config file
            s3_config_path: S3 path to config file (e.g., s3://bucket/config.yaml)
        """
        self.config_path = config_path
        self.s3_config_path = s3_config_path
        self.s3 = boto3.client('s3')
        self.config = self._load_config()
    
    def _load_config(self) -> Dict[str, Any]:
        """Load configuration from file or S3"""
        if self.s3_config_path:
            return self._load_from_s3()
        elif self.config_path:
            return self._load_from_file()
        else:
            return {}
    
    def _load_from_file(self) -> Dict[str, Any]:
        """Load config from local file"""
        path = Path(self.config_path)
        
        if path.suffix in ['.yaml', '.yml']:
            with open(path, 'r') as f:
                return yaml.safe_load(f)
        elif path.suffix == '.json':
            with open(path, 'r') as f:
                return json.load(f)
        else:
            raise ValueError(f"Unsupported config format: {path.suffix}")
    
    def _load_from_s3(self) -> Dict[str, Any]:
        """Load config from S3"""
        # Parse S3 path
        parts = self.s3_config_path.replace('s3://', '').split('/', 1)
        bucket = parts[0]
        key = parts[1]
        
        # Download config
        response = self.s3.get_object(Bucket=bucket, Key=key)
        content = response['Body'].read().decode('utf-8')
        
        # Parse based on extension
        if key.endswith('.yaml') or key.endswith('.yml'):
            return yaml.safe_load(content)
        elif key.endswith('.json'):
            return json.loads(content)
        else:
            raise ValueError(f"Unsupported config format: {key}")
    
    def get(self, key: str, default: Any = None) -> Any:
        """
        Get configuration value
        
        Args:
            key: Configuration key (supports dot notation, e.g., 'database.host')
            default: Default value if key not found
        
        Returns:
            Configuration value
        """
        keys = key.split('.')
        value = self.config
        
        for k in keys:
            if isinstance(value, dict) and k in value:
                value = value[k]
            else:
                return default
        
        return value
    
    def get_connection_config(self, connection_name: str) -> Dict[str, Any]:
        """
        Get connection configuration
        
        Args:
            connection_name: Name of the connection (e.g., 'redshift', 'teradata')
        
        Returns:
            Connection configuration dict
        """
        return self.get(f'connections.{connection_name}', {})
    
    def get_table_config(self, table_name: str) -> Dict[str, Any]:
        """
        Get table-specific configuration
        
        Args:
            table_name: Name of the table
        
        Returns:
            Table configuration dict
        """
        return self.get(f'tables.{table_name}', {})
    
    def validate_required_keys(self, required_keys: list) -> bool:
        """
        Validate that required configuration keys exist
        
        Args:
            required_keys: List of required keys (dot notation supported)
        
        Returns:
            True if all keys exist
        
        Raises:
            ValueError: If any required key is missing
        """
        missing_keys = []
        
        for key in required_keys:
            if self.get(key) is None:
                missing_keys.append(key)
        
        if missing_keys:
            raise ValueError(f"Missing required configuration keys: {', '.join(missing_keys)}")
        
        return True
