"""
Configuration Manager Module

Handles loading and managing configuration from various sources.
"""

import os
import yaml
import json
from typing import Any, Dict, Optional
import boto3
from botocore.exceptions import ClientError
import logging


class ConfigManager:
    """
    Manages application configuration from multiple sources.
    
    Supports YAML files, environment variables, and AWS Secrets Manager.
    """
    
    def __init__(self, config_path: Optional[str] = None):
        """
        Initialize configuration manager.
        
        Args:
            config_path: Path to configuration file (YAML or JSON)
        """
        self.config_path = config_path
        self.config: Dict[str, Any] = {}
        self.logger = logging.getLogger(__name__)
        self.secrets_client = None
        
        if config_path:
            self.load_from_file(config_path)
    
    def load_from_file(self, file_path: str) -> None:
        """
        Load configuration from YAML or JSON file.
        
        Args:
            file_path: Path to configuration file
        """
        try:
            with open(file_path, 'r') as f:
                if file_path.endswith('.yaml') or file_path.endswith('.yml'):
                    self.config = yaml.safe_load(f)
                elif file_path.endswith('.json'):
                    self.config = json.load(f)
                else:
                    raise ValueError(f"Unsupported file format: {file_path}")
            
            self.logger.info(f"Loaded configuration from {file_path}")
        except Exception as e:
            self.logger.error(f"Failed to load configuration: {str(e)}")
            raise
    
    def get(self, key: str, default: Any = None) -> Any:
        """
        Get configuration value by key.
        
        Supports dot notation for nested keys (e.g., 'database.host').
        Also checks environment variables as override.
        
        Args:
            key: Configuration key
            default: Default value if key not found
            
        Returns:
            Configuration value
        """
        # Check environment variable first (highest priority)
        env_key = key.upper().replace('.', '_')
        env_value = os.environ.get(env_key)
        if env_value is not None:
            return env_value
        
        # Navigate nested config
        keys = key.split('.')
        value = self.config
        
        for k in keys:
            if isinstance(value, dict) and k in value:
                value = value[k]
            else:
                return default
        
        return value
    
    def set(self, key: str, value: Any) -> None:
        """
        Set configuration value.
        
        Args:
            key: Configuration key (supports dot notation)
            value: Value to set
        """
        keys = key.split('.')
        config = self.config
        
        for k in keys[:-1]:
            if k not in config:
                config[k] = {}
            config = config[k]
        
        config[keys[-1]] = value
    
    def get_secret(self, secret_name: str, region: str = 'us-east-1') -> Dict[str, Any]:
        """
        Retrieve secret from AWS Secrets Manager.
        
        Args:
            secret_name: Name of secret in Secrets Manager
            region: AWS region
            
        Returns:
            Secret value as dictionary
        """
        if not self.secrets_client:
            self.secrets_client = boto3.client('secretsmanager', region_name=region)
        
        try:
            response = self.secrets_client.get_secret_value(SecretId=secret_name)
            
            if 'SecretString' in response:
                return json.loads(response['SecretString'])
            else:
                # Binary secret
                return response['SecretBinary']
                
        except ClientError as e:
            self.logger.error(f"Failed to retrieve secret {secret_name}: {str(e)}")
            raise
    
    def validate(self, required_keys: list) -> bool:
        """
        Validate that required configuration keys are present.
        
        Args:
            required_keys: List of required configuration keys
            
        Returns:
            True if all required keys present, False otherwise
        """
        missing_keys = []
        
        for key in required_keys:
            if self.get(key) is None:
                missing_keys.append(key)
        
        if missing_keys:
            self.logger.error(f"Missing required configuration keys: {missing_keys}")
            return False
        
        return True
    
    def to_dict(self) -> Dict[str, Any]:
        """
        Get entire configuration as dictionary.
        
        Returns:
            Configuration dictionary
        """
        return self.config.copy()
    
    def save_to_file(self, file_path: str) -> None:
        """
        Save current configuration to file.
        
        Args:
            file_path: Path to save configuration
        """
        try:
            with open(file_path, 'w') as f:
                if file_path.endswith('.yaml') or file_path.endswith('.yml'):
                    yaml.dump(self.config, f, default_flow_style=False)
                elif file_path.endswith('.json'):
                    json.dump(self.config, f, indent=2)
                else:
                    raise ValueError(f"Unsupported file format: {file_path}")
            
            self.logger.info(f"Saved configuration to {file_path}")
        except Exception as e:
            self.logger.error(f"Failed to save configuration: {str(e)}")
            raise


def load_config(config_path: str) -> ConfigManager:
    """
    Helper function to load configuration.
    
    Args:
        config_path: Path to configuration file
        
    Returns:
        Configured ConfigManager instance
    """
    return ConfigManager(config_path)
