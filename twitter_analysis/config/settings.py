# twitter_analysis/config/settings.py
from pathlib import Path
from typing import Dict, Any

class Settings:
    """Configuration settings for the Twitter archive preprocessor."""
    
    DEFAULT_CONFIG: Dict[str, Any] = {
        'output_formats': ['parquet', 'csv'],
        'logging_level': 'INFO',
        'batch_size': 1000,
        'max_workers': 4,
    }
    
    def __init__(self):
        self.config = self.DEFAULT_CONFIG.copy()
    
    def update(self, **kwargs):
        """Update configuration with new values."""
        self.config.update(kwargs)
    
    @property
    def logging_level(self) -> str:
        return self.config['logging_level']
    
    @property
    def batch_size(self) -> int:
        return self.config['batch_size']
    
    @property
    def max_workers(self) -> int:
        return self.config['max_workers']
