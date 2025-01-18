# tests/conftest.py
import pytest
from pathlib import Path

@pytest.fixture
def sample_tweet_data():
    return {
        'text': 'Hello world! #test',
        'created_at': 'Wed Oct 10 20:19:24 +0000 2018',
        'favorite_count': 0,
        'retweet_count': 0
    }

@pytest.fixture
def temp_output_dir(tmp_path):
    """Provides a temporary directory for test outputs"""
    return tmp_path / "output"